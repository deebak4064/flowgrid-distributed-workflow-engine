// The scheduler process. Stateless by design: all scheduling state lives in Postgres, so a crashed scheduler loses
// nothing and replicas can run active-active (every claim uses FOR UPDATE SKIP LOCKED / CAS updates): no leader
// election and no fail-over gap.
//
// Loops:  dispatch   (continuous)  runnable tasks -> QUEUED -> XADD, bounded by worker capacity (backpressure)
//         recovery   (1 s)         dead-worker detection, expired-lease reaping, cron/interval/once job firing
//         redispatch (5 s)         QUEUED tasks whose stream message was never published or was lost
//         aging      (5 s)         priority aging against starvation
//         retention  (60 s)        bounded history tables
import { config, log, type Config } from "./config.ts";
import { sleep, type Db } from "./db.ts";
import { m } from "./metrics.ts";
import { EVENTS_CHANNEL, type StreamQueue } from "./queue.ts";
import type { Redis } from "ioredis";
import type { ScheduleService, Settings } from "./schedules.ts";
import type { TaskStore } from "./taskStore.ts";

export class Scheduler {
  readonly cfg: Config["scheduler"];
  private running = false;
  private timers: NodeJS.Timeout[] = [];
  private loop: Promise<void> = Promise.resolve();
  private wake: (() => void) | null = null;
  private pendingWake = false;
  private sub: Redis | null = null;

  private db: Db; private store: TaskStore; private queue: StreamQueue; private schedules: ScheduleService; private settings: Settings;

  constructor(db: Db, store: TaskStore, queue: StreamQueue, schedules: ScheduleService, settings: Settings, cfg: Partial<Config["scheduler"]> = {}) {
    this.db = db; this.store = store; this.queue = queue; this.schedules = schedules; this.settings = settings;
    this.cfg = { ...config.scheduler, ...cfg };
  }

  async start() {
    await this.queue.ensureGroup().catch((e) => log("warn", "could not ensure consumer group yet", { error: e.message }));
    this.running = true;
    // Event-driven dispatch: tasks becoming runnable (or slots freeing up) wake the dispatcher immediately instead of
    // waiting for the next poll. Events are published after commit, so the dispatcher always sees the new rows. The
    // poll stays as the safety net (delayed tasks, retry backoff, lost pub/sub messages).
    this.sub = this.queue.redis.duplicate();
    this.sub.on("message", (_c, msg) => {
      if (!/"type":"(TASK_READY|WORKFLOW_CREATED|TASK_STARTED|WORKFLOW_RESUMED)"/.test(msg)) return;
      if (this.wake) this.wake(); else this.pendingWake = true; // arrived mid-round: run another round right after
    });
    await this.sub.subscribe(EVENTS_CHANNEL).catch((e) => log("warn", "dispatch wake-up subscription failed; polling only", { error: e.message }));
    this.loop = this.dispatchLoop();
    this.every(this.cfg.recoveryEveryMs, "recovery", () => this.recover());
    this.every(this.cfg.redispatchEveryMs, "redispatch", () => this.redispatch());
    this.every(5000, "aging", async () => { await this.store.ageRunnable((await this.settings.current()).agingSeconds); });
    this.every(60_000, "retention", () => this.retention());
    log("info", "scheduler started", { strategy: (await this.settings.current()).strategy });
  }

  async stop() {
    this.running = false;
    this.timers.forEach(clearInterval);
    this.wake?.();
    this.sub?.disconnect();
    await this.loop;
  }

  private async dispatchLoop() {
    let backoff = this.cfg.tickMs;
    while (this.running) {
      try {
        this.pendingWake = false;
        const n = await this.dispatchOnce();
        backoff = this.cfg.tickMs;
        if (n === 0) await this.idle();
      } catch (e) {
        if (!this.running) return;
        log("warn", "dispatch failed", { error: (e as Error).message, backoffMs: backoff });
        await sleep(backoff);
        backoff = Math.min(backoff * 2, 5000); // DB/Redis outage: back off, nothing is lost
      }
    }
  }

  /** Sleep until the next poll tick or until an event wakes us, whichever comes first (at least 5 ms between rounds). */
  private async idle() {
    await sleep(5);
    if (this.pendingWake) { this.pendingWake = false; return; }
    await new Promise<void>((resolve) => {
      const t = setTimeout(() => { this.wake = null; resolve(); }, this.cfg.tickMs);
      this.wake = () => { clearTimeout(t); this.wake = null; resolve(); };
    });
  }

  /** One dispatch round. Public for tests and benchmarks. */
  async dispatchOnce(): Promise<number> {
    const { rows } = await this.db.query(`select
        (select coalesce(sum(concurrency), 0) from worker_nodes where status = 'ACTIVE' and last_heartbeat_at > now() - $1 * interval '1 millisecond') as cap,
        (select count(*) from task_executions where status = 'QUEUED') as queued`, [this.cfg.workerTimeoutMs]);
    const cap = Number(rows[0].cap), inQueue = Number(rows[0].queued);
    m.capacity.set(cap);
    m.queueDepth.set(inQueue);
    const budget = Math.min(this.cfg.maxBatch, Math.floor(cap * this.cfg.queueFactor) - inQueue);
    if (budget <= 0) return 0;
    const batch = await this.store.claimForDispatch((await this.settings.current()).strategy, budget);
    if (batch.length === 0) return 0;
    const ids = await this.queue.publish(batch.map((d) => ({ taskId: d.taskId, attemptId: d.attemptId, orgId: d.orgId, executionId: d.executionId, taskType: d.taskType })));
    await this.store.recordMessageIds(batch, ids);
    const failed = ids.filter((x) => !x).length;
    if (failed) log("warn", "dispatches not published to Redis; redispatch sweep will retry", { count: failed });
    for (const d of batch) m.schedulingDelay.observe(Math.max(0, d.delayMs));
    m.dispatched.inc(batch.length);
    return batch.length;
  }

  async recover() {
    const dead = (await this.db.query(`update worker_nodes set status = 'DEAD'
      where status in ('ACTIVE', 'DRAINING') and last_heartbeat_at < now() - $1 * interval '1 millisecond' returning id`, [this.cfg.workerTimeoutMs])).rows.map((r) => r.id);
    if (dead.length) {
      const released = await this.store.expireLeasesOf(dead);
      let republished = 0;
      for (const w of dead) {
        try {
          const moved = await this.queue.takeOverConsumer(w);
          for (const [attemptId, msgId] of moved) await this.store.touchQueued(attemptId, msgId);
          republished += moved.length;
        } catch (e) { log("warn", "could not take over dead consumer's pending messages", { worker: w, error: (e as Error).message }); }
      }
      log("warn", "workers declared DEAD (no heartbeat), releasing their leases", { workers: dead, released, republished, timeoutMs: this.cfg.workerTimeoutMs });
      m.workersDied.inc(dead.length);
    }
    for (const attempt of await this.store.expiredLeases(200)) {
      const f = await this.store.fail(attempt, null, "lease expired: worker stopped heartbeating", "LEASE_EXPIRED", true, "LOST", true);
      if (f.committed) m.recoveredLeases.inc();
    }
    await this.schedules.fireDue();
    const r = await this.db.query("select count(*) as n from task_executions where status in ('READY', 'RETRYING') and (not_before is null or not_before <= now())");
    m.runnable.set(Number(r.rows[0].n));
  }

  async redispatch() {
    const stale = await this.store.staleQueued(this.cfg.redispatchAfterMs, 200);
    if (stale.length === 0) return;
    const lastDelivered = String((await this.queue.groupInfo())["last-delivered-id"] ?? "0-0");
    const lost = [];
    for (const s of stale) if (!(await this.queue.isInFlight(s.stream_message_id, lastDelivered))) lost.push(s);
    if (lost.length === 0) return;
    const ids = await this.queue.publish(lost.map((s) => ({ taskId: s.id, attemptId: s.attempt_id, orgId: s.org_id, executionId: s.workflow_execution_id, taskType: s.task_type })));
    for (let i = 0; i < lost.length; i++) if (ids[i]) await this.store.touchQueued(lost[i].attempt_id, ids[i]);
    log("info", "re-published queued tasks whose stream message was missing", { count: lost.length });
    m.redispatched.inc(lost.length);
  }

  /** Data retention: bounded deletes per tick so cleanup never holds long locks. */
  async retention() {
    const r = config.retention;
    const n = async (sql: string, args: unknown[] = []) => (await this.db.query(sql, args)).rowCount ?? 0;
    const hb = await n("delete from worker_heartbeats where id in (select id from worker_heartbeats where at < now() - $1 * interval '1 hour' limit 10000)", [r.heartbeatsHours]);
    const ev = await n("delete from execution_events where id in (select id from execution_events where created_at < now() - $1 * interval '1 day' limit 10000)", [r.eventsDays]);
    const ex = await n(`delete from workflow_executions where id in (select id from workflow_executions
      where status in ('SUCCEEDED', 'FAILED', 'CANCELLED') and finished_at < now() - $1 * interval '1 day' limit 1000)`, [r.executionsDays]);
    const idem = await n("delete from idempotency_records where ctid in (select ctid from idempotency_records where expires_at < now() limit 10000)");
    await n("delete from worker_nodes where status in ('DEAD', 'STOPPED') and last_heartbeat_at < now() - interval '1 day'");
    if (hb + ev + ex + idem > 0) log("info", "retention", { heartbeats: hb, events: ev, executions: ex, idempotency: idem });
  }

  private every(ms: number, name: string, fn: () => Promise<void>) {
    let busy = false;
    this.timers.push(setInterval(async () => {
      if (!this.running || busy) return;
      busy = true;
      try { await fn(); } catch (e) { log("warn", `scheduler ${name} loop failed`, { error: (e as Error).message }); } finally { busy = false; }
    }, ms));
  }
}
