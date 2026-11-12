// One worker node: fetch -> acquire ownership -> validate -> execute -> persist -> ack/report -> release.
//
// Concurrency is bounded by `concurrency` slots: the fetch loop only asks Redis for as many messages as it has free
// slots (backpressure), so the worker never buffers work it cannot start. Node runs handlers on one event loop, so
// slots are concurrent async executions (ideal for I/O-bound tasks); CPU parallelism comes from running more worker
// processes. Heartbeats renew the leases of in-flight attempts; an attempt whose lease could not be renewed
// (presumed dead, cancelled, reassigned) is aborted locally and its result is fenced off in Postgres anyway.
import crypto from "node:crypto";
import type { Redis } from "ioredis";
import { config, log, type Config } from "./config.ts";
import { sleep, withRetry, type Db } from "./db.ts";
import { chaosAfter, chaosBefore, HANDLERS, TaskFailure, type Ctx } from "./handlers.ts";
import { m } from "./metrics.ts";
import type { Message, StreamQueue } from "./queue.ts";
import type { Claimed, TaskStore } from "./taskStore.ts";

const UUID = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;
type AbortKind = "timeout" | "lease-lost" | "shutdown" | "crash";

export class Worker {
  readonly id: string;
  readonly cfg: Config["worker"];
  private inFlight = new Map<string, { task: Claimed; ctrl: AbortController }>();
  private busy = 0;
  private waiters: (() => void)[] = [];
  private running = false;
  private draining = false;
  private crashed = false;
  private timers: NodeJS.Timeout[] = [];
  private fetchDone: Promise<void> = Promise.resolve();
  private lastCpu = process.cpuUsage();
  private lastCpuAt = Date.now();
  completed = 0;
  failed = 0;
  duplicates = 0;

  private db: Db; private queue: StreamQueue; private store: TaskStore; private conn: Redis;

  constructor(db: Db, queue: StreamQueue, store: TaskStore, conn: Redis, cfg: Partial<Config["worker"]> = {}) {
    this.db = db; this.queue = queue; this.store = store; this.conn = conn;
    this.cfg = { ...config.worker, ...cfg };
    this.id = this.cfg.id || `${config.hostname}-${process.pid}-${crypto.randomBytes(2).toString("hex")}`;
  }

  get inFlightCount() { return this.busy; }

  async start() {
    await this.queue.ensureGroup();
    await this.register();
    this.running = true;
    this.fetchDone = this.fetchLoop();
    this.every(this.cfg.heartbeatMs, () => this.heartbeat());
    this.every(Math.max(1000, this.cfg.visibilityTimeoutMs / 3), () => this.reclaim());
    log("info", "worker started", { worker: this.id, concurrency: this.cfg.concurrency, leaseMs: this.cfg.leaseMs });
  }

  /** Graceful shutdown: stop fetching, let in-flight tasks finish within the grace period, hand back the rest. */
  async stop() {
    if (!this.running) return;
    this.running = false;
    this.draining = true;
    log("info", "worker draining", { worker: this.id, inFlight: this.busy });
    await this.db.query("update worker_nodes set status = 'DRAINING' where id = $1", [this.id]).catch(() => {});
    const deadline = Date.now() + this.cfg.shutdownGraceMs;
    while (this.busy > 0 && Date.now() < deadline) await sleep(50);
    for (const f of this.inFlight.values()) f.ctrl.abort("shutdown");
    while (this.busy > 0 && Date.now() < deadline + 5000) await sleep(20); // aborted attempts report themselves as LOST
    this.timers.forEach(clearInterval);
    this.conn.disconnect();
    await this.fetchDone.catch(() => {});
    await this.db.query("update worker_nodes set status = 'STOPPED', in_flight = 0 where id = $1", [this.id]).catch(() => {});
    log("info", "worker stopped", { worker: this.id });
  }

  /** Failure injection: die like kill -9 (no lease release, no status update, no acks, no result commits). */
  crash() {
    this.crashed = true;
    this.running = false;
    this.timers.forEach(clearInterval);
    for (const f of this.inFlight.values()) f.ctrl.abort("crash");
    this.conn.disconnect();
  }

  // ---------------------------------------------------------------- fetch

  private async fetchLoop() {
    let backoff = 200;
    while (this.running) {
      if (this.draining) { await sleep(500); continue; }
      const free = this.cfg.concurrency - this.busy;
      if (free <= 0) { await new Promise<void>((r) => this.waiters.push(r)); continue; }
      try {
        const msgs = await this.queue.read(this.conn, this.id, Math.min(free, 64), 1000);
        backoff = 200;
        for (const msg of msgs) this.submit(msg);
      } catch (e) {
        if (!this.running) return;
        // Redis interruption: back off and keep trying; tasks stay safe in Postgres meanwhile.
        log("warn", "fetch failed, retrying", { worker: this.id, error: (e as Error).message, backoffMs: backoff });
        await sleep(backoff);
        backoff = Math.min(backoff * 2, 5000);
      }
    }
  }

  private async reclaim() {
    if (!this.running || this.draining) return;
    const n = Math.min(this.cfg.concurrency - this.busy, 32);
    if (n <= 0) return;
    const msgs = await this.queue.reclaim(this.id, this.cfg.visibilityTimeoutMs, n, this.cfg.poisonDeliveries);
    if (msgs.length) log("info", "reclaimed un-acked messages", { worker: this.id, count: msgs.length });
    for (const msg of msgs) this.submit(msg);
  }

  private submit(msg: Message) {
    this.busy++;
    m.inFlight.set({ worker: this.id }, this.busy);
    this.process(msg)
      .catch((e) => log("error", "unexpected error processing message", { worker: this.id, messageId: msg.id, error: String(e?.stack ?? e) }))
      .finally(() => {
        this.busy--;
        m.inFlight.set({ worker: this.id }, this.busy);
        this.waiters.shift()?.();
      });
  }

  // ---------------------------------------------------------------- execute

  async process(msg: Message) {
    const { t: taskId, a: attemptId, w: execId } = msg.fields;
    if (![taskId, attemptId, execId].every((v) => UUID.test(v ?? ""))) {
      log("warn", "poison message (unparseable), moving to DLQ stream", { messageId: msg.id });
      await this.queue.deadLetter(msg, "MALFORMED_MESSAGE");
      m.poison.inc();
      return;
    }
    // Step 2: acquire execution ownership (CAS in Postgres). Retries cover a brief DB outage; if it stays down the
    // message stays un-acked and is reclaimed later.
    let task: Claimed | null;
    try {
      task = await withRetry(() => this.store.claim(taskId, attemptId, execId, this.id, this.cfg.leaseMs), this.cfg.leaseMs, () => this.running);
    } catch (e) {
      log("warn", "claim failed, leaving message for redelivery", { worker: this.id, error: (e as Error).message });
      return;
    }
    if (this.crashed) return;
    if (!task) {
      // Duplicate / stale delivery: someone else owns (or finished) this attempt. Drop it.
      this.duplicates++;
      m.duplicates.inc();
      await this.queue.ack(msg.id).catch(() => {});
      return;
    }
    await this.queue.ack(msg.id).catch(() => {}); // Postgres now owns recovery (lease); the message has done its job
    m.queueWait.observe(task.startedAt.getTime() - task.queuedAt.getTime());
    await this.execute(task);
  }

  private async execute(task: Claimed) {
    // Step 3: validate - the handler must exist in this build.
    const handler = HANDLERS.get(task.taskType);
    if (!handler) {
      await this.report(task, `no handler registered for task type ${task.taskType}`, "UNKNOWN_TASK_TYPE", false, "FAILED");
      return;
    }
    const ctrl = new AbortController();
    this.inFlight.set(task.attemptId, { task, ctrl });
    const timer = setTimeout(() => ctrl.abort("timeout"), task.timeoutMs);
    const ctx: Ctx = { ...task, workerId: this.id, signal: ctrl.signal, db: this.db };
    const t0 = performance.now();
    let outcome = "succeeded";
    // A handler that ignores its AbortSignal keeps running in the background after a timeout; its result is fenced
    // and its side effects are guarded by the idempotency key, so the race is harmless.
    const aborted = new Promise<never>((_, rej) => ctrl.signal.addEventListener("abort", () => rej(new Error("aborted")), { once: true }));
    try {
      const output = await Promise.race([(async () => {
        await chaosBefore(ctx, this.cfg.chaosFailureRate);
        const out = await handler.run(ctx);
        chaosAfter(ctx);
        return out;
      })(), aborted]);
      if (this.crashed) return;
      const c = await withRetry(() => this.store.complete(task.executionId, task.attemptId, this.id, output), this.cfg.leaseMs, () => !this.crashed);
      outcome = c.committed ? "succeeded" : "fenced";
      if (c.committed) this.completed++;
      if (c.workflowFinished && c.workflowDurationMs != null) m.workflowDuration.observe(c.workflowDurationMs);
    } catch (e) {
      if (this.crashed) return;
      const kind = ctrl.signal.aborted ? (ctrl.signal.reason as AbortKind) : null;
      if (kind === "timeout") outcome = await this.report(task, `timed out after ${task.timeoutMs} ms`, "TIMEOUT", true, "TIMED_OUT");
      else if (kind === "lease-lost") outcome = "cancelled"; // cancelled or reassigned; the state machine already moved on
      else if (kind === "shutdown") outcome = await this.report(task, "worker shutting down", "WORKER_SHUTDOWN", true, "LOST");
      else if (e instanceof TaskFailure) outcome = await this.report(task, e.message, e.errorType, e.retryable, "FAILED");
      else outcome = await this.report(task, `${(e as Error)?.name ?? "Error"}: ${(e as Error)?.message ?? e}`, (e as Error)?.name ?? "Error", true, "FAILED");
    } finally {
      clearTimeout(timer);
      this.inFlight.delete(task.attemptId);
    }
    m.taskDuration.observe({ type: task.taskType, outcome }, performance.now() - t0);
    m.tasks.inc({ type: task.taskType, outcome });
  }

  private async report(task: Claimed, error: string, errorType: string, retryable: boolean, attemptStatus: string): Promise<string> {
    this.failed++;
    try {
      const f = await withRetry(() => this.store.fail(task.attemptId, this.id, error, errorType, retryable, attemptStatus, false), this.cfg.leaseMs, () => !this.crashed);
      if (!f.committed) return "fenced";
      if (f.willRetry) m.retries.inc({ type: task.taskType });
      return f.willRetry ? "retrying" : "failed";
    } catch (e) {
      // DB stayed down: the lease lapses and the scheduler's reaper records the attempt as LOST instead.
      log("warn", "could not record failure", { worker: this.id, error: (e as Error).message });
      return "unreported";
    }
  }

  // ---------------------------------------------------------------- registration, heartbeat, leases

  private async register() {
    await this.db.query(`insert into worker_nodes(id, hostname, pid, concurrency, status, task_types, started_at, last_heartbeat_at)
      values ($1, $2, $3, $4, 'ACTIVE', $5, now(), now())
      on conflict (id) do update set status = 'ACTIVE', concurrency = excluded.concurrency, started_at = now(), last_heartbeat_at = now(),
        pid = excluded.pid, task_types = excluded.task_types, in_flight = 0`,
      [this.id, config.hostname, process.pid, this.cfg.concurrency, [...HANDLERS.keys()].sort()]);
    // Restarted with a stable id: anything we owned before the restart is gone - release it right away.
    const released = await this.store.expireLeasesOf([this.id]);
    if (released) log("info", "released leases left over from a previous run", { worker: this.id, released });
  }

  async heartbeat() {
    if (!this.running && !this.draining) return;
    const r = await this.db.query(`update worker_nodes set last_heartbeat_at = now(), in_flight = $1, completed_count = $2, failed_count = $3,
        status = case when status = 'DEAD' then 'ACTIVE' else status end where id = $4 returning status`, [this.busy, this.completed, this.failed, this.id]);
    if (r.rows.length === 0) await this.register();
    else if (r.rows[0].status === "DRAINING" && !this.draining) { this.draining = true; log("info", "set to DRAINING by an operator", { worker: this.id }); }
    else if (r.rows[0].status === "ACTIVE" && this.draining && this.running) this.draining = false;
    const cpu = process.cpuUsage(this.lastCpu), now = Date.now();
    const load = (cpu.user + cpu.system) / 1000 / Math.max(1, now - this.lastCpuAt);
    this.lastCpu = process.cpuUsage(); this.lastCpuAt = now;
    await this.db.query("insert into worker_heartbeats(worker_id, in_flight, capacity, cpu_load, heap_used_mb) values ($1, $2, $3, $4, $5)",
      [this.id, this.busy, this.cfg.concurrency, Math.round(load * 1000) / 1000, Math.round(process.memoryUsage().heapUsed / 1048576)]);
    m.utilization.set({ worker: this.id }, this.busy / this.cfg.concurrency);
    const attempts = [...this.inFlight.keys()];
    const renewed = await this.store.renewLeases(this.id, attempts, this.cfg.leaseMs);
    for (const a of attempts) {
      const f = this.inFlight.get(a);
      if (f && !renewed.has(a)) { log("info", "lost ownership of attempt, aborting", { worker: this.id, attemptId: a }); f.ctrl.abort("lease-lost"); }
    }
  }

  private every(ms: number, fn: () => Promise<void>) {
    let busy = false;
    this.timers.push(setInterval(async () => {
      if (busy) return; // never overlap a slow tick with the next one
      busy = true;
      // DB down: leases lapse and the reaper reassigns our work; our late completions are then fenced.
      try { await fn(); } catch (e) { log("warn", "worker housekeeping failed", { worker: this.id, error: (e as Error).message }); } finally { busy = false; }
    }, ms));
  }
}
