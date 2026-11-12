// Recurring (CRON / INTERVAL) and one-off (ONCE) workflow triggers, plus cluster-wide scheduler settings.
//
// Firing is one-execution-per-slot: due rows are claimed FOR UPDATE SKIP LOCKED and the execution is created in the
// same transaction with idempotency key "schedule:{job}:{slot}", so overlapping schedulers or a crash mid-fire cannot
// double-run a slot. Misfire policy after downtime: fire once for the missed slot, then continue from "now".
import { CronExpressionParser } from "cron-parser";
import { tx, type Db } from "./db.ts";
import { ApiError } from "./errors.ts";
import { STRATEGIES, type Strategy } from "./dag.ts";
import type { ExecutionService } from "./executions.ts";

export type ScheduleType = "ONCE" | "CRON" | "INTERVAL";
export interface Spec { type: ScheduleType; cron?: string | null; intervalSeconds?: number | null; runAt?: Date | null; timezone?: string | null }

export function nextFire(s: Spec, after: Date): Date | null {
  if (s.type === "ONCE") return s.runAt ?? null;
  if (s.type === "INTERVAL") return new Date(after.getTime() + (s.intervalSeconds ?? 60) * 1000);
  return CronExpressionParser.parse(s.cron!, { currentDate: after, tz: s.timezone || "UTC" }).next().toDate();
}

export function validateSpec(s: Spec) {
  const errors: string[] = [];
  if (!["ONCE", "CRON", "INTERVAL"].includes(s.type)) errors.push("type must be ONCE, CRON or INTERVAL");
  if (s.type === "ONCE" && !s.runAt) errors.push("runAt is required for ONCE");
  if (s.type === "INTERVAL" && !(s.intervalSeconds && s.intervalSeconds >= 1)) errors.push("intervalSeconds >= 1 is required for INTERVAL");
  if (s.type === "CRON") { try { CronExpressionParser.parse(s.cron ?? ""); } catch { errors.push("invalid cron expression"); } }
  try { new Intl.DateTimeFormat("en", { timeZone: s.timezone || "UTC" }); } catch { errors.push("unknown timezone"); }
  if (errors.length) throw ApiError.badRequest("Invalid schedule", errors);
}

const SELECT = `select s.id, s.workflow_id, w.name as workflow_name, s.name, s.schedule_type, s.cron, s.interval_ms, s.timezone, s.next_fire_at,
  s.last_fired_at, s.fire_count, s.enabled, s.priority, s.input, s.created_at, s.lock_version from scheduled_jobs s join workflows w on w.id = s.workflow_id`;
const view = (r: Record<string, any>) => ({ id: r.id, workflowId: r.workflow_id, workflowName: r.workflow_name, name: r.name, type: r.schedule_type as ScheduleType,
  cron: r.cron, intervalMs: r.interval_ms, timezone: r.timezone, nextFireAt: r.next_fire_at, lastFiredAt: r.last_fired_at, fireCount: r.fire_count,
  enabled: r.enabled, priority: r.priority, input: r.input, createdAt: r.created_at, lockVersion: r.lock_version });
const specOf = (j: ReturnType<typeof view>): Spec => ({ type: j.type, cron: j.cron, intervalSeconds: j.intervalMs ? j.intervalMs / 1000 : null, runAt: j.nextFireAt, timezone: j.timezone });

export class ScheduleService {
  readonly db: Db; readonly executions: ExecutionService;
  constructor(db: Db, executions: ExecutionService) { this.db = db; this.executions = executions; }

  async create(orgId: string, userId: string | null, b: { workflowId: string; name: string; input?: Record<string, unknown>; priority?: number | null; enabled?: boolean } & Spec) {
    validateSpec(b);
    const live = await this.db.query("select 1 from workflows where id = $1 and org_id = $2 and not archived", [b.workflowId, orgId]);
    if (live.rows.length === 0) throw ApiError.notFound("Workflow");
    const id = (await this.db.query(`insert into scheduled_jobs(org_id, workflow_id, name, schedule_type, cron, interval_ms, timezone, next_fire_at, input, priority, enabled, created_by)
      values ($1, $2, $3, $4, $5, $6, $7, $8, $9::jsonb, $10, $11, $12) returning id`,
      [orgId, b.workflowId, b.name, b.type, b.cron ?? null, b.intervalSeconds ? b.intervalSeconds * 1000 : null, b.timezone || "UTC",
        nextFire(b, new Date()), JSON.stringify(b.input ?? {}), b.priority ?? null, b.enabled ?? true, userId])).rows[0].id;
    return this.get(orgId, id);
  }

  async update(orgId: string, id: string, b: { expectedLockVersion?: number | null; name?: string; type?: ScheduleType; cron?: string; intervalSeconds?: number;
    runAt?: Date | null; timezone?: string; input?: Record<string, unknown>; priority?: number | null; enabled?: boolean }) {
    const cur = await this.get(orgId, id);
    if (b.expectedLockVersion != null && Number(b.expectedLockVersion) !== cur.lockVersion) throw ApiError.conflict("Schedule was modified concurrently");
    let next: Date | null = cur.nextFireAt;
    let spec: Spec | null = null;
    if (b.type) {
      spec = { type: b.type, cron: b.cron, intervalSeconds: b.intervalSeconds, runAt: b.runAt, timezone: b.timezone };
      validateSpec(spec);
      next = nextFire(spec, new Date());
    } else if (b.enabled === true && !cur.enabled && cur.type !== "ONCE") {
      next = nextFire(specOf(cur), new Date());
    }
    const r = await this.db.query(`update scheduled_jobs set name = coalesce($1, name), schedule_type = coalesce($2, schedule_type), cron = coalesce($3, cron),
        interval_ms = coalesce($4, interval_ms), timezone = coalesce($5, timezone), next_fire_at = $6, input = coalesce($7::jsonb, input),
        priority = coalesce($8, priority), enabled = coalesce($9, enabled), lock_version = lock_version + 1
      where id = $10 and org_id = $11 and lock_version = $12`,
      [b.name ?? null, spec?.type ?? null, spec?.cron ?? null, spec?.intervalSeconds ? spec.intervalSeconds * 1000 : null, spec?.timezone ?? null, next,
        b.input ? JSON.stringify(b.input) : null, b.priority ?? null, b.enabled ?? null, id, orgId, cur.lockVersion]);
    if (!r.rowCount) throw ApiError.conflict("Schedule was modified concurrently");
    return this.get(orgId, id);
  }

  async delete(orgId: string, id: string) {
    const r = await this.db.query("delete from scheduled_jobs where id = $1 and org_id = $2", [id, orgId]);
    if (!r.rowCount) throw ApiError.notFound("Schedule");
  }

  /** Manual "run now", outside the schedule (does not move next_fire_at). */
  async trigger(orgId: string, id: string, userId: string | null) {
    const j = await this.get(orgId, id);
    return this.executions.start({ orgId, workflowId: j.workflowId, input: j.input, priority: j.priority, trigger: "SCHEDULE", scheduledJobId: j.id, createdBy: userId });
  }

  async fireDue(): Promise<number> {
    return tx(this.db, async (t) => {
      const due = (await t.c.query(`select s.id, s.org_id, s.workflow_id, s.schedule_type, s.cron, s.interval_ms, s.timezone, s.next_fire_at, s.input, s.priority,
          exists (select 1 from workflows w where w.id = s.workflow_id and not w.archived) as live
        from scheduled_jobs s where s.enabled and s.next_fire_at <= now() order by s.next_fire_at limit 20 for update skip locked`)).rows;
      for (const j of due) {
        let keep = j.schedule_type !== "ONCE";
        if (!j.live) keep = false;
        else await this.executions.start({ orgId: j.org_id, workflowId: j.workflow_id, input: j.input, priority: j.priority,
          idempotencyKey: `schedule:${j.id}:${j.next_fire_at.getTime()}`, trigger: "SCHEDULE", scheduledJobId: j.id }, t);
        const next = keep ? nextFire({ type: j.schedule_type, cron: j.cron, intervalSeconds: j.interval_ms ? j.interval_ms / 1000 : null, timezone: j.timezone }, new Date()) : null;
        await t.c.query(`update scheduled_jobs set last_fired_at = now(), fire_count = fire_count + 1, next_fire_at = $1, enabled = $2,
          lock_version = lock_version + 1 where id = $3`, [next, keep && next !== null, j.id]);
      }
      return due.length;
    });
  }

  async list(orgId: string) {
    return (await this.db.query(`${SELECT} where s.org_id = $1 order by s.next_fire_at nulls last, s.name`, [orgId])).rows.map(view);
  }

  async get(orgId: string, id: string) {
    const r = await this.db.query(`${SELECT} where s.org_id = $1 and s.id = $2`, [orgId, id]);
    if (r.rows.length === 0) throw ApiError.notFound("Schedule");
    return view(r.rows[0]);
  }

  /** Planned fire times (expanded from cron/interval), delayed executions, and what actually ran, in [from, to). */
  async calendar(orgId: string, from: Date, to: Date) {
    if (to.getTime() - from.getTime() > 62 * 86_400_000) throw ApiError.badRequest("Range too large (max 62 days)");
    const out: Record<string, unknown>[] = [];
    const now = new Date();
    for (const j of await this.list(orgId)) {
      if (!j.enabled || !j.nextFireAt) continue;
      let t: Date | null = j.nextFireAt;
      for (let i = 0; t && t < to && i < 500; i++) {
        if (t >= from && t >= now) out.push({ scheduleId: j.id, workflowId: j.workflowId, title: j.name, workflowName: j.workflowName, at: t, kind: "PLANNED" });
        if (j.type === "ONCE") break;
        t = nextFire(specOf(j), t);
      }
    }
    const ran = (await this.db.query(`select e.scheduled_job_id, e.workflow_id, coalesce(s.name, w.name) as title, w.name, coalesce(e.run_at, e.created_at) as at, e.id, e.status
      from workflow_executions e join workflows w on w.id = e.workflow_id left join scheduled_jobs s on s.id = e.scheduled_job_id
      where e.org_id = $1 and (e.trigger = 'SCHEDULE' or e.run_at is not null) and coalesce(e.run_at, e.created_at) >= $2 and coalesce(e.run_at, e.created_at) < $3
      order by 5 limit 2000`, [orgId, from, to])).rows;
    for (const r of ran)
      out.push({ scheduleId: r.scheduled_job_id, workflowId: r.workflow_id, title: r.title, workflowName: r.name, at: r.at,
        kind: r.status === "PENDING" && r.at > now ? "DELAYED" : "FIRED", executionId: r.id, status: r.status });
    return out.sort((a, b) => (a.at as Date).getTime() - (b.at as Date).getTime());
  }
}

/** Cluster-wide scheduler settings stored in Postgres; every scheduler replica picks up changes within ~2 s. */
export class Settings {
  private loadedAt = 0;
  strategy: Strategy = "PRIORITY";
  agingSeconds = 30;
  readonly db: Db;
  constructor(db: Db) { this.db = db; }

  async current(): Promise<{ strategy: Strategy; agingSeconds: number }> {
    if (Date.now() - this.loadedAt > 2000) await this.reload();
    return { strategy: this.strategy, agingSeconds: this.agingSeconds };
  }

  async update(strategy?: string, agingSeconds?: number) {
    if (strategy && !(strategy in STRATEGIES)) throw ApiError.badRequest("Unknown strategy", [`strategy must be one of ${Object.keys(STRATEGIES).join(", ")}`]);
    const put = (k: string, v: unknown) => this.db.query(`insert into system_settings(key, value, updated_at) values ($1, $2::jsonb, now())
      on conflict (key) do update set value = excluded.value, updated_at = now()`, [k, JSON.stringify(v)]);
    if (strategy) await put("scheduler.strategy", strategy);
    if (agingSeconds != null) await put("scheduler.aging_seconds", Math.max(1, agingSeconds));
    await this.reload();
    return this.current();
  }

  private async reload() {
    for (const r of (await this.db.query("select key, value from system_settings")).rows) {
      if (r.key === "scheduler.strategy" && r.value in STRATEGIES) this.strategy = r.value;
      if (r.key === "scheduler.aging_seconds") this.agingSeconds = Math.max(1, Number(r.value) || 30);
    }
    this.loadedAt = Date.now();
  }
}
