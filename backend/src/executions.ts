// Execution lifecycle off the worker hot path: start, cancel, resume (workflow retry), reschedule, queries, dead letters.
import { tx, type Db, type Tx } from "./db.ts";
import { evaluate, type Condition } from "./dag.ts";
import { ApiError, pageOf } from "./errors.ts";
import { wfEvent, type Event } from "./events.ts";
import type { TaskStore } from "./taskStore.ts";

const TERMINAL = new Set(["SUCCEEDED", "FAILED", "CANCELLED"]);

export interface StartRequest {
  orgId: string; workflowId: string; version?: number | null; input?: Record<string, unknown> | null; priority?: number | null;
  deadlineSeconds?: number | null; runAt?: Date | null; idempotencyKey?: string | null; trigger?: string;
  scheduledJobId?: string | null; createdBy?: string | null;
}
export interface Started { executionId: string; created: boolean; status: string; totalTasks: number; runAt: Date | null }

const SUMMARY = `select e.id, e.workflow_id, w.name as workflow_name, e.version, e.status, e.trigger, e.priority, e.total_tasks, e.remaining_tasks,
    (select count(*) from task_executions t where t.workflow_execution_id = e.id and t.status in ('SUCCEEDED', 'SKIPPED')) as done,
    e.created_at, e.started_at, e.finished_at, e.run_at,
    (extract(epoch from (coalesce(e.finished_at, now()) - e.created_at)) * 1000)::bigint as duration_ms, e.error
  from workflow_executions e join workflows w on w.id = e.workflow_id`;

const summary = (r: Record<string, any>) => ({
  id: r.id, workflowId: r.workflow_id, workflowName: r.workflow_name, version: r.version, status: r.status, trigger: r.trigger,
  priority: r.priority, totalTasks: r.total_tasks, remainingTasks: r.remaining_tasks, completedTasks: r.done, createdAt: r.created_at,
  startedAt: r.started_at, finishedAt: r.finished_at, runAt: r.run_at, durationMs: r.duration_ms, error: r.error,
});

export class ExecutionService {
  readonly db: Db; readonly store: TaskStore;
  constructor(db: Db, store: TaskStore) { this.db = db; this.store = store; }

  /**
   * Instantiates a pinned workflow version as task rows. Roots start READY (or delayed via not_before); the rest wait
   * in PENDING with a dependency counter. Same idempotency key -> same execution (unique (org_id, idempotency_key)).
   */
  async start(r: StartRequest, t?: Tx): Promise<Started> {
    if (!t) return tx(this.db, (t2) => this.start(r, t2));
    const c = t.c;
    if (r.idempotencyKey) {
      const ex = await c.query("select id, status, total_tasks, run_at from workflow_executions where org_id = $1 and idempotency_key = $2", [r.orgId, r.idempotencyKey]);
      if (ex.rows.length) return { executionId: ex.rows[0].id, created: false, status: ex.rows[0].status, totalTasks: ex.rows[0].total_tasks, runAt: ex.rows[0].run_at };
    }
    const wf = await c.query(`select v.id, v.version from workflows w join workflow_versions v on v.workflow_id = w.id and v.version = coalesce($1, w.current_version)
      where w.id = $2 and w.org_id = $3 and not w.archived`, [r.version ?? null, r.workflowId, r.orgId]);
    if (wf.rows.length === 0) throw ApiError.notFound(r.version != null ? "Workflow version" : "Workflow");
    const { id: versionId, version } = wf.rows[0];
    const total = Number((await c.query("select count(*) as n from workflow_nodes where workflow_version_id = $1", [versionId])).rows[0].n);
    const deadline = r.deadlineSeconds ? new Date(Date.now() + r.deadlineSeconds * 1000) : null;
    const ins = await c.query(`insert into workflow_executions(org_id, workflow_id, workflow_version_id, version, status, trigger, input,
        idempotency_key, priority, deadline_at, run_at, total_tasks, remaining_tasks, scheduled_job_id, created_by)
      values ($1, $2, $3, $4, 'PENDING', $5, $6::jsonb, $7, $8, $9, $10, $11, $11, $12, $13)
      on conflict (org_id, idempotency_key) do nothing returning id`,
      [r.orgId, r.workflowId, versionId, version, r.trigger ?? "API", JSON.stringify(r.input ?? {}), r.idempotencyKey ?? null,
        r.priority ?? null, deadline, r.runAt ?? null, total, r.scheduledJobId ?? null, r.createdBy ?? null]);
    if (ins.rows.length === 0) return this.start(r, t); // lost a race on the idempotency key: return the winner
    const execId: string = ins.rows[0].id;
    const roots = await c.query(`insert into task_executions(workflow_execution_id, org_id, node_id, node_key, task_type, status, priority,
        effective_priority, deadline_at, ready_at, not_before, total_deps, pending_deps, trigger_rule, max_attempts, timeout_ms,
        backoff_base_ms, backoff_max_ms, idempotency_key)
      select $1, $2, n.id, n.node_key, n.task_type, case when coalesce(d.cnt, 0) = 0 then 'READY' else 'PENDING' end,
        coalesce($3, n.priority), coalesce($3, n.priority),
        least(case when n.deadline_ms is not null then now() + n.deadline_ms * interval '1 millisecond' end, $4::timestamptz),
        case when coalesce(d.cnt, 0) = 0 then now() end, case when coalesce(d.cnt, 0) = 0 then $5::timestamptz end,
        coalesce(d.cnt, 0), coalesce(d.cnt, 0), n.trigger_rule, n.max_attempts, n.timeout_ms, n.backoff_base_ms, n.backoff_max_ms,
        $7::text || ':' || n.node_key
      from workflow_nodes n
      left join (select to_node_id, count(*) as cnt from workflow_edges where workflow_version_id = $6 group by to_node_id) d on d.to_node_id = n.id
      where n.workflow_version_id = $6 returning id, node_key, status`,
      [execId, r.orgId, r.priority ?? null, deadline, r.runAt ?? null, versionId, execId]);
    const evs: Event[] = [wfEvent(r.orgId, execId, "WORKFLOW_CREATED", null, "PENDING", r.runAt ? `scheduled for ${r.runAt.toISOString()}` : `trigger ${r.trigger ?? "API"}`)];
    for (const x of roots.rows) if (x.status === "READY") evs.push({ orgId: r.orgId, executionId: execId, taskId: x.id, nodeKey: x.node_key, type: "TASK_READY", to: "READY" });
    await this.store.events.emit(t, evs);
    return { executionId: execId, created: true, status: "PENDING", totalTasks: total, runAt: r.runAt ?? null };
  }

  async cancel(orgId: string, execId: string, reason: string) {
    await tx(this.db, async (t) => {
      const status = await this.lockedStatus(t, orgId, execId);
      if (TERMINAL.has(status)) throw ApiError.conflict(`Execution already ${status}`);
      const evs: Event[] = [];
      await this.store.terminate(t, orgId, execId, "CANCELLED", reason, evs);
      await this.store.events.emit(t, evs);
    });
  }

  /**
   * Workflow retry = resume in place: SUCCEEDED tasks keep their outputs, everything else is re-derived in topological
   * order from its upstreams (so branch decisions are re-evaluated) and gets a fresh attempt budget. Idempotency keys
   * are stable per node, so re-running a task never duplicates its business effect.
   */
  async resume(orgId: string, execId: string, t?: Tx): Promise<number> {
    if (!t) return tx(this.db, (t2) => this.resume(orgId, execId, t2));
    const status = await this.lockedStatus(t, orgId, execId);
    if (status !== "FAILED" && status !== "CANCELLED") throw ApiError.conflict(`Only FAILED or CANCELLED executions can be retried (status ${status})`);
    const versionId = (await t.c.query("select workflow_version_id from workflow_executions where id = $1", [execId])).rows[0].workflow_version_id;
    const tasks = (await t.c.query(`select t.id, t.node_id, t.node_key, t.status, t.output, t.attempt, t.trigger_rule, n.max_attempts as node_max
      from task_executions t join workflow_nodes n on n.id = t.node_id where t.workflow_execution_id = $1 order by t.id for update of t`, [execId])).rows;
    const byNode = new Map(tasks.map((x) => [x.node_id as string, x]));
    const edges = (await t.c.query("select from_node_id, to_node_id, condition from workflow_edges where workflow_version_id = $1", [versionId])).rows as
      { from_node_id: string; to_node_id: string; condition: Condition | null }[];
    const incoming = new Map<string, typeof edges>(), outgoing = new Map<string, typeof edges>();
    for (const e of edges) {
      (incoming.get(e.to_node_id) ?? incoming.set(e.to_node_id, []).get(e.to_node_id)!).push(e);
      (outgoing.get(e.from_node_id) ?? outgoing.set(e.from_node_id, []).get(e.from_node_id)!).push(e);
    }
    const indeg = new Map([...byNode.keys()].map((n) => [n, incoming.get(n)?.length ?? 0]));
    const queue = [...byNode.keys()].filter((n) => indeg.get(n) === 0);
    const settled = new Map<string, string>();
    const evs: Event[] = [wfEvent(orgId, execId, "WORKFLOW_RESUMED", status, "RUNNING", "retry from failure point")];
    let remaining = 0;
    while (queue.length) {
      const node = queue.shift()!;
      for (const e of outgoing.get(node) ?? []) { indeg.set(e.to_node_id, indeg.get(e.to_node_id)! - 1); if (indeg.get(e.to_node_id) === 0) queue.push(e.to_node_id); }
      const x = byNode.get(node)!;
      if (x.status === "SUCCEEDED") { settled.set(node, "SUCCEEDED"); continue; }
      const inc = incoming.get(node) ?? [];
      let pending = 0, satisfied = 0;
      for (const e of inc) {
        const up = settled.get(e.from_node_id);
        if (!up) pending++;
        else if (up === "SUCCEEDED" && evaluate(e.condition, byNode.get(e.from_node_id)!.output)) satisfied++;
      }
      const next = pending > 0 ? "PENDING"
        : inc.length === 0 || (x.trigger_rule === "ANY_SUCCESS" ? satisfied >= 1 : satisfied === inc.length) ? "READY" : "SKIPPED";
      if (next === "SKIPPED") settled.set(node, "SKIPPED"); else remaining++;
      await t.c.query(`update task_executions set status = $1, pending_deps = $2, satisfied_deps = $3,
          ready_at = case when $1 = 'READY' then now() end, not_before = null, finished_at = case when $1 = 'SKIPPED' then now() end,
          error = null, error_type = null, lease_owner = null, lease_expires_at = null, max_attempts = attempt + $4, effective_priority = priority
        where id = $5`, [next, pending, satisfied, x.node_max, x.id]);
      if (next !== "PENDING") evs.push({ orgId, executionId: execId, taskId: x.id, nodeKey: x.node_key, type: `TASK_${next}`, from: x.status, to: next, message: "resumed" });
    }
    await t.c.query(`update workflow_executions set status = $1, finished_at = case when $2::int = 0 then now() end, error = null, remaining_tasks = $2
      where id = $3`, [remaining === 0 ? "SUCCEEDED" : "RUNNING", remaining, execId]);
    await t.c.query("update dead_letter_tasks set resolved_at = now(), resolution = 'REPLAYED' where workflow_execution_id = $1 and resolved_at is null", [execId]);
    await this.store.events.emit(t, evs);
    return remaining;
  }

  /** Move a not-yet-started (delayed) execution to a new start time. */
  async reschedule(orgId: string, execId: string, runAt: Date) {
    await tx(this.db, async (t) => {
      if ((await this.lockedStatus(t, orgId, execId)) !== "PENDING") throw ApiError.conflict("Only PENDING executions can be rescheduled");
      const moved = await t.c.query("update task_executions set not_before = $1 where workflow_execution_id = $2 and status = 'READY'", [runAt, execId]);
      if (!moved.rowCount) throw ApiError.conflict("Execution has already been dispatched");
      await t.c.query("update workflow_executions set run_at = $1 where id = $2", [runAt, execId]);
      await this.store.events.emit(t, [wfEvent(orgId, execId, "WORKFLOW_RESCHEDULED", "PENDING", "PENDING", `run at ${runAt.toISOString()}`)]);
    });
  }

  private async lockedStatus(t: Tx, orgId: string, execId: string): Promise<string> {
    const r = await t.c.query("select status from workflow_executions where id = $1 and org_id = $2 for update", [execId, orgId]);
    if (r.rows.length === 0) throw ApiError.notFound("Execution");
    return r.rows[0].status;
  }

  // ---------------------------------------------------------------- queries

  async list(orgId: string, q: { status?: string; workflowId?: string; q?: string; page?: unknown; size?: unknown }) {
    const { page, size } = pageOf(q.page, q.size);
    const where = ["e.org_id = $1"], args: unknown[] = [orgId];
    if (q.status) { args.push(q.status.toUpperCase()); where.push(`e.status = $${args.length}`); }
    if (q.workflowId) { args.push(q.workflowId); where.push(`e.workflow_id = $${args.length}`); }
    if (q.q?.trim()) { args.push(q.q.trim().toLowerCase() + "%", `%${q.q.trim()}%`); where.push(`(e.id::text like $${args.length - 1} or w.name ilike $${args.length})`); }
    const w = " where " + where.join(" and ");
    const total = (await this.db.query(`select count(*) as n from workflow_executions e join workflows w on w.id = e.workflow_id${w}`, args)).rows[0].n;
    const rows = (await this.db.query(`${SUMMARY}${w} order by e.created_at desc limit $${args.length + 1} offset $${args.length + 2}`, [...args, size, page * size])).rows;
    return { items: rows.map(summary), page, size, total };
  }

  async detail(orgId: string, execId: string) {
    const s = (await this.db.query(`${SUMMARY} where e.id = $1 and e.org_id = $2`, [execId, orgId])).rows;
    if (s.length === 0) throw ApiError.notFound("Execution");
    const extra = (await this.db.query("select input, deadline_at, workflow_version_id from workflow_executions where id = $1", [execId])).rows[0];
    const attempts = new Map<string, unknown[]>();
    for (const a of (await this.db.query(`select a.task_execution_id, a.attempt, a.attempt_id, a.worker_id, a.status, a.queued_at, a.started_at, a.finished_at,
        a.duration_ms, a.backoff_ms, a.error, a.error_type from retry_attempts a join task_executions t on t.id = a.task_execution_id
      where t.workflow_execution_id = $1 order by a.attempt`, [execId])).rows) {
      (attempts.get(a.task_execution_id) ?? attempts.set(a.task_execution_id, []).get(a.task_execution_id)!).push({
        attempt: a.attempt, attemptId: a.attempt_id, workerId: a.worker_id, status: a.status, queuedAt: a.queued_at, startedAt: a.started_at,
        finishedAt: a.finished_at, durationMs: a.duration_ms, backoffMs: a.backoff_ms, error: a.error, errorType: a.error_type });
    }
    const tasks = (await this.db.query(`select t.id, t.node_key, n.name, t.task_type, t.status, t.priority, t.effective_priority, t.attempt, t.max_attempts,
        t.lease_owner, t.lease_expires_at, t.ready_at, t.not_before, t.queued_at, t.started_at, t.finished_at, t.deadline_at,
        (extract(epoch from (coalesce(t.finished_at, now()) - t.started_at)) * 1000)::bigint as duration_ms, t.output,
        t.error, t.error_type, t.timeout_ms, t.trigger_rule, n.position_x, n.position_y
      from task_executions t join workflow_nodes n on n.id = t.node_id where t.workflow_execution_id = $1
      order by n.position_y, n.position_x, t.node_key`, [execId])).rows.map((t) => ({
      id: t.id, nodeKey: t.node_key, name: t.name, taskType: t.task_type, status: t.status, priority: t.priority, effectivePriority: t.effective_priority,
      attempt: t.attempt, maxAttempts: t.max_attempts, leaseOwner: t.lease_owner, leaseExpiresAt: t.lease_expires_at, readyAt: t.ready_at,
      notBefore: t.not_before, queuedAt: t.queued_at, startedAt: t.started_at, finishedAt: t.finished_at, deadlineAt: t.deadline_at,
      durationMs: t.duration_ms, output: t.output, error: t.error, errorType: t.error_type, timeoutMs: t.timeout_ms, triggerRule: t.trigger_rule,
      x: t.position_x, y: t.position_y, attempts: attempts.get(t.id) ?? [] }));
    const edges = (await this.db.query(`select f.node_key as from, t.node_key as to, e.condition from workflow_edges e
      join workflow_nodes f on f.id = e.from_node_id join workflow_nodes t on t.id = e.to_node_id where e.workflow_version_id = $1`, [extra.workflow_version_id])).rows;
    return { execution: summary(s[0]), input: extra.input, deadlineAt: extra.deadline_at, tasks, edges };
  }

  /** Logs explorer. Keyset pagination (id < cursor) stays fast on a large, append-only table. */
  async events(orgId: string, q: { executionId?: string; type?: string; q?: string; before?: unknown; size?: unknown }) {
    const { size } = pageOf(0, q.size);
    const where = ["org_id = $1"], args: unknown[] = [orgId];
    if (q.executionId) { args.push(q.executionId); where.push(`workflow_execution_id = $${args.length}`); }
    if (q.type?.trim()) { args.push(q.type.trim().toUpperCase() + "%"); where.push(`type like $${args.length}`); }
    if (q.q?.trim()) { args.push(`%${q.q.trim()}%`); where.push(`(message ilike $${args.length} or node_key ilike $${args.length} or worker_id ilike $${args.length})`); }
    if (q.before != null && q.before !== "") { args.push(Number(q.before)); where.push(`id < $${args.length}`); }
    args.push(size);
    const rows = (await this.db.query(`select id, workflow_execution_id, task_execution_id, node_key, type, from_status, to_status, attempt, worker_id, message, created_at
      from execution_events where ${where.join(" and ")} order by id desc limit $${args.length}`, args)).rows;
    const items = rows.map((r) => ({ id: r.id, executionId: r.workflow_execution_id, taskId: r.task_execution_id, nodeKey: r.node_key, type: r.type,
      from: r.from_status, to: r.to_status, attempt: r.attempt, workerId: r.worker_id, message: r.message, at: r.created_at }));
    return { items, nextCursor: items.length === size ? items[items.length - 1].id : null };
  }

  // ---------------------------------------------------------------- dead letters

  async deadLetters(orgId: string, q: { includeResolved?: boolean; reason?: string; page?: unknown; size?: unknown }) {
    const { page, size } = pageOf(q.page, q.size);
    const args: unknown[] = [orgId];
    let w = " where d.org_id = $1" + (q.includeResolved ? "" : " and d.resolved_at is null");
    if (q.reason) { args.push(q.reason); w += ` and d.reason = $${args.length}`; }
    const total = (await this.db.query(`select count(*) as n from dead_letter_tasks d${w}`, args)).rows[0].n;
    const rows = (await this.db.query(`select d.id, d.task_execution_id, d.workflow_execution_id, w.name, d.node_key, d.task_type, d.reason, d.error,
        d.attempts, d.created_at, d.resolved_at, d.resolution, e.status
      from dead_letter_tasks d left join workflow_executions e on e.id = d.workflow_execution_id left join workflows w on w.id = e.workflow_id
      ${w} order by d.created_at desc limit $${args.length + 1} offset $${args.length + 2}`, [...args, size, page * size])).rows;
    return { items: rows.map((r) => ({ id: r.id, taskExecutionId: r.task_execution_id, executionId: r.workflow_execution_id, workflowName: r.name,
      nodeKey: r.node_key, taskType: r.task_type, reason: r.reason, error: r.error, attempts: r.attempts, createdAt: r.created_at,
      resolvedAt: r.resolved_at, resolution: r.resolution, executionStatus: r.status })), page, size, total };
  }

  async replayDeadLetter(orgId: string, id: string): Promise<string> {
    return tx(this.db, async (t) => {
      const r = await t.c.query("select workflow_execution_id from dead_letter_tasks where id = $1 and org_id = $2 and resolved_at is null", [id, orgId]);
      if (r.rows.length === 0) throw ApiError.notFound("Open dead-letter task");
      await this.resume(orgId, r.rows[0].workflow_execution_id, t);
      return r.rows[0].workflow_execution_id;
    });
  }

  async discardDeadLetter(orgId: string, id: string) {
    const r = await this.db.query("update dead_letter_tasks set resolved_at = now(), resolution = 'DISCARDED' where id = $1 and org_id = $2 and resolved_at is null", [id, orgId]);
    if (!r.rowCount) throw ApiError.notFound("Open dead-letter task");
  }
}
