// The task state machine. Every transition is a single conditional UPDATE (compare-and-set) on
// (attempt_id, status[, lease_owner]); a writer that lost ownership affects zero rows and its result is discarded.
//
//   PENDING --deps settled--> READY --dispatch--> QUEUED --claim--> RUNNING --ok--> SUCCEEDED
//      |                        ^                                    |
//      +--branch not taken--> SKIPPED        RETRYING <--retryable---+--exhausted--> FAILED (+ dead letter)
//   any non-terminal --workflow cancelled/failed--> CANCELLED
//
// Lock-ordering invariant (prevents deadlocks): a transaction that touches more than one task of a workflow
// first locks that workflow_executions row; multi-row task locks are taken in id order.
import { tx, type Db, type Tx } from "./db.ts";
import { backoffMs, evaluate, STRATEGIES, type Condition, type Strategy } from "./dag.ts";
import { EventBus, wfEvent, type Event } from "./events.ts";

export interface Dispatched { taskId: string; attemptId: string; attempt: number; orgId: string; executionId: string; nodeKey: string; taskType: string; prev: string; delayMs: number }
export interface Claimed {
  taskId: string; attemptId: string; attempt: number; maxAttempts: number; orgId: string; executionId: string; nodeId: string;
  nodeKey: string; taskType: string; timeoutMs: number; idempotencyKey: string; queuedAt: Date; startedAt: Date;
  params: Record<string, unknown>; input: Record<string, unknown>; results: Record<string, unknown>;
}
export interface Completion { committed: boolean; workflowFinished: boolean; workflowDurationMs: number | null }
export interface Failure { committed: boolean; willRetry: boolean; backoffMs: number; deadLettered: boolean }
interface Edge { toNodeId: string; condition: Condition | null }

const FENCED_COMPLETION: Completion = { committed: false, workflowFinished: false, workflowDurationMs: null };
const FENCED_FAILURE: Failure = { committed: false, willRetry: false, backoffMs: 0, deadLettered: false };
const trunc = (s: string | null | undefined, n: number) => (s == null ? null : s.length <= n ? s : s.slice(0, n) + "...");

export class TaskStore {
  // Edges are immutable per workflow version, so caching them by node id can never go stale.
  // ponytail: cleared wholesale at 100k entries; swap for an LRU if a deployment has more distinct nodes.
  private edgeCache = new Map<string, Edge[]>();

  readonly db: Db; readonly events: EventBus;
  constructor(db: Db, events: EventBus) { this.db = db; this.events = events; }

  // ---------------------------------------------------------------- dispatch (scheduler)

  /**
   * Atomically moves up to `limit` runnable tasks to QUEUED in strategy order and records the attempt.
   * FOR UPDATE SKIP LOCKED lets several scheduler replicas run this concurrently without double dispatch.
   */
  async claimForDispatch(strategy: Strategy, limit: number): Promise<Dispatched[]> {
    return tx(this.db, async (t) => {
      const { rows } = await t.c.query(`
        with picked as (
          select id, status as prev, greatest(ready_at, not_before) as eligible_at
          from task_executions
          where status in ('READY', 'RETRYING') and (not_before is null or not_before <= now())
          order by ${STRATEGIES[strategy]}
          limit $1 for update skip locked
        ), upd as (
          update task_executions t
          set status = 'QUEUED', attempt = t.attempt + 1, attempt_id = gen_random_uuid(), queued_at = now(), stream_message_id = null
          from picked where t.id = picked.id
          returning t.id, t.attempt_id, t.attempt, t.org_id, t.workflow_execution_id, t.node_key, t.task_type, picked.prev, picked.eligible_at
        ), att as (
          insert into retry_attempts(task_execution_id, org_id, attempt, attempt_id, status, queued_at)
          select id, org_id, attempt, attempt_id, 'QUEUED', now() from upd
        ), ev as (
          insert into execution_events(org_id, workflow_execution_id, task_execution_id, node_key, type, from_status, to_status, attempt)
          select org_id, workflow_execution_id, id, node_key, 'TASK_QUEUED', prev, 'QUEUED', attempt from upd
        )
        select upd.*, extract(epoch from (clock_timestamp() - eligible_at)) * 1000 as delay_ms from upd`, [limit]);
      const out: Dispatched[] = rows.map((r) => ({ taskId: r.id, attemptId: r.attempt_id, attempt: r.attempt, orgId: r.org_id,
        executionId: r.workflow_execution_id, nodeKey: r.node_key, taskType: r.task_type, prev: r.prev, delayMs: Number(r.delay_ms) }));
      this.events.publishAfterCommit(t, out.map((d) => ({ orgId: d.orgId, executionId: d.executionId, taskId: d.taskId, nodeKey: d.nodeKey,
        type: "TASK_QUEUED", from: d.prev, to: "QUEUED", attempt: d.attempt })));
      return out;
    });
  }

  async recordMessageIds(batch: Dispatched[], ids: (string | null)[]) {
    const a: string[] = [], m: string[] = [];
    batch.forEach((d, i) => { if (ids[i]) { a.push(d.attemptId); m.push(ids[i]!); } });
    if (a.length === 0) return;
    await this.db.query(`update task_executions t set stream_message_id = x.m from unnest($1::uuid[], $2::text[]) as x(a, m)
                         where t.attempt_id = x.a and t.status = 'QUEUED'`, [a, m]);
  }

  // ---------------------------------------------------------------- claim (worker)

  /**
   * Acquire execution ownership: QUEUED -> RUNNING with a lease, only if the message's attempt is still current.
   * A duplicate delivery, a stale redelivery, or a task cancelled meanwhile all match zero rows.
   */
  async claim(taskId: string, attemptId: string, executionId: string, workerId: string, leaseMs: number): Promise<Claimed | null> {
    // One statement (atomic on its own, one round trip): take the lease, mark the attempt RUNNING, record the event and
    // load the task's input. Only task and attempt rows are locked here, never the workflow row.
    const { rows } = await this.db.query(`
      with t as (
        update task_executions set status = 'RUNNING', lease_owner = $1, lease_expires_at = now() + $2 * interval '1 millisecond', started_at = now()
        where id = $3 and attempt_id = $4 and status = 'QUEUED'
        returning id, attempt_id, attempt, max_attempts, org_id, workflow_execution_id, node_id, node_key, task_type, timeout_ms, idempotency_key, queued_at, started_at
      ), att as (
        update retry_attempts set status = 'RUNNING', worker_id = $1, started_at = now() where attempt_id = $4 and exists (select 1 from t)
      ), ev as (
        insert into execution_events(org_id, workflow_execution_id, task_execution_id, node_key, type, from_status, to_status, attempt, worker_id)
        select org_id, workflow_execution_id, id, node_key, 'TASK_STARTED', 'QUEUED', 'RUNNING', attempt, $1 from t
      )
      select t.*, n.params, we.input, we.status as wf_status,
        (select coalesce(jsonb_object_agg(x.node_key, coalesce(x.output, '{}'::jsonb)), '{}'::jsonb)
         from task_executions x where x.workflow_execution_id = t.workflow_execution_id and x.status = 'SUCCEEDED') as results
      from t join workflow_nodes n on n.id = t.node_id join workflow_executions we on we.id = t.workflow_execution_id`, [workerId, leaseMs, taskId, attemptId]);
    if (rows.length === 0) return null;
    const r = rows[0];
    const evs: Event[] = [{ orgId: r.org_id, executionId, taskId, nodeKey: r.node_key, type: "TASK_STARTED", from: "QUEUED", to: "RUNNING", attempt: r.attempt, workerId }];
    if (r.wf_status === "PENDING") {
      // First task of the execution. Separate autocommit statement that locks only the workflow row, so the
      // workflow-before-task lock order used by complete()/terminate() can never be inverted (no deadlock).
      const w = await this.db.query(`with u as (update workflow_executions set status = 'RUNNING', started_at = coalesce(started_at, now())
          where id = $1 and status = 'PENDING' returning org_id)
        insert into execution_events(org_id, workflow_execution_id, type, from_status, to_status) select org_id, $1, 'WORKFLOW_STARTED', 'PENDING', 'RUNNING' from u returning 1`, [executionId]);
      if (w.rowCount) evs.unshift(wfEvent(r.org_id, executionId, "WORKFLOW_STARTED", "PENDING", "RUNNING"));
    }
    this.events.publish(evs);
    return { taskId: r.id, attemptId: r.attempt_id, attempt: r.attempt, maxAttempts: r.max_attempts, orgId: r.org_id, executionId,
      nodeId: r.node_id, nodeKey: r.node_key, taskType: r.task_type, timeoutMs: r.timeout_ms, idempotencyKey: r.idempotency_key,
      queuedAt: r.queued_at, startedAt: r.started_at, params: r.params ?? {}, input: r.input ?? {}, results: r.results ?? {} };
  }

  // ---------------------------------------------------------------- completion

  async complete(executionId: string, attemptId: string, workerId: string, output: unknown): Promise<Completion> {
    return tx(this.db, async (t) => {
      // Statement 1: lock the workflow row FIRST (it is evaluated in the task update's WHERE clause, before the task row
      // is locked), then fence on (attempt_id, RUNNING, lease_owner) and close the attempt.
      const { rows } = await t.c.query(`
        with l as (select id from workflow_executions where id = $4 for update),
        u as (
          update task_executions set status = 'SUCCEEDED', output = $1::jsonb, finished_at = now(), lease_owner = null, lease_expires_at = null,
            error = null, error_type = null
          where attempt_id = $2 and status = 'RUNNING' and lease_owner = $3 and exists (select 1 from l)
          returning id, org_id, node_id, node_key, attempt
        ), a as (
          update retry_attempts set status = case when exists (select 1 from u) then 'SUCCEEDED' else 'DISCARDED' end, finished_at = now(),
            duration_ms = case when exists (select 1 from u) then (extract(epoch from (now() - coalesce(started_at, queued_at))) * 1000)::int else duration_ms end
          where attempt_id = $2 and status in ('QUEUED', 'RUNNING')
        )
        select * from u`, [JSON.stringify(output ?? {}), attemptId, workerId, executionId]);
      if (rows.length === 0) return FENCED_COMPLETION;
      const r = rows[0];
      const evs: Event[] = [{ orgId: r.org_id, executionId, taskId: r.id, nodeKey: r.node_key, type: "TASK_SUCCEEDED", from: "RUNNING", to: "SUCCEEDED", attempt: r.attempt, workerId }];
      const settled = 1 + await this.propagate(t, r.org_id, executionId, [{ node: r.node_id, output, succeeded: true }], evs);
      return this.settle(t, r.org_id, executionId, settled, evs);
    });
  }

  /**
   * Statement 2: one UPDATE settles the dependency counters of every dependent at once (whatever the fan-out width) and
   * decides each one's fate inline: READY when its trigger rule is satisfied, SKIPPED when it never can be, otherwise
   * still PENDING. Skips cascade (rare) by recursing on the newly skipped nodes.
   */
  private async propagate(t: Tx, orgId: string, executionId: string, from: { node: string; output: unknown; succeeded: boolean }[], evs: Event[]): Promise<number> {
    const to: string[] = [], taken: number[] = [];
    for (const f of from) for (const e of await this.edges(t, f.node)) { to.push(e.toNodeId); taken.push(f.succeeded && evaluate(e.condition, f.output) ? 1 : 0); }
    if (to.length === 0) return 0;
    const { rows } = await t.c.query(`
      with d as (select node, sum(tk)::int as tk, count(*)::int as n from unnest($2::uuid[], $3::int[]) as x(node, tk) group by node)
      update task_executions t set
        pending_deps = t.pending_deps - d.n, satisfied_deps = t.satisfied_deps + d.tk,
        status = case when t.pending_deps - d.n > 0 then 'PENDING'
                      when (case when t.trigger_rule = 'ANY_SUCCESS' then t.satisfied_deps + d.tk >= 1 else t.satisfied_deps + d.tk = t.total_deps end) then 'READY'
                      else 'SKIPPED' end,
        ready_at = case when t.pending_deps - d.n = 0 then now() else t.ready_at end,
        finished_at = case when t.pending_deps - d.n = 0 and not (case when t.trigger_rule = 'ANY_SUCCESS' then t.satisfied_deps + d.tk >= 1
                      else t.satisfied_deps + d.tk = t.total_deps end) then now() else t.finished_at end
      from d where t.workflow_execution_id = $1 and t.node_id = d.node and t.status = 'PENDING'
      returning t.id, t.node_id, t.node_key, t.status`, [executionId, to, taken]);
    let settled = 0;
    const skipped: { node: string; output: unknown; succeeded: boolean }[] = [];
    for (const d of rows) {
      if (d.status === "READY") evs.push({ orgId, executionId, taskId: d.id, nodeKey: d.node_key, type: "TASK_READY", from: "PENDING", to: "READY" });
      if (d.status === "SKIPPED") {
        evs.push({ orgId, executionId, taskId: d.id, nodeKey: d.node_key, type: "TASK_SKIPPED", from: "PENDING", to: "SKIPPED", message: "branch condition not met" });
        settled++;
        skipped.push({ node: d.node_id, output: null, succeeded: false });
      }
    }
    return settled + (skipped.length ? await this.propagate(t, orgId, executionId, skipped, evs) : 0);
  }

  /**
   * Statement 3: atomic countdown of unsettled tasks; the transaction that reaches zero finalises the workflow, and all
   * of this transition's events are inserted by the same statement.
   */
  private async settle(t: Tx, orgId: string, executionId: string, settledTasks: number, evs: Event[]): Promise<Completion> {
    const cols: unknown[][] = [[], [], [], [], [], [], [], []];
    for (const e of evs) [e.taskId ?? null, e.nodeKey ?? null, e.type, e.from ?? null, e.to ?? null, e.attempt ?? null, e.workerId ?? null, e.message ?? null].forEach((v, i) => cols[i].push(v));
    const { rows } = await t.c.query(`
      with s as (
        update workflow_executions set remaining_tasks = remaining_tasks - $2,
          status = case when remaining_tasks - $2 = 0 then 'SUCCEEDED' else status end,
          finished_at = case when remaining_tasks - $2 = 0 then now() else finished_at end
        where id = $1 and status in ('PENDING', 'RUNNING')
        returning remaining_tasks, org_id, extract(epoch from (now() - created_at)) * 1000 as duration_ms
      ), ev as (
        insert into execution_events(org_id, workflow_execution_id, task_execution_id, node_key, type, from_status, to_status, attempt, worker_id, message)
        select $3::uuid, $1, * from unnest($4::uuid[], $5::text[], $6::text[], $7::text[], $8::text[], $9::int[], $10::text[], $11::text[])
        union all select org_id, $1, null, null, 'WORKFLOW_SUCCEEDED', 'RUNNING', 'SUCCEEDED', null, null, null from s where remaining_tasks = 0
      )
      select * from s`, [executionId, settledTasks, orgId, ...cols]);
    const finished = rows.length > 0 && rows[0].remaining_tasks === 0;
    if (finished) evs.push(wfEvent(orgId, executionId, "WORKFLOW_SUCCEEDED", "RUNNING", "SUCCEEDED"));
    this.events.publishAfterCommit(t, evs);
    return { committed: true, workflowFinished: finished, workflowDurationMs: finished ? Number(rows[0].duration_ms) : null };
  }

  // ---------------------------------------------------------------- failure

  /**
   * Records a failed attempt. Retryable with attempts left -> RETRYING with backoff; otherwise FAILED, a dead-letter
   * row, and the workflow fails fast (remaining tasks cancelled).
   * workerId: owner that must still hold the lease, or null for the scheduler's lease reaper.
   * onlyIfLeaseExpired: reaper mode, do nothing if the worker renewed the lease meanwhile.
   */
  async fail(attemptId: string, workerId: string | null, error: string, errorType: string, retryable: boolean,
             attemptStatus: string, onlyIfLeaseExpired: boolean): Promise<Failure> {
    return tx(this.db, async (t) => {
      const exec = await t.c.query("select workflow_execution_id from task_executions where attempt_id = $1 and status = 'RUNNING'", [attemptId]);
      if (exec.rows.length === 0) { if (workerId) await this.discardAttempt(t, attemptId); return FENCED_FAILURE; }
      const executionId: string = exec.rows[0].workflow_execution_id;
      await this.lockExecution(t, executionId);
      const { rows } = await t.c.query(`select id, org_id, node_key, task_type, attempt, max_attempts, backoff_base_ms, backoff_max_ms,
          lease_owner, lease_expires_at < now() as expired
        from task_executions where attempt_id = $1 and status = 'RUNNING' for update`, [attemptId]);
      if (rows.length === 0) return FENCED_FAILURE;
      const r = rows[0];
      if ((workerId && workerId !== r.lease_owner) || (onlyIfLeaseExpired && !r.expired)) {
        if (workerId) await this.discardAttempt(t, attemptId);
        return FENCED_FAILURE;
      }
      const msg = trunc(error, 4000);
      const evs: Event[] = [];
      if (retryable && r.attempt < r.max_attempts) {
        const backoff = backoffMs(r.attempt, r.backoff_base_ms, r.backoff_max_ms);
        await t.c.query(`update task_executions set status = 'RETRYING', not_before = now() + $1 * interval '1 millisecond',
            lease_owner = null, lease_expires_at = null, error = $2, error_type = $3 where id = $4`, [backoff, msg, errorType, r.id]);
        await this.finishAttempt(t, attemptId, attemptStatus, msg, errorType, backoff);
        evs.push({ orgId: r.org_id, executionId, taskId: r.id, nodeKey: r.node_key, type: "TASK_RETRYING", from: "RUNNING", to: "RETRYING",
          attempt: r.attempt, workerId: r.lease_owner,
          message: `attempt ${r.attempt}/${r.max_attempts} ${attemptStatus.toLowerCase()}: ${trunc(msg, 300)} - retry in ${backoff} ms` });
        await this.events.emit(t, evs);
        return { committed: true, willRetry: true, backoffMs: backoff, deadLettered: false };
      }
      await t.c.query(`update task_executions set status = 'FAILED', finished_at = now(), lease_owner = null, lease_expires_at = null,
          error = $1, error_type = $2 where id = $3`, [msg, errorType, r.id]);
      await this.finishAttempt(t, attemptId, attemptStatus, msg, errorType, null);
      const reason = !retryable ? (errorType === "UNKNOWN_TASK_TYPE" ? "UNKNOWN_TASK_TYPE" : "NON_RETRYABLE")
        : attemptStatus === "LOST" ? "LEASE_LOST_EXHAUSTED" : "RETRIES_EXHAUSTED";
      await t.c.query(`insert into dead_letter_tasks(org_id, task_execution_id, workflow_execution_id, node_key, task_type, reason, error, attempts, payload)
        values ($1, $2, $3, $4, $5, $6, $7, $8, $9::jsonb)`,
        [r.org_id, r.id, executionId, r.node_key, r.task_type, reason, msg, r.attempt, JSON.stringify({ attemptId, errorType })]);
      evs.push({ orgId: r.org_id, executionId, taskId: r.id, nodeKey: r.node_key, type: "TASK_FAILED", from: "RUNNING", to: "FAILED", attempt: r.attempt, workerId: r.lease_owner, message: trunc(msg, 300) });
      evs.push({ orgId: r.org_id, executionId, taskId: r.id, nodeKey: r.node_key, type: "TASK_DEAD_LETTERED", attempt: r.attempt, message: reason });
      await this.terminate(t, r.org_id, executionId, "FAILED", `task '${r.node_key}' failed: ${trunc(msg, 500)}`, evs);
      await this.events.emit(t, evs);
      return { committed: true, willRetry: false, backoffMs: 0, deadLettered: true };
    });
  }

  /** Fail or cancel the whole execution. Caller must hold the workflow row lock. */
  async terminate(t: Tx, orgId: string, executionId: string, status: "FAILED" | "CANCELLED", error: string, evs: Event[]): Promise<boolean> {
    const upd = await t.c.query(`update workflow_executions set status = $1, error = $2, finished_at = now(), remaining_tasks = 0
      where id = $3 and status in ('PENDING', 'RUNNING')`, [status, error, executionId]);
    if (!upd.rowCount) return false;
    const { rows } = await t.c.query(`with old as (
        select id, status, attempt_id from task_executions
        where workflow_execution_id = $1 and status in ('PENDING', 'READY', 'QUEUED', 'RUNNING', 'RETRYING') order by id for update)
      update task_executions t set status = 'CANCELLED', finished_at = now(), lease_owner = null, lease_expires_at = null
      from old where t.id = old.id returning t.id, t.node_key, old.status as prev, old.attempt_id`, [executionId]);
    const attempts = rows.map((r) => r.attempt_id).filter(Boolean);
    if (attempts.length)
      await t.c.query(`update retry_attempts set status = 'CANCELLED', finished_at = now() where attempt_id = any($1::uuid[]) and status in ('QUEUED', 'RUNNING')`, [attempts]);
    for (const r of rows) evs.push({ orgId, executionId, taskId: r.id, nodeKey: r.node_key, type: "TASK_CANCELLED", from: r.prev, to: "CANCELLED" });
    evs.push(wfEvent(orgId, executionId, `WORKFLOW_${status}`, "RUNNING", status, error));
    return true;
  }

  // ---------------------------------------------------------------- leases & recovery

  /** Heartbeat lease extension. Returns the attempts this worker still owns; anything missing was fenced off. */
  async renewLeases(workerId: string, attemptIds: string[], leaseMs: number): Promise<Set<string>> {
    if (attemptIds.length === 0) return new Set();
    const { rows } = await this.db.query(`update task_executions set lease_expires_at = now() + $1 * interval '1 millisecond'
      where id in (select id from task_executions where lease_owner = $2 and status = 'RUNNING' and attempt_id = any($3::uuid[]) order by id for update)
      returning attempt_id`, [leaseMs, workerId, attemptIds]);
    return new Set(rows.map((r) => r.attempt_id));
  }

  /** Dead or restarted worker: expire its leases now so the reaper re-queues its tasks without waiting. */
  async expireLeasesOf(workerIds: string[]): Promise<number> {
    if (workerIds.length === 0) return 0;
    const r = await this.db.query(`update task_executions set lease_expires_at = now()
      where id in (select id from task_executions where status = 'RUNNING' and lease_owner = any($1::text[]) order by id for update)`, [workerIds]);
    return r.rowCount ?? 0;
  }

  async expiredLeases(limit: number): Promise<string[]> {
    const { rows } = await this.db.query("select attempt_id from task_executions where status = 'RUNNING' and lease_expires_at < now() limit $1", [limit]);
    return rows.map((r) => r.attempt_id);
  }

  /**
   * QUEUED tasks that may have no live stream message: never published (Redis down at dispatch, crash between
   * commit and XADD) after 2 s, or published long ago (message possibly lost with Redis data) after olderThanMs.
   */
  async staleQueued(olderThanMs: number, limit: number) {
    const { rows } = await this.db.query(`select id, attempt_id, org_id, workflow_execution_id, task_type, stream_message_id from task_executions
      where status = 'QUEUED' and ((stream_message_id is null and queued_at < now() - interval '2 seconds') or queued_at < now() - $1 * interval '1 millisecond')
      order by queued_at limit $2`, [olderThanMs, limit]);
    return rows as { id: string; attempt_id: string; org_id: string; workflow_execution_id: string; task_type: string; stream_message_id: string | null }[];
  }

  async touchQueued(attemptId: string, messageId: string | null) {
    await this.db.query("update task_executions set queued_at = now(), stream_message_id = coalesce($1, stream_message_id) where attempt_id = $2 and status = 'QUEUED'", [messageId, attemptId]);
  }

  /** Anti-starvation: +1 effective priority per agingSeconds waited, capped at 10. Touches only rows that change. */
  async ageRunnable(agingSeconds: number): Promise<number> {
    const r = await this.db.query(`update task_executions
      set effective_priority = least(10, priority + floor(extract(epoch from (now() - ready_at)) / $1)::int)
      where status in ('READY', 'RETRYING') and ready_at < now() - $1 * interval '1 second'
        and effective_priority < least(10, priority + floor(extract(epoch from (now() - ready_at)) / $1)::int)`, [agingSeconds]);
    return r.rowCount ?? 0;
  }

  // ---------------------------------------------------------------- helpers

  async lockExecution(t: Tx, executionId: string) {
    await t.c.query("select 1 from workflow_executions where id = $1 for update", [executionId]);
  }

  private async discardAttempt(t: Tx, attemptId: string) {
    await t.c.query("update retry_attempts set status = 'DISCARDED', finished_at = now() where attempt_id = $1 and status in ('QUEUED', 'RUNNING')", [attemptId]);
  }

  private async finishAttempt(t: Tx, attemptId: string, status: string, error: string | null, errorType: string | null, backoff: number | null) {
    await t.c.query(`update retry_attempts set status = $1, finished_at = now(), error = $2, error_type = $3, backoff_ms = $4,
        duration_ms = (extract(epoch from (now() - coalesce(started_at, queued_at))) * 1000)::int where attempt_id = $5`,
      [status, error, errorType, backoff, attemptId]);
  }

  /** Sorted by target id so concurrent completions lock dependent rows in the same order. */
  async edges(t: Tx, fromNode: string): Promise<Edge[]> {
    const hit = this.edgeCache.get(fromNode);
    if (hit) return hit;
    if (this.edgeCache.size > 100_000) this.edgeCache.clear();
    const { rows } = await t.c.query("select to_node_id, condition from workflow_edges where from_node_id = $1 order by to_node_id", [fromNode]);
    const edges = rows.map((r) => ({ toNodeId: r.to_node_id, condition: r.condition }));
    this.edgeCache.set(fromNode, edges);
    return edges;
  }
}
