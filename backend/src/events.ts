// Execution events are written to Postgres inside the state-transition transaction (durable history, logs
// explorer) and fanned out on Redis pub/sub only AFTER commit, so the live UI never shows a transition that
// was rolled back. Pub/sub is lossy by design; clients re-sync from the REST API.
import type { Redis } from "ioredis";
import type { Tx } from "./db.ts";
import { EVENTS_CHANNEL } from "./queue.ts";

export interface Event {
  orgId: string; executionId: string; taskId?: string | null; nodeKey?: string | null; type: string;
  from?: string | null; to?: string | null; attempt?: number | null; workerId?: string | null; message?: string | null;
}

export const wfEvent = (orgId: string, executionId: string, type: string, from: string | null, to: string | null, message?: string | null): Event =>
  ({ orgId, executionId, type, from, to, message });

export class EventBus {
  private redis: Redis | null;
  constructor(redis: Redis | null) { this.redis = redis; }

  /** Insert in the caller's transaction; publish once it commits. */
  async emit(t: Tx, events: Event[]) {
    if (events.length === 0) return;
    const cols: unknown[][] = [[], [], [], [], [], [], [], [], [], []];
    for (const e of events) {
      [e.orgId, e.executionId, e.taskId ?? null, e.nodeKey ?? null, e.type, e.from ?? null, e.to ?? null,
        e.attempt ?? null, e.workerId ?? null, e.message ?? null].forEach((v, i) => cols[i].push(v));
    }
    await t.c.query(
      `insert into execution_events(org_id, workflow_execution_id, task_execution_id, node_key, type, from_status, to_status, attempt, worker_id, message)
       select * from unnest($1::uuid[], $2::uuid[], $3::uuid[], $4::text[], $5::text[], $6::text[], $7::text[], $8::int[], $9::text[], $10::text[])`, cols);
    this.publishAfterCommit(t, events);
  }

  /** For events already inserted by a data-modifying CTE (dispatch path): only fan out. */
  publishAfterCommit(t: Tx, events: Event[]) {
    if (events.length) t.afterCommit(() => this.publish(events));
  }

  publish(events: Event[]) {
    if (!this.redis) return;
    const at = new Date().toISOString();
    const p = this.redis.pipeline();
    for (const e of events) {
      const m: Record<string, unknown> = { orgId: e.orgId, executionId: e.executionId, type: e.type, at };
      for (const k of ["taskId", "nodeKey", "from", "to", "attempt", "workerId", "message"] as const) if (e[k] != null) m[k] = e[k];
      p.publish(EVENTS_CHANNEL, JSON.stringify(m));
    }
    p.exec().catch(() => { /* live view degrades, state is safe in Postgres */ });
  }
}
