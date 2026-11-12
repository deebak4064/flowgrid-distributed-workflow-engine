import { inject } from "vitest";
import { migrate, sleep } from "../src/db.ts";
import type { WorkflowDefinition } from "../src/dag.ts";
import { publishDefinitions } from "../src/handlers.ts";
import { Scheduler } from "../src/scheduler.ts";
import { createSystem, type System } from "../src/system.ts";
import { Worker } from "../src/worker.ts";
import type { Config } from "../src/config.ts";

/** Fast timings so failure detection happens in about a second instead of tens of seconds. */
export const FAST_WORKER: Partial<Config["worker"]> = { leaseMs: 2000, heartbeatMs: 300, visibilityTimeoutMs: 1500, shutdownGraceMs: 3000, concurrency: 4 };
export const FAST_SCHED: Partial<Config["scheduler"]> = { tickMs: 20, workerTimeoutMs: 1500, redispatchAfterMs: 1500, recoveryEveryMs: 200, redispatchEveryMs: 300 };

export interface Env { sys: System; orgId: string; userId: string; otherOrgId: string; stops: (() => Promise<void> | void)[] }

export async function setup(): Promise<Env> {
  const sys = createSystem({ dbUrl: inject("dbUrl"), redisUrl: inject("redisUrl"), dbPool: 20 });
  await migrate(sys.db);
  await reset(sys);
  await publishDefinitions(sys.db); // truncating organizations cascades into task_definitions
  const orgId = (await sys.db.query("insert into organizations(name, slug) values ('Test Org', 'test') returning id")).rows[0].id;
  const otherOrgId = (await sys.db.query("insert into organizations(name, slug) values ('Other Org', 'other') returning id")).rows[0].id;
  const userId = (await sys.db.query("insert into users(org_id, email, display_name, password_hash, role) values ($1, 'admin@test.test', 'Admin', 'x', 'ADMIN') returning id", [orgId])).rows[0].id;
  return { sys, orgId, userId, otherOrgId, stops: [] };
}

export async function reset(sys: System) {
  await sys.db.query(`truncate organizations, users, workflows, workflow_versions, workflow_nodes, workflow_edges, workflow_executions, task_executions,
    retry_attempts, dead_letter_tasks, worker_nodes, worker_heartbeats, scheduled_jobs, execution_events, idempotency_records, audit_events,
    customers, claims, claim_decisions, notifications, financial_reports cascade`);
  await sys.db.query("update system_settings set value = '\"PRIORITY\"' where key = 'scheduler.strategy'");
  await sys.redis.flushdb();
}

export async function teardown(env: Env) {
  for (const s of env.stops.reverse()) await s();
  await env.sys.close();
}

export async function workflow(env: Env, name: string, def: WorkflowDefinition) {
  return (await env.sys.workflows.create(env.orgId, env.userId, name, "", [], def)).id;
}

export async function startWorker(env: Env, cfg: Partial<Config["worker"]> = {}) {
  const w = new Worker(env.sys.db, env.sys.queue, env.sys.store, env.sys.redis.duplicate({ maxRetriesPerRequest: null }), { ...FAST_WORKER, ...cfg });
  await w.start();
  env.stops.push(() => w.stop().catch(() => {}));
  return w;
}

export async function startScheduler(env: Env, cfg: Partial<Config["scheduler"]> = {}) {
  const s = new Scheduler(env.sys.db, env.sys.store, env.sys.queue, env.sys.schedules, env.sys.settings, { ...FAST_SCHED, ...cfg });
  await s.start();
  env.stops.push(() => s.stop());
  return s;
}

export async function waitFor<T>(fn: () => Promise<T | null | undefined | false>, timeoutMs = 20_000, everyMs = 50): Promise<T> {
  const deadline = Date.now() + timeoutMs;
  for (;;) {
    const v = await fn();
    if (v) return v;
    if (Date.now() > deadline) throw new Error(`waitFor timed out after ${timeoutMs} ms`);
    await sleep(everyMs);
  }
}

export async function execStatus(env: Env, id: string): Promise<string> {
  return (await env.sys.db.query("select status from workflow_executions where id = $1", [id])).rows[0].status;
}

export const waitTerminal = (env: Env, id: string, timeoutMs = 20_000) =>
  waitFor(async () => { const s = await execStatus(env, id); return ["SUCCEEDED", "FAILED", "CANCELLED"].includes(s) ? s : null; }, timeoutMs);

export async function tasks(env: Env, execId: string) {
  return (await env.sys.db.query("select * from task_executions where workflow_execution_id = $1 order by node_key", [execId])).rows;
}

export const node = (key: string, type = "core.noop", params: Record<string, unknown> = {}, extra: Record<string, unknown> = {}) =>
  ({ key, name: key, type, params, ...extra });
