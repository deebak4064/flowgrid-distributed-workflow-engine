// Real OS processes: one scheduler and three worker processes (node src/main.ts), exactly as deployed.
// Verifies tasks run on several independent processes truly in parallel, and that kill -9 of a worker loses nothing.
import { spawn, type ChildProcess } from "node:child_process";
import path from "node:path";
import { afterAll, beforeAll, describe, expect, inject, it } from "vitest";
import { sleep } from "../src/db.ts";
import { benchmarkFanout } from "../src/demo.ts";
import { setup, teardown, waitFor, workflow, type Env } from "./helpers.ts";

let env: Env;
const procs: ChildProcess[] = [];
const main = path.join(import.meta.dirname, "..", "src", "main.ts");

function spawnRole(role: string, extra: Record<string, string> = {}) {
  const p = spawn(process.execPath, [main], { stdio: ["ignore", "ignore", "pipe"], env: { ...process.env, FLOWGRID_ROLE: role,
    FLOWGRID_DB_URL: inject("dbUrl"), FLOWGRID_REDIS_URL: inject("redisUrl"), FLOWGRID_LOG_LEVEL: "warn",
    FLOWGRID_WORKER_TIMEOUT_MS: "1500", FLOWGRID_LEASE_MS: "2000", FLOWGRID_HEARTBEAT_MS: "300", FLOWGRID_RECOVERY_EVERY_MS: "200", FLOWGRID_TICK_MS: "20", ...extra } });
  procs.push(p);
  return p;
}

beforeAll(async () => { env = await setup(); });
afterAll(async () => { for (const p of procs) p.kill("SIGKILL"); await teardown(env); });

describe("multiple independent worker processes", () => {
  it("execute tasks in parallel across processes and survive kill -9 of one of them", async () => {
    const wf = await workflow(env, "fanout", benchmarkFanout(6, 300));
    spawnRole("scheduler");
    const workers = [0, 1, 2].map((i) => spawnRole("worker", { FLOWGRID_WORKER_ID: `proc-worker-${i}`, FLOWGRID_WORKER_CONCURRENCY: "4" }));
    await waitFor(async () => Number((await env.sys.db.query("select count(*) as n from worker_nodes where status = 'ACTIVE'")).rows[0].n) === 3, 20_000);

    const ids = await Promise.all(Array.from({ length: 12 }, () => env.sys.executions.start({ orgId: env.orgId, workflowId: wf }).then((s) => s.executionId)));
    await waitFor(async () => Number((await env.sys.db.query("select count(*) as n from task_executions where status = 'RUNNING' and lease_owner = 'proc-worker-1'")).rows[0].n) > 0);
    workers[1].kill("SIGKILL"); // a real crash: the process is gone, its leases are orphaned
    const killedAt = Date.now();
    const start = Date.now();

    await waitFor(async () => Number((await env.sys.db.query("select count(*) as n from workflow_executions where id = any($1) and status = 'SUCCEEDED'", [ids])).rows[0].n) === ids.length, 60_000, 100);
    // Without the dead-consumer takeover, messages read-but-unclaimed by the killed process waited out the 30 s visibility timeout.
    expect(Date.now() - start).toBeLessThan(15_000);
    const lost = (await env.sys.db.query("select max(finished_at) as t, count(*) as n from retry_attempts where status = 'LOST'")).rows[0];
    expect(lost.n).toBeGreaterThan(0);

    const a = (await env.sys.db.query(`select a.worker_id, a.started_at, a.finished_at from retry_attempts a where a.status = 'SUCCEEDED'`)).rows;
    expect(a).toHaveLength(12 * 8); // every task succeeded exactly once
    expect(new Set(a.map((x) => x.worker_id)).size).toBeGreaterThanOrEqual(2); // the killed worker may not have finished any
    const all = (await env.sys.db.query("select distinct worker_id from retry_attempts where worker_id is not null")).rows;
    expect(all.length).toBe(3);
    // Peak overlap of running intervals must exceed one process's concurrency (4): proof of multi-process parallelism.
    const edges = a.flatMap((x) => [[x.started_at.getTime(), 1], [x.finished_at.getTime(), -1]]).sort((p, q) => p[0] - q[0] || p[1] - q[1]);
    let cur = 0, peak = 0;
    for (const [, d] of edges) peak = Math.max(peak, (cur += d));
    expect(peak).toBeGreaterThan(4);
    const dup = (await env.sys.db.query(`select count(*) as n from (select task_execution_id from retry_attempts where status = 'SUCCEEDED' group by 1 having count(*) > 1) d`)).rows[0].n;
    expect(dup).toBe(0);
    expect(new Date(lost.t).getTime() - killedAt).toBeLessThan(5000); // orphaned leases reaped within seconds
    await sleep(100);
  });
});
