// Failure injection: worker crash, heartbeat expiry, scheduler restart with lost stream messages, crash between commit
// and publish, Postgres connection loss, Redis connection loss, graceful shutdown.
import { afterEach, beforeEach, describe, expect, it } from "vitest";
import { sleep } from "../src/db.ts";
import { GROUP, STREAM } from "../src/queue.ts";
import { Scheduler } from "../src/scheduler.ts";
import { FAST_SCHED, node, setup, startScheduler, startWorker, tasks, teardown, waitFor, waitTerminal, workflow, type Env } from "./helpers.ts";

let env: Env;
beforeEach(async () => { env = await setup(); });
afterEach(async () => { await teardown(env); });

const start = (wf: string) => env.sys.executions.start({ orgId: env.orgId, workflowId: wf }).then((s) => s.executionId);
const attempts = async (id: string) => (await env.sys.db.query(`select a.attempt, a.status, a.worker_id from retry_attempts a
  join task_executions t on t.id = a.task_execution_id where t.workflow_execution_id = $1 order by a.attempt`, [id])).rows;

describe("worker failures", () => {
  it("recovers a task from a crashed worker: heartbeat expiry -> DEAD -> lease reaped -> re-run elsewhere", async () => {
    const wf = await workflow(env, "crash", { nodes: [node("work", "core.sleep", { durationMs: 1500 })] });
    const a = await startWorker(env);
    await startScheduler(env);
    const id = await start(wf);
    await waitFor(async () => (await tasks(env, id))[0].status === "RUNNING");
    const crashedAt = Date.now();
    a.crash(); // like kill -9: no lease release, no status update, no ack
    const b = await startWorker(env);
    expect(await waitTerminal(env, id)).toBe("SUCCEEDED");
    const recoveryMs = Date.now() - crashedAt;
    const att = await attempts(id);
    expect(att.map((x) => [x.status, x.worker_id])).toEqual([["LOST", a.id], ["SUCCEEDED", b.id]]);
    expect((await env.sys.db.query("select status from worker_nodes where id = $1", [a.id])).rows[0].status).toBe("DEAD");
    // bound: worker timeout (1.5 s) + recovery tick + backoff + re-run (1.5 s)
    expect(recoveryMs).toBeLessThan(8000);
  });

  it("hands in-flight work back immediately on graceful shutdown", async () => {
    const wf = await workflow(env, "drain", { nodes: [node("work", "core.sleep", { durationMs: 3000 })] });
    const a = await startWorker(env, { shutdownGraceMs: 200 });
    await startScheduler(env);
    const id = await start(wf);
    await waitFor(async () => (await tasks(env, id))[0].status === "RUNNING");
    await a.stop();
    expect((await env.sys.db.query("select status from worker_nodes where id = $1", [a.id])).rows[0].status).toBe("STOPPED");
    expect((await tasks(env, id))[0].status).toBe("RETRYING"); // released without waiting for the lease to expire
    await startWorker(env);
    expect(await waitTerminal(env, id)).toBe("SUCCEEDED");
    expect((await attempts(id)).map((x) => x.status)).toEqual(["LOST", "SUCCEEDED"]);
  });

  it("re-delivers un-acked messages from a consumer that died before claiming (XAUTOCLAIM)", async () => {
    const wf = await workflow(env, "unacked", { nodes: [node("only")] });
    await env.sys.db.query(`insert into worker_nodes(id, hostname, concurrency, status, last_heartbeat_at) values ('fake', 't', 10, 'ACTIVE', now() + interval '1 hour')`);
    const id = await start(wf);
    await new Scheduler(env.sys.db, env.sys.store, env.sys.queue, env.sys.schedules, env.sys.settings, FAST_SCHED).dispatchOnce();
    await env.sys.queue.ensureGroup();
    await env.sys.redis.xreadgroup("GROUP", GROUP, "ghost-consumer", "COUNT", 10, "STREAMS", STREAM, ">"); // read, then "die"
    await startWorker(env, { visibilityTimeoutMs: 600 });
    expect(await waitTerminal(env, id)).toBe("SUCCEEDED");
    expect((await attempts(id)).map((x) => x.status)).toEqual(["SUCCEEDED"]);
  });
});

describe("scheduler and queue failures", () => {
  it("scheduler restart after Redis lost its stream: QUEUED tasks are re-published, nothing is lost", async () => {
    const wf = await workflow(env, "lost", { nodes: [node("a"), node("b")], edges: [{ from: "a", to: "b" }] });
    await env.sys.db.query(`insert into worker_nodes(id, hostname, concurrency, status, last_heartbeat_at) values ('fake', 't', 10, 'ACTIVE', now() + interval '1 hour')`);
    const ids = await Promise.all([start(wf), start(wf), start(wf)]);
    const crashed = new Scheduler(env.sys.db, env.sys.store, env.sys.queue, env.sys.schedules, env.sys.settings, FAST_SCHED);
    expect(await crashed.dispatchOnce()).toBe(3); // dispatched, then the scheduler "crashes"
    await env.sys.redis.del(STREAM); // ...and Redis restarts without persistence
    await env.sys.db.query("delete from worker_nodes where id = 'fake'");
    await sleep(1600); // older than redispatchAfterMs
    await startScheduler(env); // the restarted scheduler
    await startWorker(env);
    for (const id of ids) expect(await waitTerminal(env, id)).toBe("SUCCEEDED");
    for (const id of ids) expect((await attempts(id)).map((x) => x.status)).toEqual(["SUCCEEDED", "SUCCEEDED"]); // same attempt re-published, not re-run
  });

  it("publishes tasks that were committed as QUEUED but never reached Redis (crash between commit and XADD)", async () => {
    const wf = await workflow(env, "unpublished", { nodes: [node("only")] });
    const id = await start(wf);
    expect(await env.sys.store.claimForDispatch("PRIORITY", 10)).toHaveLength(1); // committed, but publish() never called
    expect((await tasks(env, id))[0].stream_message_id).toBeNull();
    await startScheduler(env);
    await startWorker(env);
    expect(await waitTerminal(env, id)).toBe("SUCCEEDED");
  });

  it("survives Redis connection loss mid-run (all client connections killed)", async () => {
    const wf = await workflow(env, "redis-kill", { nodes: [node("a", "core.sleep", { durationMs: 600 }), node("b")], edges: [{ from: "a", to: "b" }] });
    await startWorker(env);
    await startScheduler(env);
    const id = await start(wf);
    await waitFor(async () => (await tasks(env, id)).find((t) => t.node_key === "a")?.status === "RUNNING");
    await env.sys.redis.call("CLIENT", "KILL", "TYPE", "normal").catch(() => {}); // drops every client, ours included
    expect(await waitTerminal(env, id, 30_000)).toBe("SUCCEEDED");
    expect(await waitTerminal(env, await start(wf), 30_000)).toBe("SUCCEEDED"); // and new work still flows
  });

  it("survives Postgres connection loss mid-run (backends terminated)", async () => {
    const wf = await workflow(env, "pg-kill", { nodes: [node("a", "core.sleep", { durationMs: 800 }), node("b")], edges: [{ from: "a", to: "b" }] });
    await startWorker(env);
    await startScheduler(env);
    const id = await start(wf);
    await waitFor(async () => (await tasks(env, id)).find((t) => t.node_key === "a")?.status === "RUNNING");
    const killed = (await env.sys.db.query("select count(pg_terminate_backend(pid)) as n from pg_stat_activity where datname = current_database() and pid <> pg_backend_pid()")).rows[0].n;
    expect(killed).toBeGreaterThan(0);
    expect(await waitTerminal(env, id, 30_000)).toBe("SUCCEEDED");
    const t = await tasks(env, id);
    expect(t.every((x) => x.status === "SUCCEEDED")).toBe(true);
  });
});

describe("worker registration & heartbeats", () => {
  it("registers workers with their concurrency and records heartbeats", async () => {
    const w = await startWorker(env, { concurrency: 6 });
    await sleep(900);
    const row = (await env.sys.db.query("select status, concurrency, task_types from worker_nodes where id = $1", [w.id])).rows[0];
    expect(row.status).toBe("ACTIVE");
    expect(row.concurrency).toBe(6);
    expect(row.task_types).toContain("core.sleep");
    const hb = (await env.sys.db.query("select count(*) as n from worker_heartbeats where worker_id = $1", [w.id])).rows[0].n;
    expect(hb).toBeGreaterThanOrEqual(2);
  });

  it("an operator can drain a worker: it stops taking new tasks", async () => {
    const wf = await workflow(env, "drained", { nodes: [node("only")] });
    const w = await startWorker(env);
    await startScheduler(env);
    await env.sys.db.query("update worker_nodes set status = 'DRAINING' where id = $1", [w.id]);
    await sleep(700); // next heartbeat picks it up
    const id = await start(wf);
    await sleep(1000);
    expect((await tasks(env, id))[0].status).not.toBe("SUCCEEDED");
    await env.sys.db.query("update worker_nodes set status = 'ACTIVE' where id = $1", [w.id]);
    expect(await waitTerminal(env, id)).toBe("SUCCEEDED");
  });
});
