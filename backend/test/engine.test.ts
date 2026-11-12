// Integration tests of the execution engine against real Postgres + Redis: DAG execution, branching, parallelism,
// scheduling strategies, retries, dead letters, idempotency, duplicate delivery, cancellation, versioning, consistency.
import { afterEach, beforeEach, describe, expect, it } from "vitest";
import { sleep } from "../src/db.ts";
import { STREAM } from "../src/queue.ts";
import { Scheduler } from "../src/scheduler.ts";
import { execStatus, FAST_SCHED, node, setup, startScheduler, startWorker, tasks, teardown, waitFor, waitTerminal, workflow, type Env } from "./helpers.ts";
import { benchmarkFanout } from "../src/demo.ts";

let env: Env;
beforeEach(async () => { env = await setup(); });
afterEach(async () => { await teardown(env); });

const start = (wf: string, extra: Record<string, unknown> = {}) => env.sys.executions.start({ orgId: env.orgId, workflowId: wf, ...extra }).then((s) => s.executionId);
const fakeWorker = (capacity = 100) => env.sys.db.query(`insert into worker_nodes(id, hostname, concurrency, status, last_heartbeat_at)
  values ('fake', 'test', $1, 'ACTIVE', now() + interval '1 hour')`, [capacity]);

describe("DAG execution", () => {
  it("runs tasks in dependency order and executes independent branches in parallel", async () => {
    const wf = await workflow(env, "diamond", benchmarkFanout(3, 400));
    await startWorker(env, { concurrency: 4 });
    await startScheduler(env);
    const id = await start(wf);
    expect(await waitTerminal(env, id)).toBe("SUCCEEDED");
    const t = Object.fromEntries((await tasks(env, id)).map((x) => [x.node_key, x]));
    for (const b of ["t0", "t1", "t2"]) {
      expect(t[b].started_at >= t.start.finished_at).toBe(true); // dependency respected
      expect(t.join.started_at >= t[b].finished_at).toBe(true);
    }
    // All three 400 ms branches overlapped: each started before the others finished.
    const latestStart = Math.max(...["t0", "t1", "t2"].map((b) => t[b].started_at.getTime()));
    const earliestEnd = Math.min(...["t0", "t1", "t2"].map((b) => t[b].finished_at.getTime()));
    expect(latestStart).toBeLessThan(earliestEnd);
    const e = (await env.sys.db.query("select remaining_tasks from workflow_executions where id = $1", [id])).rows[0];
    expect(e.remaining_tasks).toBe(0);
  });

  it("follows only the branch whose condition holds and joins with ANY_SUCCESS", async () => {
    const wf = await workflow(env, "branch", { nodes: [node("decide"), node("yes"), node("no"), node("join", "core.noop", {}, { trigger: "ANY_SUCCESS" })],
      edges: [{ from: "decide", to: "yes", when: { path: "ok", op: "EQ", value: true } }, { from: "decide", to: "no", when: { path: "ok", op: "EQ", value: false } },
        { from: "yes", to: "join" }, { from: "no", to: "join" }] });
    await startWorker(env);
    await startScheduler(env);
    const id = await start(wf);
    expect(await waitTerminal(env, id)).toBe("SUCCEEDED");
    const s = Object.fromEntries((await tasks(env, id)).map((x) => [x.node_key, x.status]));
    expect(s).toEqual({ decide: "SUCCEEDED", yes: "SUCCEEDED", no: "SKIPPED", join: "SUCCEEDED" });
  });

  it("passes upstream outputs to downstream tasks (claims workflow end to end)", async () => {
    const { claimsDefinition } = await import("../src/demo.ts");
    await env.sys.db.query(`insert into customers(id, org_id, name, tier, policy_number, policy_limit, deductible, customer_since, prior_claims)
      select format('CUST-%s', lpad(g::text, 4, '0')), $1, 'C', 'GOLD', 'P', 50000, 500, current_date - 2000, 0 from generate_series(1, 500) g`, [env.orgId]);
    const def = claimsDefinition();
    def.nodes.forEach((n) => { n.params = {}; }); // no injected chaos in this test
    const wf = await workflow(env, "claims", def);
    await startWorker(env, { concurrency: 8 });
    await startScheduler(env);
    const ids = await Promise.all(Array.from({ length: 5 }, () => start(wf, { input: { synthetic: true } })));
    for (const id of ids) expect(await waitTerminal(env, id)).toBe("SUCCEEDED");
    const d = (await env.sys.db.query("select count(*) as n from claim_decisions where org_id = $1", [env.orgId])).rows[0].n;
    expect(d).toBe(5); // exactly one decision per claim, whichever branch was taken
  });
});

describe("scheduling strategies", () => {
  async function dispatchOrder(strategy: string, starts: Record<string, unknown>[]) {
    await env.sys.settings.update(strategy);
    const wf = await workflow(env, `s-${strategy}`, { nodes: [node("only")] });
    const ids: string[] = [];
    for (const s of starts) { ids.push(await start(wf, s)); await sleep(15); } // distinct ready_at
    const sched = new Scheduler(env.sys.db, env.sys.store, env.sys.queue, env.sys.schedules, env.sys.settings, { ...FAST_SCHED, maxBatch: 1 });
    for (let i = 0; i < ids.length; i++) await sched.dispatchOnce();
    const msgs = await env.sys.redis.xrange(STREAM, "-", "+");
    return msgs.map(([, f]) => ids.indexOf(f[f.indexOf("w") + 1]));
  }
  beforeEach(async () => { await fakeWorker(); });

  it("PRIORITY dispatches the highest effective priority first", async () => {
    expect(await dispatchOrder("PRIORITY", [{ priority: 1 }, { priority: 9 }, { priority: 5 }])).toEqual([1, 2, 0]);
  });
  it("FIFO dispatches in readiness order regardless of priority", async () => {
    expect(await dispatchOrder("FIFO", [{ priority: 1 }, { priority: 9 }, { priority: 5 }])).toEqual([0, 1, 2]);
  });
  it("EDF dispatches the earliest deadline first", async () => {
    expect(await dispatchOrder("EDF", [{ deadlineSeconds: 300 }, { deadlineSeconds: 100 }, { deadlineSeconds: 200 }])).toEqual([1, 2, 0]);
  });
  it("aging raises the effective priority of waiting tasks (anti-starvation)", async () => {
    const wf = await workflow(env, "aging", { nodes: [node("only")] });
    const id = await start(wf, { priority: 1 });
    await env.sys.db.query("update task_executions set ready_at = now() - interval '5 seconds' where workflow_execution_id = $1", [id]);
    expect(await env.sys.store.ageRunnable(1)).toBe(1);
    expect((await tasks(env, id))[0].effective_priority).toBe(6); // 1 + floor(5s / 1s)
  });
  it("dispatch is event-driven: runnable tasks are dispatched without waiting for the poll interval", async () => {
    await env.sys.db.query("delete from worker_nodes where id = 'fake'");
    const wf = await workflow(env, "wake", { nodes: [node("a"), node("b"), node("c")], edges: [{ from: "a", to: "b" }, { from: "b", to: "c" }] });
    await startWorker(env);
    await startScheduler(env, { tickMs: 5000 }); // polling alone would need >= 10 s for three sequential steps
    await sleep(300);
    const t0 = Date.now();
    const id = await start(wf);
    expect(await waitTerminal(env, id, 4000)).toBe("SUCCEEDED");
    expect(Date.now() - t0).toBeLessThan(1500);
  });
  it("backpressure: never dispatches more than capacity x queueFactor", async () => {
    await env.sys.db.query("update worker_nodes set concurrency = 2");
    const wf = await workflow(env, "bp", benchmarkFanout(10, 10));
    await start(wf);
    await env.sys.db.query("update task_executions set status = 'READY', ready_at = now(), pending_deps = 0"); // make all 12 runnable
    const sched = new Scheduler(env.sys.db, env.sys.store, env.sys.queue, env.sys.schedules, env.sys.settings, { ...FAST_SCHED, queueFactor: 2 });
    expect(await sched.dispatchOnce()).toBe(4);
    expect(await sched.dispatchOnce()).toBe(0);
  });
});

describe("retries, dead letters and workflow retry", () => {
  it("retries transient failures with exponential backoff, then succeeds", async () => {
    const wf = await workflow(env, "flaky", { nodes: [node("f", "core.flaky", { failTimes: 2 }, { retry: { maxAttempts: 5, backoffMs: 200, maxBackoffMs: 5000 } })] });
    await startWorker(env);
    await startScheduler(env);
    const id = await start(wf);
    expect(await waitTerminal(env, id)).toBe("SUCCEEDED");
    const a = (await env.sys.db.query(`select a.* from retry_attempts a join task_executions t on t.id = a.task_execution_id
      where t.workflow_execution_id = $1 order by a.attempt`, [id])).rows;
    expect(a.map((x) => x.status)).toEqual(["FAILED", "FAILED", "SUCCEEDED"]);
    expect(a[0].backoff_ms).toBeGreaterThanOrEqual(100); expect(a[0].backoff_ms).toBeLessThanOrEqual(200);
    expect(a[1].backoff_ms).toBeGreaterThanOrEqual(200); expect(a[1].backoff_ms).toBeLessThanOrEqual(400);
    for (const i of [1, 2]) expect(a[i].queued_at.getTime() - a[i - 1].finished_at.getTime()).toBeGreaterThanOrEqual(a[i - 1].backoff_ms - 5);
  });

  it("dead-letters exhausted tasks, fails the workflow fast, and resumes from the failure point", async () => {
    const wf = await workflow(env, "dlq", { nodes: [node("a"), node("b", "core.flaky", { failTimes: 2 }, { retry: { maxAttempts: 2, backoffMs: 50 } }), node("c")],
      edges: [{ from: "a", to: "b" }, { from: "b", to: "c" }] });
    await startWorker(env);
    await startScheduler(env);
    const id = await start(wf);
    expect(await waitTerminal(env, id)).toBe("FAILED");
    let t = Object.fromEntries((await tasks(env, id)).map((x) => [x.node_key, x]));
    expect([t.a.status, t.b.status, t.c.status]).toEqual(["SUCCEEDED", "FAILED", "CANCELLED"]);
    const dl = (await env.sys.db.query("select * from dead_letter_tasks where workflow_execution_id = $1", [id])).rows;
    expect(dl).toHaveLength(1);
    expect(dl[0]).toMatchObject({ reason: "RETRIES_EXHAUSTED", attempts: 2, node_key: "b" });
    await env.sys.executions.replayDeadLetter(env.orgId, dl[0].id);
    expect(await waitTerminal(env, id)).toBe("SUCCEEDED");
    t = Object.fromEntries((await tasks(env, id)).map((x) => [x.node_key, x]));
    expect(t.a.attempt).toBe(1); // succeeded work is kept, not re-run
    expect(t.b.attempt).toBe(3);
    expect((await env.sys.db.query("select resolution from dead_letter_tasks where id = $1", [dl[0].id])).rows[0].resolution).toBe("REPLAYED");
  });

  it("sends non-retryable failures straight to the dead-letter table", async () => {
    const wf = await workflow(env, "perm", { nodes: [node("p", "core.flaky", { permanent: true }, { retry: { maxAttempts: 5 } })] });
    await startWorker(env);
    await startScheduler(env);
    const id = await start(wf);
    expect(await waitTerminal(env, id)).toBe("FAILED");
    const dl = (await env.sys.db.query("select reason, attempts from dead_letter_tasks where workflow_execution_id = $1", [id])).rows[0];
    expect(dl).toEqual({ reason: "NON_RETRYABLE", attempts: 1 });
  });

  it("times out a hung task and records TIMED_OUT", async () => {
    const wf = await workflow(env, "slow", { nodes: [node("s", "core.sleep", { durationMs: 5000 }, { timeoutMs: 300, retry: { maxAttempts: 1 } })] });
    await startWorker(env);
    await startScheduler(env);
    const id = await start(wf);
    expect(await waitTerminal(env, id)).toBe("FAILED");
    const a = (await env.sys.db.query(`select a.status, a.error_type from retry_attempts a join task_executions t on t.id = a.task_execution_id where t.workflow_execution_id = $1`, [id])).rows;
    expect(a).toEqual([{ status: "TIMED_OUT", error_type: "TIMEOUT" }]);
  });
});

describe("delivery guarantees", () => {
  it("idempotent processing: a crash after the side effect never duplicates the business record", async () => {
    const wf = await workflow(env, "notify", { nodes: [node("n", "core.notify", { chaos: { crashAfterEffectTimes: 2 } }, { retry: { maxAttempts: 5, backoffMs: 20 } })] });
    await startWorker(env);
    await startScheduler(env);
    const id = await start(wf);
    expect(await waitTerminal(env, id)).toBe("SUCCEEDED");
    expect((await tasks(env, id))[0].attempt).toBe(3); // the effect "ran" three times...
    const n = (await env.sys.db.query("select count(*) as n from notifications where idempotency_key = $1", [`${id}:n`])).rows[0].n;
    expect(n).toBe(1); // ...but exactly one notification exists
  });

  it("drops duplicate stream deliveries via the ownership check", async () => {
    const wf = await workflow(env, "dup", { nodes: [node("only", "core.sleep", { durationMs: 200 })] });
    const id = await start(wf);
    await fakeWorker();
    const sched = new Scheduler(env.sys.db, env.sys.store, env.sys.queue, env.sys.schedules, env.sys.settings, FAST_SCHED);
    expect(await sched.dispatchOnce()).toBe(1);
    const [[, fields]] = await env.sys.redis.xrange(STREAM, "-", "+");
    await env.sys.redis.xadd(STREAM, "*", ...fields); // the same dispatch delivered twice more
    await env.sys.redis.xadd(STREAM, "*", ...fields);
    const w = await startWorker(env);
    expect(await waitTerminal(env, id)).toBe("SUCCEEDED");
    await waitFor(async () => w.duplicates >= 2);
    const attempts = (await env.sys.db.query(`select count(*) as n from retry_attempts a join task_executions t on t.id = a.task_execution_id where t.workflow_execution_id = $1`, [id])).rows[0].n;
    expect(attempts).toBe(1);
    const ok = (await env.sys.db.query("select count(*) as n from execution_events where workflow_execution_id = $1 and type = 'TASK_SUCCEEDED'", [id])).rows[0].n;
    expect(ok).toBe(1);
  });

  it("parks poison messages in the DLQ stream instead of redelivering them forever", async () => {
    await startWorker(env);
    await env.sys.redis.xadd(STREAM, "*", "t", "not-a-uuid", "garbage", "1");
    await waitFor(async () => (await env.sys.redis.xlen("fg:tasks:dlq")) === 1);
    const pending = (await env.sys.redis.xpending(STREAM, "fg-workers")) as [number];
    expect(pending[0]).toBe(0);
  });

  it("an idempotency key returns the same execution, even under concurrent submits", async () => {
    const wf = await workflow(env, "idem", { nodes: [node("only")] });
    const results = await Promise.all(Array.from({ length: 10 }, () => env.sys.executions.start({ orgId: env.orgId, workflowId: wf, idempotencyKey: "order-42" })));
    expect(new Set(results.map((r) => r.executionId)).size).toBe(1);
    expect(results.filter((r) => r.created)).toHaveLength(1);
  });

  it("fan-in under concurrent completions finalizes each workflow exactly once", async () => {
    const wf = await workflow(env, "fanin", benchmarkFanout(30, 0, "core.noop"));
    await startWorker(env, { concurrency: 16 });
    await startWorker(env, { concurrency: 16 });
    await startScheduler(env);
    const ids = await Promise.all(Array.from({ length: 6 }, () => start(wf)));
    for (const id of ids) expect(await waitTerminal(env, id)).toBe("SUCCEEDED");
    const r = (await env.sys.db.query(`select e.id, e.remaining_tasks,
        (select count(*) from execution_events v where v.workflow_execution_id = e.id and v.type = 'WORKFLOW_SUCCEEDED') as fin,
        (select count(*) from task_executions t where t.workflow_execution_id = e.id and t.status = 'SUCCEEDED') as ok,
        (select max(attempt) from task_executions t where t.workflow_execution_id = e.id) as max_attempt
      from workflow_executions e where e.id = any($1)`, [ids])).rows;
    for (const x of r) expect({ remaining: x.remaining_tasks, fin: x.fin, ok: x.ok, maxAttempt: x.max_attempt }).toEqual({ remaining: 0, fin: 1, ok: 32, maxAttempt: 1 });
  });
});

describe("lifecycle", () => {
  it("cancels a running execution and fences the in-flight result", async () => {
    const wf = await workflow(env, "cancel", { nodes: [node("long", "core.sleep", { durationMs: 4000 }), node("after")], edges: [{ from: "long", to: "after" }] });
    const w = await startWorker(env);
    await startScheduler(env);
    const id = await start(wf);
    await waitFor(async () => (await tasks(env, id)).find((t) => t.node_key === "long")?.status === "RUNNING");
    await env.sys.executions.cancel(env.orgId, id, "test");
    expect(await execStatus(env, id)).toBe("CANCELLED");
    await waitFor(async () => w.inFlightCount === 0, 5000); // heartbeat sees the lost lease and aborts the handler
    const s = Object.fromEntries((await tasks(env, id)).map((x) => [x.node_key, x.status]));
    expect(s).toEqual({ long: "CANCELLED", after: "CANCELLED" });
    const evs = (await env.sys.db.query("select type from execution_events where workflow_execution_id = $1", [id])).rows.map((r) => r.type);
    expect(evs).not.toContain("TASK_SUCCEEDED");
    await expect(env.sys.executions.cancel(env.orgId, id, "again")).rejects.toThrow(/already CANCELLED/);
  });

  it("keeps in-flight executions on their pinned workflow version", async () => {
    const wf = await workflow(env, "versioned", { nodes: [node("a", "core.sleep", { durationMs: 1500 })] });
    await startWorker(env);
    await startScheduler(env);
    const v1 = await start(wf);
    await waitFor(async () => (await tasks(env, v1))[0].status === "RUNNING");
    await env.sys.workflows.update(env.orgId, env.userId, wf, { definition: { nodes: [node("a"), node("b")], edges: [{ from: "a", to: "b" }] }, changeNote: "v2" });
    const v2 = await start(wf);
    expect(await waitTerminal(env, v1)).toBe("SUCCEEDED");
    expect(await waitTerminal(env, v2)).toBe("SUCCEEDED");
    const ver = async (id: string) => (await env.sys.db.query("select version from workflow_executions where id = $1", [id])).rows[0].version;
    expect([await ver(v1), (await tasks(env, v1)).length, (await tasks(env, v1))[0].task_type]).toEqual([1, 1, "core.sleep"]);
    expect([await ver(v2), (await tasks(env, v2)).length, (await tasks(env, v2))[0].task_type]).toEqual([2, 2, "core.noop"]);
  });

  it("delays an execution until runAt and supports rescheduling", async () => {
    const wf = await workflow(env, "delayed", { nodes: [node("only"), node("next")], edges: [{ from: "only", to: "next" }] });
    await startWorker(env);
    await startScheduler(env);
    const id = await start(wf, { runAt: new Date(Date.now() + 60_000) });
    await sleep(700);
    expect(await execStatus(env, id)).toBe("PENDING"); // not dispatched before its time
    const runAt = new Date(Date.now() + 300);
    await env.sys.executions.reschedule(env.orgId, id, runAt);
    expect(await waitTerminal(env, id)).toBe("SUCCEEDED");
    const t = Object.fromEntries((await tasks(env, id)).map((x) => [x.node_key, x]));
    expect(t.only.queued_at.getTime()).toBeGreaterThanOrEqual(runAt.getTime() - 5);
    expect(t.next.not_before).toBeNull(); // only roots carry the start time, so rescheduling moves the whole run
  });

  it("fires a recurring schedule exactly once per slot, even with two schedulers racing", async () => {
    const wf = await workflow(env, "cron", { nodes: [node("only")] });
    const job = await env.sys.schedules.create(env.orgId, env.userId, { workflowId: wf, name: "every minute", type: "CRON", cron: "* * * * *" });
    await env.sys.db.query("update scheduled_jobs set next_fire_at = now() - interval '1 second' where id = $1", [job.id]);
    const fired = await Promise.all([env.sys.schedules.fireDue(), env.sys.schedules.fireDue(), env.sys.schedules.fireDue()]);
    expect(fired.reduce((a, b) => a + b, 0)).toBe(1);
    const n = (await env.sys.db.query("select count(*) as n from workflow_executions where scheduled_job_id = $1", [job.id])).rows[0].n;
    expect(n).toBe(1);
    const next = (await env.sys.schedules.get(env.orgId, job.id)).nextFireAt!;
    expect(next.getTime()).toBeGreaterThan(Date.now());
  });
});
