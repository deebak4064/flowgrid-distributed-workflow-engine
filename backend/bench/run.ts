// Reproducible load tests against the real system: one scheduler process + N worker processes (node src/main.ts),
// real Postgres and Redis. Every number is computed afterwards from the timestamps Postgres recorded, never estimated.
//
//   node bench/run.ts [scenario-id ...]       (default: all scenarios)
//   env: BENCH_DB_URL, BENCH_REDIS_URL, BENCH_OUT (output directory)
import { spawn, execSync, type ChildProcess } from "node:child_process";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import pg from "pg";
import { migrate, sleep } from "../src/db.ts";
import { benchmarkFanout } from "../src/demo.ts";
import { publishDefinitions } from "../src/handlers.ts";
import { createSystem, type System } from "../src/system.ts";

const DB = process.env.BENCH_DB_URL ?? "postgres://flowgrid:flowgrid@127.0.0.1:5432/flowgrid_bench";
const REDIS = process.env.BENCH_REDIS_URL ?? "redis://127.0.0.1:6379/2";
const OUT = process.env.BENCH_OUT ?? path.join(import.meta.dirname, "..", "..", "benchmarks");
const MAIN = path.join(import.meta.dirname, "..", "src", "main.ts");

interface Scenario {
  id: string; group: string; label: string; workers: number; concurrency: number; tasks: number; width: number; durationMs: number;
  taskType?: string; strategy?: "FIFO" | "PRIORITY" | "EDF"; failureRate?: number; killWorkerAt?: number; notify?: boolean;
  mixedPriority?: boolean; openLoopSeconds?: number; queueFactor?: number;
}

// tasks = executions x (width + 2). Fan-out/fan-in DAG: start -> width parallel tasks -> join.
const S: Scenario[] = [
  { id: "scale-1k", group: "scale", label: "1,000 tasks", workers: 4, concurrency: 32, tasks: 1000, width: 8, durationMs: 10 },
  { id: "scale-10k", group: "scale", label: "10,000 tasks", workers: 4, concurrency: 32, tasks: 10_000, width: 8, durationMs: 10 },
  { id: "scale-50k", group: "scale", label: "50,000 tasks", workers: 4, concurrency: 32, tasks: 50_000, width: 8, durationMs: 10 },
  { id: "workers-1", group: "workers", label: "1 worker", workers: 1, concurrency: 16, tasks: 10_000, width: 8, durationMs: 50 },
  { id: "workers-2", group: "workers", label: "2 workers", workers: 2, concurrency: 16, tasks: 10_000, width: 8, durationMs: 50 },
  { id: "workers-4", group: "workers", label: "4 workers", workers: 4, concurrency: 16, tasks: 10_000, width: 8, durationMs: 50 },
  { id: "conc-4", group: "concurrency", label: "concurrency 4", workers: 2, concurrency: 4, tasks: 5000, width: 8, durationMs: 50 },
  { id: "conc-16", group: "concurrency", label: "concurrency 16", workers: 2, concurrency: 16, tasks: 10_000, width: 8, durationMs: 50 },
  { id: "conc-64", group: "concurrency", label: "concurrency 64", workers: 2, concurrency: 64, tasks: 10_000, width: 8, durationMs: 50 },
  { id: "dur-0", group: "duration", label: "0 ms (noop)", workers: 4, concurrency: 32, tasks: 10_000, width: 8, durationMs: 0, taskType: "core.noop" },
  { id: "dur-10", group: "duration", label: "10 ms", workers: 4, concurrency: 32, tasks: 10_000, width: 8, durationMs: 10 },
  { id: "dur-100", group: "duration", label: "100 ms", workers: 4, concurrency: 32, tasks: 10_000, width: 8, durationMs: 100 },
  // Saturated, mixed priorities and deadlines: how each strategy orders the same backlog.
  { id: "strategy-fifo", group: "strategy", label: "FIFO", workers: 1, concurrency: 8, tasks: 3000, width: 0, durationMs: 20, strategy: "FIFO", mixedPriority: true, queueFactor: 1 },
  { id: "strategy-priority", group: "strategy", label: "PRIORITY (+aging)", workers: 1, concurrency: 8, tasks: 3000, width: 0, durationMs: 20, strategy: "PRIORITY", mixedPriority: true, queueFactor: 1 },
  { id: "strategy-edf", group: "strategy", label: "EDF", workers: 1, concurrency: 8, tasks: 3000, width: 0, durationMs: 20, strategy: "EDF", mixedPriority: true, queueFactor: 1 },
  // Light, open-loop load: pure scheduling overhead when capacity is available.
  { id: "latency-light", group: "latency", label: "light load (open loop)", workers: 2, concurrency: 16, tasks: 2000, width: 8, durationMs: 10, openLoopSeconds: 20 },
  // Failure injection: 10% transient failures + a business side effect on every task + kill -9 of one worker.
  { id: "failure-recovery", group: "failure", label: "10% failures + kill -9", workers: 4, concurrency: 16, tasks: 10_000, width: 8, durationMs: 20,
    taskType: "core.notify", notify: true, failureRate: 0.1, killWorkerAt: 0.4 },
];

// ---------------------------------------------------------------- resource sampling (/proc)
const HZ = 100;
function procStat(pid: number): { cpu: number; rss: number } | null {
  try {
    const st = fs.readFileSync(`/proc/${pid}/stat`, "utf8").split(") ")[1].split(" ");
    const rss = Number(/VmRSS:\s+(\d+)/.exec(fs.readFileSync(`/proc/${pid}/status`, "utf8"))?.[1] ?? 0) * 1024;
    return { cpu: (Number(st[11]) + Number(st[12])) / HZ, rss };
  } catch { return null; }
}
const pgrep = (name: string) => { try { return execSync(`pgrep -x ${name}`).toString().trim().split("\n").map(Number); } catch { return []; } };

class Sampler {
  samples: { t: number; group: string; cpuPct: number; rssMb: number }[] = [];
  private last = new Map<number, { cpu: number; t: number }>();
  private timer?: NodeJS.Timeout;
  start(groups: () => Record<string, number[]>) {
    this.timer = setInterval(() => {
      const now = Date.now();
      for (const [group, pids] of Object.entries(groups())) {
        let cpuPct = 0, rss = 0;
        for (const pid of pids) {
          const s = procStat(pid);
          if (!s) continue;
          const prev = this.last.get(pid);
          if (prev) cpuPct += ((s.cpu - prev.cpu) / ((now - prev.t) / 1000)) * 100;
          this.last.set(pid, { cpu: s.cpu, t: now });
          rss += s.rss;
        }
        this.samples.push({ t: now, group, cpuPct, rssMb: rss / 1048576 });
      }
    }, 1000);
  }
  stop() {
    clearInterval(this.timer);
    const out: Record<string, { avgCpuPct: number; peakCpuPct: number; peakRssMb: number }> = {};
    for (const g of new Set(this.samples.map((s) => s.group))) {
      const xs = this.samples.filter((s) => s.group === g).slice(1);
      out[g] = { avgCpuPct: r1(avg(xs.map((s) => s.cpuPct))), peakCpuPct: r1(Math.max(0, ...xs.map((s) => s.cpuPct))), peakRssMb: r1(Math.max(0, ...xs.map((s) => s.rssMb))) };
    }
    return out;
  }
}
const avg = (xs: number[]) => (xs.length ? xs.reduce((a, b) => a + b, 0) / xs.length : 0);
const r1 = (x: number) => Math.round(x * 10) / 10;

// ---------------------------------------------------------------- one scenario
async function run(sys: System, sc: Scenario, orgId: string) {
  console.log(`\n=== ${sc.id}: ${sc.label} (${sc.tasks} tasks, ${sc.workers}x${sc.concurrency}, ${sc.durationMs} ms) ===`);
  await sys.db.query(`truncate workflow_executions, task_executions, retry_attempts, dead_letter_tasks, execution_events, idempotency_records,
    worker_nodes, worker_heartbeats, notifications, workflows, workflow_versions, workflow_nodes, workflow_edges cascade`);
  await sys.redis.flushdb();
  await sys.settings.update(sc.strategy ?? "PRIORITY", 30);
  const type = sc.taskType ?? "core.sleep";
  const def = sc.width > 0 ? benchmarkFanout(sc.width, sc.durationMs, type)
    : { nodes: [{ key: "only", name: "only", type, params: { durationMs: sc.durationMs } }], edges: [] };
  if (sc.failureRate) for (const n of def.nodes) n.params = { ...n.params, chaos: { failRate: sc.failureRate } };
  for (const n of def.nodes) n.retry = { maxAttempts: 6, backoffMs: 50, maxBackoffMs: 2000 };
  const wf = (await sys.workflows.create(orgId, null, `bench-${sc.id}`, "", [], def)).id;
  const perExec = def.nodes.length, executions = Math.round(sc.tasks / perExec);

  const env = { ...process.env, FLOWGRID_DB_URL: DB, FLOWGRID_REDIS_URL: REDIS, FLOWGRID_LOG_LEVEL: "error", FLOWGRID_DEMO_ENABLED: "false",
    FLOWGRID_WORKER_TIMEOUT_MS: "3000", FLOWGRID_LEASE_MS: "5000", FLOWGRID_HEARTBEAT_MS: "1000", FLOWGRID_RECOVERY_EVERY_MS: "500",
    FLOWGRID_QUEUE_FACTOR: String(sc.queueFactor ?? 2), FLOWGRID_DB_POOL: "10" };
  const procs: { role: string; p: ChildProcess }[] = [];
  const spawnRole = (role: string, extra: Record<string, string> = {}) => {
    const p = spawn(process.execPath, [MAIN], { stdio: ["ignore", "ignore", "inherit"], env: { ...env, FLOWGRID_ROLE: role, ...extra } });
    procs.push({ role, p });
    return p;
  };
  spawnRole("scheduler");
  for (let i = 0; i < sc.workers; i++) spawnRole("worker", { FLOWGRID_WORKER_ID: `bench-w${i}`, FLOWGRID_WORKER_CONCURRENCY: String(sc.concurrency) });
  for (;;) {
    if (Number((await sys.db.query("select count(*) as n from worker_nodes where status = 'ACTIVE'")).rows[0].n) === sc.workers) break;
    await sleep(200);
  }
  const sampler = new Sampler();
  sampler.start(() => ({
    scheduler: procs.filter((x) => x.role === "scheduler").map((x) => x.p.pid!),
    workers: procs.filter((x) => x.role === "worker" && x.p.exitCode === null && !x.p.killed).map((x) => x.p.pid!),
    postgres: pgrep("postgres"), redis: pgrep("redis-server"),
  }));

  // Submit. Closed-loop scenarios pre-load the whole backlog (held back with a far-future runAt) and then release it at
  // T0, so the measured window is pure execution. The open-loop scenario submits at a steady rate instead.
  const t0Submit = Date.now();
  const rnd = (n: number) => Math.floor(Math.random() * n);
  const startOne = (hold: boolean) => sys.executions.start({ orgId, workflowId: wf, trigger: "BENCHMARK",
    runAt: hold ? new Date(Date.now() + 3_600_000) : null,
    priority: sc.mixedPriority ? 1 + rnd(9) : null, deadlineSeconds: sc.mixedPriority ? 5 + rnd(120) : null });
  let T0: number;
  if (sc.openLoopSeconds) {
    T0 = Date.now();
    const gap = (sc.openLoopSeconds * 1000) / executions;
    for (let i = 0; i < executions; i++) { await startOne(false); const due = T0 + (i + 1) * gap; if (due > Date.now()) await sleep(due - Date.now()); }
  } else {
    for (let i = 0; i < executions; i += 25) await Promise.all(Array.from({ length: Math.min(25, executions - i) }, () => startOne(true)));
    const rel = await sys.db.query("update task_executions set not_before = now() where status = 'READY' returning now() as t");
    T0 = new Date(rel.rows[0].t).getTime();
    await sys.db.query("update workflow_executions set run_at = to_timestamp($1 / 1000.0)", [T0]);
  }
  const submitMs = Date.now() - t0Submit;
  console.log(`submitted ${executions} executions (${executions * perExec} tasks) in ${submitMs} ms`);

  let killedAt: number | null = null;
  const total = executions * perExec;
  let lastProgress = { n: -1, at: Date.now() };
  for (;;) {
    const progress = Number((await sys.db.query("select count(*) as n from retry_attempts where finished_at is not null")).rows[0].n);
    if (progress !== lastProgress.n) lastProgress = { n: progress, at: Date.now() };
    else if (Date.now() - lastProgress.at > 60_000) throw new Error(`no progress for 60 s (${progress} attempts finished)`);
    const done = Number((await sys.db.query("select count(*) as n from workflow_executions where status in ('SUCCEEDED', 'FAILED', 'CANCELLED')")).rows[0].n);
    if (sc.killWorkerAt && killedAt === null) {
      const settled = Number((await sys.db.query("select count(*) as n from task_executions where status in ('SUCCEEDED', 'SKIPPED', 'FAILED')")).rows[0].n);
      if (settled >= total * sc.killWorkerAt) {
        const victim = procs.find((x) => x.role === "worker")!;
        victim.p.kill("SIGKILL");
        killedAt = Date.now();
        console.log(`kill -9 worker ${victim.p.pid} at ${settled}/${total} tasks`);
      }
    }
    if (done === executions) break;
    if (Date.now() - T0 > 15 * 60_000) throw new Error("scenario timed out");
    await sleep(250);
  }
  const resources = sampler.stop();
  for (const x of procs) x.p.kill("SIGTERM");
  await sleep(500);
  for (const x of procs) if (x.p.exitCode === null) x.p.kill("SIGKILL");

  const result = await analyze(sys.db, sc, { T0, killedAt, submitMs, executions, perExec });
  return { ...result, resources };
}

// ---------------------------------------------------------------- analysis (all from Postgres timestamps)
async function analyze(db: pg.Pool, sc: Scenario, x: { T0: number; killedAt: number | null; submitMs: number; executions: number; perExec: number }) {
  const one = async (sql: string, args: unknown[] = []) => (await db.query(sql, args)).rows[0];
  const pct = (expr: string) => `percentile_cont(0.5) within group (order by ${expr}) as p50, percentile_cont(0.95) within group (order by ${expr}) as p95,
    percentile_cont(0.99) within group (order by ${expr}) as p99, avg(${expr}) as avg, max(${expr}) as max`;
  const round = (o: Record<string, number | null>) => Object.fromEntries(Object.entries(o).map(([k, v]) => [k, v == null ? null : Math.round(Number(v) * 10) / 10]));
  const ms = (a: string, b: string) => `extract(epoch from (${a} - ${b})) * 1000`;

  const counts = await one(`select count(*) filter (where status = 'SUCCEEDED') as succeeded, count(*) filter (where status = 'FAILED') as failed,
      extract(epoch from (max(finished_at) - to_timestamp($1 / 1000.0))) * 1000 as makespan_ms from workflow_executions`, [x.T0]);
  const tasks = await one(`select count(*) filter (where status = 'SUCCEEDED') as succeeded, count(*) filter (where status = 'FAILED') as failed,
      count(*) filter (where attempt > 1) as retried, count(*) filter (where attempt > 1 and status = 'SUCCEEDED') as retried_ok from task_executions`);
  const attempts = await one(`select count(*) as total, count(*) filter (where status = 'SUCCEEDED') as ok, count(*) filter (where status in ('FAILED', 'TIMED_OUT')) as failed,
      count(*) filter (where status = 'LOST') as lost, sum(duration_ms) filter (where started_at is not null) as busy_ms from retry_attempts`);
  const window = await one(`select extract(epoch from (max(finished_at) - to_timestamp($1 / 1000.0))) * 1000 as ms from retry_attempts where status = 'SUCCEEDED'`, [x.T0]);
  const windowS = Number(window.ms) / 1000;
  const exec = round(await one(`select ${pct("duration_ms")} from retry_attempts where status = 'SUCCEEDED'`));
  const sched = round(await one(`select ${pct(ms("queued_at", "greatest(ready_at, not_before)"))} from task_executions where attempt = 1 and queued_at is not null`));
  const qwait = round(await one(`select ${pct(ms("started_at", "queued_at"))} from retry_attempts where started_at is not null`));
  const e2eTask = round(await one(`select ${pct(ms("finished_at", "greatest(ready_at, not_before)"))} from task_executions where status = 'SUCCEEDED' and ready_at is not null`));
  const wfDur = round(await one(`select ${pct(ms("finished_at", "greatest(created_at, run_at)"))} from workflow_executions where status = 'SUCCEEDED'`));
  const capacity = sc.workers * sc.concurrency;
  // Peak simultaneous running attempts (sweep line over [started, finished]).
  const peak = await one(`with e as (select started_at as t, 1 as d from retry_attempts where started_at is not null and finished_at is not null
      union all select finished_at, -1 from retry_attempts where started_at is not null and finished_at is not null)
    select max(running) as peak from (select sum(d) over (order by t, d rows unbounded preceding) as running from e) s`);
  const perWorker = (await db.query("select worker_id, count(*) as n from retry_attempts where status = 'SUCCEEDED' group by 1 order by 1")).rows;
  const doubleSuccess = (await one(`select count(*) as n from (select task_execution_id from retry_attempts where status = 'SUCCEEDED' group by 1 having count(*) > 1) d`)).n;
  const fanIn = await one(`select count(*) filter (where n <> 1) as bad from (select workflow_execution_id, count(*) as n from execution_events
      where type = 'WORKFLOW_SUCCEEDED' group by 1) d`);

  const out: Record<string, unknown> = {
    id: sc.id, group: sc.group, label: sc.label,
    config: { workers: sc.workers, concurrencyPerWorker: sc.concurrency, totalSlots: capacity, taskType: sc.taskType ?? "core.sleep", taskDurationMs: sc.durationMs,
      dag: sc.width > 0 ? `fan-out/fan-in, width ${sc.width} (${x.perExec} tasks per workflow)` : "single task", executions: x.executions, tasks: x.executions * x.perExec,
      strategy: sc.strategy ?? "PRIORITY", queueFactor: sc.queueFactor ?? 2, injectedFailureRate: sc.failureRate ?? 0, openLoopSeconds: sc.openLoopSeconds ?? null },
    submission: { ms: x.submitMs },
    outcome: { workflowsSucceeded: counts.succeeded, workflowsFailed: counts.failed, tasksSucceeded: tasks.succeeded, tasksFailed: tasks.failed, attempts: attempts.total },
    throughput: { tasksPerSec: r1(tasks.succeeded / windowS), workflowsPerSec: r1(counts.succeeded / windowS), windowMs: Math.round(Number(window.ms)), makespanMs: Math.round(Number(counts.makespan_ms)) },
    latencyMs: { taskExecution: exec, schedulingDelay: sched, queueWait: qwait, taskEndToEnd: e2eTask, workflowCompletion: wfDur },
    workers: { utilization: r1((Number(attempts.busy_ms) / (capacity * Number(window.ms))) * 1000) / 10, peakParallelTasks: peak.peak, perWorkerTasks: perWorker },
    correctness: { tasksSucceededMoreThanOnce: doubleSuccess, workflowsFinalizedOtherThanOnce: fanIn.bad },
  };
  if (sc.failureRate || sc.killWorkerAt) {
    out.failures = { failedAttempts: attempts.failed, lostAttempts: attempts.lost, tasksRetried: tasks.retried,
      retrySuccessRate: tasks.retried ? r1((tasks.retried_ok / tasks.retried) * 1000) / 10 : null, deadLettered: tasks.failed };
  }
  if (sc.notify) {
    const e = await one(`select count(*) as rows, count(distinct idempotency_key) as keys from notifications`);
    const effectsAttempted = await one(`select count(*) as n from retry_attempts a join task_executions t on t.id = a.task_execution_id where a.status in ('SUCCEEDED', 'FAILED', 'LOST')`);
    out.businessEffects = { notificationRows: e.rows, distinctLogicalOperations: e.keys, duplicateRows: e.rows - e.keys,
      duplicateBusinessEffectRate: e.keys ? (e.rows - e.keys) / e.keys : 0, handlerAttempts: effectsAttempted.n };
  }
  if (x.killedAt) {
    const r = await one(`select min(a.finished_at) as first_reaped, max(a.finished_at) as last_reaped, count(*) as lost from retry_attempts a where a.status = 'LOST'`);
    const rec = await one(`select max(b.finished_at) as recovered from retry_attempts a join retry_attempts b on b.task_execution_id = a.task_execution_id and b.status = 'SUCCEEDED'
      where a.status = 'LOST'`);
    out.recovery = { killedAt: new Date(x.killedAt).toISOString(), orphanedAttempts: r.lost,
      detectionAndReapMs: r.last_reaped ? new Date(r.last_reaped).getTime() - x.killedAt : null,
      allOrphanedTasksCompletedMs: rec.recovered ? new Date(rec.recovered).getTime() - x.killedAt : null };
  }
  if (sc.mixedPriority) {
    out.byPriority = (await db.query(`select priority, count(*) as tasks, ${pct(ms("started_at", "greatest(ready_at, not_before)"))}
      from task_executions where status = 'SUCCEEDED' group by 1 order by 1`)).rows.map((r) => ({ priority: r.priority, tasks: r.tasks, waitMs: round({ p50: r.p50, p95: r.p95, max: r.max }) }));
    const dl = await one("select count(*) filter (where finished_at > deadline_at) as missed, count(*) as total from task_executions where deadline_at is not null and status = 'SUCCEEDED'");
    out.deadlines = { missed: dl.missed, total: dl.total, missRate: dl.total ? Math.round((dl.missed / dl.total) * 10_000) / 10_000 : null };
  }
  console.log(JSON.stringify({ tasksPerSec: (out.throughput as any).tasksPerSec, p95exec: exec.p95, p95sched: sched.p95, util: (out.workers as any).utilization }));
  return out;
}

function environment() {
  const cpu = os.cpus();
  const v = (cmd: string) => { try { return execSync(cmd).toString().trim(); } catch { return "unknown"; } };
  return { cpuModel: cpu[0]?.model, logicalCpus: cpu.length, memoryGb: Math.round(os.totalmem() / 1073741824 * 10) / 10, os: `${os.type()} ${os.release()} (${os.arch()})`,
    node: process.version, postgres: v("psql --version"), redis: v("redis-server --version | cut -d' ' -f3"),
    topology: "everything on one machine: Postgres, Redis, scheduler process, worker processes and the load generator share the same CPUs",
    postgresConfig: "shared_buffers=512MB, synchronous_commit=on, fsync=on (defaults otherwise)", redisConfig: "appendonly yes (everysec)" };
}

// ---------------------------------------------------------------- main
const wanted = process.argv.slice(2);
const scenarios = wanted.length ? S.filter((s) => wanted.includes(s.id) || wanted.includes(s.group)) : S;
const admin = new pg.Client({ connectionString: DB.replace(/\/[^/]+$/, "/postgres") });
await admin.connect();
if (!(await admin.query("select 1 from pg_database where datname = 'flowgrid_bench'")).rows.length) await admin.query("create database flowgrid_bench");
await admin.end();
const sys = createSystem({ dbUrl: DB, redisUrl: REDIS, dbPool: 25 });
await migrate(sys.db);
await publishDefinitions(sys.db);
let orgId = (await sys.db.query("select id from organizations where slug = 'bench'")).rows[0]?.id;
if (!orgId) orgId = (await sys.db.query("insert into organizations(name, slug) values ('Benchmark', 'bench') returning id")).rows[0].id;

fs.mkdirSync(OUT, { recursive: true });
const file = path.join(OUT, "benchmark_results.json");
const prev = fs.existsSync(file) ? JSON.parse(fs.readFileSync(file, "utf8")) : { scenarios: [] };
const started = new Date().toISOString();
const results = [...prev.scenarios.filter((s: { id: string }) => !scenarios.some((x) => x.id === s.id))];
for (const sc of scenarios) {
  const t = Date.now();
  const r = await run(sys, sc, orgId);
  results.push({ ...r, testDurationMs: Date.now() - t, ranAt: new Date(t).toISOString() });
  results.sort((a, b) => S.findIndex((s) => s.id === a.id) - S.findIndex((s) => s.id === b.id));
  fs.writeFileSync(file, JSON.stringify({ generatedAt: new Date().toISOString(), lastRunStartedAt: started, environment: environment(), scenarios: results }, null, 2));
}
await sys.close();
console.log(`\nwrote ${file}`);
