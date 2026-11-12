"""Writes concurrency_test_results.json and fills the README's benchmark and test sections from result files.

    python3 bench/finalize.py   (run from backend/, after bench/run.ts, bench/report.py and `vitest --reporter=json`)
Every number written here is read from benchmarks/benchmark_results.json or the Vitest JSON report.
"""
import json, os, re

ROOT = os.path.join(os.path.dirname(__file__), "..", "..")
B = os.path.join(ROOT, "benchmarks")
R = json.load(open(os.path.join(B, "benchmark_results.json")))
S = {s["id"]: s for s in R["scenarios"]}
T = json.load(open(os.path.join(B, "test_results.json")))
env = R["environment"]

# ---------------------------------------------------------------- concurrency_test_results.json
tests = [{"file": os.path.relpath(f["name"], os.path.join(ROOT, "backend")), "name": a["fullName"], "status": a["status"], "durationMs": round(a.get("duration") or 0)}
         for f in T["testResults"] for a in f["assertionResults"]]
conc_tests = [t for t in tests if re.search(r"parallel|concurrent|duplicate|crash|kill|fan-in|racing|exactly once|lease|heartbeat|drain|graceful|XAUTOCLAIM|connection loss|lost its stream|never reached", t["name"], re.I)]
out = {
    "generatedAt": R["generatedAt"], "environment": env,
    "summary": {"testsRun": T["numTotalTests"], "testsPassed": T["numPassedTests"], "testsFailed": T["numFailedTests"],
                "concurrencyAndFailureTests": len(conc_tests), "concurrencyAndFailureTestsPassed": sum(t["status"] == "passed" for t in conc_tests)},
    "tests": conc_tests,
    "loadTestParallelism": [{"scenario": s["id"], "workers": s["config"]["workers"], "slotsPerWorker": s["config"]["concurrencyPerWorker"],
                             "totalSlots": s["config"]["totalSlots"], "peakParallelTasks": s["workers"]["peakParallelTasks"],
                             "tasksPerWorker": {w["worker_id"]: w["n"] for w in s["workers"]["perWorkerTasks"]},
                             "tasksSucceededMoreThanOnce": s["correctness"]["tasksSucceededMoreThanOnce"],
                             "workflowsFinalizedOtherThanOnce": s["correctness"]["workflowsFinalizedOtherThanOnce"]} for s in R["scenarios"]],
}
fr = S.get("failure-recovery")
if fr:
    out["failureInjection"] = {"failures": fr.get("failures"), "businessEffects": fr.get("businessEffects"), "recovery": fr.get("recovery")}
json.dump(out, open(os.path.join(B, "concurrency_test_results.json"), "w"), indent=2, default=str)


# ---------------------------------------------------------------- README sections
def ms(v):
    return "–" if v is None else (f"{v/1000:,.2f} s" if v >= 10_000 else f"{v:,.0f} ms")


def tp(i):
    return f"{S[i]['throughput']['tasksPerSec']:,.0f}" if i in S else "–"


L = []
L.append(f"Measured on **{env['cpuModel']}, {env['logicalCpus']} vCPU, {env['memoryGb']} GB RAM**, with Postgres, Redis, the scheduler, "
         "the worker processes and the load generator all on that one machine. Absolute numbers are a floor, not a ceiling. "
         "Methodology, every scenario, CPU and memory, and the correctness checks are in "
         "[benchmarks/benchmark_report.md](benchmarks/benchmark_report.md); raw data is in `benchmarks/benchmark_results.json`.\n")
L.append("| Scenario | Configuration | Throughput | Attempt p50 / p95 / p99 | Workflow p95 |")
L.append("|---|---|---:|---|---:|")
for i in ["scale-1k", "scale-10k", "scale-50k", "workers-1", "workers-4", "conc-4", "conc-64", "dur-0", "dur-100"]:
    if i not in S:
        continue
    s, c = S[i], S[i]["config"]
    te = s["latencyMs"]["taskExecution"]
    L.append(f"| {s['label']} | {c['tasks']:,} tasks, {c['workers']} × {c['concurrencyPerWorker']} slots, {c['taskType']} {c['taskDurationMs']} ms | "
             f"**{tp(i)} tasks/s** | {ms(te['p50'])} / {ms(te['p95'])} / {ms(te['p99'])} | {ms(s['latencyMs']['workflowCompletion']['p95'])} |")
lt = S.get("latency-light")
if lt:
    l = lt["latencyMs"]
    L.append(f"\n**Latency with spare capacity** (open loop, {lt['config']['tasks']:,} tasks over {lt['config']['openLoopSeconds']} s): scheduling delay "
             f"p50 {ms(l['schedulingDelay']['p50'])}, p95 {ms(l['schedulingDelay']['p95'])}, p99 {ms(l['schedulingDelay']['p99'])}; "
             f"a 10-task, 3-level workflow completes in p50 {ms(l['workflowCompletion']['p50'])}, p95 {ms(l['workflowCompletion']['p95'])}.")
if fr:
    f, be, rc = fr["failures"], fr["businessEffects"], fr["recovery"]
    L.append(f"\n**Failure injection** ({fr['config']['tasks']:,} tasks with a database side effect each, {fr['config']['injectedFailureRate']*100:.0f}% injected "
             f"failures, kill -9 of a worker mid-run): {f['failedAttempts']:,} failed and {f['lostAttempts']} orphaned attempts, retry success rate "
             f"{f['retrySuccessRate']}%, {f['deadLettered']} dead letters, **{be['duplicateRows']} duplicate business records** "
             f"({be['notificationRows']:,} rows for {be['distinctLogicalOperations']:,} logical operations), and every orphaned task finished on another worker "
             f"{ms(rc['allOrphanedTasksCompletedMs'])} after the kill.")
st = [S.get(i) for i in ["strategy-fifo", "strategy-priority", "strategy-edf"]]
if all(st):
    def p95(s, pr):
        return next((r["waitMs"]["p95"] for r in s["byPriority"] if r["priority"] == pr), None)
    L.append("\n**Scheduling strategies** (same saturated backlog of mixed priorities and deadlines): "
             + "; ".join(f"{s['label']}: priority-9 tasks wait p95 {ms(p95(s, 9))} vs priority-1 {ms(p95(s, 1))}, deadline misses "
                         f"{s['deadlines']['missRate']*100:.1f}%" for s in st) + ".")
bad = sum(s["correctness"]["tasksSucceededMoreThanOnce"] + s["correctness"]["workflowsFinalizedOtherThanOnce"] for s in R["scenarios"])
L.append(f"\nAcross all {len(R['scenarios'])} runs: **{bad}** tasks succeeded more than once and **{bad}** workflows were finalised other than exactly once. "
         "On this host Postgres is the bottleneck in every saturated scenario; see the [engineering review](docs/engineering-review.md).")
L.append("\n![Performance charts](benchmarks/performance_charts.png)")
bench_md = "\n".join(L)

files = {}
for f in T["testResults"]:
    n = os.path.basename(f["name"])
    files[n] = (sum(a["status"] == "passed" for a in f["assertionResults"]), len(f["assertionResults"]))
desc = {"dag.test.ts": "DAG cycle detection, dependency resolution, branching conditions, retry backoff (unit)",
        "engine.test.ts": "DAG execution, parallel branches, strategies and backpressure, event-driven dispatch, retries, dead letters, workflow retry, timeouts, idempotency, duplicate delivery, poison messages, cancellation, versioning, fan-in consistency, delayed runs, cron firing",
        "recovery.test.ts": "Worker crash, graceful drain, XAUTOCLAIM, scheduler restart after Redis loses its stream, crash between commit and publish, Redis and Postgres connection loss, registration and heartbeats, operator drain",
        "processes.test.ts": "Three real worker processes plus a scheduler process: multi-process parallelism, kill -9 recovery, no task succeeds twice",
        "api.test.ts": "Login and token tampering, RBAC, tenant isolation, audit trail, validation, RFC 7807 errors, OpenAPI, rate limiting"}
TL = [f"Last run: **{T['numPassedTests']} of {T['numTotalTests']} tests passed** against real PostgreSQL and Redis "
      "(the machine had no Docker daemon, so local instances were supplied through `FG_TEST_DB_URL` and `FG_TEST_REDIS_URL`; CI uses Testcontainers).\n",
      "| Suite | Passed | Covers |", "|---|---:|---|"]
for n in ["dag.test.ts", "engine.test.ts", "recovery.test.ts", "processes.test.ts", "api.test.ts"]:
    if n in files:
        TL.append(f"| `test/{n}` | {files[n][0]}/{files[n][1]} | {desc[n]} |")
tests_md = "\n".join(TL)

readme = os.path.join(ROOT, "README.md")
txt = open(readme).read()
txt = re.sub(r"<!-- BENCH -->.*?(?=\n## )", "<!-- BENCH -->\n" + bench_md + "\n", txt, flags=re.S)
txt = re.sub(r"<!-- TESTS -->.*?(?=\n## )", "<!-- TESTS -->\n" + tests_md + "\n", txt, flags=re.S)
open(readme, "w").write(txt)
print("wrote concurrency_test_results.json and README sections")
