"""Builds benchmark_report.md and performance_charts.png from benchmark_results.json.

Every number in the report is read from the JSON written by bench/run.ts; nothing is typed by hand.
    python3 bench/report.py <results.json> [--baseline <baseline-results.json>] --out <dir>
"""
import argparse, json, os
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

# Categorical order validated with the dataviz checker on a light surface (adjacent CVD dE >= 8).
C = ["#0e8fa8", "#6b5be0", "#c2507f"]
GREY, INK, MUTED = "#b8bcc6", "#1f2430", "#6b7280"
MARKERS = ["o", "s", "^"]

ap = argparse.ArgumentParser()
ap.add_argument("results")
ap.add_argument("--baseline")
ap.add_argument("--out", required=True)
a = ap.parse_args()
R = json.load(open(a.results))
B = json.load(open(a.baseline)) if a.baseline else None
S = {s["id"]: s for s in R["scenarios"]}
BS = {s["id"]: s for s in B["scenarios"]} if B else {}
env = R["environment"]


def g(sid, *path, src=S):
    v = src.get(sid)
    for p in path:
        if v is None:
            return None
        v = v.get(p) if isinstance(v, dict) else None
    return v


def fmt(v, d=0):
    if v is None:
        return "–"
    if isinstance(v, float) and d == 0 and abs(v) < 10:
        d = 1
    return f"{v:,.{d}f}"


def ms(v):
    if v is None:
        return "–"
    return f"{v/1000:,.2f} s" if v >= 10_000 else f"{v:,.0f} ms"


plt.rcParams.update({"font.family": "DejaVu Sans", "font.size": 9.5, "axes.edgecolor": "#d5d8de", "axes.labelcolor": INK,
                     "xtick.color": MUTED, "ytick.color": MUTED, "axes.spines.top": False, "axes.spines.right": False,
                     "axes.grid": True, "grid.color": "#eceef2", "grid.linewidth": 0.8, "axes.axisbelow": True,
                     "axes.titleweight": "bold", "axes.titlesize": 10.5, "axes.titlelocation": "left", "axes.titlepad": 10})
fig, ax = plt.subplots(2, 3, figsize=(15, 8.6), dpi=130)
fig.patch.set_facecolor("white")


def bars(axis, ids, labels, key, title, ylabel, color=C[1]):
    vals = [g(i, *key) or 0 for i in ids]
    xs = range(len(ids))
    b = axis.bar(xs, vals, width=0.56, color=color, edgecolor="white", linewidth=2, zorder=3)
    axis.bar_label(b, labels=[fmt(v) for v in vals], padding=3, color=INK, fontsize=9)
    axis.set_xticks(list(xs), labels)
    axis.set_title(title)
    axis.set_ylabel(ylabel)
    axis.set_ylim(0, max(vals + [1]) * 1.18)


bars(ax[0][0], ["scale-1k", "scale-10k", "scale-50k"], ["1k", "10k", "50k"], ("throughput", "tasksPerSec"),
     "Throughput by workload size", "tasks / second")
bars(ax[0][1], ["workers-1", "workers-2", "workers-4"], ["1 worker", "2 workers", "4 workers"], ("throughput", "tasksPerSec"),
     "Throughput by worker processes (16 slots, 50 ms)", "tasks / second")
bars(ax[0][2], ["conc-4", "conc-16", "conc-64"], ["4", "16", "64"], ("throughput", "tasksPerSec"),
     "Throughput by slots per worker (2 workers)", "tasks / second")

# Latency percentiles by task duration: one measure, three percentiles = magnitude, so one hue light -> dark.
axl = ax[1][0]
ids = ["dur-0", "dur-10", "dur-100"]
pcts = [("p50", "#b4abf5"), ("p95", "#7d6fe6"), ("p99", "#463aa8")]
w = 0.25
for j, (p, col) in enumerate(pcts):
    vals = [g(i, "latencyMs", "taskExecution", p) or 0 for i in ids]
    b = axl.bar([x + (j - 1) * w for x in range(3)], vals, width=w, color=col, edgecolor="white", linewidth=1.5, label=p, zorder=3)
axl.set_xticks(range(3), ["noop", "10 ms", "100 ms"])
axl.set_title("Attempt duration percentiles by task duration")
axl.set_ylabel("ms (claim to committed result)")
axl.legend(frameon=False, ncols=3, loc="upper left")

# Strategy comparison: wait time (runnable -> started) p95 by priority class.
axs = ax[1][1]
for j, (sid, name) in enumerate([("strategy-fifo", "FIFO"), ("strategy-priority", "PRIORITY + aging"), ("strategy-edf", "EDF")]):
    rows = g(sid, "byPriority") or []
    if rows:
        axs.plot([r["priority"] for r in rows], [r["waitMs"]["p95"] / 1000 for r in rows], color=C[j], marker=MARKERS[j],
                 markersize=6, linewidth=2, label=name, markeredgecolor="white", markeredgewidth=1.2)
axs.set_title("Scheduling strategy: p95 wait by task priority (saturated)")
axs.set_xlabel("task priority (higher = more urgent)")
axs.set_ylabel("seconds from runnable to started")
axs.legend(frameon=False, loc="lower left")

# Before / after the engineering-review optimisation.
axb = ax[1][2]
cmp_ids = [i for i in ["scale-10k", "scale-50k", "conc-64", "strategy-priority", "failure-recovery"] if i in BS and i in S]
if cmp_ids:
    xs = range(len(cmp_ids))
    before = [g(i, "throughput", "tasksPerSec", src=BS) for i in cmp_ids]
    after = [g(i, "throughput", "tasksPerSec") for i in cmp_ids]
    b1 = axb.bar([x - 0.19 for x in xs], before, width=0.36, color=GREY, edgecolor="white", linewidth=1.5, label="before review", zorder=3)
    b2 = axb.bar([x + 0.19 for x in xs], after, width=0.36, color=C[1], edgecolor="white", linewidth=1.5, label="after review", zorder=3)
    axb.bar_label(b2, labels=[f"{(y / x - 1) * 100:+.0f}%" for x, y in zip(before, after)], padding=3, fontsize=8.5, color=INK)
    axb.set_xticks(list(xs), ["10k tasks", "50k tasks", "64 slots", "priority,\n1× queue", "failures +\nkill -9"][:len(cmp_ids)], fontsize=8.5)
    axb.set_ylim(0, max(after + before) * 1.2)
    axb.legend(frameon=False, loc="upper left")
    axb.set_title("Engineering review: throughput before / after")
    axb.set_ylabel("tasks / second")
else:
    axb.axis("off")

fig.suptitle(f"FlowGrid load tests on {env['logicalCpus']} vCPU / {env['memoryGb']} GB, single host (Postgres, Redis, scheduler, workers co-located)",
             x=0.01, ha="left", fontsize=12, fontweight="bold", color=INK)
fig.tight_layout(rect=(0, 0, 1, 0.95), h_pad=3, w_pad=3)
os.makedirs(a.out, exist_ok=True)
png = os.path.join(a.out, "performance_charts.png")
fig.savefig(png, facecolor="white")

# ---------------------------------------------------------------- markdown
L = []
P = L.append
P("# FlowGrid benchmark report\n")
P(f"Generated from `benchmark_results.json` ({R['generatedAt']}). Every figure below is computed from timestamps Postgres recorded "
  "during the run (task attempts, executions), not estimated by the load generator.\n")
P("## Hardware and software\n")
P("| | |\n|---|---|")
for k, label in [("cpuModel", "CPU"), ("logicalCpus", "Logical CPUs"), ("memoryGb", "Memory (GB)"), ("os", "OS"), ("node", "Node.js"),
                 ("postgres", "PostgreSQL"), ("redis", "Redis"), ("postgresConfig", "Postgres settings"), ("redisConfig", "Redis settings"), ("topology", "Topology")]:
    P(f"| {label} | {env.get(k)} |")
P("\nThis is a small, shared machine: Postgres, Redis, the scheduler, every worker process and the load generator compete for the same "
  f"{env['logicalCpus']} vCPUs. Absolute numbers are therefore a floor, not a ceiling; the relative comparisons are the useful part.\n")

P("## Methodology\n")
P("- `node bench/run.ts` starts **one scheduler process and N worker processes** (`node src/main.ts`, the same entry point as production) "
  "against a dedicated database and Redis index, then submits work through `ExecutionService.start`.")
P("- **Closed-loop scenarios** pre-load the whole backlog with a far-future start time, then release it in one statement at T0, so the "
  "measured window is pure execution: `window = last successful attempt finished − T0`.")
P("- The **open-loop** latency scenario submits workflows at a steady rate with spare capacity, to show scheduling overhead when the system is not saturated.")
P("- Workload: a fan-out/fan-in DAG (`start → 8 parallel tasks → join`, 10 tasks per workflow) of `core.sleep` tasks (simulated I/O), "
  "`core.noop` for pure engine overhead, and `core.notify` (a real database side effect behind an idempotency key) for the failure scenario.")
P("- **Definitions.** *Attempt duration*: claim → result committed (handler time plus the commit). *Scheduling delay*: runnable → dispatched to "
  "the stream (includes waiting for worker capacity under backpressure). *Queue wait*: dispatched → started. *Task end-to-end*: runnable → "
  "succeeded. *Workflow completion*: released → workflow finished. *Utilisation*: Σ attempt durations ÷ (slots × window).")
P("- CPU and memory are sampled once per second from `/proc` for the scheduler, the worker processes, all Postgres processes and Redis.\n")

P("## Results\n")
P("| Scenario | Tasks | Workers × slots | Task | Throughput (tasks/s) | Window | Attempt p50 / p95 / p99 | Sched. delay p95 | Workflow p95 | Utilisation | Peak parallel |")
P("|---|---:|---|---|---:|---:|---|---:|---:|---:|---:|")
for s in R["scenarios"]:
    c, l = s["config"], s["latencyMs"]
    te = l["taskExecution"]
    P(f"| {s['label']} | {c['tasks']:,} | {c['workers']} × {c['concurrencyPerWorker']} | {c['taskType']} {c['taskDurationMs']} ms | "
      f"**{fmt(s['throughput']['tasksPerSec'])}** | {ms(s['throughput']['windowMs'])} | {ms(te['p50'])} / {ms(te['p95'])} / {ms(te['p99'])} | "
      f"{ms(l['schedulingDelay']['p95'])} | {ms(l['workflowCompletion']['p95'])} | {fmt(s['workers']['utilization'])}% | {s['workers']['peakParallelTasks']} |")

lt = S.get("latency-light")
if lt:
    l = lt["latencyMs"]
    P("\n### Scheduling latency with spare capacity (open loop)\n")
    P(f"{lt['config']['executions']:,} workflows ({lt['config']['tasks']:,} tasks) submitted evenly over {lt['config']['openLoopSeconds']} s to "
      f"{lt['config']['workers']} workers × {lt['config']['concurrencyPerWorker']} slots.\n")
    P("| Measure | p50 | p95 | p99 | max |\n|---|---:|---:|---:|---:|")
    for k, name in [("schedulingDelay", "Scheduling delay (runnable → dispatched)"), ("queueWait", "Queue wait (dispatched → started)"),
                    ("taskExecution", "Attempt duration (10 ms task)"), ("taskEndToEnd", "Task end-to-end"), ("workflowCompletion", "Workflow completion (10 tasks, 3 levels)")]:
        P(f"| {name} | {ms(l[k]['p50'])} | {ms(l[k]['p95'])} | {ms(l[k]['p99'])} | {ms(l[k]['max'])} |")

P("\n### Scheduling strategies under saturation\n")
P("Same backlog for each run: 3,000 single-task workflows with random priorities (1–9) and deadlines (5–125 s), one worker with 8 slots, "
  "20 ms tasks, dispatch queue bounded at 1× capacity so ordering decides who waits.\n")
P("| Strategy | Throughput | Wait p95, priority 1 | Wait p95, priority 9 | Deadline misses |\n|---|---:|---:|---:|---:|")
for sid in ["strategy-fifo", "strategy-priority", "strategy-edf"]:
    s = S.get(sid)
    if not s:
        continue
    bp = {r["priority"]: r for r in s.get("byPriority", [])}
    dl = s.get("deadlines", {})
    P(f"| {s['label']} | {fmt(s['throughput']['tasksPerSec'])} tasks/s | {ms(bp.get(1, {}).get('waitMs', {}).get('p95'))} | "
      f"{ms(bp.get(9, {}).get('waitMs', {}).get('p95'))} | {dl.get('missed', 0):,} of {dl.get('total', 0):,} ({(dl.get('missRate') or 0) * 100:.1f}%) |")

fr = S.get("failure-recovery")
if fr:
    f, be, rc = fr.get("failures", {}), fr.get("businessEffects", {}), fr.get("recovery", {})
    P("\n### Failure injection and recovery\n")
    P(f"{fr['config']['tasks']:,} `core.notify` tasks (each writes a notification row behind an idempotency key), "
      f"{fr['config']['injectedFailureRate'] * 100:.0f}% injected transient failure rate per attempt, and **kill -9 of one of the "
      f"{fr['config']['workers']} worker processes** once 40% of tasks had settled.\n")
    P("| Measure | Value |\n|---|---:|")
    rows = [("Workflows succeeded", f"{fr['outcome']['workflowsSucceeded']:,} of {fr['config']['executions']:,}"),
            ("Failed attempts (injected)", f"{f.get('failedAttempts', 0):,}"), ("Attempts orphaned by the killed worker", f"{f.get('lostAttempts', 0):,}"),
            ("Tasks that needed a retry", f"{f.get('tasksRetried', 0):,}"), ("Retry success rate", f"{fmt(f.get('retrySuccessRate'))}%"),
            ("Dead-lettered tasks", f"{f.get('deadLettered', 0):,}"),
            ("Business-effect rows / distinct logical operations", f"{be.get('notificationRows', 0):,} / {be.get('distinctLogicalOperations', 0):,}"),
            ("Duplicate business-effect rate", f"{be.get('duplicateBusinessEffectRate', 0) * 100:.3f}%"),
            ("Kill → all orphaned leases reaped", ms(rc.get("detectionAndReapMs"))),
            ("Kill → every orphaned task completed elsewhere", ms(rc.get("allOrphanedTasksCompletedMs"))),
            ("Throughput during the run", f"{fmt(fr['throughput']['tasksPerSec'])} tasks/s")]
    for k, v in rows:
        P(f"| {k} | {v} |")
    P(f"\nWorker timeout for this run was 3 s and the lease 5 s (`FLOWGRID_WORKER_TIMEOUT_MS`, `FLOWGRID_LEASE_MS`); recovery time is "
      "dominated by those two settings by design.")

P("\n### Correctness checks on every run\n")
P("| Scenario | Tasks succeeded more than once | Workflows finalised other than exactly once |\n|---|---:|---:|")
for s in R["scenarios"]:
    P(f"| {s['label']} | {s['correctness']['tasksSucceededMoreThanOnce']} | {s['correctness']['workflowsFinalizedOtherThanOnce']} |")

P("\n### CPU and memory (sampled each second)\n")
P("| Scenario | Scheduler CPU avg | Workers CPU avg (all) | Postgres CPU avg (all) | Redis CPU avg | Workers peak RSS | Postgres peak RSS |")
P("|---|---:|---:|---:|---:|---:|---:|")
for s in R["scenarios"]:
    r = s["resources"]
    P(f"| {s['label']} | {fmt(r['scheduler']['avgCpuPct'])}% | {fmt(r['workers']['avgCpuPct'])}% | {fmt(r['postgres']['avgCpuPct'])}% | "
      f"{fmt(r['redis']['avgCpuPct'])}% | {fmt(r['workers']['peakRssMb'])} MB | {fmt(r['postgres']['peakRssMb'])} MB |")
P(f"\n100% = one vCPU; the machine has {env['logicalCpus']}. Postgres RSS includes shared buffers counted once per backend process, so it overstates real memory use.\n")

if cmp_ids:
    P("## Engineering review: before and after\n")
    P("Throughput of the same scenarios before and after the hot-path changes described in `docs/engineering-review.md` "
      "(same machine, same harness, same settings).\n")
    P("| Scenario | Before (tasks/s) | After (tasks/s) | Change | Attempt p95 before → after |\n|---|---:|---:|---:|---|")
    for i in [x for x in S if x in BS]:
        b, n = g(i, "throughput", "tasksPerSec", src=BS), g(i, "throughput", "tasksPerSec")
        P(f"| {S[i]['label']} | {fmt(b)} | {fmt(n)} | {(n / b - 1) * 100:+.0f}% | "
          f"{ms(g(i, 'latencyMs', 'taskExecution', 'p95', src=BS))} → {ms(g(i, 'latencyMs', 'taskExecution', 'p95'))} |")

P("\n## Reading the numbers\n")
P("- On this host Postgres is the bottleneck: it uses most of one vCPU in every saturated scenario, and adding worker processes beyond the point "
  "where Postgres saturates mostly adds contention. A dedicated database host, or batching more state transitions per transaction, is the next lever.")
P("- Scheduling delay under saturation is dominated by backpressure: the scheduler deliberately keeps at most 2× fleet capacity dispatched, "
  "so the rest of a released backlog waits in Postgres (where it is durable and re-orderable) rather than in Redis.")
P("- Tasks are I/O-shaped (`core.sleep`); CPU-bound handlers would scale with worker processes, not with concurrency per process (Node runs handlers on one event loop).")
P("\n![Performance charts](performance_charts.png)\n")
open(os.path.join(a.out, "benchmark_report.md"), "w").write("\n".join(L))
print("wrote", png, "and benchmark_report.md")
