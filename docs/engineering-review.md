# Engineering review

After the first complete implementation passed its tests, I reviewed it against the checklist below, using the
multi-process tests, the load-test harness and `pg_stat_statements` as evidence. Each finding records what was
observed, what changed, and the measured effect. "Before" numbers come from `benchmarks/baseline_benchmark_results.json`
or from the targeted runs noted inline; "after" numbers come from `benchmarks/benchmark_results.json`.

## Findings that led to changes

### 1. Messages read by a crashed worker waited out the 30 s visibility timeout

**Found by:** `test/processes.test.ts` (three real worker processes, one killed with `SIGKILL`). The test took
**42.8 s** to finish 96 tasks that need about 1 s of work.

**Cause:** the scheduler declared the dead worker `DEAD` and released its *leases* within seconds, but messages that worker
had read from the stream and not yet claimed in Postgres stayed in its Redis pending-entries list. Only `XAUTOCLAIM`
recovered them, after `visibilityTimeoutMs` (30 s). The redispatch sweep correctly skipped them because they looked
"in flight".

**Change:** when the scheduler declares a worker dead, it `XCLAIM`s that consumer's pending entries, re-publishes them as
new stream entries, acks the originals, and removes the consumer from the group (`StreamQueue.takeOverConsumer`).
Duplicates are harmless because the Postgres ownership check drops the second delivery.

**Effect:** the same test runs in **5.4 s**, and now asserts that bound (`< 15 s`).

### 2. Rescheduling a delayed execution left downstream tasks on the old start time

**Found by:** the benchmark harness stalling: tasks released early stayed `READY` but never dispatched.

**Cause:** `ExecutionService.start` stamped `not_before = runAt` on **every** task row, not only the roots. `reschedule()`
moves only the roots, so every non-root task still waited for the original `runAt`.

**Change:** only root tasks carry `not_before`; downstream tasks become runnable when their dependencies settle.
`test/engine.test.ts › delays an execution until runAt and supports rescheduling` now covers a two-step chain and asserts that the
downstream `not_before` is null.

### 3. The dispatcher polled every 100 ms, which dominated latency at light load

**Found by:** the open-loop `latency-light` scenario in the baseline: scheduling delay p50 **89.2 ms**, p95 **104.5 ms**,
p99 **109.1 ms**, almost exactly the poll interval.

**Change:** the scheduler subscribes to the post-commit event channel and wakes the dispatch loop on `TASK_READY`,
`WORKFLOW_CREATED`, `TASK_STARTED` (a slot is freeing) and `WORKFLOW_RESUMED`. A wake signal that arrives while a round is
already running sets a flag, so another round follows immediately. The 100 ms poll stays as the safety net for delayed
tasks, retry backoff and lost pub/sub messages.

**Effect** (same scenario, targeted run): scheduling delay p50 / p95 / p99 **7.5 / 35.0 / 50.6 ms** (was 89.2 / 104.5 / 109.1);
workflow completion p50 **122 ms** (was 334 ms), p95 **173 ms** (was 404 ms). A regression test
(`dispatch is event-driven`) runs a 3-step chain with a 5 s poll interval and requires it to finish in under 1.5 s.

It also raised saturated throughput wherever the dispatch buffer is small. The strategy scenarios bound it to 1× capacity, and
there the poll left slots idle between refills. Waking on `TASK_STARTED` refills as soon as a slot is taken: those
scenarios went from about 100 to about 220 tasks/s in the full rerun (see the before/after table in
`benchmarks/benchmark_report.md`).

### 4. Hot path round trips, and what actually limits throughput on this host

**Observation:** claiming a task took about 7 round trips (begin, five statements, commit). Completing one took 8, or 16
for a task with 8 dependents (one counter update per edge).

**Change:** claim is now **one statement** (lease, attempt row, event and input in a single CTE). Completion is **three
statements** whatever the fan-out: (1) lock the workflow row, fence on the attempt, close the attempt; (2) one `UPDATE`
that settles every dependent's counters and decides `READY` / `SKIPPED` inline; (3) the countdown, finalisation and all
events in one statement.

**Effect:** measured alone, it was neutral (scale-10k 282 → 298 tasks/s, dur-0 349 → 314 tasks/s in targeted runs, within
run-to-run noise). In the full rerun, combined with finding 3, the scale scenarios improved by 13% (1k), 15% (10k) and
42% (50k), while the fixed-duration scenarios moved only 1–2%. The fewer round trips matter most when the database is
across a network.

**Why it was neutral:** `pg_stat_statements` during a 10,000-task noop run showed the completion statement taking
**95.2%** of Postgres execution time at a mean of **46.7 ms**, against **0.73 ms** for a claim. That time is lock waiting:
the 8 parallel branches of a workflow finish together and serialise on the workflow row, and the whole 2-vCPU host was
CPU-saturated (Postgres about 106%, workers about 70% of the 200% available).

**Options rejected:**
- Taking the workflow lock last would shorten the wait but invert the lock order used by cancellation and failure,
  reintroducing deadlocks.
- `synchronous_commit = off` for task transitions would remove the fsync from the critical section. Correctness would
  survive (lost transitions are re-run after lease expiry, and business effects are idempotent), but it trades durability
  for speed. That should be an explicit operator decision, not a default.

The documented next lever is a dedicated database host, or per-workflow sharding of the countdown row.

## Checklist items reviewed without a code change

| Area | What I checked | Result |
|---|---|---|
| Race conditions | Two workers completing siblings of a fan-in at once; duplicate deliveries; a cancel racing a completion; concurrent idempotent submits | Every transition is a compare-and-set; fan-in finalises exactly once (`fan-in under concurrent completions…` test, plus a `workflowsFinalizedOtherThanOnce = 0` check on every benchmark run). |
| Scheduling starvation | Low-priority work under a sustained high-priority backlog | `PRIORITY` ages `effective_priority` (+1 per `agingSeconds`, capped at 10). The strategy comparison in the benchmark report shows the per-priority waits. |
| Database lock contention | `pg_stat_statements`, lock-wait time per statement | See finding 4. Locks are taken in one order everywhere: workflow row first, task rows in id order. |
| Queue bottlenecks / unbounded queues | Redis stream length, worker-local buffering | Dispatch is bounded to `capacity × queueFactor`, so the backlog stays in Postgres where it is durable and re-orderable. Workers read only as many messages as they have free slots. The stream is trimmed (`MAXLEN ~ 1,000,000`). |
| Memory leaks | Long-lived maps and buffers | The edge cache is cleared at 100k entries. SSE clients are dropped past 1 MiB of buffered output. The throughput counter map is pruned every second. Worker RSS in the 50k-task run is in the benchmark report. |
| Worker failure scenarios | kill -9, graceful stop, DB loss, Redis loss, hung handler | All covered by `test/recovery.test.ts` and `test/processes.test.ts`. |
| Duplicate execution | Same attempt executed twice, effect applied twice | A task can run twice (at-least-once). A result commits at most once per attempt (fencing). Business effects apply once (idempotency records written in the same transaction as the effect). Measured: 0 duplicate rows in the failure benchmark. |
| Improper shutdown | `SIGTERM` during work | Workers drain for up to 20 s, then hand unfinished attempts back as `LOST` with immediate retry, instead of waiting for lease expiry (`hands in-flight work back immediately on graceful shutdown`). |
| Poor API performance | N+1 queries, offset pagination on append-only tables | The worker list uses a lateral `json_agg` instead of one query per worker. The logs explorer uses keyset pagination (`id < cursor`). Every list endpoint clamps `size` to 200. |

## Not done, deliberately

No Kafka, no Kubernetes manifests and no microservices beyond the three process roles. The roles are separate processes
because they fail and scale differently. Splitting further would add network hops to a system whose measured bottleneck
is the database.
