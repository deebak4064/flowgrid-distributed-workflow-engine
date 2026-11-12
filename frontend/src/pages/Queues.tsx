import { useQuery } from "@tanstack/react-query";
import { Link } from "react-router-dom";
import { api } from "@/lib/api";
import { ms, num, shortId } from "@/lib/format";
import { BarsChart, SERIES } from "@/components/charts";
import { ErrorNote, Mono, PageHeader, Panel, Table } from "@/components/ui";

interface QueueInfo {
  redisAvailable: boolean; redisError?: string;
  redis?: { stream: string; group: string; length: number; deadLetterLength: number; pending: number; lag: number | null; lastDeliveredId: string;
    consumers: { name: string; pending: number; idle: number; inactive?: number }[] };
  byStatus: { status: string; count: number }[];
  runnableByPriority: { priority: number; count: number; oldest_age_ms: number }[];
  runnableByType: { task_type: string; count: number }[];
  oldestQueued: { id: string; node_key: string; task_type: string; attempt: number; priority: number; stream_message_id?: string; workflow_execution_id: string; age_ms: number }[];
}

const STAGES = [
  { status: "PENDING", label: "Waiting on dependencies", note: "Not yet runnable" },
  { status: "READY", label: "Runnable", note: "Waiting for capacity" },
  { status: "RETRYING", label: "Retrying", note: "Backoff, then runnable" },
  { status: "QUEUED", label: "Dispatched", note: "In the Redis stream" },
  { status: "RUNNING", label: "Running", note: "Leased by a worker" },
];

export default function Queues() {
  const { data, error } = useQuery({ queryKey: ["queues"], queryFn: () => api<QueueInfo>("/queues"), refetchInterval: 2000 });
  const count = (s: string) => data?.byStatus.find((b) => b.status === s)?.count ?? 0;
  const r = data?.redis;
  return (
    <div className="space-y-5">
      <PageHeader title="Queues" description="Postgres holds the logical queue (task states); Redis Streams carries dispatch messages to the worker consumer group. Backpressure keeps at most twice the fleet's capacity dispatched." />
      <ErrorNote error={error} />
      {data && !data.redisAvailable && <ErrorNote error={{ message: `Redis is unreachable: ${data.redisError}. Tasks stay safe in Postgres and are re-published once it returns.` }} />}

      <section aria-label="Task pipeline" className="grid gap-px overflow-hidden rounded-lg border border-line-soft bg-line-soft sm:grid-cols-5">
        {STAGES.map((s) => (
          <div key={s.status} className="bg-surface px-4 py-4">
            <div className="tnum text-2xl font-semibold">{num(count(s.status))}</div>
            <div className="mt-0.5 text-sm text-ink">{s.label}</div>
            <div className="text-2xs text-ink-3">{s.note}</div>
          </div>
        ))}
      </section>

      <div className="grid gap-5 xl:grid-cols-2">
        <Panel title={<>Stream <Mono className="ml-1">{r?.stream}</Mono></>}>
          {r ? (
            <>
              <dl className="grid grid-cols-2 gap-4 text-sm sm:grid-cols-4">
                <div><dt className="text-xs text-ink-3">Entries</dt><dd className="tnum">{num(r.length)}</dd></div>
                <div><dt className="text-xs text-ink-3">Lag (undelivered)</dt><dd className="tnum">{r.lag == null ? "—" : num(r.lag)}</dd></div>
                <div><dt className="text-xs text-ink-3">Pending (un-acked)</dt><dd className="tnum">{num(r.pending)}</dd></div>
                <div><dt className="text-xs text-ink-3">Poison messages</dt><dd className="tnum">{num(r.deadLetterLength)}</dd></div>
              </dl>
              <h3 className="mb-1 mt-5 text-xs text-ink-3">Consumers in group <Mono>{r.group}</Mono></h3>
              <Table>
                <thead><tr><th>Consumer</th><th className="text-right">Pending</th><th className="text-right">Idle</th></tr></thead>
                <tbody>
                  {r.consumers.map((c) => (
                    <tr key={c.name}><td><Mono>{c.name}</Mono></td><td className="tnum text-right">{c.pending}</td><td className="tnum text-right text-ink-2">{ms(c.idle)}</td></tr>
                  ))}
                </tbody>
              </Table>
              <p className="mt-3 text-xs text-ink-3">Messages un-acked for 30 s are reclaimed with XAUTOCLAIM by a live worker. Entries are trimmed approximately at 1M.</p>
            </>
          ) : <p className="text-sm text-ink-3">Loading…</p>}
        </Panel>
        <Panel title="Runnable tasks by effective priority">
          <BarsChart data={(data?.runnableByPriority ?? []).map((p) => ({ ...p, priority: `P${p.priority}` }))} x="priority"
            series={[{ key: "count", label: "Tasks", color: SERIES[1] }]} height={200} />
          <p className="mt-2 text-xs text-ink-3">Waiting tasks gain one priority level per aging interval, so low-priority work cannot starve.</p>
        </Panel>
      </div>

      <div className="grid gap-5 xl:grid-cols-[1fr_1.4fr]">
        <Panel title="Backlog by task type" bodyClass="p-0">
          <Table>
            <thead><tr><th>Task type</th><th className="text-right">Waiting or dispatched</th></tr></thead>
            <tbody>
              {data?.runnableByType.map((t) => <tr key={t.task_type}><td><Mono>{t.task_type}</Mono></td><td className="tnum text-right">{num(t.count)}</td></tr>)}
              {data?.runnableByType.length === 0 && <tr><td colSpan={2} className="text-ink-3">Backlog is empty.</td></tr>}
            </tbody>
          </Table>
        </Panel>
        <Panel title="Longest-waiting dispatched tasks" bodyClass="p-0">
          <Table>
            <thead><tr><th>Task</th><th>Type</th><th>Attempt</th><th>Message</th><th className="text-right">Waiting</th></tr></thead>
            <tbody>
              {data?.oldestQueued.map((t) => (
                <tr key={t.id}>
                  <td><Link to={`/executions/${t.workflow_execution_id}`} className="hover:text-violet">{t.node_key}</Link> <Mono>{shortId(t.workflow_execution_id)}</Mono></td>
                  <td><Mono>{t.task_type}</Mono></td><td className="tnum">{t.attempt}</td>
                  <td><Mono>{t.stream_message_id ?? "not published"}</Mono></td><td className="tnum text-right">{ms(t.age_ms)}</td>
                </tr>
              ))}
              {data?.oldestQueued.length === 0 && <tr><td colSpan={5} className="text-ink-3">No dispatched tasks are waiting.</td></tr>}
            </tbody>
          </Table>
        </Panel>
      </div>
    </div>
  );
}
