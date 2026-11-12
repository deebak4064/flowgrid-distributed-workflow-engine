import { useQuery } from "@tanstack/react-query";
import { useState } from "react";
import { api, type Metrics } from "@/lib/api";
import { ms, num, pct } from "@/lib/format";
import { BarsChart, Legend, LinesChart, SERIES, STATUS } from "@/components/charts";
import { ErrorNote, Mono, PageHeader, Panel, Reading, Select, Table } from "@/components/ui";

const WINDOWS = [[15, "15 minutes"], [60, "1 hour"], [360, "6 hours"], [1440, "24 hours"]] as const;

export default function Analytics() {
  const [w, setW] = useState(60);
  const { data: m, error } = useQuery({ queryKey: ["metrics", w], queryFn: () => api<Metrics>(`/metrics?windowMinutes=${w}`), refetchInterval: 10000 });
  const fmt = (t: string) => new Date(t).toLocaleTimeString([], { hour: "2-digit", minute: "2-digit" });
  const series = (m?.series ?? []).map((s) => ({ ...s, t: fmt(s.t), rate: +(s.completed / (m!.bucketSeconds || 1)).toFixed(2) }));

  return (
    <div className="space-y-5">
      <PageHeader title="Performance analytics" description="Computed from attempt records in Postgres. Each process also exposes Prometheus metrics at /metrics."
        actions={<Select value={w} onChange={(e) => setW(+e.target.value)} aria-label="Time window">{WINDOWS.map(([v, l]) => <option key={v} value={v}>Last {l}</option>)}</Select>} />
      <ErrorNote error={error} />
      <div className="grid grid-cols-2 gap-6 rounded-lg border border-line-soft bg-surface p-5 md:grid-cols-4 xl:grid-cols-7">
        <Reading label="Throughput" value={m ? `${num(m.tasks.completed_per_sec, 2)}/s` : "—"} />
        <Reading label="p50 execution" value={ms(m?.tasks.p50_ms)} />
        <Reading label="p95 execution" value={ms(m?.tasks.p95_ms)} />
        <Reading label="p99 execution" value={ms(m?.tasks.p99_ms)} />
        <Reading label="Avg queue wait" value={ms(m?.tasks.avg_queue_wait_ms)} sub={`p95 ${ms(m?.tasks.p95_queue_wait_ms)}`} />
        <Reading label="Avg scheduling delay" value={ms(m?.scheduling.avg_delay_ms)} sub={`p95 ${ms(m?.scheduling.p95_delay_ms)}`} />
        <Reading label="Workflow p95 end-to-end" value={ms(m?.workflows.p95_duration_ms)} sub={`avg ${ms(m?.workflows.avg_duration_ms)}`} />
      </div>

      <div className="grid gap-5 xl:grid-cols-2">
        <Panel title="Completed tasks per second">
          <LinesChart data={series} x="t" series={[{ key: "rate", label: "Tasks/s", color: SERIES[0] }]} format={(v) => v.toFixed(1)} />
        </Panel>
        <Panel title="Execution duration" actions={<Legend items={[{ label: "Average", color: SERIES[0] }, { label: "p95", color: SERIES[1] }]} />}>
          <LinesChart data={series} x="t" format={(v) => ms(v)} series={[
            { key: "avg_duration_ms", label: "Average", color: SERIES[0] }, { key: "p95_duration_ms", label: "p95", color: SERIES[1] }]} />
        </Panel>
        <Panel title="Failures and retries" actions={<Legend items={[{ label: "Failed attempts", color: STATUS.bad }, { label: "Retries", color: STATUS.warn }]} />}>
          <LinesChart data={series} x="t" series={[{ key: "failed", label: "Failed attempts", color: STATUS.bad }, { key: "retried", label: "Retries", color: STATUS.warn }]} />
        </Panel>
        <Panel title="p95 duration by task type">
          <BarsChart layout="vertical" height={Math.max(160, (m?.byTaskType.length ?? 0) * 26)} x="task_type" format={(v) => ms(v)}
            data={(m?.byTaskType ?? []).filter((t) => t.p95_ms != null)} series={[{ key: "p95_ms", label: "p95", color: SERIES[1] }]} />
        </Panel>
      </div>

      <Panel title="By task type" bodyClass="p-0">
        <Table>
          <thead><tr><th>Task type</th><th className="text-right">Succeeded</th><th className="text-right">Failed attempts</th><th className="text-right">Failure rate</th><th className="text-right">Average</th><th className="text-right">p95</th></tr></thead>
          <tbody>
            {m?.byTaskType.map((t) => (
              <tr key={t.task_type}>
                <td><Mono className="text-ink">{t.task_type}</Mono></td>
                <td className="tnum text-right">{num(t.succeeded)}</td><td className="tnum text-right">{num(t.failed)}</td>
                <td className="tnum text-right">{pct(t.failed / Math.max(1, t.succeeded + t.failed))}</td>
                <td className="tnum text-right">{ms(t.avg_duration_ms)}</td><td className="tnum text-right">{ms(t.p95_ms)}</td>
              </tr>
            ))}
            {m?.byTaskType.length === 0 && <tr><td colSpan={6} className="text-ink-3">No attempts finished in this window.</td></tr>}
          </tbody>
        </Table>
      </Panel>
    </div>
  );
}
