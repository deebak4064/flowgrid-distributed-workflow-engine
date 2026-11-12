import { useQuery, useQueryClient } from "@tanstack/react-query";
import { useState } from "react";
import { api, session, type Worker } from "@/lib/api";
import { ago, cx, ms, num, pct } from "@/lib/format";
import { AreaSpark } from "@/components/charts";
import { Button, Empty, ErrorNote, Mono, PageHeader, Panel, Status } from "@/components/ui";

function health(w: Worker): { label: string; tone: string } {
  if (w.status === "DEAD") return { label: "No heartbeat: tasks were reassigned", tone: "text-bad" };
  if (w.status === "DRAINING") return { label: "Finishing in-flight work, not taking new tasks", tone: "text-warn" };
  if (w.heartbeat_age_ms > 6000) return { label: "Heartbeat late", tone: "text-warn" };
  return { label: "Healthy", tone: "text-ok" };
}

export default function Workers() {
  const qc = useQueryClient();
  const [showStopped, setShowStopped] = useState(false);
  const [err, setErr] = useState<unknown>(null);
  const { data, error } = useQuery({ queryKey: ["workers", showStopped], queryFn: () => api<Worker[]>(`/workers?includeStopped=${showStopped}`), refetchInterval: 3000 });
  const isAdmin = session.user()?.role === "ADMIN";
  const active = data?.filter((w) => w.status === "ACTIVE") ?? [];
  const cap = active.reduce((s, w) => s + w.concurrency, 0), busy = active.reduce((s, w) => s + w.in_flight, 0);

  async function act(w: Worker, action: "drain" | "resume") {
    setErr(null);
    try { await api(`/workers/${encodeURIComponent(w.id)}/${action}`, { method: "POST" }); qc.invalidateQueries({ queryKey: ["workers"] }); }
    catch (e) { setErr(e); }
  }

  return (
    <div className="space-y-5">
      <PageHeader title="Workers" description="Independent worker processes register on start and heartbeat every 2 seconds. A worker silent for 10 seconds is declared dead and its leases are released."
        actions={<label className="flex items-center gap-2 text-sm text-ink-2"><input type="checkbox" className="accent-violet" checked={showStopped} onChange={(e) => setShowStopped(e.target.checked)} />Show stopped</label>} />
      <ErrorNote error={error ?? err} />
      <div className="flex flex-wrap gap-8 rounded-lg border border-line-soft bg-surface px-5 py-4 text-sm">
        <div><div className="tnum text-xl font-semibold">{active.length}</div><div className="text-xs text-ink-2">Active workers</div></div>
        <div><div className="tnum text-xl font-semibold">{busy} / {cap}</div><div className="text-xs text-ink-2">Execution slots in use</div></div>
        <div><div className="tnum text-xl font-semibold text-cyan">{cap ? pct(busy / cap, 0) : "—"}</div><div className="text-xs text-ink-2">Fleet utilization</div></div>
        <div><div className="tnum text-xl font-semibold">{data?.filter((w) => w.status === "DEAD").length ?? 0}</div><div className="text-xs text-ink-2">Dead</div></div>
      </div>
      {data?.length === 0 && <Panel><Empty title="No workers registered">Start one with <Mono>FLOWGRID_WORKER_ENABLED=true</Mono> (docker compose up --scale worker=3).</Empty></Panel>}
      <div className="grid gap-4 lg:grid-cols-2">
        {data?.map((w) => {
          const h = health(w);
          const series = [...w.recent].reverse().map((r) => ({ ...r, cpu: Math.round((r.cpu_load ?? 0) * 100) }));
          return (
            <Panel key={w.id} title={<span className="flex items-center gap-3"><Mono className="text-ink">{w.id}</Mono><Status value={w.status} /></span>}
              actions={isAdmin && (w.status === "ACTIVE" ? <Button size="sm" onClick={() => act(w, "drain")}>Drain</Button>
                : w.status === "DRAINING" ? <Button size="sm" onClick={() => act(w, "resume")}>Resume</Button> : null)}>
              <p className={cx("mb-3 text-xs", h.tone)}>{h.label}</p>
              <div className="grid grid-cols-2 gap-4 sm:grid-cols-4">
                <div><div className="text-xs text-ink-3">Slots</div><div className="tnum">{w.in_flight} / {w.concurrency}</div></div>
                <div><div className="text-xs text-ink-3">Completed</div><div className="tnum">{num(w.completed_count)}</div></div>
                <div><div className="text-xs text-ink-3">Failed attempts</div><div className="tnum">{num(w.failed_count)}</div></div>
                <div><div className="text-xs text-ink-3">Last heartbeat</div><div className="tnum">{ms(w.heartbeat_age_ms)} ago</div></div>
              </div>
              <div className="mt-4 grid grid-cols-2 gap-4">
                <div><div className="mb-1 text-2xs text-ink-3">Busy slots, last {series.length} heartbeats</div><AreaSpark data={series} k="in_flight" color="#36d6f0" max={w.concurrency} /></div>
                <div><div className="mb-1 text-2xs text-ink-3">Process CPU %</div><AreaSpark data={series} k="cpu" color="#9b8cff" max={100} /></div>
              </div>
              <div className="mt-3 flex flex-wrap gap-x-5 gap-y-1 text-2xs text-ink-3">
                <span>Host {w.hostname}</span><span>PID {w.pid}</span><span>Started {ago(w.started_at)}</span>
                <span>{w.task_type_count} task types</span><span>Heap {series.at(-1)?.heap_used_mb ?? "—"} MB</span>
              </div>
            </Panel>
          );
        })}
      </div>
    </div>
  );
}
