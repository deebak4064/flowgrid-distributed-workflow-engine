import { useQuery, useQueryClient } from "@tanstack/react-query";
import { useState } from "react";
import { Link, useNavigate } from "react-router-dom";
import { api, session, type ExecutionSummary, type Metrics, type Page, type Worker } from "@/lib/api";
import { ago, cx, ms, num, pct, shortId } from "@/lib/format";
import { useLive, useThroughput } from "@/components/Shell";
import { Pipeline } from "@/components/Pipeline";
import { Legend, LinesChart, STATUS } from "@/components/charts";
import { Button, Dialog, ErrorNote, Field, Input, Mono, PageHeader, Panel, Reading, Status } from "@/components/ui";

function ClaimsDialog({ open, onOpenChange }: { open: boolean; onOpenChange: (o: boolean) => void }) {
  const [count, setCount] = useState(25);
  const [failureRate, setFailureRate] = useState(0.1);
  const [crash, setCrash] = useState(0.05);
  const [err, setErr] = useState<unknown>(null);
  const [busy, setBusy] = useState(false);
  const nav = useNavigate();
  async function submit() {
    setBusy(true); setErr(null);
    try {
      const r = await api<{ executionIds: string[] }>("/demo/claims", { method: "POST", json: { count, failureRate, crashAfterEffectRate: crash } });
      onOpenChange(false);
      nav(count === 1 ? `/executions/${r.executionIds[0]}` : "/live");
    } catch (e) { setErr(e); } finally { setBusy(false); }
  }
  return (
    <Dialog open={open} onOpenChange={onOpenChange} title="Submit synthetic claims"
      description="Each claim runs the insurance-claim-processing workflow. Injected failures exercise retries; crash-after-effect proves idempotency.">
      <div className="space-y-3">
        <Field label="Number of claims"><Input type="number" min={1} max={2000} value={count} onChange={(e) => setCount(+e.target.value)} /></Field>
        <Field label="Transient failure rate per task" hint="0 – 0.9. Failed attempts are retried with exponential backoff.">
          <Input type="number" step={0.05} min={0} max={0.9} value={failureRate} onChange={(e) => setFailureRate(+e.target.value)} />
        </Field>
        <Field label="Crash after side effect rate" hint="Fails the attempt after its database write committed; the retry must not duplicate it.">
          <Input type="number" step={0.05} min={0} max={0.9} value={crash} onChange={(e) => setCrash(+e.target.value)} />
        </Field>
        <ErrorNote error={err} />
        <div className="flex justify-end gap-2 pt-2">
          <Button variant="ghost" onClick={() => onOpenChange(false)}>Cancel</Button>
          <Button variant="primary" onClick={submit} disabled={busy}>{busy ? "Submitting…" : `Submit ${count} claims`}</Button>
        </div>
      </div>
    </Dialog>
  );
}

export default function Overview() {
  const qc = useQueryClient();
  const [dialog, setDialog] = useState(false);
  const metrics = useQuery({ queryKey: ["metrics", 15], queryFn: () => api<Metrics>("/metrics?windowMinutes=15"), refetchInterval: 5000 });
  const recent = useQuery({ queryKey: ["executions", "recent"], queryFn: () => api<Page<ExecutionSummary>>("/executions?size=8"), refetchInterval: 8000 });
  const workers = useQuery({ queryKey: ["workers"], queryFn: () => api<Worker[]>("/workers"), refetchInterval: 5000 });
  useLive((e) => { if (e.type.startsWith("WORKFLOW_")) { qc.invalidateQueries({ queryKey: ["executions", "recent"] }); qc.invalidateQueries({ queryKey: ["metrics", 15] }); } });

  const m = metrics.data;
  const tp = useThroughput();
  const rate = tp.slice(-5).reduce((a, b) => a + b, 0) / 5;
  const series = (m?.series ?? []).map((s) => ({ ...s, t: new Date(s.t).toLocaleTimeString([], { hour: "2-digit", minute: "2-digit" }) }));
  const canOperate = session.user()?.role !== "VIEWER";

  return (
    <div className="space-y-5">
      <PageHeader title="Operations overview" description="Your organization's workflows over the last 15 minutes, computed from execution records."
        actions={canOperate && <Button variant="primary" onClick={() => setDialog(true)}>Submit synthetic claims</Button>} />
      <ClaimsDialog open={dialog} onOpenChange={setDialog} />
      <ErrorNote error={metrics.error} />

      <Pipeline m={m} rate={rate} />

      <div className="grid grid-cols-2 gap-x-8 gap-y-5 border-y border-line-soft py-5 md:grid-cols-4">
        <Reading label="p95 task duration" value={m ? ms(m.tasks.p95_ms) : "—"} sub={m && `p50 ${ms(m.tasks.p50_ms)}, p99 ${ms(m.tasks.p99_ms)}`} />
        <Reading label="Attempt failure rate" value={m ? pct(m.tasks.failure_rate) : "—"} tone={m && m.tasks.failure_rate > 0.1 ? "warn" : undefined}
          sub={m && `${num(m.tasks.retries)} tasks retried`} />
        <Reading label="p95 workflow end-to-end" value={m ? ms(m.workflows.p95_duration_ms) : "—"} sub={m && `${num(m.workflows.succeeded)} of ${num(m.workflows.started)} succeeded`} />
        <Reading label="Open dead letters" value={m ? num(m.deadLetters) : "—"} tone={m && m.deadLetters > 0 ? "bad" : undefined}
          sub={<Link to="/dead-letters" className="underline decoration-line underline-offset-2 hover:text-ink">Review dead letters</Link>} />
      </div>

      <div className="grid gap-5 xl:grid-cols-[1.6fr_1fr]">
        <Panel title="Task outcomes" actions={<Legend items={[{ label: "Completed", color: STATUS.ok }, { label: "Failed attempts", color: STATUS.bad }, { label: "Retries", color: STATUS.warn }]} />}>
          <LinesChart data={series} x="t" height={240} series={[
            { key: "completed", label: "Completed", color: STATUS.ok },
            { key: "failed", label: "Failed attempts", color: STATUS.bad },
            { key: "retried", label: "Retries", color: STATUS.warn }]}
            empty={<span>No task activity in the last 15 minutes. {canOperate && "Submit synthetic claims to start some."}</span>} />
        </Panel>
        <Panel title="Workflows" bodyClass="p-0">
          <dl className="grid grid-cols-3 border-b border-line-soft px-4 py-3 text-sm">
            <div><dt className="text-xs text-ink-3">Started</dt><dd className="tnum text-lg font-semibold">{num(m?.workflows.started)}</dd></div>
            <div><dt className="text-xs text-ink-3">Succeeded</dt><dd className="tnum text-lg font-semibold text-ok">{num(m?.workflows.succeeded)}</dd></div>
            <div><dt className="text-xs text-ink-3">p95 end-to-end</dt><dd className="tnum text-lg font-semibold">{ms(m?.workflows.p95_duration_ms)}</dd></div>
          </dl>
          <ul className="divide-y divide-line-soft">
            {(recent.data?.items ?? []).map((e) => (
              <li key={e.id}>
                <Link to={`/executions/${e.id}`} className="flex items-center gap-3 px-4 py-2 text-sm hover:bg-raised/50">
                  <Status value={e.status} />
                  <span className="min-w-0 flex-1 truncate">{e.workflowName}</span>
                  <Mono>{shortId(e.id)}</Mono>
                  <span className="tnum w-16 text-right text-xs text-ink-3">{ago(e.createdAt)}</span>
                </Link>
              </li>
            ))}
            {recent.data?.items.length === 0 && <li className="px-4 py-6 text-sm text-ink-3">No executions yet.</li>}
          </ul>
        </Panel>
      </div>

      <Panel title="Worker fleet" actions={<Link to="/workers" className="text-xs text-ink-2 hover:text-ink">Manage workers</Link>}>
        {workers.data?.length ? (
          <div className="grid gap-3 sm:grid-cols-2 xl:grid-cols-4">
            {workers.data.map((w) => (
              <div key={w.id} className="rounded-md border border-line-soft bg-ground/50 p-3">
                <div className="flex items-center justify-between gap-2"><Mono className="truncate text-ink">{w.id}</Mono><Status value={w.status} /></div>
                <div className="mt-3 flex h-1.5 gap-px overflow-hidden rounded-full bg-line-soft" aria-label={`${w.in_flight} of ${w.concurrency} slots busy`}>
                  {Array.from({ length: w.concurrency }, (_, i) => <span key={i} className={cx("flex-1", i < w.in_flight ? "bg-cyan" : "bg-transparent")} />)}
                </div>
                <div className="mt-2 flex justify-between text-2xs text-ink-3">
                  <span className="tnum">{w.in_flight}/{w.concurrency} slots</span>
                  <span className="tnum">{num(w.completed_count)} done</span>
                  <span className="tnum">beat {ms(w.heartbeat_age_ms)} ago</span>
                </div>
              </div>
            ))}
          </div>
        ) : <p className="text-sm text-ink-3">No workers registered. Start a worker process to execute tasks.</p>}
      </Panel>
    </div>
  );
}
