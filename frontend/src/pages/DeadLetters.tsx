import { useQuery, useQueryClient } from "@tanstack/react-query";
import { Fragment, useState } from "react";
import { Link } from "react-router-dom";
import { api, qs, session, type DeadLetter, type Metrics, type Page } from "@/lib/api";
import { ago, num, pct, shortId } from "@/lib/format";
import { Button, Empty, ErrorNote, Mono, PageHeader, Pager, Panel, Reading, Select, Status, Table } from "@/components/ui";

const REASONS: Record<string, string> = {
  RETRIES_EXHAUSTED: "Retries exhausted", NON_RETRYABLE: "Non-retryable error", LEASE_LOST_EXHAUSTED: "Worker lost on every attempt",
  POISON_MESSAGE: "Poison message", UNKNOWN_TASK_TYPE: "Unknown task type",
};

export default function DeadLetters() {
  const qc = useQueryClient();
  const [reason, setReason] = useState("");
  const [resolved, setResolved] = useState(false);
  const [page, setPage] = useState(0);
  const [expanded, setExpanded] = useState<string | null>(null);
  const [err, setErr] = useState<unknown>(null);
  const list = useQuery({ queryKey: ["dlq", reason, resolved, page], refetchInterval: 5000,
    queryFn: () => api<Page<DeadLetter>>(`/dead-letter-tasks${qs({ reason, includeResolved: resolved, page, size: 25 })}`) });
  const m = useQuery({ queryKey: ["metrics", 60], queryFn: () => api<Metrics>("/metrics?windowMinutes=60"), refetchInterval: 10000 });
  const poison = useQuery({ queryKey: ["poison"], queryFn: () => api<Record<string, string>[]>("/dead-letter-tasks/poison-messages?count=20"), enabled: session.user()?.role !== "VIEWER" });
  const canOperate = session.user()?.role !== "VIEWER";

  async function act(d: DeadLetter, action: "replay" | "discard") {
    setErr(null);
    try { await api(`/dead-letter-tasks/${d.id}/${action}`, { method: "POST" }); qc.invalidateQueries({ queryKey: ["dlq"] }); }
    catch (e) { setErr(e); }
  }

  const t = m.data?.tasks;
  return (
    <div className="space-y-5">
      <PageHeader title="Retries and dead letters" description="Tasks that exhausted their retry budget or failed permanently. Replaying resumes the execution from the failed task; completed tasks keep their results and idempotency keys prevent duplicate side effects." />
      <div className="grid grid-cols-2 gap-6 rounded-lg border border-line-soft bg-surface p-5 md:grid-cols-4">
        <Reading label="Retries, last hour" value={num(t?.retries)} />
        <Reading label="Failed attempts, last hour" value={num(t?.failed_attempts)} />
        <Reading label="Attempt failure rate" value={pct(t?.failure_rate)} />
        <Reading label="Open dead letters" value={num(m.data?.deadLetters)} tone={m.data && m.data.deadLetters > 0 ? "bad" : undefined} />
      </div>
      <ErrorNote error={list.error ?? err} />
      <Panel bodyClass="p-0" title={
        <div className="flex flex-wrap items-center gap-3">
          <Select value={reason} onChange={(e) => { setReason(e.target.value); setPage(0); }} aria-label="Reason">
            <option value="">All reasons</option>{Object.entries(REASONS).map(([k, v]) => <option key={k} value={k}>{v}</option>)}
          </Select>
          <label className="flex items-center gap-2 text-sm font-normal text-ink-2"><input type="checkbox" className="accent-violet" checked={resolved} onChange={(e) => setResolved(e.target.checked)} />Include resolved</label>
        </div>}>
        {list.data?.items.length === 0 ? <Empty title="No dead letters">Every failed task either recovered on retry or has been resolved.</Empty> : (
          <Table>
            <thead><tr><th>Task</th><th>Reason</th><th>Attempts</th><th>Execution</th><th>When</th><th /></tr></thead>
            <tbody>
              {list.data?.items.map((d) => (
                <Fragment key={d.id}>
                  <tr className="hover:bg-raised/40">
                    <td><button className="text-left" onClick={() => setExpanded(expanded === d.id ? null : d.id)} aria-expanded={expanded === d.id}>
                      <span className="font-medium">{d.nodeKey}</span> <Mono>{d.taskType}</Mono>
                      <div className="max-w-md truncate text-xs text-bad">{d.error}</div></button></td>
                    <td className="text-ink-2">{REASONS[d.reason] ?? d.reason}</td>
                    <td className="tnum">{d.attempts}</td>
                    <td><Link to={`/executions/${d.executionId}`} className="hover:text-violet">{d.workflowName}</Link> <Mono>{shortId(d.executionId)}</Mono>
                      {d.executionStatus && <div className="mt-0.5"><Status value={d.executionStatus} /></div>}</td>
                    <td className="text-ink-2">{ago(d.createdAt)}{d.resolution && <div className="text-xs text-ink-3">{d.resolution.toLowerCase()} {ago(d.resolvedAt)}</div>}</td>
                    <td className="whitespace-nowrap text-right">
                      {canOperate && !d.resolvedAt && <>
                        <Button size="sm" variant="primary" onClick={() => act(d, "replay")}>Replay</Button>{" "}
                        <Button size="sm" variant="ghost" onClick={() => act(d, "discard")}>Discard</Button>
                      </>}
                    </td>
                  </tr>
                  {expanded === d.id && (
                    <tr key={`${d.id}-x`}><td colSpan={6} className="bg-ground/50"><pre className="whitespace-pre-wrap font-mono text-xs text-bad">{d.error}</pre></td></tr>
                  )}
                </Fragment>
              ))}
            </tbody>
          </Table>
        )}
        {list.data && <Pager page={page} size={list.data.size} total={list.data.total} onPage={setPage} />}
      </Panel>
      {canOperate && (
        <Panel title="Poison messages" bodyClass="p-0">
          {poison.data?.length ? (
            <Table>
              <thead><tr><th>Stream id</th><th>Reason</th><th>Task</th><th>Attempt id</th></tr></thead>
              <tbody>{poison.data.map((p) => <tr key={p.id}><td><Mono>{p.original_id}</Mono></td><td>{p.reason}</td><td><Mono>{p.t}</Mono></td><td><Mono>{p.a}</Mono></td></tr>)}</tbody>
            </Table>
          ) : <p className="px-4 py-4 text-sm text-ink-3">No unparseable or unroutable stream messages. They are moved to <Mono>fg:tasks:dlq</Mono> instead of being redelivered forever.</p>}
        </Panel>
      )}
    </div>
  );
}
