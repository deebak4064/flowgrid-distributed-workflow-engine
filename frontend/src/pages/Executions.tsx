import { useQuery } from "@tanstack/react-query";
import { useState } from "react";
import { Link, useSearchParams } from "react-router-dom";
import { api, qs, type ExecutionSummary, type Page, type WorkflowSummary } from "@/lib/api";
import { ago, ms, shortId, time } from "@/lib/format";
import { Empty, ErrorNote, Input, Mono, PageHeader, Pager, Panel, Select, Status, Table } from "@/components/ui";

const STATUSES = ["", "PENDING", "RUNNING", "SUCCEEDED", "FAILED", "CANCELLED"];

export default function Executions() {
  const [params, setParams] = useSearchParams();
  const status = params.get("status") ?? "";
  const workflowId = params.get("workflowId") ?? "";
  const [q, setQ] = useState(params.get("q") ?? "");
  const page = Number(params.get("page") ?? 0);
  const set = (k: string, v: string) => { const p = new URLSearchParams(params); if (v) p.set(k, v); else p.delete(k); if (k !== "page") p.delete("page"); setParams(p, { replace: true }); };
  const list = useQuery({ queryKey: ["executions", status, workflowId, params.get("q"), page],
    queryFn: () => api<Page<ExecutionSummary>>(`/executions${qs({ status, workflowId, q: params.get("q"), page, size: 25 })}`), refetchInterval: 5000 });
  const workflows = useQuery({ queryKey: ["workflows", "all"], queryFn: () => api<Page<WorkflowSummary>>("/workflows?size=200") });

  return (
    <div>
      <PageHeader title="Executions" description="Every workflow run, newest first. Search by execution id prefix or workflow name." />
      <Panel bodyClass="p-0" title={
        <form className="flex flex-wrap items-center gap-2" onSubmit={(e) => { e.preventDefault(); set("q", q); }}>
          <Input value={q} onChange={(e) => setQ(e.target.value)} placeholder="Execution id or workflow" className="w-60" aria-label="Search executions" />
          <Select value={status} onChange={(e) => set("status", e.target.value)} aria-label="Status">
            {STATUSES.map((s) => <option key={s} value={s}>{s ? s[0] + s.slice(1).toLowerCase() : "Any status"}</option>)}
          </Select>
          <Select value={workflowId} onChange={(e) => set("workflowId", e.target.value)} aria-label="Workflow" className="max-w-56">
            <option value="">All workflows</option>
            {workflows.data?.items.map((w) => <option key={w.id} value={w.id}>{w.name}</option>)}
          </Select>
        </form>}>
        <ErrorNote error={list.error} />
        {list.data?.items.length === 0 ? <Empty title="No executions match these filters" /> : (
          <Table>
            <thead><tr><th>Status</th><th>Workflow</th><th>Execution</th><th>Tasks</th><th>Trigger</th><th>Started</th><th className="text-right">Duration</th></tr></thead>
            <tbody>
              {list.data?.items.map((e) => (
                <tr key={e.id} className="hover:bg-raised/40">
                  <td><Status value={e.status} /></td>
                  <td><Link to={`/executions/${e.id}`} className="font-medium hover:text-violet">{e.workflowName}</Link> <span className="text-xs text-ink-3">v{e.version}</span>
                    {e.error && <div className="max-w-md truncate text-xs text-bad" title={e.error}>{e.error}</div>}</td>
                  <td><Link to={`/executions/${e.id}`}><Mono className="hover:text-ink">{shortId(e.id)}</Mono></Link></td>
                  <td className="tnum text-ink-2">{e.completedTasks}/{e.totalTasks}</td>
                  <td className="text-ink-2">{e.trigger.toLowerCase()}</td>
                  <td className="text-ink-2" title={time(e.createdAt, true)}>{ago(e.createdAt)}</td>
                  <td className="tnum text-right text-ink-2">{ms(e.durationMs)}</td>
                </tr>
              ))}
            </tbody>
          </Table>
        )}
        {list.data && <Pager page={page} size={list.data.size} total={list.data.total} onPage={(p) => set("page", String(p))} />}
      </Panel>
    </div>
  );
}
