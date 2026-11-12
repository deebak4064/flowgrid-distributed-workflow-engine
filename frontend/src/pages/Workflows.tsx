import { useQuery } from "@tanstack/react-query";
import { useState } from "react";
import { Link, useNavigate } from "react-router-dom";
import { api, qs, session, type Page, type WorkflowSummary } from "@/lib/api";
import { ago, num, pct } from "@/lib/format";
import { Button, Dialog, Empty, ErrorNote, Field, Input, PageHeader, Pager, Panel, Select, Status, Table, Tag, Textarea } from "@/components/ui";

export function RunDialog({ workflow, open, onOpenChange }: { workflow: { id: string; name: string } | null; open: boolean; onOpenChange: (o: boolean) => void }) {
  const nav = useNavigate();
  const [input, setInput] = useState("{}");
  const [priority, setPriority] = useState("");
  const [when, setWhen] = useState<"now" | "delay" | "at">("now");
  const [delay, setDelay] = useState(60);
  const [at, setAt] = useState("");
  const [deadline, setDeadline] = useState("");
  const [key, setKey] = useState("");
  const [err, setErr] = useState<unknown>(null);
  async function run() {
    setErr(null);
    let parsed: unknown;
    try { parsed = JSON.parse(input || "{}"); } catch { setErr(new Error("Input must be valid JSON")); return; }
    try {
      const r = await api<{ executionId: string }>(`/workflows/${workflow!.id}/execute`, {
        method: "POST", json: {
          input: parsed, priority: priority === "" ? undefined : Number(priority), idempotencyKey: key || undefined,
          delaySeconds: when === "delay" ? delay : undefined, runAt: when === "at" && at ? new Date(at).toISOString() : undefined,
          deadlineSeconds: deadline ? Number(deadline) : undefined,
        },
      });
      onOpenChange(false);
      nav(`/executions/${r.executionId}`);
    } catch (e) { setErr(e); }
  }
  return (
    <Dialog open={open} onOpenChange={onOpenChange} title={`Run ${workflow?.name ?? ""}`} description="Start an execution of the current version.">
      <div className="space-y-3">
        <Field label="Input (JSON)"><Textarea rows={5} value={input} onChange={(e) => setInput(e.target.value)} spellCheck={false} /></Field>
        <div className="grid grid-cols-2 gap-3">
          <Field label="Start">
            <Select value={when} onChange={(e) => setWhen(e.target.value as typeof when)} className="w-full">
              <option value="now">Immediately</option><option value="delay">After a delay</option><option value="at">At a time</option>
            </Select>
          </Field>
          {when === "delay" && <Field label="Delay (seconds)"><Input type="number" min={0} value={delay} onChange={(e) => setDelay(+e.target.value)} /></Field>}
          {when === "at" && <Field label="Run at"><Input type="datetime-local" value={at} onChange={(e) => setAt(e.target.value)} /></Field>}
          <Field label="Priority override" hint="0–10, blank keeps per-task priority"><Input type="number" min={0} max={10} value={priority} onChange={(e) => setPriority(e.target.value)} /></Field>
          <Field label="Deadline (seconds)" hint="Used by earliest-deadline-first"><Input type="number" min={1} value={deadline} onChange={(e) => setDeadline(e.target.value)} /></Field>
        </div>
        <Field label="Idempotency key" hint="Repeating a request with the same key returns the same execution."><Input value={key} onChange={(e) => setKey(e.target.value)} placeholder="optional" /></Field>
        <ErrorNote error={err} />
        <div className="flex justify-end gap-2 pt-1"><Button variant="ghost" onClick={() => onOpenChange(false)}>Cancel</Button><Button variant="primary" onClick={run}>Start execution</Button></div>
      </div>
    </Dialog>
  );
}

export default function Workflows() {
  const [q, setQ] = useState("");
  const [page, setPage] = useState(0);
  const [running, setRunning] = useState<WorkflowSummary | null>(null);
  const { data, error } = useQuery({ queryKey: ["workflows", q, page], queryFn: () => api<Page<WorkflowSummary>>(`/workflows${qs({ q, page, size: 25 })}`), refetchInterval: 10000 });
  const role = session.user()?.role;
  return (
    <div>
      <PageHeader title="Workflows" description="Versioned DAG definitions. Editing publishes a new version; runs in progress keep the version they started with."
        actions={role === "ADMIN" && <Link to="/workflows/new"><Button variant="primary">New workflow</Button></Link>} />
      <RunDialog workflow={running} open={!!running} onOpenChange={(o) => !o && setRunning(null)} />
      <Panel title={<Input value={q} onChange={(e) => { setQ(e.target.value); setPage(0); }} placeholder="Search workflows" className="w-64" aria-label="Search workflows" />} bodyClass="p-0">
        <ErrorNote error={error} />
        {data && data.items.length === 0 ? <Empty title="No workflows found">{q ? "Try a different search." : "Create one in the visual builder."}</Empty> : (
          <Table>
            <thead><tr><th>Name</th><th>Version</th><th>Tasks</th><th>Runs</th><th>Success</th><th>Last run</th><th /></tr></thead>
            <tbody>
              {data?.items.map((w) => {
                const finished = w.succeeded + w.failed;
                return (
                  <tr key={w.id} className="hover:bg-raised/40">
                    <td className="max-w-md">
                      <Link to={`/workflows/${w.id}`} className="font-medium text-ink hover:text-violet">{w.name}</Link>
                      <div className="mt-0.5 truncate text-xs text-ink-3">{w.description}</div>
                      <div className="mt-1 flex flex-wrap gap-1">{w.tags.map((t) => <Tag key={t}>{t}</Tag>)}</div>
                    </td>
                    <td className="tnum text-ink-2">v{w.currentVersion}</td>
                    <td className="tnum text-ink-2">{w.nodeCount}</td>
                    <td className="tnum text-ink-2">{num(w.executions)}{w.running > 0 && <span className="ml-1.5 text-cyan">({w.running} active)</span>}</td>
                    <td className="tnum text-ink-2">{finished ? pct(w.succeeded / finished, 0) : "—"}</td>
                    <td className="text-ink-2">{w.lastStatus ? <span className="flex items-center gap-2"><Status value={w.lastStatus} /><span className="text-xs text-ink-3">{ago(w.lastRunAt)}</span></span> : "Never run"}</td>
                    <td className="text-right">
                      {role !== "VIEWER" && <Button size="sm" onClick={() => setRunning(w)}>Run</Button>}
                    </td>
                  </tr>
                );
              })}
            </tbody>
          </Table>
        )}
        {data && <Pager page={page} size={data.size} total={data.total} onPage={setPage} />}
      </Panel>
    </div>
  );
}
