import { useQuery, useQueryClient } from "@tanstack/react-query";
import { useMemo, useState } from "react";
import { Link } from "react-router-dom";
import { api, session, type CalendarEntry, type Page, type Schedule, type WorkflowSummary } from "@/lib/api";
import { ago, cx, ms, time } from "@/lib/format";
import { Button, Dialog, Empty, ErrorNote, Field, Input, Mono, PageHeader, Panel, Select, Table, Textarea } from "@/components/ui";

const DAY = 86_400_000;

function startOfWeek(d: Date) {
  const x = new Date(d.getFullYear(), d.getMonth(), d.getDate());
  x.setDate(x.getDate() - ((x.getDay() + 6) % 7));
  return x;
}

function describe(s: Schedule) {
  if (s.type === "CRON") return <>cron <Mono>{s.cron}</Mono> {s.timezone !== "UTC" && s.timezone}</>;
  if (s.type === "INTERVAL") return <>every {ms(s.intervalMs)}</>;
  return <>once</>;
}

function ScheduleDialog({ open, onOpenChange, editing }: { open: boolean; onOpenChange: (o: boolean) => void; editing: Schedule | null }) {
  const qc = useQueryClient();
  const wfs = useQuery({ queryKey: ["workflows", "all"], queryFn: () => api<Page<WorkflowSummary>>("/workflows?size=200") });
  const [form, setForm] = useState(() => ({
    workflowId: editing?.workflowId ?? "", name: editing?.name ?? "", type: editing?.type ?? "CRON", cron: editing?.cron ?? "0 */15 * * * *",
    intervalSeconds: editing?.intervalMs ? editing.intervalMs / 1000 : 300, runAt: "", timezone: editing?.timezone ?? "UTC",
    priority: editing?.priority?.toString() ?? "", input: JSON.stringify(editing?.input ?? {}, null, 2),
  }));
  const [err, setErr] = useState<unknown>(null);
  const set = (k: keyof typeof form, v: string | number) => setForm((f) => ({ ...f, [k]: v }));
  async function save() {
    setErr(null);
    let input: unknown;
    try { input = JSON.parse(form.input || "{}"); } catch { setErr(new Error("Input must be valid JSON")); return; }
    const body = {
      workflowId: form.workflowId, name: form.name, type: form.type, cron: form.type === "CRON" ? form.cron : undefined,
      intervalSeconds: form.type === "INTERVAL" ? Number(form.intervalSeconds) : undefined,
      runAt: form.type === "ONCE" && form.runAt ? new Date(form.runAt).toISOString() : undefined, timezone: form.timezone,
      priority: form.priority === "" ? undefined : Number(form.priority), input,
    };
    try {
      if (editing) await api(`/schedules/${editing.id}`, { method: "PATCH", json: { ...body, expectedLockVersion: editing.lockVersion } });
      else await api("/schedules", { method: "POST", json: body });
      qc.invalidateQueries({ queryKey: ["schedules"] });
      qc.invalidateQueries({ queryKey: ["calendar"] });
      onOpenChange(false);
    } catch (e) { setErr(e); }
  }
  return (
    <Dialog open={open} onOpenChange={onOpenChange} title={editing ? `Edit ${editing.name}` : "New schedule"}
      description="Each fire time runs exactly one execution, even with several schedulers. After downtime a missed slot fires once.">
      <div className="space-y-3">
        <Field label="Workflow">
          <Select className="w-full" value={form.workflowId} onChange={(e) => set("workflowId", e.target.value)} disabled={!!editing}>
            <option value="">Choose a workflow</option>{wfs.data?.items.map((w) => <option key={w.id} value={w.id}>{w.name}</option>)}
          </Select>
        </Field>
        <Field label="Name"><Input value={form.name} onChange={(e) => set("name", e.target.value)} /></Field>
        <div className="grid grid-cols-2 gap-3">
          <Field label="Repeats">
            <Select className="w-full" value={form.type} onChange={(e) => set("type", e.target.value)}>
              <option value="CRON">On a cron schedule</option><option value="INTERVAL">At a fixed interval</option><option value="ONCE">Once, at a time</option>
            </Select>
          </Field>
          {form.type === "CRON" && <Field label="Cron expression" hint="sec min hour day month weekday (5-field also accepted)"><Input className="font-mono text-xs" value={form.cron} onChange={(e) => set("cron", e.target.value)} /></Field>}
          {form.type === "INTERVAL" && <Field label="Interval (seconds)"><Input type="number" min={1} value={form.intervalSeconds} onChange={(e) => set("intervalSeconds", e.target.value)} /></Field>}
          {form.type === "ONCE" && <Field label="Run at"><Input type="datetime-local" value={form.runAt} onChange={(e) => set("runAt", e.target.value)} /></Field>}
          <Field label="Time zone"><Input value={form.timezone} onChange={(e) => set("timezone", e.target.value)} /></Field>
          <Field label="Priority" hint="Blank keeps per-task priority"><Input type="number" min={0} max={10} value={form.priority} onChange={(e) => set("priority", e.target.value)} /></Field>
        </div>
        <Field label="Input (JSON)"><Textarea rows={4} value={form.input} onChange={(e) => set("input", e.target.value)} /></Field>
        <ErrorNote error={err} />
        <div className="flex justify-end gap-2"><Button variant="ghost" onClick={() => onOpenChange(false)}>Cancel</Button>
          <Button variant="primary" onClick={save} disabled={!form.workflowId || !form.name}>{editing ? "Save schedule" : "Create schedule"}</Button></div>
      </div>
    </Dialog>
  );
}

export default function Schedules() {
  const qc = useQueryClient();
  const [anchor, setAnchor] = useState(() => startOfWeek(new Date()));
  const [day, setDay] = useState<string | null>(null);
  const [dialog, setDialog] = useState<{ open: boolean; editing: Schedule | null }>({ open: false, editing: null });
  const [err, setErr] = useState<unknown>(null);
  const from = anchor, to = new Date(anchor.getTime() + 35 * DAY);
  const schedules = useQuery({ queryKey: ["schedules"], queryFn: () => api<Schedule[]>("/schedules"), refetchInterval: 10000 });
  const cal = useQuery({ queryKey: ["calendar", from.toISOString()], refetchInterval: 15000,
    queryFn: () => api<CalendarEntry[]>(`/schedules/calendar?from=${from.toISOString()}&to=${to.toISOString()}`) });
  const canOperate = session.user()?.role !== "VIEWER";

  const byDay = useMemo(() => {
    const m: Record<string, CalendarEntry[]> = {};
    cal.data?.forEach((e) => { const k = new Date(e.at).toDateString(); (m[k] ??= []).push(e); });
    return m;
  }, [cal.data]);
  const days = Array.from({ length: 35 }, (_, i) => new Date(anchor.getTime() + i * DAY));
  const today = new Date().toDateString();

  async function act(s: Schedule, what: "toggle" | "trigger" | "delete") {
    setErr(null);
    try {
      if (what === "toggle") await api(`/schedules/${s.id}`, { method: "PATCH", json: { enabled: !s.enabled } });
      if (what === "trigger") await api(`/schedules/${s.id}/trigger`, { method: "POST" });
      if (what === "delete") await api(`/schedules/${s.id}`, { method: "DELETE" });
      qc.invalidateQueries({ queryKey: ["schedules"] });
      qc.invalidateQueries({ queryKey: ["calendar"] });
    } catch (e) { setErr(e); }
  }

  return (
    <div className="space-y-5">
      <PageHeader title="Scheduling" description="Recurring, interval and one-off triggers, with what is planned and what already ran."
        actions={canOperate && <Button variant="primary" onClick={() => setDialog({ open: true, editing: null })}>New schedule</Button>} />
      {dialog.open && <ScheduleDialog open onOpenChange={(o) => !o && setDialog({ open: false, editing: null })} editing={dialog.editing} />}
      <ErrorNote error={err ?? cal.error} />

      <Panel title={`${from.toLocaleDateString(undefined, { month: "long", day: "numeric" })} to ${new Date(to.getTime() - DAY).toLocaleDateString(undefined, { month: "long", day: "numeric" })}`}
        actions={<>
          <Button size="sm" variant="ghost" onClick={() => setAnchor(new Date(anchor.getTime() - 7 * DAY))}>Previous week</Button>
          <Button size="sm" variant="ghost" onClick={() => setAnchor(startOfWeek(new Date()))}>This week</Button>
          <Button size="sm" variant="ghost" onClick={() => setAnchor(new Date(anchor.getTime() + 7 * DAY))}>Next week</Button>
        </>} bodyClass="p-0">
        <div className="grid grid-cols-7 border-b border-line-soft text-xs text-ink-3">
          {["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"].map((d) => <div key={d} className="px-2 py-1.5">{d}</div>)}
        </div>
        <div className="grid grid-cols-7">
          {days.map((d) => {
            const k = d.toDateString(), list = byDay[k] ?? [];
            const fired = list.filter((e) => e.kind === "FIRED"), planned = list.filter((e) => e.kind !== "FIRED");
            return (
              <button key={k} onClick={() => setDay(day === k ? null : k)} aria-pressed={day === k}
                className={cx("min-h-24 border-b border-r border-line-soft p-1.5 text-left align-top hover:bg-raised/40", day === k && "bg-raised/60")}>
                <div className={cx("tnum mb-1 text-xs", k === today ? "font-semibold text-violet" : "text-ink-3")}>{d.getDate()}</div>
                {fired.length > 0 && (
                  <div className="mb-1 flex items-center gap-1 text-2xs text-ink-2">
                    <span className="size-1.5 rounded-full bg-ok" />{fired.length} ran
                    {fired.some((f) => f.status === "FAILED") && <span className="text-bad">, {fired.filter((f) => f.status === "FAILED").length} failed</span>}
                  </div>
                )}
                {planned.slice(0, 3).map((e, i) => (
                  <div key={i} className={cx("mb-0.5 truncate rounded px-1 py-px text-2xs", e.kind === "DELAYED" ? "bg-warn/12 text-warn" : "border border-violet/40 text-ink-2")}>
                    {time(e.at).slice(0, 5)} {e.title}
                  </div>
                ))}
                {planned.length > 3 && <div className="text-2xs text-ink-3">{planned.length - 3} more</div>}
              </button>
            );
          })}
        </div>
        {day && (
          <div className="border-t border-line-soft p-4">
            <h3 className="mb-2 text-sm font-medium">{new Date(day).toLocaleDateString(undefined, { weekday: "long", month: "long", day: "numeric" })}</h3>
            {(byDay[day] ?? []).length === 0 ? <p className="text-sm text-ink-3">Nothing scheduled.</p> : (
              <ul className="space-y-1 text-sm">
                {byDay[day].map((e, i) => (
                  <li key={i} className="flex items-center gap-3">
                    <span className="tnum w-16 text-ink-3">{time(e.at)}</span>
                    <span className="w-20 text-xs text-ink-3">{e.kind === "FIRED" ? e.status?.toLowerCase() : e.kind.toLowerCase()}</span>
                    {e.executionId ? <Link to={`/executions/${e.executionId}`} className="hover:text-violet">{e.title}</Link> : <span>{e.title}</span>}
                    <span className="text-xs text-ink-3">{e.workflowName}</span>
                  </li>
                ))}
              </ul>
            )}
          </div>
        )}
      </Panel>

      <Panel title="Schedules" bodyClass="p-0">
        {schedules.data?.length === 0 ? <Empty title="No schedules yet" /> : (
          <Table>
            <thead><tr><th>Name</th><th>Workflow</th><th>Repeats</th><th>Next run</th><th>Last run</th><th>Runs</th><th /></tr></thead>
            <tbody>
              {schedules.data?.map((s) => (
                <tr key={s.id} className={cx(!s.enabled && "opacity-60")}>
                  <td className="font-medium">{s.name}{!s.enabled && <span className="ml-2 text-xs text-ink-3">paused</span>}</td>
                  <td><Link to={`/workflows/${s.workflowId}`} className="text-ink-2 hover:text-violet">{s.workflowName}</Link></td>
                  <td className="text-ink-2">{describe(s)}</td>
                  <td className="text-ink-2" title={s.nextFireAt}>{s.enabled && s.nextFireAt ? ago(s.nextFireAt) : "—"}</td>
                  <td className="text-ink-2">{ago(s.lastFiredAt)}</td>
                  <td className="tnum">{s.fireCount}</td>
                  <td className="whitespace-nowrap text-right">
                    {canOperate && <>
                      <Button size="sm" variant="ghost" onClick={() => act(s, "trigger")}>Run now</Button>
                      <Button size="sm" variant="ghost" onClick={() => act(s, "toggle")}>{s.enabled ? "Pause" : "Resume"}</Button>
                      <Button size="sm" variant="ghost" onClick={() => setDialog({ open: true, editing: s })}>Edit</Button>
                      <Button size="sm" variant="ghost" className="text-bad" onClick={() => act(s, "delete")}>Delete</Button>
                    </>}
                  </td>
                </tr>
              ))}
            </tbody>
          </Table>
        )}
      </Panel>
    </div>
  );
}
