import { useQuery, useQueryClient } from "@tanstack/react-query";
import { useState } from "react";
import { api, session, type Page, type Role, type User } from "@/lib/api";
import { ago, time } from "@/lib/format";
import { Button, Dialog, ErrorNote, Field, Input, Mono, PageHeader, Panel, Select, TabContent, TabList, Tabs, TabTrigger, Table } from "@/components/ui";

const ROLE_TEXT: Record<Role, string> = {
  ADMIN: "Everything, including workflow definitions, users and scheduler settings",
  OPERATOR: "Run, cancel, retry and schedule workflows; replay dead letters",
  VIEWER: "Read-only access to every screen",
};
const STRATEGIES = {
  FIFO: "First ready, first dispatched. Fair and predictable; urgent work waits its turn.",
  PRIORITY: "Highest effective priority first. Waiting tasks age upward so low priority cannot starve.",
  EDF: "Earliest deadline first. Minimises missed deadlines while the system keeps up; tasks without a deadline go last.",
} as const;

function UsersTab() {
  const qc = useQueryClient();
  const users = useQuery({ queryKey: ["org-users"], queryFn: () => api<User[]>("/org/users") });
  const [open, setOpen] = useState(false);
  const [form, setForm] = useState({ email: "", displayName: "", password: "", role: "OPERATOR" as Role });
  const [err, setErr] = useState<unknown>(null);
  const isAdmin = session.user()?.role === "ADMIN";
  async function create() {
    setErr(null);
    try { await api("/org/users", { method: "POST", json: form }); setOpen(false); qc.invalidateQueries({ queryKey: ["org-users"] }); }
    catch (e) { setErr(e); }
  }
  return (
    <Panel title="Members" bodyClass="p-0" actions={isAdmin && <Button size="sm" variant="primary" onClick={() => setOpen(true)}>Add member</Button>}>
      <Dialog open={open} onOpenChange={setOpen} title="Add member" description="They sign in with this email and password.">
        <div className="space-y-3">
          <Field label="Email"><Input type="email" value={form.email} onChange={(e) => setForm({ ...form, email: e.target.value })} /></Field>
          <Field label="Name"><Input value={form.displayName} onChange={(e) => setForm({ ...form, displayName: e.target.value })} /></Field>
          <Field label="Password" hint="At least 10 characters"><Input type="password" value={form.password} onChange={(e) => setForm({ ...form, password: e.target.value })} /></Field>
          <Field label="Role" hint={ROLE_TEXT[form.role]}>
            <Select className="w-full" value={form.role} onChange={(e) => setForm({ ...form, role: e.target.value as Role })}>
              <option value="ADMIN">Admin</option><option value="OPERATOR">Operator</option><option value="VIEWER">Viewer</option>
            </Select>
          </Field>
          <ErrorNote error={err} />
          <div className="flex justify-end gap-2"><Button variant="ghost" onClick={() => setOpen(false)}>Cancel</Button><Button variant="primary" onClick={create}>Add member</Button></div>
        </div>
      </Dialog>
      <Table>
        <thead><tr><th>Name</th><th>Email</th><th>Role</th><th>Last sign-in</th></tr></thead>
        <tbody>{users.data?.map((u) => (
          <tr key={u.id}><td className="font-medium">{u.displayName}</td><td className="text-ink-2">{u.email}</td>
            <td><span className="text-ink">{u.role.toLowerCase()}</span><div className="text-2xs text-ink-3">{ROLE_TEXT[u.role]}</div></td><td className="text-ink-2">{ago(u.lastLoginAt)}</td></tr>
        ))}</tbody>
      </Table>
    </Panel>
  );
}

function SchedulerTab() {
  const qc = useQueryClient();
  const cfg = useQuery({ queryKey: ["scheduler-config"], queryFn: () => api<{ strategy: keyof typeof STRATEGIES; agingSeconds: number }>("/scheduler/config") });
  const [err, setErr] = useState<unknown>(null);
  const isAdmin = session.user()?.role === "ADMIN";
  async function update(body: object) {
    setErr(null);
    try { await api("/scheduler/config", { method: "PUT", json: body }); qc.invalidateQueries({ queryKey: ["scheduler-config"] }); }
    catch (e) { setErr(e); }
  }
  return (
    <Panel title="Dispatch strategy">
      <p className="mb-4 max-w-[70ch] text-sm text-ink-2">Applies to the whole cluster. Every scheduler replica picks up a change within about two seconds; tasks already dispatched are not reordered.</p>
      <div className="grid gap-3 md:grid-cols-3" role="radiogroup" aria-label="Scheduling strategy">
        {(Object.keys(STRATEGIES) as (keyof typeof STRATEGIES)[]).map((s) => (
          <button key={s} role="radio" aria-checked={cfg.data?.strategy === s} disabled={!isAdmin} onClick={() => update({ strategy: s })}
            className={`rounded-lg border p-4 text-left ${cfg.data?.strategy === s ? "border-violet bg-violet/8" : "border-line-soft hover:border-line"}`}>
            <div className="font-medium">{s === "EDF" ? "Earliest deadline first" : s === "FIFO" ? "First in, first out" : "Priority with aging"}</div>
            <p className="mt-1 text-xs leading-relaxed text-ink-2">{STRATEGIES[s]}</p>
          </button>
        ))}
      </div>
      <div className="mt-5 flex items-end gap-3">
        <Field label="Aging interval (seconds)" hint="A waiting task gains one priority level per interval, up to 10">
          <Input type="number" min={1} max={3600} defaultValue={cfg.data?.agingSeconds} key={cfg.data?.agingSeconds} disabled={!isAdmin}
            onBlur={(e) => +e.target.value !== cfg.data?.agingSeconds && update({ agingSeconds: +e.target.value })} className="w-32" />
        </Field>
      </div>
      <div className="mt-3"><ErrorNote error={err} /></div>
    </Panel>
  );
}

function AuditTab() {
  const [page] = useState(0);
  const audit = useQuery({ queryKey: ["audit", page], queryFn: () => api<Page<{ id: number; actorEmail: string; action: string; resourceType: string; resourceId: string; details: string; ip: string; createdAt: string }>>(`/audit?page=${page}&size=50`) });
  return (
    <Panel title="Audit trail" bodyClass="p-0">
      <ErrorNote error={audit.error} />
      <Table>
        <thead><tr><th>When</th><th>Who</th><th>Action</th><th>Resource</th><th>From</th></tr></thead>
        <tbody>{audit.data?.items.map((a) => (
          <tr key={a.id}><td className="whitespace-nowrap text-ink-2">{time(a.createdAt, true)}</td><td>{a.actorEmail ?? "system"}</td>
            <td><Mono className="text-ink">{a.action}</Mono></td><td className="text-ink-2">{a.resourceType} <Mono>{a.resourceId?.slice(0, 12)}</Mono></td><td><Mono>{a.ip}</Mono></td></tr>
        ))}</tbody>
      </Table>
    </Panel>
  );
}

export default function Settings() {
  const user = session.user();
  return (
    <div>
      <PageHeader title="Settings" description={<>Signed in as {user?.email} in <b className="font-medium text-ink">{user?.orgName}</b>. Data is isolated per organization; workers are shared infrastructure.</>} />
      <Tabs defaultValue="members">
        <TabList>
          <TabTrigger value="members">Members and roles</TabTrigger>
          <TabTrigger value="scheduler">Scheduler</TabTrigger>
          {user?.role === "ADMIN" && <TabTrigger value="audit">Audit trail</TabTrigger>}
        </TabList>
        <TabContent value="members"><UsersTab /></TabContent>
        <TabContent value="scheduler"><SchedulerTab /></TabContent>
        <TabContent value="audit"><AuditTab /></TabContent>
      </Tabs>
    </div>
  );
}
