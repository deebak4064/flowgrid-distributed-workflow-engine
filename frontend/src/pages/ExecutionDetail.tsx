import { useQuery, useQueryClient } from "@tanstack/react-query";
import { AnimatePresence, motion } from "motion/react";
import { useMemo, useRef, useState } from "react";
import { X } from "lucide-react";
import { Link, useParams } from "react-router-dom";
import { api, session, type ExecEvent, type ExecutionDetail as Detail, type TaskStatus } from "@/lib/api";
import { ago, cx, ms, shortId, time } from "@/lib/format";
import { useLive } from "@/components/Shell";
import { LiveDag, STATE_COLOR } from "@/components/Dag";
import { Timeline } from "@/components/Timeline";
import { Button, Empty, ErrorNote, JsonView, Mono, Panel, Status, STATUS_STYLE, TabContent, TabList, Tabs, TabTrigger } from "@/components/ui";

const ORDER: TaskStatus[] = ["SUCCEEDED", "RUNNING", "QUEUED", "RETRYING", "READY", "PENDING", "FAILED", "CANCELLED", "SKIPPED"];

const TASK_EVENT_STATUS: Record<string, TaskStatus> = {
  TASK_READY: "READY", TASK_QUEUED: "QUEUED", TASK_STARTED: "RUNNING", TASK_SUCCEEDED: "SUCCEEDED", TASK_FAILED: "FAILED",
  TASK_RETRYING: "RETRYING", TASK_CANCELLED: "CANCELLED", TASK_SKIPPED: "SKIPPED",
};

export default function ExecutionDetail() {
  const { id } = useParams();
  const qc = useQueryClient();
  const [selected, setSelected] = useState<string | null>(null);
  const [feed, setFeed] = useState<ExecEvent[]>([]);
  const [actionError, setActionError] = useState<unknown>(null);
  const refetchTimer = useRef<number | null>(null);
  const { data, error, isLoading } = useQuery({
    queryKey: ["execution", id], queryFn: () => api<Detail>(`/executions/${id}`),
    refetchInterval: (q) => (q.state.data && ["SUCCEEDED", "FAILED", "CANCELLED"].includes(q.state.data.execution.status) ? false : 5000),
  });
  const history = useQuery({ queryKey: ["execution-events", id], queryFn: () => api<{ items: ExecEvent[] }>(`/executions/${id}/events?size=200`) });

  // Live: patch node state instantly from the event (drives the animation), then re-read the authoritative view.
  useLive((e) => {
    if (e.executionId !== id) return;
    setFeed((f) => [e, ...f].slice(0, 200));
    const to = TASK_EVENT_STATUS[e.type];
    if (to && e.nodeKey) {
      qc.setQueryData<Detail>(["execution", id], (d) => d && {
        ...d, tasks: d.tasks.map((t) => (t.nodeKey === e.nodeKey ? { ...t, status: to, attempt: e.attempt ?? t.attempt,
          startedAt: to === "RUNNING" ? e.at : t.startedAt, leaseOwner: e.workerId ?? t.leaseOwner } : t)),
      });
    }
    if (refetchTimer.current === null)
      refetchTimer.current = window.setTimeout(() => { refetchTimer.current = null; qc.invalidateQueries({ queryKey: ["execution", id] }); }, 600);
  });

  const events = useMemo(() => {
    const seen = new Set<string>();
    return [...feed, ...(history.data?.items ?? [])].filter((e) => {
      const k = `${e.type}|${e.nodeKey}|${e.attempt}|${e.to}`;
      if (seen.has(k)) return false;
      seen.add(k);
      return true;
    });
  }, [feed, history.data]);

  if (isLoading) return <p className="text-sm text-ink-3">Loading execution…</p>;
  if (error || !data) return <ErrorNote error={error ?? "Execution not found"} />;
  const ex = data.execution;
  const task = data.tasks.find((t) => t.nodeKey === selected) ?? null;
  const canOperate = session.user()?.role !== "VIEWER";
  const terminal = ["SUCCEEDED", "FAILED", "CANCELLED"].includes(ex.status);
  const counts = ORDER.map((st) => [st, data.tasks.filter((t) => t.status === st).length] as const).filter(([, n]) => n > 0);

  async function act(path: string) {
    setActionError(null);
    try {
      await api(path, { method: "POST", json: {} });
      qc.invalidateQueries({ queryKey: ["execution", id] });
    } catch (e) { setActionError(e); }
  }

  return (
    <div className="space-y-5">
      <div className="flex flex-wrap items-start justify-between gap-4">
        <div>
          <div className="text-xs text-ink-3"><Link to="/executions" className="hover:text-ink">Executions</Link> / <Mono>{shortId(ex.id)}</Mono></div>
          <h1 className="mt-1 flex items-center gap-3 text-[1.75rem] font-semibold leading-tight tracking-[-0.02em]">
            <Link to={`/workflows/${ex.workflowId}`} className="hover:text-violet">{ex.workflowName}</Link>
            <span className="text-sm font-normal text-ink-3">v{ex.version}</span>
            <Status value={ex.status} />
          </h1>
        </div>
        {canOperate && (
          <div className="flex gap-2">
            {!terminal && <Button variant="danger" onClick={() => act(`/executions/${id}/cancel`)}>Cancel execution</Button>}
            {(ex.status === "FAILED" || ex.status === "CANCELLED") && <Button variant="primary" onClick={() => act(`/executions/${id}/retry`)}>Retry from failure</Button>}
          </div>
        )}
      </div>
      <ErrorNote error={actionError} />

      <dl className="grid grid-cols-2 gap-x-6 gap-y-3 rounded-lg border border-line-soft bg-surface px-4 py-3 text-sm sm:grid-cols-3 lg:grid-cols-6">
        {[
          ["Progress", <span className="tnum">{ex.completedTasks} / {ex.totalTasks} tasks</span>],
          ["Duration", <span className="tnum">{ms(ex.durationMs)}</span>],
          ["Trigger", ex.trigger.toLowerCase()],
          ["Priority", ex.priority ?? "per task"],
          ["Created", <span title={ex.createdAt}>{time(ex.createdAt, true)}</span>],
          ["Finished", ex.finishedAt ? time(ex.finishedAt, true) : ex.runAt && ex.status === "PENDING" ? `starts ${ago(ex.runAt)}` : "—"],
        ].map(([k, v]) => (
          <div key={String(k)}><dt className="text-xs text-ink-3">{k}</dt><dd className="mt-0.5 text-ink">{v}</dd></div>
        ))}
      </dl>
      {ex.error && <div className="rounded-md border border-bad/30 bg-bad/8 px-3 py-2 text-sm text-bad">{ex.error}</div>}

      <div className="flex h-1.5 gap-[2px] overflow-hidden rounded-full" role="img" aria-label={counts.map(([st, n]) => `${n} ${STATUS_STYLE[st].label.toLowerCase()}`).join(", ")}>
        {ORDER.flatMap((st) => data.tasks.filter((t) => t.status === st)).map((t) => (
          <span key={t.nodeKey} className="flex-1 transition-colors duration-500" style={{ background: t.status === "SKIPPED" ? "var(--color-line-soft)" : STATE_COLOR[t.status] }} />
        ))}
      </div>

      <div className="relative">
        <LiveDag tasks={data.tasks} edges={data.edges} selected={selected} onSelect={setSelected} height={600} />
        <ul className="absolute left-3 top-3 flex flex-wrap gap-1.5" aria-label="Tasks by state">
          {counts.map(([st, n]) => (
            <li key={st} className="flex items-center gap-1.5 rounded-full border border-line-soft bg-surface/90 px-2.5 py-1 text-xs backdrop-blur">
              <span className={cx("size-1.5 rounded-full", STATUS_STYLE[st].dot, st === "RUNNING" && "fg-pulse")} />
              <span className={STATUS_STYLE[st].text}>{STATUS_STYLE[st].label}</span><span className="tnum font-medium text-ink">{n}</span>
            </li>
          ))}
        </ul>
        {!task && <p className="pointer-events-none absolute bottom-3 left-3 text-xs text-ink-3">Select a task to see its attempts, worker, timing and output.</p>}
        <AnimatePresence>
          {task && (
            <motion.aside key={task.nodeKey} initial={{ opacity: 0, x: 24 }} animate={{ opacity: 1, x: 0 }} exit={{ opacity: 0, x: 24 }} transition={{ duration: 0.18 }}
              className="absolute inset-y-3 right-3 w-[min(380px,calc(100%-1.5rem))] overflow-y-auto rounded-[10px] border border-line bg-surface/95 p-4 backdrop-blur" aria-label={`Task ${task.name}`}>
              <div className="mb-3 flex items-start justify-between gap-3">
                <h2 className="text-base font-semibold leading-snug">{task.name}</h2>
                <button onClick={() => setSelected(null)} className="rounded p-1 text-ink-3 hover:bg-raised hover:text-ink" aria-label="Close task details"><X size={16} /></button>
              </div>
              <div className="space-y-4 text-sm">
                <div className="flex items-center justify-between"><Status value={task.status} /><Mono>{task.taskType}</Mono></div>
                <dl className="grid grid-cols-2 gap-3">
                  {[
                    ["Attempts", `${task.attempt} of ${task.maxAttempts}`],
                    ["Duration", ms(task.durationMs)],
                    ["Worker", task.leaseOwner ?? task.attempts.at(-1)?.workerId ?? "—"],
                    ["Priority", task.effectivePriority > task.priority ? `${task.priority} → ${task.effectivePriority} (aged)` : task.priority],
                    ["Timeout", ms(task.timeoutMs)],
                    ["Trigger rule", task.triggerRule === "ANY_SUCCESS" ? "any upstream" : "all upstream"],
                  ].map(([k, v]) => (
                    <div key={String(k)} className="min-w-0"><dt className="text-xs text-ink-3">{k}</dt><dd className="truncate text-ink" title={String(v)}>{v}</dd></div>
                  ))}
                </dl>
                {task.error && (
                  <div>
                    <div className="mb-1 text-xs text-ink-3">Last error {task.errorType && <Mono className="text-bad">{task.errorType}</Mono>}</div>
                    <p className="rounded-md bg-bad/8 px-2.5 py-2 font-mono text-xs text-bad">{task.error}</p>
                  </div>
                )}
                {task.attempts.length > 0 && (
                  <div>
                    <div className="mb-1 text-xs text-ink-3">Attempt history</div>
                    <ol className="space-y-1">
                      {task.attempts.map((a) => (
                        <li key={a.attemptId} className="flex items-center justify-between gap-2 rounded bg-ground/60 px-2 py-1 text-xs">
                          <span className="tnum text-ink-3">#{a.attempt}</span>
                          <span className={cx("font-medium", a.status === "SUCCEEDED" ? "text-ok" : ["FAILED", "TIMED_OUT"].includes(a.status) ? "text-bad" : a.status === "LOST" ? "text-warn" : "text-ink-2")}>
                            {a.status.toLowerCase().replace("_", " ")}
                          </span>
                          <span className="tnum text-ink-2">{ms(a.durationMs)}</span>
                          <Mono className="max-w-24 truncate">{a.workerId ?? "—"}</Mono>
                          {a.backoffMs != null && <span className="tnum text-warn" title="Backoff before next attempt">+{ms(a.backoffMs)}</span>}
                        </li>
                      ))}
                    </ol>
                  </div>
                )}
                <div><div className="mb-1 text-xs text-ink-3">Output</div><JsonView value={task.output} /></div>
              </div>
            </motion.aside>
          )}
        </AnimatePresence>
      </div>

      <Tabs defaultValue="timeline">
        <TabList>
          <TabTrigger value="timeline">Timeline</TabTrigger>
          <TabTrigger value="events">Events ({events.length})</TabTrigger>
          <TabTrigger value="input">Input</TabTrigger>
        </TabList>
        <TabContent value="timeline">
          <Panel><Timeline tasks={data.tasks} start={ex.createdAt} end={ex.finishedAt} selected={selected} onSelect={setSelected} /></Panel>
        </TabContent>
        <TabContent value="events">
          <Panel bodyClass="p-0">
            {events.length === 0 ? <Empty title="No events yet" /> : (
              <ol className="max-h-[420px] divide-y divide-line-soft overflow-y-auto text-xs">
                {events.map((e, i) => (
                  <li key={`${e.type}-${e.nodeKey}-${e.attempt}-${i}`} className="grid grid-cols-[88px_150px_1fr] gap-3 px-4 py-1.5">
                    <span className="tnum text-ink-3">{time(e.at)}</span>
                    <span className="truncate font-mono text-ink-2">{e.type.toLowerCase()}</span>
                    <span className="truncate text-ink-2">
                      {e.nodeKey && <b className="font-medium text-ink">{e.nodeKey}</b>} {e.attempt ? `attempt ${e.attempt}` : ""} {e.workerId ? `on ${e.workerId}` : ""} {e.message}
                    </span>
                  </li>
                ))}
              </ol>
            )}
          </Panel>
        </TabContent>
        <TabContent value="input"><Panel><JsonView value={data.input} className="max-h-[420px]" /></Panel></TabContent>
      </Tabs>
    </div>
  );
}
