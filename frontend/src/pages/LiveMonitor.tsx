import { useQuery, useQueryClient } from "@tanstack/react-query";
import { AnimatePresence, motion, useReducedMotion } from "motion/react";
import { useEffect, useRef, useState } from "react";
import { Link } from "react-router-dom";
import { api, type ExecEvent, type ExecutionSummary, type Page } from "@/lib/api";
import { ago, cx, ms, num, shortId, time } from "@/lib/format";
import { useLive, useLiveState } from "@/components/Shell";
import { Empty, Mono, PageHeader, Panel, Select } from "@/components/ui";

const TONE: Record<string, string> = {
  TASK_SUCCEEDED: "text-ok", WORKFLOW_SUCCEEDED: "text-ok", TASK_FAILED: "text-bad", WORKFLOW_FAILED: "text-bad", TASK_DEAD_LETTERED: "text-bad",
  TASK_RETRYING: "text-warn", TASK_STARTED: "text-cyan", TASK_QUEUED: "text-queued", WORKFLOW_CANCELLED: "text-ink-3", TASK_CANCELLED: "text-ink-3",
};

export default function LiveMonitor() {
  const qc = useQueryClient();
  const reduce = useReducedMotion();
  const live = useLiveState();
  const [feed, setFeed] = useState<ExecEvent[]>([]);
  const [filter, setFilter] = useState("all");
  const [rate, setRate] = useState(0);
  const counter = useRef(0);
  const paused = useRef(false);
  const running = useQuery({ queryKey: ["executions", "active"], queryFn: () => api<Page<ExecutionSummary>>("/executions?status=RUNNING&size=50"), refetchInterval: 3000 });
  const pending = useQuery({ queryKey: ["executions", "pending"], queryFn: () => api<Page<ExecutionSummary>>("/executions?status=PENDING&size=20"), refetchInterval: 5000 });

  useLive((e) => {
    counter.current++;
    if (paused.current) return;
    setFeed((f) => [e, ...f].slice(0, 120));
    if (e.type.startsWith("WORKFLOW_")) qc.invalidateQueries({ queryKey: ["executions"] });
  });
  // Measured client-side from the SSE stream: events actually received per second.
  useEffect(() => { const t = setInterval(() => { setRate(counter.current); counter.current = 0; }, 1000); return () => clearInterval(t); }, []);

  const shown = feed.filter((e) => filter === "all" || (filter === "problems" ? /FAILED|RETRYING|DEAD|CANCELLED/.test(e.type) : e.type.startsWith(filter)));

  return (
    <div className="space-y-5">
      <PageHeader title="Live execution monitor" description="Active executions and the organization's event stream as it happens."
        actions={<span className="text-sm text-ink-2"><b className="tnum text-ink">{num(rate)}</b> events/s{live !== "live" && <span className="ml-2 text-warn">({live})</span>}</span>} />
      <div className="grid gap-5 xl:grid-cols-[1.2fr_1fr]">
        <Panel title={`Running (${running.data?.total ?? 0})`} bodyClass="p-0">
          {running.data?.items.length === 0 ? <Empty title="Nothing is running">Start a workflow or submit synthetic claims from the overview.</Empty> : (
            <ul className="divide-y divide-line-soft">
              <AnimatePresence initial={false}>
                {running.data?.items.map((e) => {
                  const done = e.completedTasks / Math.max(1, e.totalTasks);
                  return (
                    <motion.li key={e.id} layout={!reduce} initial={{ opacity: 0 }} animate={{ opacity: 1 }} exit={{ opacity: 0 }}>
                      <Link to={`/executions/${e.id}`} className="block px-4 py-2.5 hover:bg-raised/50">
                        <div className="flex items-center gap-3 text-sm">
                          <span className="min-w-0 flex-1 truncate font-medium">{e.workflowName}</span>
                          <Mono>{shortId(e.id)}</Mono>
                          <span className="tnum w-16 text-right text-xs text-ink-3">{ms(e.durationMs)}</span>
                        </div>
                        <div className="mt-2 flex items-center gap-3">
                          <div className="h-1 flex-1 overflow-hidden rounded-full bg-line-soft">
                            <motion.div className="h-full bg-cyan" initial={false} animate={{ width: `${done * 100}%` }} transition={{ duration: reduce ? 0 : 0.4 }} />
                          </div>
                          <span className="tnum text-2xs text-ink-3">{e.completedTasks}/{e.totalTasks}</span>
                        </div>
                      </Link>
                    </motion.li>
                  );
                })}
              </AnimatePresence>
            </ul>
          )}
          {(pending.data?.total ?? 0) > 0 && (
            <div className="border-t border-line-soft px-4 py-3">
              <div className="mb-2 text-xs text-ink-3">Waiting to start ({pending.data!.total})</div>
              <ul className="space-y-1 text-sm">
                {pending.data!.items.slice(0, 6).map((e) => (
                  <li key={e.id}><Link to={`/executions/${e.id}`} className="flex justify-between gap-3 hover:text-violet">
                    <span className="truncate">{e.workflowName}</span><span className="text-xs text-ink-3">{e.runAt ? `starts ${ago(e.runAt)}` : "queued"}</span>
                  </Link></li>
                ))}
              </ul>
            </div>
          )}
        </Panel>
        <Panel title="Event stream" bodyClass="p-0" actions={
          <Select value={filter} onChange={(e) => setFilter(e.target.value)} aria-label="Filter events">
            <option value="all">All events</option><option value="problems">Failures and retries</option>
            <option value="WORKFLOW_">Workflow events</option><option value="TASK_">Task events</option>
          </Select>}>
          <ol className="h-[560px] overflow-y-auto font-mono text-[0.7rem]" onMouseEnter={() => (paused.current = true)} onMouseLeave={() => (paused.current = false)}
            aria-live="off">
            {shown.length === 0 && <li className="px-4 py-6 font-sans text-sm text-ink-3">Waiting for events…</li>}
            {shown.map((e, i) => (
              <li key={`${e.at}-${e.type}-${e.nodeKey}-${i}`} className="grid grid-cols-[70px_140px_1fr] gap-2 border-b border-line-soft/60 px-4 py-1">
                <span className="text-ink-3">{time(e.at)}</span>
                <span className={cx("truncate", TONE[e.type] ?? "text-ink-2")}>{e.type.toLowerCase()}</span>
                <Link to={`/executions/${e.executionId}`} className="truncate text-ink-2 hover:text-ink">
                  {shortId(e.executionId)} {e.nodeKey && <span className="text-ink">{e.nodeKey}</span>} {e.attempt ? `#${e.attempt}` : ""} {e.workerId ?? ""} {e.message ?? ""}
                </Link>
              </li>
            ))}
          </ol>
          <p className="border-t border-line-soft px-4 py-2 text-2xs text-ink-3">Hover the stream to pause it.</p>
        </Panel>
      </div>
    </div>
  );
}
