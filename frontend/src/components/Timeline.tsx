import { useEffect, useState } from "react";
import type { TaskView } from "@/lib/api";
import { cx, ms } from "@/lib/format";

const ATTEMPT_COLOR: Record<string, string> = {
  SUCCEEDED: "bg-ok", FAILED: "bg-bad", TIMED_OUT: "bg-bad", LOST: "bg-warn", RUNNING: "bg-cyan", QUEUED: "bg-queued",
  CANCELLED: "bg-ink-3", DISCARDED: "bg-ink-3",
};

/**
 * Gantt view of every attempt: a thin segment while queued (dispatch -> claim), a solid bar while executing.
 * Gaps between attempts are retry backoff.
 */
export function Timeline({ tasks, start, end, selected, onSelect }: {
  tasks: TaskView[]; start: string; end?: string; selected?: string | null; onSelect?: (k: string) => void;
}) {
  const [now, setNow] = useState(Date.now());
  useEffect(() => { if (end) return; const t = setInterval(() => setNow(Date.now()), 500); return () => clearInterval(t); }, [end]);
  const t0 = new Date(start).getTime();
  const t1 = Math.max(end ? new Date(end).getTime() : now, t0 + 1);
  const span = t1 - t0;
  const x = (iso?: string) => (iso ? ((new Date(iso).getTime() - t0) / span) * 100 : 0);
  const ordered = [...tasks].sort((a, b) => (a.readyAt ?? a.startedAt ?? "~").localeCompare(b.readyAt ?? b.startedAt ?? "~"));
  const ticks = [0, 0.25, 0.5, 0.75, 1];

  return (
    <div className="text-xs">
      <div className="relative ml-44 h-5 border-b border-line-soft text-2xs text-ink-3">
        {ticks.map((f) => (
          <span key={f} className="tnum absolute -translate-x-1/2" style={{ left: `${f * 100}%` }}>{ms(span * f)}</span>
        ))}
      </div>
      <ul>
        {ordered.map((t) => (
          <li key={t.id}>
            <button onClick={() => onSelect?.(t.nodeKey)}
              className={cx("flex w-full items-center rounded py-1 text-left hover:bg-raised/50", selected === t.nodeKey && "bg-raised")}>
              <span className="w-44 shrink-0 truncate pr-3 text-ink-2">{t.name}</span>
              <span className="relative h-4 flex-1">
                <span className="absolute inset-y-[7px] left-0 right-0 bg-line-soft" />
                {t.attempts.map((a) => {
                  const qs = x(a.queuedAt), st = a.startedAt ? x(a.startedAt) : null;
                  const fin = a.finishedAt ? x(a.finishedAt) : ((now - t0) / span) * 100;
                  return (
                    <span key={a.attemptId}>
                      <span className="absolute inset-y-[6px] bg-queued/45" style={{ left: `${qs}%`, width: `${Math.max(0.3, (st ?? fin) - qs)}%` }} />
                      {st !== null && (
                        <span title={`attempt ${a.attempt}: ${a.status.toLowerCase()}${a.durationMs != null ? `, ${ms(a.durationMs)}` : ""}${a.workerId ? ` on ${a.workerId}` : ""}`}
                          className={cx("absolute inset-y-[3px] rounded-sm", ATTEMPT_COLOR[a.status] ?? "bg-ink-3")}
                          style={{ left: `${st}%`, width: `${Math.max(0.5, fin - st)}%` }} />
                      )}
                    </span>
                  );
                })}
                {t.status === "SKIPPED" && <span className="absolute left-1 top-0 text-2xs text-ink-3">skipped</span>}
              </span>
            </button>
          </li>
        ))}
      </ul>
      <div className="mt-3 flex flex-wrap gap-4 text-2xs text-ink-3">
        <span className="flex items-center gap-1.5"><span className="h-1 w-4 bg-queued/45" />Waiting in queue</span>
        <span className="flex items-center gap-1.5"><span className="h-2 w-4 rounded-sm bg-ok" />Succeeded</span>
        <span className="flex items-center gap-1.5"><span className="h-2 w-4 rounded-sm bg-bad" />Failed or timed out</span>
        <span className="flex items-center gap-1.5"><span className="h-2 w-4 rounded-sm bg-warn" />Lost (worker died)</span>
        <span className="flex items-center gap-1.5"><span className="h-2 w-4 rounded-sm bg-cyan" />Running</span>
      </div>
    </div>
  );
}
