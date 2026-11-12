import { useQuery, useQueryClient } from "@tanstack/react-query";
import {
  Activity, AlarmClock, BarChart3, Boxes, CalendarClock, Gauge, Inbox, LogOut, Menu, ScrollText, Server, Settings, Workflow, X,
} from "lucide-react";
import { createContext, useCallback, useContext, useEffect, useRef, useState, type ReactNode } from "react";
import { NavLink, Outlet, useLocation, useNavigate } from "react-router-dom";
import { api, session, type ExecEvent, type Metrics } from "@/lib/api";
import { cx, num } from "@/lib/format";
import { useEventStream, type LiveState } from "@/lib/live";

// ---------------------------------------------------------------- one org-wide SSE connection, shared by all screens

type Listener = (e: ExecEvent) => void;
const LiveCtx = createContext<{ state: LiveState; subscribe: (l: Listener) => () => void }>({ state: "connecting", subscribe: () => () => {} });

export function useLive(listener: Listener) {
  const { subscribe } = useContext(LiveCtx);
  const ref = useRef(listener);
  ref.current = listener;
  useEffect(() => subscribe((e) => ref.current(e)), [subscribe]);
}
export const useLiveState = () => useContext(LiveCtx).state;

function LiveProvider({ children }: { children: ReactNode }) {
  const listeners = useRef(new Set<Listener>());
  const qc = useQueryClient();
  const pending = useRef<number | null>(null);
  const state = useEventStream("/stream", (e) => {
    listeners.current.forEach((l) => l(e));
    // Coalesce cache refreshes: under load hundreds of events per second arrive.
    if (pending.current === null) {
      pending.current = window.setTimeout(() => {
        pending.current = null;
        qc.invalidateQueries({ queryKey: ["pulse"] });
      }, 1500);
    }
  });
  const subscribe = useCallback((l: Listener) => { listeners.current.add(l); return () => { listeners.current.delete(l); }; }, []);
  return <LiveCtx.Provider value={{ state, subscribe }}>{children}</LiveCtx.Provider>;
}

// ---------------------------------------------------------------- navigation

const NAV: { group: string; items: { to: string; label: string; icon: typeof Activity }[] }[] = [
  { group: "Operate", items: [
    { to: "/", label: "Overview", icon: Gauge },
    { to: "/live", label: "Live monitor", icon: Activity },
    { to: "/executions", label: "Executions", icon: ScrollText },
    { to: "/dead-letters", label: "Retries & dead letters", icon: Inbox },
  ] },
  { group: "Build", items: [
    { to: "/workflows", label: "Workflows", icon: Workflow },
    { to: "/schedules", label: "Schedules", icon: CalendarClock },
  ] },
  { group: "Infrastructure", items: [
    { to: "/workers", label: "Workers", icon: Server },
    { to: "/queues", label: "Queues", icon: Boxes },
  ] },
  { group: "Observe", items: [
    { to: "/analytics", label: "Performance", icon: BarChart3 },
    { to: "/logs", label: "Logs", icon: AlarmClock },
  ] },
];

function Brand() {
  return (
    <div className="flex items-center gap-2.5 px-2">
      <svg viewBox="0 0 32 32" className="size-7" aria-hidden>
        <rect width="32" height="32" rx="8" fill="#232937" />
        <g fill="none" strokeWidth="2.4" strokeLinecap="round"><path d="M8 10h6l4 6h6" stroke="#2BDDF2" /><path d="M8 22h6l4-6" stroke="#9A8BFF" /></g>
        <circle cx="8" cy="10" r="2.4" fill="#9A8BFF" /><circle cx="8" cy="22" r="2.4" fill="#9A8BFF" /><circle cx="24" cy="16" r="2.6" fill="#2BDDF2" />
      </svg>
      <div className="leading-tight">
        <div className="text-[0.95rem] font-semibold tracking-tight">FlowGrid</div>
        <div className="text-2xs text-ink-3">{session.user()?.orgName}</div>
      </div>
    </div>
  );
}

/** Completed tasks per second over the last 60 s, counted from the live event stream (not polled, not estimated). */
export function useThroughput() {
  const [buckets, setBuckets] = useState<number[]>(() => Array(60).fill(0));
  const counts = useRef<Map<number, number>>(new Map());
  useLive((e) => {
    if (e.type !== "TASK_SUCCEEDED") return;
    const s = Math.floor(Date.now() / 1000);
    counts.current.set(s, (counts.current.get(s) ?? 0) + 1);
  });
  useEffect(() => {
    const t = setInterval(() => {
      const now = Math.floor(Date.now() / 1000);
      setBuckets(Array.from({ length: 60 }, (_, i) => counts.current.get(now - 60 + i) ?? 0));
      for (const k of counts.current.keys()) if (k < now - 61) counts.current.delete(k);
    }, 1000);
    return () => clearInterval(t);
  }, []);
  return buckets;
}

function Sparkline({ values }: { values: number[] }) {
  const max = Math.max(1, ...values);
  return (
    <svg viewBox="0 0 120 24" className="h-6 w-[120px]" aria-hidden preserveAspectRatio="none">
      {values.map((v, i) => {
        const h = v === 0 ? 1 : 2 + (v / max) * 22;
        return <rect key={i} x={i * 2} y={24 - h} width={1.4} height={h} fill={v === 0 ? "var(--color-line)" : "var(--color-cyan)"} />;
      })}
    </svg>
  );
}

function Pulse() {
  const live = useLiveState();
  const { data } = useQuery({ queryKey: ["pulse"], queryFn: () => api<Metrics>("/metrics?windowMinutes=5"), refetchInterval: 5000 });
  const tp = useThroughput();
  const rate = tp.slice(-5).reduce((a, b) => a + b, 0) / 5;
  const dot = { live: "bg-ok", connecting: "bg-ink-3", reconnecting: "bg-warn" }[live];
  const label = { live: "Live", connecting: "Connecting", reconnecting: "Reconnecting" }[live];
  return (
    <div className="flex items-center gap-6 text-xs text-ink-2">
      <span className="hidden items-center gap-3 md:flex" title="Tasks completed per second, last 60 seconds, from the live event stream">
        <Sparkline values={tp} />
        <span><b className="tnum font-semibold text-ink">{rate.toFixed(rate < 10 ? 1 : 0)}</b> tasks/s</span>
      </span>
      {data && (
        <>
          <span className="hidden sm:inline"><b className="tnum font-semibold text-ink">{num(data.workers.active)}</b> workers</span>
          <span className="hidden sm:inline"><b className="tnum font-semibold text-cyan">{num(data.queue.running)}</b> running</span>
          <span><b className="tnum font-semibold text-ink">{num(data.queue.queued + data.queue.runnable)}</b> waiting</span>
        </>
      )}
      <span className="flex items-center gap-1.5 border-l border-line-soft pl-5" title="Event stream connection">
        <span className={cx("size-1.5 rounded-full", dot, live === "live" && "fg-pulse")} />{label}
      </span>
    </div>
  );
}

export function Shell() {
  const [open, setOpen] = useState(false);
  const loc = useLocation();
  const nav = useNavigate();
  const user = session.user();
  useEffect(() => setOpen(false), [loc.pathname]);

  const sidebar = (
    <nav aria-label="Main" className="flex h-full flex-col gap-5 overflow-y-auto px-3 py-4">
      <Brand />
      {NAV.map((g) => (
        <div key={g.group}>
          <div className="mb-1 px-2 text-2xs text-ink-3">{g.group}</div>
          <ul className="space-y-px">
            {g.items.map((i) => (
              <li key={i.to}>
                <NavLink to={i.to} end={i.to === "/"}
                  className={({ isActive }) => cx("flex items-center gap-2.5 rounded-md px-2 py-1.5 text-sm",
                    isActive ? "bg-raised text-ink shadow-[inset_2px_0_0_var(--color-violet)]" : "text-ink-2 hover:bg-raised/60 hover:text-ink")}>
                  <i.icon size={15} strokeWidth={1.8} aria-hidden />{i.label}
                </NavLink>
              </li>
            ))}
          </ul>
        </div>
      ))}
      <div className="mt-auto space-y-px">
        <NavLink to="/settings" className={({ isActive }) => cx("flex items-center gap-2.5 rounded-md px-2 py-1.5 text-sm", isActive ? "bg-raised text-ink" : "text-ink-2 hover:bg-raised/60 hover:text-ink")}>
          <Settings size={15} strokeWidth={1.8} aria-hidden />Settings
        </NavLink>
        <button onClick={() => { session.clear(); nav("/login"); }} className="flex w-full items-center gap-2.5 rounded-md px-2 py-1.5 text-sm text-ink-2 hover:bg-raised/60 hover:text-ink">
          <LogOut size={15} strokeWidth={1.8} aria-hidden />Sign out
        </button>
        <div className="px-2 pt-2 text-2xs text-ink-3">{user?.email} <span className="text-ink-2">({user?.role.toLowerCase()})</span></div>
      </div>
    </nav>
  );

  return (
    <LiveProvider>
      <div className="flex h-full">
        <aside className="hidden w-60 shrink-0 border-r border-line-soft bg-surface lg:block">{sidebar}</aside>
        {open && (
          <div className="fixed inset-0 z-40 lg:hidden" role="dialog" aria-modal="true">
            <div className="absolute inset-0 bg-black/60" onClick={() => setOpen(false)} />
            <aside className="absolute inset-y-0 left-0 w-64 border-r border-line bg-surface">{sidebar}</aside>
          </div>
        )}
        <div className="flex min-w-0 flex-1 flex-col">
          <header className="flex h-14 shrink-0 items-center justify-between gap-3 border-b border-line-soft bg-ground/80 px-4 backdrop-blur">
            <button className="rounded p-1 text-ink-2 hover:bg-raised lg:hidden" onClick={() => setOpen((o) => !o)} aria-label="Toggle navigation">
              {open ? <X size={18} /> : <Menu size={18} />}
            </button>
            <div className="hidden lg:block" />
            <Pulse />
          </header>
          <main className="min-w-0 flex-1 overflow-y-auto px-4 py-7 md:px-10"><div className="mx-auto max-w-[1600px]"><Outlet /></div></main>
        </div>
      </div>
    </LiveProvider>
  );
}
