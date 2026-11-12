import type { CSSProperties, ReactNode } from "react";
import type { Metrics } from "@/lib/api";
import { cx, ms, num } from "@/lib/format";

/**
 * The dispatch pipeline, live: every task moves left to right through the real stores that hold it.
 * Counts come from /metrics; particles move only while the event stream reports completions, at a rate tied to it.
 */
function Connector({ rate, active }: { rate: number; active: boolean }) {
  const n = active ? Math.min(5, Math.max(1, Math.ceil(rate / 8))) : 0;
  const d = 1.5;
  return (
    <div className="relative hidden h-full items-center xl:flex" aria-hidden>
      <div className={cx("h-px w-full", active ? "bg-cyan/40" : "bg-line")} />
      <svg className="absolute right-0 size-2 translate-x-1" viewBox="0 0 8 8"><path d="M0 0 8 4 0 8z" fill={active ? "var(--color-cyan)" : "var(--color-line)"} /></svg>
      <div className="absolute inset-x-0 top-1/2" style={{ offsetPath: "path('M 0 0 H 72')" } as CSSProperties}>
        {Array.from({ length: n }, (_, i) => (
          <span key={i} className="fg-particle" style={{ offsetPath: "path('M 0 0 H 72')", "--d": `${d}s`, "--delay": `${(-d * i) / n}s` } as CSSProperties} />
        ))}
      </div>
    </div>
  );
}

function Stage({ value, label, store, children, tone }: { value: ReactNode; label: string; store: string; children?: ReactNode; tone?: string }) {
  return (
    <div className="flex min-w-0 flex-col justify-between gap-4 rounded-lg border border-line-soft bg-ground/70 p-4">
      <div className="flex items-baseline justify-between gap-2">
        <span className="text-sm font-medium text-ink">{label}</span>
        <span className="text-2xs text-ink-3">{store}</span>
      </div>
      <div className={cx("tnum text-[2.75rem] font-medium leading-none tracking-[-0.03em]", tone ?? "text-ink")}>{value}</div>
      <div className="min-h-8 text-xs text-ink-3">{children}</div>
    </div>
  );
}

export function Pipeline({ m, rate }: { m?: Metrics; rate: number }) {
  const flowing = rate > 0;
  const q = m?.queue, w = m?.workers;
  const cells = Math.min(w?.capacity ?? 0, 64);
  const busy = w && w.capacity ? Math.round((w.in_flight / w.capacity) * cells) : 0;
  return (
    <section aria-label="Dispatch pipeline" className="grid-paper overflow-hidden rounded-[10px] border border-line-soft p-5">
      <div className="mb-5 flex flex-wrap items-end justify-between gap-3">
        <div>
          <h2 className="text-lg font-semibold tracking-[-0.01em]">Dispatch pipeline</h2>
          <p className="mt-0.5 max-w-[68ch] text-sm text-ink-2">Postgres holds every task's state, Redis Streams hands work to the fleet, workers lease and run it.</p>
        </div>
        <p className="tnum text-sm text-ink-2"><b className="font-semibold text-cyan">{rate.toFixed(rate < 10 ? 1 : 0)}</b> tasks/s right now</p>
      </div>
      <div className="grid gap-3 xl:grid-cols-[1fr_72px_1fr_72px_1fr_72px_1fr] xl:gap-0">
        <Stage label="Runnable" store="Postgres" value={num(q?.runnable)}>
          {q && <>{num(q.waiting_on_dependencies)} waiting on dependencies{q.backing_off > 0 && <>, <span className="text-warn">{num(q.backing_off)} backing off</span></>}
            {q.oldest_runnable_age_ms > 0 && <><br />oldest waiting {ms(q.oldest_runnable_age_ms)}</>}</>}
        </Stage>
        <Connector rate={rate} active={flowing || (q?.queued ?? 0) > 0} />
        <Stage label="Dispatched" store="Redis stream" value={num(q?.queued)}>
          {w && <>held to at most 2× fleet capacity ({num(w.capacity * 2)})</>}
        </Stage>
        <Connector rate={rate} active={flowing} />
        <Stage label="Running" store={`${num(w?.active)} workers`} value={num(q?.running)} tone={(q?.running ?? 0) > 0 ? "text-cyan" : undefined}>
          {w && (
            <div>
              <div className="flex h-2 gap-[2px]" role="img" aria-label={`${w.in_flight} of ${w.capacity} worker slots busy`}>
                {Array.from({ length: cells }, (_, i) => <span key={i} className={cx("flex-1 rounded-[1px]", i < busy ? "bg-cyan" : "bg-line")} />)}
              </div>
              <div className="mt-1.5">{num(w.in_flight)} of {num(w.capacity)} slots busy</div>
            </div>
          )}
        </Stage>
        <Connector rate={rate} active={flowing} />
        <Stage label="Settled" store={`last ${m?.windowMinutes ?? 15} min`} value={num(m?.tasks.succeeded)} tone="text-ok">
          {m && <>{num(m.tasks.failed_attempts)} failed attempts, {num(m.tasks.retries)} retried<br />{num(m.deadLetters)} open dead letters</>}
        </Stage>
      </div>
    </section>
  );
}
