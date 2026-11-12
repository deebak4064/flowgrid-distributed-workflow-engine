import { Area, AreaChart, Bar, BarChart, CartesianGrid, Line, LineChart, ResponsiveContainer, Tooltip, XAxis, YAxis } from "recharts";
import type { ReactNode } from "react";

/** Categorical order validated (OKLab CVD ΔE ≥ 8 adjacent) on the slate surface #1b1f27. Status colours are separate. */
export const SERIES = ["#1aa5c2", "#8070f5", "#d0609a"];
export const STATUS = { ok: "#45c08a", bad: "#ef6b6b", warn: "#f0b04a" };

const axis = { stroke: "#2f3542", tick: { fill: "#6f7787", fontSize: 11 }, tickLine: false, axisLine: false } as const;

export function ChartTooltip({ active, payload, label, format }: {
  active?: boolean; payload?: { name: string; value: number; color: string }[]; label?: string; format?: (v: number) => string;
}) {
  if (!active || !payload?.length) return null;
  return (
    <div className="rounded-md border border-line bg-raised px-2.5 py-2 text-xs shadow-lg">
      <div className="mb-1 text-ink-3">{label}</div>
      {payload.map((p) => (
        <div key={p.name} className="flex items-center gap-2">
          <span className="size-2 rounded-sm" style={{ background: p.color }} />
          <span className="text-ink-2">{p.name}</span>
          <span className="tnum ml-auto pl-3 font-medium text-ink">{p.value == null ? "—" : format ? format(p.value) : p.value.toLocaleString()}</span>
        </div>
      ))}
    </div>
  );
}

export function Legend({ items }: { items: { label: string; color: string }[] }) {
  return (
    <div className="flex flex-wrap gap-4 text-xs text-ink-2">
      {items.map((i) => <span key={i.label} className="flex items-center gap-1.5"><span className="h-0.5 w-3.5 rounded" style={{ background: i.color }} />{i.label}</span>)}
    </div>
  );
}

export type Series = { key: string; label: string; color: string };

export function LinesChart({ data, x, series, height = 220, format, empty }: {
  data: Record<string, unknown>[]; x: string; series: Series[]; height?: number; format?: (v: number) => string; empty?: ReactNode;
}) {
  if (!data.length) return <div className="flex items-center justify-center text-sm text-ink-3" style={{ height }}>{empty ?? "No data in this window"}</div>;
  return (
    <div style={{ height }}>
      <ResponsiveContainer>
        <LineChart data={data} margin={{ top: 8, right: 8, left: -12, bottom: 0 }}>
          <CartesianGrid stroke="#252a34" vertical={false} />
          <XAxis dataKey={x} {...axis} minTickGap={36} />
          <YAxis {...axis} width={56} tickFormatter={format} />
          <Tooltip content={<ChartTooltip format={format} />} cursor={{ stroke: "#6f7787", strokeDasharray: "3 3" }} />
          {series.map((s) => <Line key={s.key} type="monotone" dataKey={s.key} name={s.label} stroke={s.color} strokeWidth={2} dot={false} activeDot={{ r: 4, strokeWidth: 2, stroke: "#1b1f27" }} isAnimationActive={false} connectNulls />)}
        </LineChart>
      </ResponsiveContainer>
    </div>
  );
}

export function AreaSpark({ data, k, color, height = 36, max }: { data: Record<string, unknown>[]; k: string; color: string; height?: number; max?: number }) {
  return (
    <div style={{ height }}>
      <ResponsiveContainer>
        <AreaChart data={data} margin={{ top: 2, right: 0, left: 0, bottom: 0 }}>
          <YAxis hide domain={[0, max ?? "auto"]} />
          <Area type="monotone" dataKey={k} stroke={color} strokeWidth={1.5} fill={color} fillOpacity={0.15} isAnimationActive={false} />
        </AreaChart>
      </ResponsiveContainer>
    </div>
  );
}

export function BarsChart({ data, x, series, height = 220, format, layout = "horizontal" }: {
  data: Record<string, unknown>[]; x: string; series: Series[]; height?: number; format?: (v: number) => string; layout?: "horizontal" | "vertical";
}) {
  if (!data.length) return <div className="flex items-center justify-center text-sm text-ink-3" style={{ height }}>No data in this window</div>;
  const vertical = layout === "vertical";
  return (
    <div style={{ height }}>
      <ResponsiveContainer>
        <BarChart data={data} layout={vertical ? "vertical" : "horizontal"} margin={{ top: 8, right: 12, left: vertical ? 8 : -12, bottom: 0 }} barGap={2}>
          <CartesianGrid stroke="#252a34" vertical={vertical} horizontal={!vertical} />
          {vertical ? <><XAxis type="number" {...axis} tickFormatter={format} /><YAxis type="category" dataKey={x} {...axis} width={150} /></>
            : <><XAxis dataKey={x} {...axis} /><YAxis {...axis} width={56} tickFormatter={format} /></>}
          <Tooltip content={<ChartTooltip format={format} />} cursor={{ fill: "#ffffff08" }} />
          {series.map((s) => <Bar key={s.key} dataKey={s.key} name={s.label} fill={s.color} radius={vertical ? [0, 4, 4, 0] : [4, 4, 0, 0]} maxBarSize={22} isAnimationActive={false} />)}
        </BarChart>
      </ResponsiveContainer>
    </div>
  );
}
