import { twMerge } from "tailwind-merge";

/** Class join that resolves Tailwind conflicts (a later w-48 beats a component's w-full). */
export const cx = (...c: (string | false | null | undefined)[]) => twMerge(c.filter(Boolean).join(" "));

export function ms(v: number | null | undefined): string {
  if (v === null || v === undefined || Number.isNaN(v)) return "—";
  if (v < 1) return `${v.toFixed(2)} ms`;
  if (v < 1000) return `${Math.round(v)} ms`;
  if (v < 60_000) return `${(v / 1000).toFixed(v < 10_000 ? 2 : 1)} s`;
  if (v < 3_600_000) return `${Math.floor(v / 60_000)}m ${Math.round((v % 60_000) / 1000)}s`;
  return `${Math.floor(v / 3_600_000)}h ${Math.round((v % 3_600_000) / 60_000)}m`;
}

export function num(v: number | null | undefined, digits = 0): string {
  if (v === null || v === undefined) return "—";
  return Number(v).toLocaleString(undefined, { maximumFractionDigits: digits, minimumFractionDigits: digits });
}

export function pct(v: number | null | undefined, digits = 1): string {
  if (v === null || v === undefined) return "—";
  return `${(Number(v) * 100).toFixed(digits)}%`;
}

export function ago(iso?: string | null): string {
  if (!iso) return "—";
  const d = Date.now() - new Date(iso).getTime();
  const future = d < 0;
  const a = Math.abs(d);
  const s = a < 60_000 ? `${Math.max(1, Math.round(a / 1000))}s` : a < 3_600_000 ? `${Math.round(a / 60_000)}m`
    : a < 86_400_000 ? `${Math.round(a / 3_600_000)}h` : `${Math.round(a / 86_400_000)}d`;
  return future ? `in ${s}` : `${s} ago`;
}

export function time(iso?: string | null, withDate = false): string {
  if (!iso) return "—";
  const d = new Date(iso);
  return withDate
    ? d.toLocaleString(undefined, { month: "short", day: "numeric", hour: "2-digit", minute: "2-digit", second: "2-digit" })
    : d.toLocaleTimeString(undefined, { hour: "2-digit", minute: "2-digit", second: "2-digit" });
}

export const shortId = (id?: string | null) => (id ? id.slice(0, 8) : "—");
