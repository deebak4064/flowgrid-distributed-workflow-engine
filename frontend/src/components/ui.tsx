// shadcn/ui-style primitives (Radix + cva), restyled for the FlowGrid control plane.
import * as DialogPrimitive from "@radix-ui/react-dialog";
import * as TabsPrimitive from "@radix-ui/react-tabs";
import { cva, type VariantProps } from "class-variance-authority";
import { X } from "lucide-react";
import { forwardRef, type ButtonHTMLAttributes, type InputHTMLAttributes, type ReactNode, type SelectHTMLAttributes, type TextareaHTMLAttributes } from "react";
import { cx } from "@/lib/format";
import type { TaskStatus } from "@/lib/api";

const button = cva(
  "inline-flex items-center justify-center gap-1.5 whitespace-nowrap rounded-md text-sm font-medium transition-colors disabled:pointer-events-none disabled:opacity-45",
  {
    variants: {
      variant: {
        primary: "bg-violet text-[#16112e] hover:bg-[#ab9eff]",
        secondary: "bg-raised text-ink border border-line hover:bg-hover",
        ghost: "text-ink-2 hover:text-ink hover:bg-raised",
        danger: "bg-transparent border border-bad/50 text-bad hover:bg-bad/10",
      },
      size: { sm: "h-7 px-2.5 text-xs", md: "h-8 px-3", lg: "h-10 px-4" },
    },
    defaultVariants: { variant: "secondary", size: "md" },
  },
);

export const Button = forwardRef<HTMLButtonElement, ButtonHTMLAttributes<HTMLButtonElement> & VariantProps<typeof button>>(
  ({ className, variant, size, ...p }, ref) => <button ref={ref} className={cx(button({ variant, size }), className)} {...p} />,
);
Button.displayName = "Button";

export const Input = forwardRef<HTMLInputElement, InputHTMLAttributes<HTMLInputElement>>(({ className, ...p }, ref) => (
  <input ref={ref} className={cx("h-8 w-full rounded-md border border-line bg-ground px-2.5 text-sm text-ink placeholder:text-ink-3 focus:border-violet focus:outline-none", className)} {...p} />
));
Input.displayName = "Input";

export const Textarea = forwardRef<HTMLTextAreaElement, TextareaHTMLAttributes<HTMLTextAreaElement>>(({ className, ...p }, ref) => (
  <textarea ref={ref} className={cx("w-full rounded-md border border-line bg-ground px-2.5 py-2 font-mono text-xs text-ink placeholder:text-ink-3 focus:border-violet focus:outline-none", className)} {...p} />
));
Textarea.displayName = "Textarea";

export function Select({ className, children, ...p }: SelectHTMLAttributes<HTMLSelectElement>) {
  return (
    <select className={cx("h-8 rounded-md border border-line bg-ground px-2 text-sm text-ink focus:border-violet focus:outline-none", className)} {...p}>
      {children}
    </select>
  );
}

export function Field({ label, hint, children }: { label: string; hint?: string; children: ReactNode }) {
  return (
    <label className="block space-y-1">
      <span className="text-xs text-ink-2">{label}</span>
      {children}
      {hint && <span className="block text-2xs text-ink-3">{hint}</span>}
    </label>
  );
}

/** Surfaces have two levels only: panel (slate) on ground (charcoal). Hierarchy comes from spacing, not shadows. */
export function Panel({ title, actions, children, className, bodyClass }: { title?: ReactNode; actions?: ReactNode; children: ReactNode; className?: string; bodyClass?: string }) {
  return (
    <section className={cx("rounded-[10px] border border-line-soft bg-surface", className)}>
      {(title || actions) && (
        <header className="flex min-h-11 items-center justify-between gap-3 border-b border-line-soft px-4 py-2">
          <h2 className="text-sm font-semibold text-ink">{title}</h2>
          <div className="flex items-center gap-2">{actions}</div>
        </header>
      )}
      <div className={cx("p-4", bodyClass)}>{children}</div>
    </section>
  );
}

export function PageHeader({ title, description, actions }: { title: string; description?: ReactNode; actions?: ReactNode }) {
  return (
    <div className="mb-6 flex flex-wrap items-end justify-between gap-4">
      <div>
        <h1 className="text-[1.75rem] font-semibold leading-tight tracking-[-0.02em] text-ink">{title}</h1>
        {description && <p className="mt-1 max-w-[70ch] text-sm text-ink-2">{description}</p>}
      </div>
      {actions && <div className="flex flex-wrap items-center gap-2">{actions}</div>}
    </div>
  );
}

/** A reading: value large and tabular, label small beneath. Used for real numbers only. */
export function Reading({ label, value, sub, tone }: { label: string; value: ReactNode; sub?: ReactNode; tone?: "ok" | "warn" | "bad" | "cyan" | "violet" }) {
  const toneClass = { ok: "text-ok", warn: "text-warn", bad: "text-bad", cyan: "text-cyan", violet: "text-violet" }[tone ?? "ok"];
  return (
    <div className="min-w-0">
      <div className={cx("tnum text-2xl font-semibold tracking-tight", tone ? toneClass : "text-ink")}>{value}</div>
      <div className="mt-0.5 text-xs text-ink-2">{label}</div>
      {sub && <div className="mt-0.5 text-2xs text-ink-3">{sub}</div>}
    </div>
  );
}

export const STATUS_STYLE: Record<string, { dot: string; text: string; bg: string; label: string }> = {
  PENDING: { dot: "bg-ink-3", text: "text-ink-2", bg: "bg-ink-3/10", label: "Pending" },
  READY: { dot: "bg-ink-2", text: "text-ink-2", bg: "bg-ink-2/10", label: "Ready" },
  QUEUED: { dot: "bg-queued", text: "text-queued", bg: "bg-queued/12", label: "Queued" },
  RUNNING: { dot: "bg-cyan", text: "text-cyan", bg: "bg-cyan/10", label: "Running" },
  SUCCEEDED: { dot: "bg-ok", text: "text-ok", bg: "bg-ok/10", label: "Completed" },
  FAILED: { dot: "bg-bad", text: "text-bad", bg: "bg-bad/10", label: "Failed" },
  RETRYING: { dot: "bg-warn", text: "text-warn", bg: "bg-warn/10", label: "Retrying" },
  CANCELLED: { dot: "bg-ink-3", text: "text-ink-3", bg: "bg-ink-3/10", label: "Cancelled" },
  SKIPPED: { dot: "bg-transparent border border-ink-3", text: "text-ink-3", bg: "bg-transparent", label: "Skipped" },
  ACTIVE: { dot: "bg-ok", text: "text-ok", bg: "bg-ok/10", label: "Active" },
  DRAINING: { dot: "bg-warn", text: "text-warn", bg: "bg-warn/10", label: "Draining" },
  DEAD: { dot: "bg-bad", text: "text-bad", bg: "bg-bad/10", label: "Dead" },
  STOPPED: { dot: "bg-ink-3", text: "text-ink-3", bg: "bg-ink-3/10", label: "Stopped" },
};

export function Status({ value, className }: { value: TaskStatus | string; className?: string }) {
  const s = STATUS_STYLE[value] ?? STATUS_STYLE.PENDING;
  return (
    <span className={cx("inline-flex items-center gap-1.5 rounded-full px-2 py-0.5 text-xs font-medium", s.bg, s.text, className)}>
      <span className={cx("size-1.5 rounded-full", s.dot, value === "RUNNING" && "fg-pulse")} />
      {s.label}
    </span>
  );
}

export function Tag({ children }: { children: ReactNode }) {
  return <span className="rounded border border-line px-1.5 py-px text-2xs text-ink-2">{children}</span>;
}

export function Mono({ children, className }: { children: ReactNode; className?: string }) {
  return <span className={cx("font-mono text-[0.75rem] text-ink-2", className)}>{children}</span>;
}

export function Empty({ title, children, action }: { title: string; children?: ReactNode; action?: ReactNode }) {
  return (
    <div className="flex flex-col items-center justify-center gap-2 py-12 text-center">
      <p className="text-sm font-medium text-ink">{title}</p>
      {children && <p className="max-w-[48ch] text-sm text-ink-2">{children}</p>}
      {action && <div className="mt-2">{action}</div>}
    </div>
  );
}

export function ErrorNote({ error }: { error: unknown }) {
  if (!error) return null;
  const e = error as { message?: string; errors?: string[] };
  return (
    <div role="alert" className="rounded-md border border-bad/40 bg-bad/8 px-3 py-2 text-sm text-bad">
      {e.message ?? String(error)}
      {e.errors && e.errors.length > 0 && <ul className="mt-1 list-disc pl-5 text-xs">{e.errors.map((x) => <li key={x}>{x}</li>)}</ul>}
    </div>
  );
}

export function Table({ children, className }: { children: ReactNode; className?: string }) {
  return (
    <div className={cx("overflow-x-auto", className)}>
      <table className="w-full border-collapse text-sm [&_td]:border-t [&_td]:border-line-soft [&_td]:px-3 [&_td]:py-2 [&_th]:px-3 [&_th]:py-2 [&_th]:text-left [&_th]:text-xs [&_th]:font-medium [&_th]:text-ink-3">
        {children}
      </table>
    </div>
  );
}

export function Pager({ page, size, total, onPage }: { page: number; size: number; total: number; onPage: (p: number) => void }) {
  const pages = Math.max(1, Math.ceil(total / size));
  return (
    <div className="flex items-center justify-between px-3 py-2 text-xs text-ink-3">
      <span className="tnum">{total.toLocaleString()} total</span>
      <div className="flex items-center gap-2">
        <Button size="sm" variant="ghost" disabled={page === 0} onClick={() => onPage(page - 1)}>Previous</Button>
        <span className="tnum">Page {page + 1} of {pages}</span>
        <Button size="sm" variant="ghost" disabled={page + 1 >= pages} onClick={() => onPage(page + 1)}>Next</Button>
      </div>
    </div>
  );
}

export const Tabs = TabsPrimitive.Root;
export function TabList({ children }: { children: ReactNode }) {
  return <TabsPrimitive.List className="mb-4 flex gap-1 border-b border-line-soft">{children}</TabsPrimitive.List>;
}
export function TabTrigger({ value, children }: { value: string; children: ReactNode }) {
  return (
    <TabsPrimitive.Trigger value={value}
      className="-mb-px border-b-2 border-transparent px-3 py-2 text-sm text-ink-2 hover:text-ink data-[state=active]:border-violet data-[state=active]:text-ink">
      {children}
    </TabsPrimitive.Trigger>
  );
}
export const TabContent = TabsPrimitive.Content;

export function Dialog({ open, onOpenChange, title, description, children, wide }: {
  open: boolean; onOpenChange: (o: boolean) => void; title: string; description?: string; children: ReactNode; wide?: boolean;
}) {
  return (
    <DialogPrimitive.Root open={open} onOpenChange={onOpenChange}>
      <DialogPrimitive.Portal>
        <DialogPrimitive.Overlay className="fixed inset-0 z-40 bg-black/55" />
        <DialogPrimitive.Content className={cx("fixed left-1/2 top-1/2 z-50 max-h-[88vh] w-[calc(100vw-2rem)] -translate-x-1/2 -translate-y-1/2 overflow-y-auto rounded-lg border border-line bg-surface p-5", wide ? "max-w-3xl" : "max-w-lg")}>
          <div className="mb-4 flex items-start justify-between gap-4">
            <div>
              <DialogPrimitive.Title className="text-base font-semibold">{title}</DialogPrimitive.Title>
              {description && <DialogPrimitive.Description className="mt-1 text-sm text-ink-2">{description}</DialogPrimitive.Description>}
            </div>
            <DialogPrimitive.Close className="rounded p-1 text-ink-3 hover:bg-raised hover:text-ink" aria-label="Close"><X size={16} /></DialogPrimitive.Close>
          </div>
          {children}
        </DialogPrimitive.Content>
      </DialogPrimitive.Portal>
    </DialogPrimitive.Root>
  );
}

export function JsonView({ value, className }: { value: unknown; className?: string }) {
  return (
    <pre className={cx("max-h-72 overflow-auto rounded-md border border-line-soft bg-ground p-3 font-mono text-[0.72rem] leading-relaxed text-ink-2", className)}>
      {value === undefined || value === null ? "—" : JSON.stringify(value, null, 2)}
    </pre>
  );
}
