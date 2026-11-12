import { useState, type FormEvent } from "react";
import { useNavigate } from "react-router-dom";
import { api, session, type User } from "@/lib/api";
import { Button, ErrorNote, Field, Input } from "@/components/ui";

const DEMO = [
  { email: "admin@acme.test", role: "Admin" },
  { email: "operator@acme.test", role: "Operator" },
  { email: "viewer@acme.test", role: "Viewer" },
];

/** The insurance-claim workflow FlowGrid ships with, drawn as the sign-in mark: settled steps in green, one running. */
const N = [
  { k: "submit", l: "Submit", x: 20, y: 110, s: "ok" }, { k: "docs", l: "Documents", x: 150, y: 40, s: "ok" },
  { k: "cust", l: "Customer", x: 150, y: 180, s: "ok" }, { k: "amount", l: "Amount", x: 280, y: 180, s: "ok" },
  { k: "risk", l: "Risk", x: 410, y: 110, s: "run" }, { k: "approve", l: "Approve", x: 540, y: 40, s: "idle" },
  { k: "review", l: "Review", x: 540, y: 180, s: "idle" }, { k: "notify", l: "Notify", x: 670, y: 110, s: "idle" },
] as const;
const E: [string, string][] = [["submit", "docs"], ["submit", "cust"], ["cust", "amount"], ["docs", "risk"], ["amount", "risk"], ["risk", "approve"], ["risk", "review"], ["approve", "notify"], ["review", "notify"]];
const W = 96, H = 34;
const COLOR = { ok: "#4cc38a", run: "#2bddf2", idle: "#2d3443" };

function Mark() {
  const at = (k: string) => N.find((n) => n.k === k)!;
  return (
    <svg viewBox="0 0 786 254" className="w-full" aria-hidden>
      {E.map(([a, b]) => {
        const p = at(a), q = at(b);
        const x1 = p.x + W, y1 = p.y + H / 2, x2 = q.x, y2 = q.y + H / 2, mx = (x1 + x2) / 2;
        const d = `M${x1} ${y1} C ${mx} ${y1}, ${mx} ${y2}, ${x2} ${y2}`;
        const live = q.s === "run";
        return (
          <g key={a + b}>
            <path d={d} fill="none" stroke={live ? "#2bddf2" : p.s === "ok" && q.s === "ok" ? "#4cc38a88" : "#2d3443"} strokeWidth={live ? 1.8 : 1.3} />
            {live && <circle r="3.2" fill="#2bddf2" className="fg-mark-dot"><animateMotion dur="1.3s" repeatCount="indefinite" path={d} /></circle>}
          </g>
        );
      })}
      {N.map((n) => (
        <g key={n.k}>
          <rect x={n.x} y={n.y} width={W} height={H} rx="7" fill="#1a1f29" stroke={n.s === "run" ? "#2bddf2" : "#2d3443"} strokeWidth={n.s === "run" ? 1.5 : 1} />
          <rect x={n.x} y={n.y + 6} width="3" height={H - 12} rx="1.5" fill={COLOR[n.s]} />
          <text x={n.x + 14} y={n.y + 21.5} fill={n.s === "idle" ? "#6c7588" : "#e8ebf2"} fontSize="12.5" fontFamily="Instrument Sans Variable, sans-serif" fontWeight="600">{n.l}</text>
        </g>
      ))}
      <style>{`@media (prefers-reduced-motion:reduce){.fg-mark-dot{display:none}}`}</style>
    </svg>
  );
}

export default function Login() {
  const nav = useNavigate();
  const [email, setEmail] = useState("operator@acme.test");
  const [password, setPassword] = useState("");
  const [error, setError] = useState<unknown>(null);
  const [busy, setBusy] = useState(false);

  async function submit(e: FormEvent) {
    e.preventDefault();
    setBusy(true);
    setError(null);
    try {
      const r = await api<{ token: string; user: User }>("/auth/login", { method: "POST", json: { email, password } });
      session.save(r.token, r.user);
      nav("/", { replace: true });
    } catch (err) {
      setError(err);
    } finally {
      setBusy(false);
    }
  }

  return (
    <div className="grid min-h-full lg:grid-cols-[1.35fr_1fr]">
      <section className="grid-paper relative hidden flex-col justify-between overflow-hidden border-r border-line-soft p-12 lg:flex">
        <div className="flex items-center gap-2.5">
          <svg viewBox="0 0 32 32" className="size-8" aria-hidden>
            <rect width="32" height="32" rx="8" fill="#232937" />
            <g fill="none" strokeWidth="2.4" strokeLinecap="round"><path d="M8 10h6l4 6h6" stroke="#2BDDF2" /><path d="M8 22h6l4-6" stroke="#9A8BFF" /></g>
            <circle cx="8" cy="10" r="2.4" fill="#9A8BFF" /><circle cx="8" cy="22" r="2.4" fill="#9A8BFF" /><circle cx="24" cy="16" r="2.6" fill="#2BDDF2" />
          </svg>
          <span className="text-lg font-semibold tracking-tight">FlowGrid</span>
        </div>
        <div className="max-w-[44rem]">
          <h1 className="text-[3.25rem] font-semibold leading-[1.02] tracking-[-0.035em] [font-stretch:88%]">
            Workflows that finish, even when machines don't.
          </h1>
          <p className="mt-5 max-w-[52ch] text-base leading-relaxed text-ink-2">
            Define work as a dependency graph. FlowGrid schedules it, leases each task to a worker, and recovers the work
            of any worker that crashes, without running a business effect twice.
          </p>
          <div className="mt-12"><Mark /></div>
        </div>
        <p className="text-xs text-ink-3">Postgres for state, Redis Streams for dispatch, at-least-once delivery with idempotent effects.</p>
      </section>

      <div className="flex items-center justify-center p-6 sm:p-12">
        <form onSubmit={submit} className="w-full max-w-sm space-y-5">
          <div>
            <h2 className="text-2xl font-semibold tracking-[-0.02em]">Sign in</h2>
            <p className="mt-1 text-sm text-ink-2">Use your organization account.</p>
          </div>
          <Field label="Email"><Input className="h-10" type="email" autoComplete="username" value={email} onChange={(e) => setEmail(e.target.value)} required /></Field>
          <Field label="Password"><Input className="h-10" type="password" autoComplete="current-password" value={password} onChange={(e) => setPassword(e.target.value)} required /></Field>
          <ErrorNote error={error} />
          <Button type="submit" variant="primary" size="lg" className="w-full" disabled={busy}>{busy ? "Signing in…" : "Sign in"}</Button>
          <div className="border-t border-line-soft pt-5 text-xs text-ink-2">
            <p className="mb-2.5">Demo accounts use the password <code className="font-mono text-ink">flowgrid-demo</code>. Pick a role to fill them in:</p>
            <div className="grid grid-cols-3 gap-1.5">
              {DEMO.map((d) => (
                <button type="button" key={d.email} onClick={() => { setEmail(d.email); setPassword("flowgrid-demo"); }}
                  className={`rounded-md border px-2 py-1.5 text-left hover:border-violet hover:text-ink ${email === d.email ? "border-violet/70 text-ink" : "border-line"}`}>
                  <span className="block font-medium">{d.role}</span>
                </button>
              ))}
            </div>
          </div>
        </form>
      </div>
    </div>
  );
}
