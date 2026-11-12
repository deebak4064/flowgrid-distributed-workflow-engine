// Task handlers: the "execution functions" a workflow node references by type.
// Handlers must be safe to run more than once for the same logical task (delivery is at-least-once). Any externally
// visible side effect goes through once(), keyed by the task's stable idempotency key.
import crypto from "node:crypto";
import type pg from "pg";
import { tx, type Db } from "./db.ts";

export interface Ctx {
  taskId: string; attemptId: string; attempt: number; maxAttempts: number; executionId: string; orgId: string; nodeKey: string;
  taskType: string; idempotencyKey: string; workerId: string; params: Record<string, any>; input: Record<string, any>;
  results: Record<string, any>; signal: AbortSignal; db: Db;
}
export interface Handler {
  type: string; description: string; timeoutMs: number; maxAttempts: number; backoffMs: number;
  run(ctx: Ctx): Promise<Record<string, unknown>>;
}

/** Throw from a handler to control retry classification. Any other error is treated as retryable. */
export class TaskFailure extends Error {
  readonly retryable: boolean; readonly errorType: string;
  constructor(message: string, retryable: boolean, errorType: string) { super(message); this.retryable = retryable; this.errorType = errorType; }
  static permanent = (m: string) => new TaskFailure(m, false, "BUSINESS_RULE");
  static transient = (m: string) => new TaskFailure(m, true, "TRANSIENT");
}

/** Node parameter first, then execution input: lets a workflow pin a value or leave it to the caller. */
export const arg = (ctx: Ctx, name: string) => ctx.params?.[name] ?? ctx.input?.[name];
/** Output of an already-completed task of this execution. */
const res = (ctx: Ctx, nodeKey: string) => ctx.results?.[nodeKey] ?? {};

export function abortableSleep(ms: number, signal: AbortSignal): Promise<void> {
  return new Promise((resolve, reject) => {
    if (signal.aborted) return reject(signal.reason);
    const t = setTimeout(() => { signal.removeEventListener("abort", onAbort); resolve(); }, ms);
    const onAbort = () => { clearTimeout(t); reject(signal.reason); };
    signal.addEventListener("abort", onAbort, { once: true });
  });
}

/**
 * Effectively-once business effects on top of at-least-once delivery. The idempotency row is INSERTed before the
 * effect, in the same transaction:
 *  - first execution: insert succeeds, effect runs, result stored, all commit atomically;
 *  - retry after commit: insert conflicts, the stored result is returned, the effect is NOT repeated;
 *  - crash before commit: everything rolls back, the retry starts clean;
 *  - two concurrent duplicates: the second INSERT blocks on the primary key until the first commits, then conflicts.
 */
export async function once(ctx: Ctx, scope: string, effect: (c: pg.PoolClient) => Promise<Record<string, unknown>>) {
  return tx(ctx.db, async ({ c }) => {
    const ins = await c.query("insert into idempotency_records(scope, key, org_id) values ($1, $2, $3) on conflict do nothing", [scope, ctx.idempotencyKey, ctx.orgId]);
    if (!ins.rowCount) return (await c.query("select result from idempotency_records where scope = $1 and key = $2", [scope, ctx.idempotencyKey])).rows[0].result;
    const result = await effect(c);
    await c.query("update idempotency_records set result = $1::jsonb where scope = $2 and key = $3", [JSON.stringify(result), scope, ctx.idempotencyKey]);
    return result;
  });
}

// ---------------------------------------------------------------- failure injection

/**
 * Configurable failure injection, driven by a node's (or the execution input's) "chaos" object:
 *   {"chaos": {"latencyMs": [20, 80], "failRate": 0.2, "permanentFailRate": 0.01, "timeoutRate": 0.05,
 *              "crashAfterEffectRate": 0.1, "crashAfterEffectTimes": 2}}
 * plus the process-wide FLOWGRID_CHAOS_FAILURE_RATE. crashAfterEffect fails the attempt AFTER the handler's side
 * effects committed, which is exactly the case idempotency keys exist for.
 */
const roll = (p: unknown) => typeof p === "number" && p > 0 && Math.random() < p;
export async function chaosBefore(ctx: Ctx, globalFailureRate: number) {
  const c = arg(ctx, "chaos") ?? {};
  const l = c.latencyMs;
  if (Array.isArray(l) && l.length === 2) await abortableSleep(l[0] + Math.random() * (l[1] - l[0]), ctx.signal);
  else if (typeof l === "number") await abortableSleep(l, ctx.signal);
  if (roll(c.timeoutRate)) await abortableSleep(2 ** 31 - 1, ctx.signal); // the worker's timeout fires
  if (roll(c.permanentFailRate)) throw TaskFailure.permanent("injected permanent failure");
  if (roll(c.failRate) || roll(globalFailureRate)) throw TaskFailure.transient("injected transient failure");
}
export function chaosAfter(ctx: Ctx) {
  const c = arg(ctx, "chaos") ?? {};
  if (roll(c.crashAfterEffectRate) || (typeof c.crashAfterEffectTimes === "number" && ctx.attempt <= c.crashAfterEffectTimes))
    throw new TaskFailure("injected crash after side effect, before completion was persisted", true, "CRASH_AFTER_EFFECT");
}

// ---------------------------------------------------------------- core

const h = (type: string, description: string, timeoutMs: number, maxAttempts: number, backoffMs: number, run: Handler["run"]): Handler =>
  ({ type, description, timeoutMs, maxAttempts, backoffMs, run });

const core: Handler[] = [
  h("core.noop", "Does nothing; measures pure engine overhead", 5000, 3, 200, async () => ({ ok: true })),
  h("core.sleep", "Simulated I/O-bound work: sleeps durationMs (number or [min,max])", 60_000, 3, 200, async (ctx) => {
    const d = arg(ctx, "durationMs");
    const ms = Array.isArray(d) ? Math.round(d[0] + Math.random() * (d[1] - d[0])) : typeof d === "number" ? d : 50;
    await abortableSleep(ms, ctx.signal);
    return { sleptMs: ms };
  }),
  h("core.compute", "CPU-bound work: iterated SHA-256 (iterations param); yields to the event loop every 1024 rounds", 60_000, 3, 200, async (ctx) => {
    const n = Number(arg(ctx, "iterations") ?? 10_000);
    let hsh = Buffer.from(ctx.idempotencyKey);
    for (let i = 0; i < n; i++) {
      if ((i & 1023) === 0) { await new Promise((r) => setImmediate(r)); ctx.signal.throwIfAborted(); }
      hsh = crypto.createHash("sha256").update(hsh).digest();
    }
    return { iterations: n, digest: hsh.toString("hex").slice(0, 16) };
  }),
  h("core.flaky", "Fails the first failTimes attempts (transient), then succeeds; permanent=true never succeeds", 10_000, 5, 200, async (ctx) => {
    if (arg(ctx, "permanent")) throw TaskFailure.permanent("configured to fail permanently");
    const failTimes = Number(arg(ctx, "failTimes") ?? 1);
    if (ctx.attempt <= failTimes) throw TaskFailure.transient(`simulated outage (attempt ${ctx.attempt} of ${failTimes} failing)`);
    return { succeededOnAttempt: ctx.attempt };
  }),
  h("core.notify", "Records an outbound notification exactly once per logical task", 10_000, 5, 300, async (ctx) => {
    const recipient = String(arg(ctx, "recipient") ?? "ops@flowgrid.local"), subject = String(arg(ctx, "subject") ?? "FlowGrid notification");
    return once(ctx, "core.notify", async (c) => {
      const id = (await c.query(`insert into notifications(org_id, channel, recipient, subject, body, idempotency_key)
        values ($1, 'email', $2, $3, $4, $5) returning id`, [ctx.orgId, recipient, subject, `execution ${ctx.executionId}`, ctx.idempotencyKey])).rows[0].id;
      return { notificationId: id, recipient };
    });
  }),
];

// ---------------------------------------------------------------- insurance claims

/** Deterministic PRNG (mulberry32) seeded from a string, so a retried task regenerates identical synthetic data. */
export function rng(seed: string) {
  let a = crypto.createHash("md5").update(seed).digest().readUInt32LE(0);
  const next = () => { a = (a + 0x6d2b79f5) | 0; let t = Math.imul(a ^ (a >>> 15), 1 | a); t = (t + Math.imul(t ^ (t >>> 7), 61 | t)) ^ t; return ((t ^ (t >>> 14)) >>> 0) / 4294967296; };
  return { next, int: (n: number) => Math.floor(next() * n), gauss: () => Math.sqrt(-2 * Math.log(1 - next())) * Math.cos(2 * Math.PI * next()) };
}

export const REQUIRED_DOCUMENTS: Record<string, string[]> = {
  AUTO: ["police_report", "photos", "repair_estimate"], HEALTH: ["medical_report", "invoice"],
  PROPERTY: ["photos", "ownership_proof", "repair_estimate"], TRAVEL: ["boarding_pass", "receipts"],
};
const TYPES = ["AUTO", "AUTO", "AUTO", "HEALTH", "HEALTH", "PROPERTY", "TRAVEL"];
const DESCRIPTIONS = ["Rear-end collision at a traffic signal", "Water damage from burst pipe", "Emergency room visit after fall",
  "Lost luggage on connecting flight", "Hail damage to roof and vehicle", "Windscreen cracked by debris", "Outpatient surgery and follow-up",
  "Kitchen fire, smoke damage", "Trip cancelled due to illness"];
export const CUSTOMER_COUNT = 500;

export function syntheticClaim(seed: string, claimNumber: string) {
  const r = rng(seed);
  const type = TYPES[r.int(TYPES.length)];
  const base = { AUTO: 4000, HEALTH: 2500, PROPERTY: 12_000 }[type] ?? 900;
  const amount = Math.round(base * Math.exp(r.gauss() * 0.8) * 100) / 100; // log-normal-ish, long tail of large claims
  const docs = [...REQUIRED_DOCUMENTS[type]];
  if (r.next() < 0.12) docs.splice(r.int(docs.length), 1); // ~12% arrive incomplete
  return { claimNumber, customerId: `CUST-${String(1 + r.int(CUSTOMER_COUNT)).padStart(4, "0")}`, claimType: type, amount: Math.max(50, amount),
    incidentDate: new Date(Date.now() - (1 + r.int(60)) * 86_400_000).toISOString().slice(0, 10), description: DESCRIPTIONS[r.int(DESCRIPTIONS.length)], documents: docs };
}

const decision = (type: string, description: string, outcome: string, claimStatus: string) => h(type, description, 10_000, 5, 300, async (ctx) => {
  const claim = res(ctx, "submit"), risk = res(ctx, "risk");
  const payable = outcome === "REJECTED" ? 0 : Number(res(ctx, "amount").payable ?? 0);
  return once(ctx, type, async (c) => {
    const id = (await c.query(`insert into claim_decisions(org_id, claim_id, decision, risk_score, payable_amount, reason, idempotency_key)
      values ($1, $2, $3, $4, $5, $6, $7) returning id`, [ctx.orgId, claim.claimId, outcome, risk.riskScore, payable, (risk.factors ?? []).join("; "), ctx.idempotencyKey])).rows[0].id;
    await c.query("update claims set status = $1 where id = $2", [claimStatus, claim.claimId]);
    const hash = [...String(claim.claimNumber)].reduce((a, ch) => (a * 31 + ch.charCodeAt(0)) | 0, 0);
    return { decisionId: id, decision: outcome, payable, assignee: outcome === "MANUAL_REVIEW" ? `adjuster-${1 + (Math.abs(hash) % 12)}` : "system" };
  });
});

const claims: Handler[] = [
  h("claims.submit", "Persist the submitted claim (idempotent)", 10_000, 4, 300, async (ctx) => {
    const inp = ctx.input ?? {};
    const claimNumber = `CLM-${ctx.executionId.slice(0, 8).toUpperCase()}`;
    // Seeded from the execution id: a retry regenerates the identical claim.
    const claim: Record<string, any> = inp.synthetic || !inp.customerId ? syntheticClaim(ctx.executionId, claimNumber) : { claimNumber, ...inp };
    if (!(Number(claim.amount) > 0)) throw TaskFailure.permanent("claim amount must be positive");
    return once(ctx, "claims.submit", async (c) => {
      const id = (await c.query(`insert into claims(org_id, claim_number, customer_id, claim_type, amount, incident_date, description, documents, workflow_execution_id, idempotency_key)
        values ($1, $2, $3, $4, $5, $6::date, $7, $8::jsonb, $9, $10) returning id`,
        [ctx.orgId, claim.claimNumber, claim.customerId, claim.claimType, claim.amount, claim.incidentDate, claim.description ?? "",
          JSON.stringify(claim.documents ?? []), ctx.executionId, ctx.idempotencyKey])).rows[0].id;
      const { chaos: _c, synthetic: _s, ...clean } = claim;
      return { ...clean, claimId: id };
    });
  }),
  h("claims.validate_documents", "Check required documents for the claim type", 10_000, 3, 300, async (ctx) => {
    const claim = res(ctx, "submit");
    const required = REQUIRED_DOCUMENTS[claim.claimType] ?? [];
    const received: string[] = claim.documents ?? [];
    const missing = required.filter((d) => !received.includes(d));
    return { valid: missing.length === 0, missing, received };
  }),
  h("claims.fetch_customer", "Retrieve policy holder and coverage (core policy system)", 8000, 5, 500, async (ctx) => {
    const id = res(ctx, "submit").customerId;
    const r = (await ctx.db.query(`select id, name, tier, policy_number, policy_limit, deductible, customer_since, prior_claims
      from customers where org_id = $1 and id = $2`, [ctx.orgId, id])).rows[0];
    if (!r) throw TaskFailure.permanent(`customer ${id} not found in policy system`);
    return { customerId: r.id, name: r.name, tier: r.tier, policyNumber: r.policy_number, policyLimit: r.policy_limit, deductible: r.deductible,
      tenureYears: Math.round((Date.now() - new Date(r.customer_since).getTime()) / (365.25 * 86_400_000) * 100) / 100, priorClaims: r.prior_claims };
  }),
  h("claims.validate_amount", "Apply deductible and policy limit", 5000, 3, 200, async (ctx) => {
    const amount = Number(res(ctx, "submit").amount), cust = res(ctx, "customer");
    const limit = Number(cust.policyLimit), deductible = Number(cust.deductible);
    const payable = Math.round(Math.min(Math.max(amount - deductible, 0), limit) * 100) / 100;
    return { claimed: amount, deductible, policyLimit: limit, payable, exceedsLimit: amount > limit, belowDeductible: amount <= deductible,
      limitRatio: Math.round((amount / limit) * 10_000) / 10_000 };
  }),
  h("claims.risk_analysis", "Score fraud/complexity risk and choose the approval route", 15_000, 3, 500, async (ctx) => {
    const claim = res(ctx, "submit"), docs = res(ctx, "documents"), cust = res(ctx, "customer"), amt = res(ctx, "amount");
    let score = 10;
    const factors: string[] = [];
    const missing = (docs.missing ?? []).length;
    if (missing > 0) { score += 20 * missing; factors.push(`${missing} required document(s) missing`); }
    const ratio = Number(amt.limitRatio ?? 0);
    score += Math.min(30, ratio * 30);
    if (ratio > 0.5) factors.push(`claim is ${Math.round(ratio * 100)}% of policy limit`);
    const prior = Number(cust.priorClaims ?? 0);
    if (prior > 0) { score += 8 * prior; factors.push(`${prior} prior claim(s)`); }
    if (Number(cust.tenureYears) < 1) { score += 15; factors.push("customer for less than a year"); }
    if (Number(claim.amount) > 25_000) { score += 10; factors.push("high-value claim"); }
    score -= cust.tier === "PLATINUM" ? 10 : cust.tier === "GOLD" ? 5 : 0;
    score = Math.max(0, Math.min(100, Math.round(score * 10) / 10));
    const route = amt.belowDeductible || score >= 75 ? "REJECT" : score < 35 && docs.valid && !amt.exceedsLimit ? "AUTO_APPROVE" : "MANUAL_REVIEW";
    return { riskScore: score, route, factors: factors.length ? factors : ["no risk indicators"] };
  }),
  decision("claims.auto_approve", "Approve and schedule payout", "AUTO_APPROVED", "APPROVED"),
  decision("claims.manual_review", "Route to an adjuster's review queue", "MANUAL_REVIEW", "IN_REVIEW"),
  decision("claims.reject", "Reject the claim with reasons", "REJECTED", "REJECTED"),
  h("claims.notify", "Notify the policy holder of the decision", 10_000, 5, 500, async (ctx) => {
    const claim = res(ctx, "submit"), cust = res(ctx, "customer");
    const d = [res(ctx, "approve"), res(ctx, "review"), res(ctx, "reject")].find((x) => x.decision) ?? {};
    const recipient = `${String(cust.customerId).toLowerCase()}@customers.example`;
    const subject = `Claim ${claim.claimNumber}: ${d.decision ?? "UPDATED"}`;
    return once(ctx, "claims.notify", async (c) => {
      const id = (await c.query(`insert into notifications(org_id, claim_id, channel, recipient, subject, body, idempotency_key)
        values ($1, $2, 'email', $3, $4, $5, $6) returning id`, [ctx.orgId, claim.claimId, recipient, subject,
        `Dear ${cust.name}, your claim was ${String(d.decision ?? "updated").toLowerCase()}. Payable: ${d.payable ?? 0}`, ctx.idempotencyKey])).rows[0].id;
      return { notificationId: id, recipient, subject };
    });
  }),
];

// ---------------------------------------------------------------- month-end financial reporting

const REGIONS = ["APAC", "EMEA", "NA", "LATAM"], PRODUCTS = ["auto", "health", "property", "travel", "life"];
/** Ledger rows generated deterministically from (period, rows), so parallel aggregators see identical data. */
function* ledger(period: string, rows: number) {
  const r = rng(`${period}:${rows}`);
  for (let i = 0; i < rows; i++) yield { region: REGIONS[r.int(4)], product: PRODUCTS[r.int(5)], cents: (r.next() < 0.5 ? -1 : 1) * Math.floor(r.next() * 500_000) };
}
const period = (ctx: Ctx) => String(arg(ctx, "period") || new Date(new Date().getFullYear(), new Date().getMonth() - 1, 15).toISOString().slice(0, 7));

const finance: Handler[] = [
  h("finance.extract_ledger", "Extract the general ledger for the period", 30_000, 4, 1000, async (ctx) => {
    const p = period(ctx), rows = Number(arg(ctx, "rows") ?? 20_000);
    let debits = 0, credits = 0;
    for (const e of ledger(p, rows)) if (e.cents < 0) debits -= e.cents; else credits += e.cents;
    return { period: p, rows, debitsCents: debits, creditsCents: credits, netCents: credits - debits };
  }),
  h("finance.aggregate", "Aggregate ledger net by a dimension (param: dimension=region|product)", 30_000, 3, 500, async (ctx) => {
    const ex = res(ctx, "extract"), dim = String(arg(ctx, "dimension") ?? "region");
    const groups: Record<string, number> = {};
    for (const e of ledger(ex.period, ex.rows)) { const k = dim === "product" ? e.product : e.region; groups[k] = (groups[k] ?? 0) + e.cents; }
    return { dimension: dim, groups, totalCents: Object.values(groups).reduce((a, b) => a + b, 0) };
  }),
  h("finance.reconcile", "Check every aggregate ties back to the extracted net", 10_000, 2, 500, async (ctx) => {
    const net = res(ctx, "extract").netCents, a = res(ctx, "by_region").totalCents, b = res(ctx, "by_product").totalCents;
    if (net !== a || net !== b) throw TaskFailure.permanent(`ledger does not reconcile: net=${net} region=${a} product=${b}`);
    return { balanced: true, netCents: net };
  }),
  h("finance.render_report", "Render and store the month-end report (idempotent)", 20_000, 4, 1000, async (ctx) => {
    const ex = res(ctx, "extract");
    const summary = { extract: ex, byRegion: res(ctx, "by_region").groups, byProduct: res(ctx, "by_product").groups };
    return once(ctx, "finance.render_report", async (c) => {
      const id = (await c.query("insert into financial_reports(org_id, period, summary, idempotency_key) values ($1, $2, $3::jsonb, $4) returning id",
        [ctx.orgId, ex.period, JSON.stringify(summary), ctx.idempotencyKey])).rows[0].id;
      return { reportId: id, period: ex.period };
    });
  }),
];

export const HANDLERS = new Map([...core, ...claims, ...finance].map((x) => [x.type, x]));

/** Publish the registry to task_definitions so the API can validate workflow definitions against it. */
export async function publishDefinitions(db: Db) {
  for (const x of HANDLERS.values())
    await db.query(`insert into task_definitions(org_id, type, description, category, default_timeout_ms, default_max_attempts, default_backoff_ms)
      values (null, $1, $2, $3, $4, $5, $6) on conflict (org_id, type) do update set description = excluded.description, category = excluded.category,
      default_timeout_ms = excluded.default_timeout_ms, default_max_attempts = excluded.default_max_attempts, default_backoff_ms = excluded.default_backoff_ms`,
      [x.type, x.description, x.type.split(".")[0], x.timeoutMs, x.maxAttempts, x.backoffMs]);
}
