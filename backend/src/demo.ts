// Demo applications on the engine: tenants, users of every role, synthetic policy holders, three unrelated business
// workflows (insurance claims, month-end reporting, document pipeline) and their schedules. Seeded once.
import { hashPassword } from "./auth.ts";
import { config, log } from "./config.ts";
import { tx, type Db } from "./db.ts";
import type { NodeDef, WorkflowDefinition } from "./dag.ts";
import { ApiError } from "./errors.ts";
import { CUSTOMER_COUNT, syntheticClaim } from "./handlers.ts";
import type { System } from "./system.ts";

const node = (key: string, name: string, type: string, params: Record<string, unknown>, priority: number, x: number, y: number, extra: Partial<NodeDef> = {}): NodeDef =>
  ({ key, name, type, params, priority, position: { x, y }, ...extra });

/** submit -> (documents || customer) -> amount -> risk -> {approve | review | reject} -> notify (ANY_SUCCESS join). */
export const claimsDefinition = (): WorkflowDefinition => ({
  nodes: [
    node("submit", "Claim submission", "claims.submit", {}, 7, 0, 160),
    node("documents", "Document validation", "claims.validate_documents", {}, 6, 280, 40),
    node("customer", "Customer information retrieval", "claims.fetch_customer", { chaos: { latencyMs: [20, 120], failRate: 0.05 } }, 6, 280, 280,
      { timeoutMs: 6000, retry: { maxAttempts: 5, backoffMs: 300, maxBackoffMs: 5000 } }),
    node("amount", "Claim amount validation", "claims.validate_amount", {}, 6, 560, 280),
    node("risk", "Risk analysis", "claims.risk_analysis", { chaos: { latencyMs: [40, 160] } }, 6, 840, 160),
    node("approve", "Auto-approve", "claims.auto_approve", {}, 7, 1120, 0),
    node("review", "Manual review routing", "claims.manual_review", {}, 5, 1120, 160),
    node("reject", "Reject claim", "claims.reject", {}, 5, 1120, 320),
    node("notify", "Notification generation", "claims.notify", {}, 5, 1400, 160, { trigger: "ANY_SUCCESS" }),
  ],
  edges: [
    { from: "submit", to: "documents" }, { from: "submit", to: "customer" }, { from: "customer", to: "amount" },
    { from: "documents", to: "risk" }, { from: "amount", to: "risk" },
    { from: "risk", to: "approve", when: { path: "route", op: "EQ", value: "AUTO_APPROVE" } },
    { from: "risk", to: "review", when: { path: "route", op: "EQ", value: "MANUAL_REVIEW" } },
    { from: "risk", to: "reject", when: { path: "route", op: "EQ", value: "REJECT" } },
    { from: "approve", to: "notify" }, { from: "review", to: "notify" }, { from: "reject", to: "notify" },
  ],
});

export const reportDefinition = (): WorkflowDefinition => ({
  nodes: [
    node("extract", "Extract general ledger", "finance.extract_ledger", { rows: 20000 }, 6, 0, 120),
    node("by_region", "Aggregate by region", "finance.aggregate", { dimension: "region" }, 6, 280, 0),
    node("by_product", "Aggregate by product line", "finance.aggregate", { dimension: "product" }, 6, 280, 240),
    node("reconcile", "Reconcile totals", "finance.reconcile", {}, 7, 560, 120),
    node("render", "Render report", "finance.render_report", {}, 6, 840, 120),
    node("publish", "Publish to stakeholders", "core.notify", { recipient: "finance-leads@acme.test", subject: "Month-end report ready" }, 5, 1120, 120),
  ],
  edges: [{ from: "extract", to: "by_region" }, { from: "extract", to: "by_product" }, { from: "by_region", to: "reconcile" },
    { from: "by_product", to: "reconcile" }, { from: "reconcile", to: "render" }, { from: "render", to: "publish" }],
});

export const documentDefinition = (): WorkflowDefinition => {
  const steps = [["received", "Document received"], ["validation", "Document validation"], ["extraction", "Data extraction"], ["risk", "Risk analysis"], ["report", "Report generation"]];
  return { nodes: steps.map(([k, n], i) => node(k, n, "core.sleep", { durationMs: [80, 400] }, 5, i * 280, 80)),
    edges: steps.slice(1).map(([k], i) => ({ from: steps[i][0], to: k })) };
};

/** Fan-out / fan-in used by load tests: start -> width parallel tasks -> join. */
export const benchmarkFanout = (width: number, durationMs: number, type = "core.sleep"): WorkflowDefinition => ({
  nodes: [node("start", "Fan-out", type, { durationMs }, 5, 0, 120),
    ...Array.from({ length: width }, (_, i) => node(`t${i}`, `Branch ${i}`, type, { durationMs }, 5, 280, i * 60)),
    node("join", "Fan-in", type, { durationMs }, 5, 560, 120)],
  edges: [...Array.from({ length: width }, (_, i) => ({ from: "start", to: `t${i}` })), ...Array.from({ length: width }, (_, i) => ({ from: `t${i}`, to: "join" }))],
});

export async function seed(sys: System) {
  const db = sys.db;
  const already = await tx(db, async ({ c }) => {
    await c.query("select pg_advisory_xact_lock(727274)"); // several API replicas may boot at once
    if (Number((await c.query("select count(*) as n from organizations")).rows[0].n) > 0) return true;
    const hash = hashPassword(config.demo.password);
    const org = async (name: string, slug: string) => (await c.query("insert into organizations(name, slug) values ($1, $2) returning id", [name, slug])).rows[0].id;
    const user = async (o: string, email: string, name: string, role: string) =>
      (await c.query("insert into users(org_id, email, display_name, password_hash, role) values ($1, $2, $3, $4, $5) returning id", [o, email, name, hash, role])).rows[0].id;
    const acme = await org("Acme Insurance", "acme"), globex = await org("Globex Finance", "globex");
    await user(acme, "admin@acme.test", "Ava Admin", "ADMIN");
    await user(acme, "operator@acme.test", "Omar Operator", "OPERATOR");
    await user(acme, "viewer@acme.test", "Vera Viewer", "VIEWER");
    await user(globex, "admin@globex.test", "Gus Globex", "ADMIN");
    await customers(c, acme, CUSTOMER_COUNT);
    await customers(c, globex, 100);
    return false;
  });
  if (already) return;
  const ids = (await db.query("select o.slug, o.id as org, u.id as admin from organizations o join users u on u.org_id = o.id and u.role = 'ADMIN'")).rows;
  const acme = ids.find((r) => r.slug === "acme"), globex = ids.find((r) => r.slug === "globex");
  const claims = await sys.workflows.create(acme.org, acme.admin, "insurance-claim-processing", "Automated claim handling with risk-based approval routing", ["claims", "finance-ops"], claimsDefinition());
  const report = await sys.workflows.create(acme.org, acme.admin, "monthly-financial-report", "Month-end ledger extraction, parallel aggregation, reconciliation and reporting", ["finance", "reporting"], reportDefinition());
  const docs = await sys.workflows.create(acme.org, acme.admin, "document-intelligence-pipeline", "Document received -> validation -> extraction -> risk analysis -> report", ["documents"], documentDefinition());
  await sys.workflows.create(acme.org, acme.admin, "benchmark-diamond", "Fan-out/fan-in of core.sleep tasks used by load tests", ["benchmark"], benchmarkFanout(3, 50));
  await sys.workflows.create(globex.org, globex.admin, "monthly-financial-report", "Globex month-end close", ["finance"], reportDefinition());
  await sys.schedules.create(acme.org, acme.admin, { workflowId: claims.id, name: "Claims intake sweep", type: "INTERVAL", intervalSeconds: 300, input: { synthetic: true }, priority: 6 });
  await sys.schedules.create(acme.org, acme.admin, { workflowId: report.id, name: "Month-end close", type: "CRON", cron: "0 0 6 1 * *", priority: 8 });
  await sys.schedules.create(acme.org, acme.admin, { workflowId: docs.id, name: "Hourly document batch", type: "CRON", cron: "0 0 * * * *", priority: 4 });
  log("info", "demo data seeded", { users: ["admin@acme.test", "operator@acme.test", "viewer@acme.test", "admin@globex.test"] });
}

/** Deterministic synthetic policy holders (md5-derived attributes). */
async function customers(c: { query: Db["query"] }, org: string, n: number) {
  await c.query(`insert into customers(id, org_id, name, tier, policy_number, policy_limit, deductible, customer_since, prior_claims)
    select format('CUST-%s', lpad(g::text, 4, '0')), $1,
      (array['Asha','Ben','Chen','Diya','Elif','Femi','Grace','Hiro','Ines','Jamal','Kavya','Luca'])[1 + g % 12]
        || ' ' || (array['Iyer','Okafor','Silva','Novak','Haddad','Kim','Murphy','Sato','Rossi','Mensah'])[1 + (g / 12) % 10],
      (array['BRONZE','SILVER','SILVER','GOLD','GOLD','PLATINUM'])[1 + (('x' || substr(md5(g::text), 1, 4))::bit(16)::int % 6)],
      format('POL-%s', upper(substr(md5('p' || g), 1, 8))),
      (array[10000, 25000, 50000, 100000])[1 + (('x' || substr(md5('l' || g), 1, 4))::bit(16)::int % 4)],
      (array[250, 500, 1000])[1 + (('x' || substr(md5('d' || g), 1, 4))::bit(16)::int % 3)],
      current_date - ((('x' || substr(md5('s' || g), 1, 4))::bit(16)::int % 3650) || ' days')::interval,
      ('x' || substr(md5('c' || g), 1, 4))::bit(16)::int % 4
    from generate_series(1, $2) g`, [org, n]);
}

/** Submit a batch of synthetic claims, optionally with injected transient failures / crashes after side effects. */
export async function submitClaims(sys: System, orgId: string, userId: string, b: { count?: number; failureRate?: number; crashAfterEffectRate?: number; priority?: number; seed?: number }) {
  const wf = (await sys.db.query("select id from workflows where org_id = $1 and name = 'insurance-claim-processing' and not archived", [orgId])).rows[0];
  if (!wf) throw ApiError.notFound("insurance-claim-processing workflow");
  const count = b.count ?? 10, seedBase = b.seed ?? Date.now();
  const chaos: Record<string, number> = {};
  if (b.failureRate) chaos.failRate = b.failureRate;
  if (b.crashAfterEffectRate) chaos.crashAfterEffectRate = b.crashAfterEffectRate;
  const ids: string[] = [];
  for (let i = 0; i < count; i++) {
    const claim: Record<string, unknown> = syntheticClaim(`${seedBase}:${i}`, `CLM-${crypto.randomUUID().slice(0, 10).toUpperCase()}`);
    if (Object.keys(chaos).length) claim.chaos = chaos;
    ids.push((await sys.executions.start({ orgId, workflowId: wf.id, input: claim, priority: b.priority, trigger: "API", createdBy: userId })).executionId);
  }
  return { submitted: count, executionIds: ids.slice(0, 50) };
}

/** Business outcome of processed claims, incl. a duplicate-effect check on idempotency keys. */
export async function claimsSummary(db: Db, orgId: string) {
  const q = async (sql: string) => (await db.query(sql, [orgId])).rows;
  return {
    claims: await q("select status, count(*) as count, round(sum(amount), 2) as amount from claims where org_id = $1 group by 1"),
    decisions: await q("select decision, count(*) as count, round(avg(risk_score), 1) as avg_risk, round(sum(payable_amount), 2) as payable from claim_decisions where org_id = $1 group by 1"),
    notifications: (await q("select count(*) as n from notifications where org_id = $1"))[0].n,
    // A claim must have at most one decision and one notification, however many times its tasks were retried.
    claimsWithDuplicateDecisions: (await q("select count(*) as n from (select claim_id from claim_decisions where org_id = $1 group by claim_id having count(*) > 1) d"))[0].n,
    claimsWithDuplicateNotifications: (await q("select count(*) as n from (select claim_id from notifications where org_id = $1 and claim_id is not null group by claim_id having count(*) > 1) d"))[0].n,
  };
}
