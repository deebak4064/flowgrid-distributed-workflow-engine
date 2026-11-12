// REST API (v1), Server-Sent Events, OpenAPI docs and Prometheus scrape endpoint.
// Every query is scoped to the caller's organization (tenant isolation); roles gate every mutating route.
import Fastify, { type FastifyReply, type FastifyRequest } from "fastify";
import swagger from "@fastify/swagger";
import swaggerUi from "@fastify/swagger-ui";
import type { Redis } from "ioredis";
import { hasRole, signToken, verifyPassword, hashPassword, verifyToken, type Principal, type Role } from "./auth.ts";
import { config, log } from "./config.ts";
import { claimsSummary, submitClaims } from "./demo.ts";
import { ApiError, pageOf } from "./errors.ts";
import { registry } from "./metrics.ts";
import { EVENTS_CHANNEL } from "./queue.ts";
import type { System } from "./system.ts";

declare module "fastify" {
  interface FastifyRequest { user: Principal }
  interface FastifyContextConfig { role?: Role; public?: boolean; noRateLimit?: boolean }
}

// ---------------------------------------------------------------- JSON schema helpers (validation + OpenAPI)
const str = (maxLength = 200, extra: object = {}) => ({ type: "string", maxLength, ...extra });
const int = (minimum: number, maximum: number) => ({ type: "integer", minimum, maximum });
const obj = (properties: Record<string, unknown>, required: string[] = []) => ({ type: "object", properties, required });
const uuid = { type: "string", format: "uuid" };
const idParams = obj({ id: uuid }, ["id"]);
const free = { type: "object", additionalProperties: true };
const condition = obj({ path: str(200), op: { type: "string", enum: ["EQ", "NEQ", "GT", "GTE", "LT", "LTE", "IN", "EXISTS", "TRUTHY", "eq", "neq", "gt", "gte", "lt", "lte", "in", "exists", "truthy"] }, value: {} }, ["path", "op"]);
const definition = obj({
  nodes: { type: "array", minItems: 1, maxItems: 200, items: obj({
    key: str(64, { pattern: "^[a-z0-9][a-z0-9_-]{0,63}$" }), name: str(120, { minLength: 1 }), type: str(120, { minLength: 1 }), params: free,
    timeoutMs: { type: ["integer", "null"] }, priority: { type: ["integer", "null"] }, deadlineMs: { type: ["integer", "null"] },
    retry: { type: ["object", "null"], properties: { maxAttempts: { type: ["integer", "null"] }, backoffMs: { type: ["integer", "null"] }, maxBackoffMs: { type: ["integer", "null"] } } },
    trigger: { type: ["string", "null"], enum: ["ALL_SUCCESS", "ANY_SUCCESS", null] },
    position: { type: ["object", "null"], properties: { x: { type: "number" }, y: { type: "number" } } },
  }, ["key", "name", "type"]) },
  edges: { type: "array", maxItems: 1000, items: obj({ from: str(64), to: str(64), when: { anyOf: [condition, { type: "null" }] } }, ["from", "to"]) },
}, ["nodes"]);
const scheduleBody = (required: string[]) => obj({
  workflowId: uuid, name: str(120, { minLength: 1 }), type: { type: "string", enum: ["ONCE", "CRON", "INTERVAL"] }, cron: { type: ["string", "null"], maxLength: 120 },
  intervalSeconds: { type: ["integer", "null"], minimum: 1, maximum: 31_536_000 }, runAt: { type: ["string", "null"], format: "date-time" },
  timezone: { type: ["string", "null"], maxLength: 60 }, input: free, priority: { type: ["integer", "null"], minimum: 0, maximum: 10 }, enabled: { type: "boolean" },
  expectedLockVersion: { type: ["integer", "null"] },
}, required);

export async function buildApi(sys: System, opts: { logger?: boolean } = {}) {
  const app = Fastify({ logger: opts.logger ? { level: config.logLevel } : false, trustProxy: true, bodyLimit: 1_048_576,
    ajv: { customOptions: { removeAdditional: true, coerceTypes: true, useDefaults: true } } });
  const { db, executions, workflows, schedules, settings, queue } = sys;

  await app.register(swagger, { openapi: { info: { title: "FlowGrid API", version: "1.0.0", description: "Distributed workflow scheduling and execution" },
    components: { securitySchemes: { bearer: { type: "http", scheme: "bearer", bearerFormat: "JWT" } } }, security: [{ bearer: [] }] } });
  await app.register(swaggerUi, { routePrefix: "/docs" });

  const audit = (req: FastifyRequest, action: string, resourceType: string, resourceId: unknown, details: Record<string, unknown> = {}, who: Partial<Principal> = req.user) =>
    db.query(`insert into audit_events(org_id, user_id, actor_email, action, resource_type, resource_id, details, ip) values ($1, $2, $3, $4, $5, $6, $7::jsonb, $8)`,
      [who?.orgId ?? null, who?.userId ?? null, who?.email ?? null, action, resourceType, resourceId == null ? null : String(resourceId), JSON.stringify(details), req.ip])
      .catch((e) => log("warn", "audit write failed", { error: e.message }));

  // ---------------------------------------------------------------- auth, RBAC, rate limiting
  app.addHook("onRequest", async (req, reply) => {
    if (!req.url.startsWith("/api/") || req.routeOptions.config?.public) return;
    const h = req.headers.authorization;
    const p = h?.startsWith("Bearer ") ? verifyToken(h.slice(7)) : null;
    if (!p) throw new ApiError(401, "unauthorized", "Missing or invalid bearer token");
    req.user = p;
  });
  app.addHook("preHandler", async (req, reply) => {
    const need = req.routeOptions.config?.role;
    if (need && !hasRole(req.user, need)) throw ApiError.forbidden();
    if (!req.url.startsWith("/api/") || req.routeOptions.config?.noRateLimit) return;
    // Fixed window in Redis (INCR + EXPIRE): the limit holds across every API replica. Keyed by user, or by client IP
    // for login (credential-stuffing guard). Fails open if Redis is down: control-plane availability beats strict limiting.
    const login = req.url.startsWith("/api/v1/auth/login");
    const limit = login ? config.api.loginRateLimitPerMinute : config.api.rateLimitPerMinute;
    const key = `fg:rl:${login ? `login:ip:${req.ip}` : `u:${req.user.userId}`}:${Math.floor(Date.now() / 60_000)}`;
    try {
      const n = await sys.redis.incr(key);
      if (n === 1) await sys.redis.expire(key, 61);
      reply.header("X-RateLimit-Limit", limit).header("X-RateLimit-Remaining", Math.max(0, limit - n));
      if (n > limit) {
        const retry = 60 - (Math.floor(Date.now() / 1000) % 60);
        reply.header("Retry-After", retry);
        throw new ApiError(429, "rate_limited", `Rate limit exceeded; retry in ${retry}s`);
      }
    } catch (e) {
      if (e instanceof ApiError) throw e;
      log("warn", "rate limiter unavailable, failing open", { error: (e as Error).message });
    }
  });
  app.addHook("onSend", async (_req, reply, payload) => {
    reply.header("X-Content-Type-Options", "nosniff").header("X-Frame-Options", "DENY").header("Referrer-Policy", "no-referrer");
    return payload;
  });

  // One RFC 7807 problem shape for every error.
  app.setErrorHandler((err: any, req, reply) => {
    let status = 500, code = "internal_error", detail = "Unexpected server error", errors: string[] = [];
    if (err instanceof ApiError) ({ status, code, message: detail, errors } = err);
    else if (err.validation) { status = 400; code = "validation_failed"; detail = "Request validation failed";
      errors = err.validation.map((v: any) => `${(v.instancePath || v.params?.missingProperty || "").replace(/^\//, "").replace(/\//g, ".") || "body"}: ${v.message}`); }
    else if (err.code === "23505") { status = 409; code = "conflict"; detail = "Request conflicts with existing data"; }
    else if (err.code === "22P02" || err.code === "22007") { status = 400; code = "malformed_request"; detail = "Malformed request parameter"; }
    else if (err.statusCode && err.statusCode < 500) { status = err.statusCode; code = "request_error"; detail = err.message; }
    else log("error", "unhandled API error", { url: req.url, error: String(err?.stack ?? err) });
    reply.status(status).type("application/problem+json").send({ type: "about:blank", title: httpTitle(status), status, detail, code, ...(errors.length ? { errors } : {}) });
  });
  app.setNotFoundHandler((req, reply) => reply.status(404).type("application/problem+json").send({ title: "Not Found", status: 404, detail: `No route ${req.method} ${req.url}`, code: "not_found" }));

  // ---------------------------------------------------------------- infra
  app.get("/health", { schema: { hide: true } }, async (_req, reply) => {
    const [pgOk, redisOk] = await Promise.all([db.query("select 1").then(() => true, () => false), sys.redis.ping().then(() => true, () => false)]);
    reply.status(pgOk ? 200 : 503); // Redis down degrades dispatch and live events but the API still serves
    return { status: pgOk ? "UP" : "DOWN", postgres: pgOk ? "UP" : "DOWN", redis: redisOk ? "UP" : "DOWN" };
  });
  app.get("/metrics", { schema: { hide: true } }, async (_req, reply) => { reply.type(registry.contentType); return registry.metrics(); });

  const v1 = "/api/v1";

  // ---------------------------------------------------------------- auth & organization
  const userView = (u: any) => ({ id: u.id, email: u.email, displayName: u.display_name, role: u.role, orgId: u.org_id, orgName: u.org_name, createdAt: u.created_at, lastLoginAt: u.last_login_at });
  const USERS = "select u.*, o.name as org_name from users u join organizations o on o.id = u.org_id";
  const dummyHash = hashPassword(crypto.randomUUID()); // compared when the e-mail is unknown: timing does not reveal accounts

  app.post(`${v1}/auth/login`, { config: { public: true }, schema: { tags: ["Auth"], security: [], body: obj({ email: str(200, { format: "email" }), password: str(200, { minLength: 1 }) }, ["email", "password"]) } }, async (req) => {
    const { email, password } = req.body as { email: string; password: string };
    const u = (await db.query(`${USERS} where lower(u.email) = lower($1)`, [email.trim()])).rows[0];
    const ok = verifyPassword(password, u?.password_hash ?? dummyHash);
    if (!u || !ok) {
      await audit(req, "auth.login_failed", "user", null, { email }, { orgId: u?.org_id, userId: u?.id, email });
      throw new ApiError(401, "invalid_credentials", "Invalid email or password");
    }
    await db.query("update users set last_login_at = now() where id = $1", [u.id]);
    const principal = { userId: u.id, orgId: u.org_id, email: u.email, role: u.role };
    const { token, expiresAt } = signToken(principal);
    await audit(req, "auth.login", "user", u.id, {}, principal);
    return { token, expiresAt, user: userView(u) };
  });
  app.get(`${v1}/me`, { schema: { tags: ["Auth"] } }, async (req) => userView((await db.query(`${USERS} where u.id = $1`, [req.user.userId])).rows[0]));
  app.get(`${v1}/org/users`, { schema: { tags: ["Auth"] } }, async (req) => (await db.query(`${USERS} where u.org_id = $1 order by u.email`, [req.user.orgId])).rows.map(userView));
  app.post(`${v1}/org/users`, { config: { role: "ADMIN" }, schema: { tags: ["Auth"], body: obj({ email: str(200, { format: "email" }), displayName: str(120, { minLength: 1 }),
    password: str(200, { minLength: 10 }), role: { type: "string", enum: ["ADMIN", "OPERATOR", "VIEWER"] } }, ["email", "displayName", "password", "role"]) } }, async (req, reply) => {
    const b = req.body as { email: string; displayName: string; password: string; role: Role };
    const r = await db.query(`insert into users(org_id, email, display_name, password_hash, role) values ($1, lower($2), $3, $4, $5)
      on conflict (email) do nothing returning id`, [req.user.orgId, b.email.trim(), b.displayName.trim(), hashPassword(b.password), b.role]);
    if (!r.rows.length) throw ApiError.conflict("Email already registered");
    await audit(req, "user.create", "user", r.rows[0].id, { email: b.email, role: b.role });
    reply.status(201);
    return userView((await db.query(`${USERS} where u.id = $1`, [r.rows[0].id])).rows[0]);
  });
  app.get(`${v1}/audit`, { config: { role: "ADMIN" }, schema: { tags: ["Auth"] } }, async (req) => {
    const { page, size } = pageOf((req.query as any).page, (req.query as any).size);
    const rows = (await db.query(`select id, actor_email, action, resource_type, resource_id, details::text, ip, created_at from audit_events
      where org_id = $1 order by id desc limit $2 offset $3`, [req.user.orgId, size, page * size])).rows;
    const total = (await db.query("select count(*) as n from audit_events where org_id = $1", [req.user.orgId])).rows[0].n;
    return { items: rows.map((r) => ({ id: r.id, actorEmail: r.actor_email, action: r.action, resourceType: r.resource_type, resourceId: r.resource_id,
      details: r.details, ip: r.ip, createdAt: r.created_at })), page, size, total };
  });

  // ---------------------------------------------------------------- workflows
  app.post(`${v1}/workflows`, { config: { role: "ADMIN" }, schema: { tags: ["Workflows"], summary: "Create a workflow (version 1) from a DAG definition",
    body: obj({ name: str(120, { pattern: "^[A-Za-z0-9][A-Za-z0-9 _.-]*$" }), description: str(2000), tags: { type: "array", maxItems: 20, items: str(40) }, definition }, ["name", "definition"]) } }, async (req, reply) => {
    const b = req.body as any;
    const d = await workflows.create(req.user.orgId, req.user.userId, b.name.trim(), b.description, b.tags, b.definition);
    await audit(req, "workflow.create", "workflow", d.id, { name: d.name, version: d.currentVersion });
    reply.status(201);
    return d;
  });
  app.get(`${v1}/workflows`, { schema: { tags: ["Workflows"], querystring: obj({ q: str(120), page: int(0, 1e6), size: int(1, 200) }) } },
    async (req) => workflows.list(req.user.orgId, req.query as any));
  app.post(`${v1}/workflows/validate`, { schema: { tags: ["Workflows"], summary: "Dry-run validation: cycle detection, unknown task types, topological levels", body: definition } },
    async (req) => workflows.validate(req.body as any));
  app.get(`${v1}/workflows/:id`, { schema: { tags: ["Workflows"], params: idParams } }, async (req) => workflows.detail(req.user.orgId, (req.params as any).id));
  app.get(`${v1}/workflows/:id/versions/:version`, { schema: { tags: ["Workflows"], params: obj({ id: uuid, version: int(1, 1e6) }, ["id", "version"]) } },
    async (req) => workflows.version(req.user.orgId, (req.params as any).id, (req.params as any).version));
  app.put(`${v1}/workflows/:id`, { config: { role: "ADMIN" }, schema: { tags: ["Workflows"], summary: "Publish a new version (optimistic concurrency via expectedLockVersion)", params: idParams,
    body: obj({ expectedLockVersion: { type: ["integer", "null"] }, description: str(2000), tags: { type: "array", maxItems: 20, items: str(40) }, definition, changeNote: str(500) }) } }, async (req) => {
    const d = await workflows.update(req.user.orgId, req.user.userId, (req.params as any).id, req.body as any);
    await audit(req, "workflow.update", "workflow", d.id, { version: d.currentVersion });
    return d;
  });
  app.delete(`${v1}/workflows/:id`, { config: { role: "ADMIN" }, schema: { tags: ["Workflows"], params: idParams } }, async (req, reply) => {
    await workflows.archive(req.user.orgId, (req.params as any).id);
    await audit(req, "workflow.archive", "workflow", (req.params as any).id);
    reply.status(204).send();
  });
  app.post(`${v1}/workflows/:id/execute`, { config: { role: "OPERATOR" }, schema: { tags: ["Workflows"], summary: "Start an execution now, after a delay, or at runAt. Idempotent per idempotencyKey.",
    params: idParams, body: { anyOf: [obj({ input: free, priority: int(0, 10), deadlineSeconds: int(1, 31_536_000), runAt: { type: "string", format: "date-time" },
      delaySeconds: int(0, 31_536_000), idempotencyKey: str(200), version: int(1, 1e6) }), { type: "null" }] } } }, async (req, reply) => {
    const b = (req.body ?? {}) as any;
    const runAt = b.runAt ? new Date(b.runAt) : b.delaySeconds ? new Date(Date.now() + b.delaySeconds * 1000) : null;
    const s = await executions.start({ orgId: req.user.orgId, workflowId: (req.params as any).id, version: b.version, input: b.input, priority: b.priority,
      deadlineSeconds: b.deadlineSeconds, runAt, idempotencyKey: b.idempotencyKey, trigger: "API", createdBy: req.user.userId });
    if (s.created) await audit(req, "workflow.execute", "execution", s.executionId, { workflowId: (req.params as any).id });
    reply.status(s.created ? 201 : 200);
    return s;
  });
  app.get(`${v1}/task-definitions`, { schema: { tags: ["Workflows"] } }, async (req) => workflows.taskDefinitions(req.user.orgId));

  // ---------------------------------------------------------------- executions
  app.get(`${v1}/executions`, { schema: { tags: ["Executions"], querystring: obj({ status: { type: "string", enum: ["PENDING", "RUNNING", "SUCCEEDED", "FAILED", "CANCELLED", ""] },
    workflowId: uuid, q: str(120), page: int(0, 1e6), size: int(1, 200) }) } }, async (req) => executions.list(req.user.orgId, req.query as any));
  app.get(`${v1}/executions/:id`, { schema: { tags: ["Executions"], params: idParams } }, async (req) => executions.detail(req.user.orgId, (req.params as any).id));
  app.get(`${v1}/executions/:id/events`, { schema: { tags: ["Executions"], params: idParams, querystring: obj({ before: { type: "integer" }, size: int(1, 200) }) } },
    async (req) => executions.events(req.user.orgId, { executionId: (req.params as any).id, before: (req.query as any).before, size: (req.query as any).size ?? 200 }));
  app.post(`${v1}/executions/:id/cancel`, { config: { role: "OPERATOR" }, schema: { tags: ["Executions"], params: idParams, body: { anyOf: [obj({ reason: str(500) }), { type: "null" }] } } }, async (req) => {
    const reason = `cancelled by ${req.user.email}${(req.body as any)?.reason ? `: ${(req.body as any).reason}` : ""}`;
    await executions.cancel(req.user.orgId, (req.params as any).id, reason);
    await audit(req, "execution.cancel", "execution", (req.params as any).id);
    return { status: "CANCELLED" };
  });
  app.post(`${v1}/executions/:id/retry`, { config: { role: "OPERATOR" }, schema: { tags: ["Executions"], summary: "Resume a FAILED/CANCELLED execution from the failure point", params: idParams } }, async (req) => {
    const remaining = await executions.resume(req.user.orgId, (req.params as any).id);
    await audit(req, "execution.retry", "execution", (req.params as any).id, { tasksToRun: remaining });
    return { status: remaining === 0 ? "SUCCEEDED" : "RUNNING", tasksToRun: remaining };
  });
  app.post(`${v1}/executions/:id/reschedule`, { config: { role: "OPERATOR" }, schema: { tags: ["Executions"], params: idParams, body: obj({ runAt: { type: "string", format: "date-time" } }, ["runAt"]) } }, async (req) => {
    const runAt = new Date((req.body as any).runAt);
    await executions.reschedule(req.user.orgId, (req.params as any).id, runAt);
    await audit(req, "execution.reschedule", "execution", (req.params as any).id, { runAt });
    return { runAt };
  });
  app.get(`${v1}/events`, { schema: { tags: ["Executions"], summary: "Logs explorer (keyset paginated)", querystring: obj({ executionId: uuid, type: str(60), q: str(200), before: { type: "integer" }, size: int(1, 200) }) } },
    async (req) => executions.events(req.user.orgId, req.query as any));

  // ---------------------------------------------------------------- dead letters
  app.get(`${v1}/dead-letter-tasks`, { schema: { tags: ["Dead letters"], querystring: obj({ includeResolved: { type: "boolean" }, reason: str(40), page: int(0, 1e6), size: int(1, 200) }) } },
    async (req) => executions.deadLetters(req.user.orgId, req.query as any));
  app.get(`${v1}/dead-letter-tasks/poison-messages`, { config: { role: "OPERATOR" }, schema: { tags: ["Dead letters"], querystring: obj({ count: int(1, 500) }) } },
    async (req) => (await queue.recentDeadLetters((req.query as any).count ?? 50)).filter((m: Record<string, string>) => m.o === req.user.orgId));
  app.post(`${v1}/dead-letter-tasks/:id/replay`, { config: { role: "OPERATOR" }, schema: { tags: ["Dead letters"], params: idParams } }, async (req) => {
    const executionId = await executions.replayDeadLetter(req.user.orgId, (req.params as any).id);
    await audit(req, "dead_letter.replay", "dead_letter_task", (req.params as any).id, { executionId });
    return { executionId, status: "REPLAYED" };
  });
  app.post(`${v1}/dead-letter-tasks/:id/discard`, { config: { role: "OPERATOR" }, schema: { tags: ["Dead letters"], params: idParams } }, async (req) => {
    await executions.discardDeadLetter(req.user.orgId, (req.params as any).id);
    await audit(req, "dead_letter.discard", "dead_letter_task", (req.params as any).id);
    return { status: "DISCARDED" };
  });

  // ---------------------------------------------------------------- scheduling
  const specBody = (b: any) => ({ ...b, runAt: b.runAt ? new Date(b.runAt) : null });
  app.get(`${v1}/schedules`, { schema: { tags: ["Scheduling"] } }, async (req) => schedules.list(req.user.orgId));
  app.post(`${v1}/schedules`, { config: { role: "OPERATOR" }, schema: { tags: ["Scheduling"], body: scheduleBody(["workflowId", "name", "type"]) } }, async (req, reply) => {
    const j = await schedules.create(req.user.orgId, req.user.userId, specBody(req.body));
    await audit(req, "schedule.create", "schedule", j.id, { type: j.type });
    reply.status(201);
    return j;
  });
  app.get(`${v1}/schedules/calendar`, { schema: { tags: ["Scheduling"], querystring: obj({ from: { type: "string", format: "date-time" }, to: { type: "string", format: "date-time" } }) } }, async (req) => {
    const q = req.query as any;
    const from = q.from ? new Date(q.from) : new Date(Math.floor(Date.now() / 86_400_000) * 86_400_000 - 7 * 86_400_000);
    return schedules.calendar(req.user.orgId, from, q.to ? new Date(q.to) : new Date(from.getTime() + 35 * 86_400_000));
  });
  app.patch(`${v1}/schedules/:id`, { config: { role: "OPERATOR" }, schema: { tags: ["Scheduling"], params: idParams, body: scheduleBody([]) } }, async (req) => {
    const j = await schedules.update(req.user.orgId, (req.params as any).id, specBody(req.body));
    await audit(req, "schedule.update", "schedule", j.id);
    return j;
  });
  app.delete(`${v1}/schedules/:id`, { config: { role: "OPERATOR" }, schema: { tags: ["Scheduling"], params: idParams } }, async (req, reply) => {
    await schedules.delete(req.user.orgId, (req.params as any).id);
    await audit(req, "schedule.delete", "schedule", (req.params as any).id);
    reply.status(204).send();
  });
  app.post(`${v1}/schedules/:id/trigger`, { config: { role: "OPERATOR" }, schema: { tags: ["Scheduling"], params: idParams } }, async (req) => {
    const s = await schedules.trigger(req.user.orgId, (req.params as any).id, req.user.userId);
    await audit(req, "schedule.trigger", "schedule", (req.params as any).id, { executionId: s.executionId });
    return s;
  });
  app.get(`${v1}/scheduler/config`, { schema: { tags: ["Scheduling"] } }, async () => settings.current());
  app.put(`${v1}/scheduler/config`, { config: { role: "ADMIN" }, schema: { tags: ["Scheduling"], body: obj({ strategy: { type: "string", enum: ["FIFO", "PRIORITY", "EDF"] }, agingSeconds: int(1, 3600) }) } }, async (req) => {
    const c = await settings.update((req.body as any).strategy, (req.body as any).agingSeconds);
    await audit(req, "scheduler.config", "system", "scheduler", c);
    return c;
  });

  // ---------------------------------------------------------------- monitoring (read models computed from Postgres + Redis at request time)
  app.get(`${v1}/metrics`, { schema: { tags: ["Monitoring"], summary: "Throughput, latency percentiles, failure/retry rates, queue depth, worker utilisation", querystring: obj({ windowMinutes: int(1, 1440) }) } },
    async (req) => metrics(sys, req.user.orgId, (req.query as any).windowMinutes ?? 15));
  app.get(`${v1}/workers`, { schema: { tags: ["Monitoring"], querystring: obj({ includeStopped: { type: "boolean" } }) } }, async (req) =>
    (await db.query(`select w.id, w.hostname, w.pid, w.concurrency, w.status, w.started_at, w.last_heartbeat_at, w.in_flight, w.completed_count, w.failed_count,
        w.app_version, coalesce(array_length(w.task_types, 1), 0) as task_type_count, (extract(epoch from (now() - w.last_heartbeat_at)) * 1000)::bigint as heartbeat_age_ms,
        case when w.concurrency = 0 then 0 else round(w.in_flight::numeric / w.concurrency, 4) end as utilization,
        (select count(*) from task_executions t where t.status = 'RUNNING' and t.lease_owner = w.id) as leased_tasks,
        coalesce((select json_agg(h order by h.at desc) from (select at, in_flight, capacity, cpu_load, heap_used_mb from worker_heartbeats
          where worker_id = w.id order by at desc limit 30) h), '[]') as recent
      from worker_nodes w where $1 or w.status in ('ACTIVE', 'DRAINING', 'DEAD') order by w.status, w.started_at desc limit 200`, [Boolean((req.query as any).includeStopped)])).rows);
  for (const [action, to, from] of [["drain", "DRAINING", "ACTIVE"], ["resume", "ACTIVE", "DRAINING"]] as const)
    app.post(`${v1}/workers/:id/${action}`, { config: { role: "ADMIN" }, schema: { tags: ["Monitoring"], params: obj({ id: str(200) }, ["id"]) } }, async (req) => {
      const r = await db.query("update worker_nodes set status = $1 where id = $2 and status = $3", [to, (req.params as any).id, from]);
      if (!r.rowCount) throw ApiError.conflict(`Worker is not ${from}`);
      await audit(req, `worker.${to.toLowerCase()}`, "worker", (req.params as any).id);
      return { id: (req.params as any).id, status: to };
    });
  app.get(`${v1}/queues`, { schema: { tags: ["Monitoring"], summary: "Redis stream / consumer-group state plus the Postgres view of the logical queue" } }, async (req) => {
    const org = req.user.orgId;
    const out: Record<string, unknown> = {};
    try { out.redis = await queue.stats(); out.redisAvailable = true; } catch (e) { out.redisAvailable = false; out.redisError = (e as Error).message; }
    out.byStatus = (await db.query(`select status, count(*) as count from task_executions where org_id = $1 and status in ('PENDING', 'READY', 'QUEUED', 'RUNNING', 'RETRYING') group by status`, [org])).rows;
    out.runnableByPriority = (await db.query(`select effective_priority as priority, count(*) as count, (extract(epoch from (now() - min(ready_at))) * 1000)::bigint as oldest_age_ms
      from task_executions where org_id = $1 and status in ('READY', 'RETRYING') and (not_before is null or not_before <= now()) group by 1 order by 1 desc`, [org])).rows;
    out.runnableByType = (await db.query(`select task_type, count(*) as count from task_executions where org_id = $1 and status in ('READY', 'RETRYING', 'QUEUED') group by 1 order by 2 desc limit 20`, [org])).rows;
    out.oldestQueued = (await db.query(`select id, node_key, task_type, attempt, priority, queued_at, stream_message_id, workflow_execution_id,
      (extract(epoch from (now() - queued_at)) * 1000)::bigint as age_ms from task_executions where org_id = $1 and status = 'QUEUED' order by queued_at limit 20`, [org])).rows;
    return out;
  });

  // ---------------------------------------------------------------- demo
  if (config.demo.enabled) {
    app.post(`${v1}/demo/claims`, { config: { role: "OPERATOR" }, schema: { tags: ["Demo: insurance claims"], summary: "Submit synthetic claims, optionally with injected failures",
      body: obj({ count: int(1, 2000), failureRate: { type: "number", minimum: 0, maximum: 0.9 }, crashAfterEffectRate: { type: "number", minimum: 0, maximum: 0.9 }, priority: int(0, 10), seed: { type: "integer" } }) } }, async (req) => {
      const r = await submitClaims(sys, req.user.orgId, req.user.userId, req.body as any);
      await audit(req, "demo.claims", "workflow", null, { count: r.submitted });
      return r;
    });
    app.get(`${v1}/demo/claims/summary`, { schema: { tags: ["Demo: insurance claims"] } }, async (req) => claimsSummary(db, req.user.orgId));
  }

  // ---------------------------------------------------------------- live events (SSE)
  const hub = new SseHub(sys.redis.duplicate());
  await hub.start();
  app.addHook("onClose", async () => hub.stop());
  app.get(`${v1}/stream`, { config: { noRateLimit: true }, schema: { tags: ["Executions"], summary: "Organization-wide live event stream (Server-Sent Events)" } },
    async (req, reply) => hub.subscribe(req, reply, req.user.orgId, null));
  app.get(`${v1}/executions/:id/stream`, { config: { noRateLimit: true }, schema: { tags: ["Executions"], summary: "Live events of one execution (Server-Sent Events)", params: idParams } }, async (req, reply) => {
    await executions.detail(req.user.orgId, (req.params as any).id); // 404 unless the execution belongs to the caller's org
    return hub.subscribe(req, reply, req.user.orgId, (req.params as any).id);
  });

  return app;
}

/**
 * Fans Redis pub/sub execution events out to SSE clients, filtered by org (tenant isolation) and optionally by
 * execution. A client whose socket buffer grows past 1 MiB is disconnected (it re-syncs via REST) so a slow
 * client can never stall the others.
 */
class SseHub {
  private subs = new Set<{ org: string; exec: string | null; reply: FastifyReply }>();
  private keepalive?: NodeJS.Timeout;
  private sub: Redis;
  constructor(sub: Redis) { this.sub = sub; }

  async start() {
    this.sub.on("message", (_ch, msg) => {
      if (this.subs.size === 0) return;
      let e: { orgId?: string; executionId?: string };
      try { e = JSON.parse(msg); } catch { return; }
      for (const s of this.subs) {
        if (s.org !== e.orgId || (s.exec && s.exec !== e.executionId)) continue;
        if (s.reply.raw.writableLength > 1_048_576) { s.reply.raw.end(); this.subs.delete(s); continue; }
        s.reply.raw.write(`event: execution-event\ndata: ${msg}\n\n`);
      }
    });
    await this.sub.subscribe(EVENTS_CHANNEL).catch((e) => log("warn", "SSE subscribe failed (Redis down?)", { error: e.message }));
    this.keepalive = setInterval(() => { for (const s of this.subs) s.reply.raw.write(": keepalive\n\n"); }, 15_000);
  }

  subscribe(req: FastifyRequest, reply: FastifyReply, org: string, exec: string | null) {
    reply.hijack();
    reply.raw.writeHead(200, { "Content-Type": "text/event-stream", "Cache-Control": "no-cache, no-transform", Connection: "keep-alive", "X-Accel-Buffering": "no" });
    reply.raw.write("event: ready\ndata: {}\n\n");
    const s = { org, exec, reply };
    this.subs.add(s);
    req.raw.on("close", () => this.subs.delete(s));
  }

  stop() {
    clearInterval(this.keepalive);
    for (const s of this.subs) s.reply.raw.end();
    this.sub.disconnect();
  }
}

async function metrics(sys: System, org: string, windowMinutes: number) {
  const w = Math.max(1, Math.min(windowMinutes, 1440));
  const one = async (sql: string, args: unknown[]) => (await sys.db.query(sql, args)).rows[0];
  const bucketSeconds = Math.max(10, Math.floor((w * 60) / 40));
  return {
    windowMinutes: w, bucketSeconds,
    queue: await one(`select count(*) filter (where status = 'QUEUED') as queued,
        count(*) filter (where status in ('READY', 'RETRYING') and (not_before is null or not_before <= now())) as runnable,
        count(*) filter (where status = 'RETRYING' and not_before > now()) as backing_off,
        count(*) filter (where status = 'READY' and not_before > now()) as delayed,
        count(*) filter (where status = 'RUNNING') as running, count(*) filter (where status = 'PENDING') as waiting_on_dependencies,
        coalesce(extract(epoch from (now() - min(ready_at) filter (where status in ('READY', 'RETRYING') and (not_before is null or not_before <= now())))) * 1000, 0)::bigint as oldest_runnable_age_ms
      from task_executions where org_id = $1 and status in ('PENDING', 'READY', 'QUEUED', 'RUNNING', 'RETRYING')`, [org]),
    workers: await one(`select count(*) as active, coalesce(sum(concurrency), 0) as capacity, coalesce(sum(in_flight), 0) as in_flight,
        case when coalesce(sum(concurrency), 0) = 0 then 0 else round(sum(in_flight)::numeric / sum(concurrency), 4) end as utilization
      from worker_nodes where status = 'ACTIVE' and last_heartbeat_at > now() - $1 * interval '1 millisecond'`, [config.scheduler.workerTimeoutMs]),
    tasks: await one(`select count(*) filter (where status = 'SUCCEEDED') as succeeded, count(*) filter (where status in ('FAILED', 'TIMED_OUT', 'LOST')) as failed_attempts,
        count(*) filter (where attempt > 1) as retries, round(count(*) filter (where status = 'SUCCEEDED') / ($2 * 60.0), 3) as completed_per_sec,
        case when count(*) = 0 then 0 else round(count(*) filter (where status in ('FAILED', 'TIMED_OUT', 'LOST'))::numeric / count(*), 4) end as failure_rate,
        round(avg(duration_ms) filter (where status = 'SUCCEEDED'), 1) as avg_duration_ms,
        percentile_cont(0.5) within group (order by duration_ms) filter (where status = 'SUCCEEDED') as p50_ms,
        percentile_cont(0.95) within group (order by duration_ms) filter (where status = 'SUCCEEDED') as p95_ms,
        percentile_cont(0.99) within group (order by duration_ms) filter (where status = 'SUCCEEDED') as p99_ms,
        round(avg(extract(epoch from (started_at - queued_at)) * 1000)::numeric, 1) as avg_queue_wait_ms,
        percentile_cont(0.95) within group (order by extract(epoch from (started_at - queued_at)) * 1000) as p95_queue_wait_ms
      from retry_attempts where org_id = $1 and finished_at > now() - $2 * interval '1 minute' and status not in ('DISCARDED', 'CANCELLED')`, [org, w]),
    scheduling: await one(`select round(avg(extract(epoch from (queued_at - greatest(ready_at, not_before))) * 1000)::numeric, 1) as avg_delay_ms,
        percentile_cont(0.95) within group (order by extract(epoch from (queued_at - greatest(ready_at, not_before))) * 1000) as p95_delay_ms
      from task_executions where org_id = $1 and attempt = 1 and finished_at > now() - $2 * interval '1 minute'`, [org, w]),
    workflows: await one(`select count(*) as started, count(*) filter (where status = 'SUCCEEDED') as succeeded, count(*) filter (where status = 'FAILED') as failed,
        count(*) filter (where status = 'CANCELLED') as cancelled, count(*) filter (where status in ('PENDING', 'RUNNING')) as active,
        round(avg(extract(epoch from (finished_at - created_at)) * 1000) filter (where status = 'SUCCEEDED')::numeric, 1) as avg_duration_ms,
        percentile_cont(0.95) within group (order by extract(epoch from (finished_at - created_at)) * 1000) filter (where status = 'SUCCEEDED') as p95_duration_ms
      from workflow_executions where org_id = $1 and created_at > now() - $2 * interval '1 minute'`, [org, w]),
    deadLetters: (await one("select count(*) as n from dead_letter_tasks where org_id = $1 and resolved_at is null", [org])).n,
    series: (await sys.db.query(`select date_bin($1 * interval '1 second', finished_at, timestamptz '2000-01-01') as t,
        count(*) filter (where status = 'SUCCEEDED') as completed, count(*) filter (where status in ('FAILED', 'TIMED_OUT', 'LOST')) as failed,
        count(*) filter (where attempt > 1) as retried, round(avg(duration_ms) filter (where status = 'SUCCEEDED'), 1) as avg_duration_ms,
        percentile_cont(0.95) within group (order by duration_ms) filter (where status = 'SUCCEEDED') as p95_duration_ms
      from retry_attempts where org_id = $2 and finished_at > now() - $3 * interval '1 minute' and status not in ('DISCARDED', 'CANCELLED') group by 1 order by 1`, [bucketSeconds, org, w])).rows,
    byTaskType: (await sys.db.query(`select t.task_type, count(*) filter (where a.status = 'SUCCEEDED') as succeeded,
        count(*) filter (where a.status in ('FAILED', 'TIMED_OUT', 'LOST')) as failed, round(avg(a.duration_ms) filter (where a.status = 'SUCCEEDED'), 1) as avg_duration_ms,
        percentile_cont(0.95) within group (order by a.duration_ms) filter (where a.status = 'SUCCEEDED') as p95_ms
      from retry_attempts a join task_executions t on t.id = a.task_execution_id
      where a.org_id = $1 and a.finished_at > now() - $2 * interval '1 minute' and a.status not in ('DISCARDED', 'CANCELLED') group by 1 order by 2 desc limit 30`, [org, w])).rows,
  };
}

const httpTitle = (s: number) => ({ 400: "Bad Request", 401: "Unauthorized", 403: "Forbidden", 404: "Not Found", 409: "Conflict", 429: "Too Many Requests" } as Record<number, string>)[s] ?? (s >= 500 ? "Internal Server Error" : "Error");
