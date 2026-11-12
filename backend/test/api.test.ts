// HTTP API: authentication, role-based permissions, organization isolation, validation, rate limiting, audit trail.
import type { FastifyInstance } from "fastify";
import { afterAll, beforeAll, describe, expect, it } from "vitest";
import { buildApi } from "../src/api.ts";
import { hashPassword } from "../src/auth.ts";
import { config } from "../src/config.ts";
import { setup, teardown, type Env } from "./helpers.ts";

let env: Env, app: FastifyInstance;
const tokens: Record<string, string> = {};
const def = { nodes: [{ key: "a", name: "A", type: "core.noop" }, { key: "b", name: "B", type: "core.noop" }], edges: [{ from: "a", to: "b" }] };
const call = (method: string, url: string, who?: string, payload?: unknown) =>
  app.inject({ method: method as any, url: `/api/v1${url}`, headers: who ? { authorization: `Bearer ${tokens[who]}` } : {}, payload: payload as any });

beforeAll(async () => {
  env = await setup();
  const hash = hashPassword("correct horse battery");
  for (const [email, role, org] of [["admin@a.test", "ADMIN", env.orgId], ["op@a.test", "OPERATOR", env.orgId], ["viewer@a.test", "VIEWER", env.orgId], ["admin@b.test", "ADMIN", env.otherOrgId]])
    await env.sys.db.query("insert into users(org_id, email, display_name, password_hash, role) values ($1, $2, $2, $3, $4)", [org, email, hash, role]);
  app = await buildApi(env.sys);
  for (const e of ["admin@a.test", "op@a.test", "viewer@a.test", "admin@b.test"]) {
    const r = await app.inject({ method: "POST", url: "/api/v1/auth/login", payload: { email: e, password: "correct horse battery" } });
    tokens[e.split("@")[0] + (e.includes("@b") ? "B" : "")] = r.json().token;
  }
});
afterAll(async () => { await app.close(); await teardown(env); });

describe("authentication", () => {
  it("rejects bad credentials with a uniform error and audits the attempt", async () => {
    const r = await app.inject({ method: "POST", url: "/api/v1/auth/login", payload: { email: "admin@a.test", password: "wrong" } });
    expect(r.statusCode).toBe(401);
    expect(r.json()).toMatchObject({ code: "invalid_credentials", status: 401 });
    const unknown = await app.inject({ method: "POST", url: "/api/v1/auth/login", payload: { email: "nobody@a.test", password: "wrong" } });
    expect(unknown.json().detail).toBe(r.json().detail);
    const a = (await env.sys.db.query("select count(*) as n from audit_events where action = 'auth.login_failed'")).rows[0].n;
    expect(a).toBe(2);
  });
  it("requires a valid, untampered bearer token", async () => {
    expect((await call("GET", "/workflows")).statusCode).toBe(401);
    const [h, p, s] = tokens.admin.split(".");
    const forged = Buffer.from(JSON.stringify({ ...JSON.parse(Buffer.from(p, "base64url").toString()), role: "ADMIN", org: env.otherOrgId })).toString("base64url");
    const r = await app.inject({ method: "GET", url: "/api/v1/workflows", headers: { authorization: `Bearer ${h}.${forged}.${s}` } });
    expect(r.statusCode).toBe(401);
  });
});

describe("role-based permissions", () => {
  let wfId = "";
  it("only ADMIN can create workflows", async () => {
    expect((await call("POST", "/workflows", "op", { name: "w1", definition: def })).statusCode).toBe(403);
    const r = await call("POST", "/workflows", "admin", { name: "w1", definition: def });
    expect(r.statusCode).toBe(201);
    wfId = r.json().id;
  });
  it("VIEWER cannot execute; OPERATOR can, idempotently", async () => {
    expect((await call("POST", `/workflows/${wfId}/execute`, "viewer", {})).statusCode).toBe(403);
    const first = await call("POST", `/workflows/${wfId}/execute`, "op", { idempotencyKey: "k-1" });
    const again = await call("POST", `/workflows/${wfId}/execute`, "op", { idempotencyKey: "k-1" });
    expect([first.statusCode, again.statusCode]).toEqual([201, 200]);
    expect(again.json().executionId).toBe(first.json().executionId);
  });
  it("organization isolation: another tenant can neither see nor run this tenant's resources", async () => {
    expect((await call("GET", `/workflows/${wfId}`, "adminB")).statusCode).toBe(404);
    expect((await call("POST", `/workflows/${wfId}/execute`, "adminB", {})).statusCode).toBe(404);
    expect((await call("GET", "/workflows", "adminB")).json().total).toBe(0);
    const exec = (await call("GET", "/executions", "admin")).json().items[0].id;
    expect((await call("GET", `/executions/${exec}`, "adminB")).statusCode).toBe(404);
    expect((await call("POST", `/executions/${exec}/cancel`, "adminB", {})).statusCode).toBe(404);
  });
  it("records an audit trail of mutating actions", async () => {
    const actions = (await env.sys.db.query("select action from audit_events where org_id = $1", [env.orgId])).rows.map((r) => r.action);
    expect(actions).toEqual(expect.arrayContaining(["workflow.create", "workflow.execute", "auth.login"]));
  });
});

describe("validation and errors", () => {
  it("rejects cyclic workflows with the cycle in the error", async () => {
    const r = await call("POST", "/workflows", "admin", { name: "cyclic", definition: { ...def, edges: [{ from: "a", to: "b" }, { from: "b", to: "a" }] } });
    expect(r.statusCode).toBe(400);
    expect(r.json().errors.join()).toMatch(/cyclic dependency detected: a -> b -> a/);
  });
  it("returns RFC 7807 problems for malformed input", async () => {
    const r = await call("POST", "/workflows", "admin", { name: "x", definition: { nodes: [{ key: "Bad Key!", name: "n", type: "core.noop" }] } });
    expect(r.statusCode).toBe(400);
    expect(r.headers["content-type"]).toMatch(/problem\+json/);
    expect(r.json()).toMatchObject({ code: "validation_failed", status: 400 });
    expect((await call("GET", "/executions/not-a-uuid", "admin")).statusCode).toBe(400);
  });
  it("paginates and filters", async () => {
    const r = (await call("GET", "/executions?size=1&page=0&status=PENDING", "admin")).json();
    expect(r).toMatchObject({ page: 0, size: 1 });
    expect(r.items.every((x: any) => x.status === "PENDING")).toBe(true);
  });
  it("serves OpenAPI documentation", async () => {
    const r = await app.inject({ method: "GET", url: "/docs/json" });
    expect(r.statusCode).toBe(200);
    expect(Object.keys(r.json().paths)).toEqual(expect.arrayContaining(["/api/v1/workflows", "/api/v1/workflows/{id}/execute", "/api/v1/executions/{id}", "/api/v1/dead-letter-tasks"]));
  });
});

describe("rate limiting", () => {
  it("limits login attempts per client IP", async () => {
    const old = config.api.loginRateLimitPerMinute;
    config.api.loginRateLimitPerMinute = 3;
    await env.sys.redis.del(...(await env.sys.redis.keys("fg:rl:login:*")).concat("x"));
    const codes = [];
    for (let i = 0; i < 5; i++) codes.push((await app.inject({ method: "POST", url: "/api/v1/auth/login", payload: { email: "admin@a.test", password: "wrong" } })).statusCode);
    config.api.loginRateLimitPerMinute = old;
    expect(codes).toEqual([401, 401, 401, 429, 429]);
  });
});
