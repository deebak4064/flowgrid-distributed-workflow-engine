// Postgres access: pool, transactions with after-commit hooks, migrations, transient-error retry.
import pg from "pg";
import fs from "node:fs";
import path from "node:path";
import { config, log } from "./config.ts";

// int8 / numeric come back as strings by default; FlowGrid's counts and amounts fit in a double.
pg.types.setTypeParser(20, (v) => Number(v));
pg.types.setTypeParser(1700, (v) => Number(v));

export type Db = pg.Pool;
export type Client = pg.PoolClient | pg.Pool;

/** A transaction handle: run queries on `c`, register side effects that must only happen after COMMIT. */
export interface Tx {
  c: pg.PoolClient;
  afterCommit(fn: () => void): void;
}

export function createPool(url = config.dbUrl, max = config.dbPool): Db {
  const pool = new pg.Pool({ connectionString: url, max, connectionTimeoutMillis: 5000, idleTimeoutMillis: 30_000 });
  pool.on("error", (e) => log("warn", "idle db client error", { error: e.message }));
  return pool;
}

export async function tx<T>(db: Db, fn: (t: Tx) => Promise<T>): Promise<T> {
  const c = await db.connect();
  const hooks: (() => void)[] = [];
  try {
    await c.query("begin");
    const out = await fn({ c, afterCommit: (h) => hooks.push(h) });
    await c.query("commit");
    for (const h of hooks) {
      try { h(); } catch (e) { log("debug", "after-commit hook failed", { error: String(e) }); }
    }
    return out;
  } catch (e) {
    try { await c.query("rollback"); } catch { /* connection may be gone */ }
    throw e;
  } finally {
    c.release();
  }
}

/** Connection loss, deadlock victim, serialization failure, or "too many clients": safe to retry. */
export function isTransient(e: unknown): boolean {
  const err = e as { code?: string; message?: string };
  if (err?.code && ["40P01", "40001", "53300", "57P01", "57P02", "57P03", "08000", "08003", "08006", "ECONNREFUSED", "ECONNRESET", "ETIMEDOUT", "EPIPE"].includes(err.code)) return true;
  return /Connection terminated|connection timeout|timeout exceeded when trying to connect|Client has encountered a connection error/i.test(err?.message ?? "");
}

export const sleep = (ms: number) => new Promise<void>((r) => setTimeout(r, ms));

/** Retry transient DB failures with jittered backoff until `budgetMs` is spent; then rethrow. */
export async function withRetry<T>(op: () => Promise<T>, budgetMs: number, isAlive: () => boolean = () => true): Promise<T> {
  const deadline = Date.now() + budgetMs;
  let backoff = 100;
  for (;;) {
    try {
      return await op();
    } catch (e) {
      if (!isTransient(e) || Date.now() + backoff > deadline || !isAlive()) throw e;
      log("warn", "transient db failure, retrying", { error: (e as Error).message, backoffMs: backoff });
      await sleep(backoff + Math.random() * backoff / 2);
      backoff = Math.min(backoff * 2, 2000);
    }
  }
}

/** Plain-SQL migrations applied in filename order, once each, under an advisory lock (safe with many replicas). */
export async function migrate(db: Db, dir = path.join(import.meta.dirname, "..", "migrations")) {
  await tx(db, async ({ c }) => {
    await c.query("select pg_advisory_xact_lock(727273)");
    await c.query("create table if not exists schema_migrations (name text primary key, applied_at timestamptz not null default now())");
    const done = new Set((await c.query("select name from schema_migrations")).rows.map((r) => r.name));
    for (const f of fs.readdirSync(dir).filter((f) => f.endsWith(".sql")).sort()) {
      if (done.has(f)) continue;
      await c.query(fs.readFileSync(path.join(dir, f), "utf8"));
      await c.query("insert into schema_migrations(name) values ($1)", [f]);
      log("info", "applied migration", { migration: f });
    }
  });
}
