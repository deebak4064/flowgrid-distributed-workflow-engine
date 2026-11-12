// Integration tests run against REAL Postgres and Redis. By default they are started with Testcontainers (CI);
// set FG_TEST_DB_URL / FG_TEST_REDIS_URL to reuse already-running instances (e.g. on a machine without Docker).
import type { TestProject } from "vitest/node";

export default async function setup(project: TestProject) {
  if (process.env.FG_TEST_DB_URL && process.env.FG_TEST_REDIS_URL) {
    project.provide("dbUrl", process.env.FG_TEST_DB_URL);
    project.provide("redisUrl", process.env.FG_TEST_REDIS_URL);
    return;
  }
  const { PostgreSqlContainer } = await import("@testcontainers/postgresql");
  const { RedisContainer } = await import("@testcontainers/redis");
  const [pg, redis] = await Promise.all([
    new PostgreSqlContainer("postgres:16-alpine").withDatabase("flowgrid").withUsername("flowgrid").withPassword("flowgrid").start(),
    new RedisContainer("redis:7-alpine").start(),
  ]);
  project.provide("dbUrl", pg.getConnectionUri());
  project.provide("redisUrl", redis.getConnectionUrl());
  return async () => { await Promise.all([pg.stop(), redis.stop()]); };
}

declare module "vitest" {
  export interface ProvidedContext { dbUrl: string; redisUrl: string }
}
