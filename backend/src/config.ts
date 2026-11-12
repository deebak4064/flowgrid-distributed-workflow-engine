// All runtime configuration comes from the environment (12-factor). Defaults suit local development.
import os from "node:os";

const env = (k: string, d: string) => process.env[k] ?? d;
const int = (k: string, d: number) => Number.parseInt(env(k, String(d)), 10);
const num = (k: string, d: number) => Number.parseFloat(env(k, String(d)));
const bool = (k: string, d: boolean) => env(k, String(d)) === "true";

export const config = {
  role: env("FLOWGRID_ROLE", "all"), // api | scheduler | worker | all
  port: int("PORT", 8080),
  metricsPort: int("FLOWGRID_METRICS_PORT", 0), // scheduler/worker Prometheus endpoint; 0 = off
  dbUrl: env("FLOWGRID_DB_URL", "postgres://flowgrid:flowgrid@127.0.0.1:5432/flowgrid"),
  dbPool: int("FLOWGRID_DB_POOL", 10),
  redisUrl: env("FLOWGRID_REDIS_URL", "redis://127.0.0.1:6379"),
  logLevel: env("FLOWGRID_LOG_LEVEL", "info"),
  api: {
    rateLimitPerMinute: int("FLOWGRID_RATE_LIMIT", 1200),
    loginRateLimitPerMinute: int("FLOWGRID_LOGIN_RATE_LIMIT", 20),
    corsOrigin: env("FLOWGRID_CORS_ORIGIN", ""),
  },
  scheduler: {
    tickMs: int("FLOWGRID_TICK_MS", 100),
    maxBatch: int("FLOWGRID_MAX_BATCH", 500),
    queueFactor: num("FLOWGRID_QUEUE_FACTOR", 2), // backpressure: at most capacity*factor dispatched-not-started
    workerTimeoutMs: int("FLOWGRID_WORKER_TIMEOUT_MS", 10_000),
    redispatchAfterMs: int("FLOWGRID_REDISPATCH_AFTER_MS", 30_000),
    recoveryEveryMs: int("FLOWGRID_RECOVERY_EVERY_MS", 1000),
    redispatchEveryMs: int("FLOWGRID_REDISPATCH_EVERY_MS", 5000),
  },
  worker: {
    id: env("FLOWGRID_WORKER_ID", ""),
    concurrency: int("FLOWGRID_WORKER_CONCURRENCY", 8),
    leaseMs: int("FLOWGRID_LEASE_MS", 15_000),
    heartbeatMs: int("FLOWGRID_HEARTBEAT_MS", 2000),
    visibilityTimeoutMs: int("FLOWGRID_VISIBILITY_TIMEOUT_MS", 30_000),
    shutdownGraceMs: int("FLOWGRID_SHUTDOWN_GRACE_MS", 20_000),
    poisonDeliveries: int("FLOWGRID_POISON_DELIVERIES", 5),
    chaosFailureRate: num("FLOWGRID_CHAOS_FAILURE_RATE", 0),
  },
  security: {
    jwtSecret: env("FLOWGRID_JWT_SECRET", ""),
    tokenTtlMinutes: int("FLOWGRID_TOKEN_TTL_MINUTES", 480),
  },
  retention: { heartbeatsHours: 6, eventsDays: 30, executionsDays: 90 },
  demo: { enabled: bool("FLOWGRID_DEMO_ENABLED", true), password: env("FLOWGRID_DEMO_PASSWORD", "flowgrid-demo") },
  hostname: os.hostname(),
};

export type Config = typeof config;

/** Structured JSON logs (one object per line), shared by every process role. */
const LEVELS = { debug: 10, info: 20, warn: 30, error: 40 } as const;
type Level = keyof typeof LEVELS;
export function log(level: Level, msg: string, fields: Record<string, unknown> = {}) {
  if (LEVELS[level] < LEVELS[(config.logLevel as Level) in LEVELS ? (config.logLevel as Level) : "info"]) return;
  const line = JSON.stringify({ t: new Date().toISOString(), level, role: config.role, pid: process.pid, msg, ...fields });
  (level === "error" || level === "warn" ? process.stderr : process.stdout).write(line + "\n");
}
