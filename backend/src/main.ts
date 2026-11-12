// Process entry point. One codebase, three deployable roles (FLOWGRID_ROLE): api | scheduler | worker, or "all" for dev.
// Splitting them gives failure isolation (a crashing handler never takes the API down) and independent scaling of workers.
import { buildApi } from "./api.ts";
import { config, log } from "./config.ts";
import { migrate, sleep } from "./db.ts";
import { seed } from "./demo.ts";
import { publishDefinitions } from "./handlers.ts";
import { serveMetrics } from "./metrics.ts";
import { Scheduler } from "./scheduler.ts";
import { createSystem } from "./system.ts";
import { Worker } from "./worker.ts";

const roles = config.role === "all" ? ["api", "scheduler", "worker"] : config.role.split(",");
const sys = createSystem();

// Wait for Postgres (compose may start us first), then migrate. Every role may run this; an advisory lock serialises it.
for (let i = 0; ; i++) {
  try { await migrate(sys.db); break; } catch (e) {
    if (i > 60) throw e;
    log("warn", "waiting for postgres", { error: (e as Error).message });
    await sleep(1000);
  }
}
await publishDefinitions(sys.db);

const stops: (() => Promise<void>)[] = [];
if (roles.includes("api")) {
  if (config.demo.enabled) await seed(sys);
  const app = await buildApi(sys, { logger: false });
  await app.listen({ port: config.port, host: "0.0.0.0" });
  log("info", "api listening", { port: config.port });
  stops.push(() => app.close());
}
if (roles.includes("scheduler")) {
  const s = new Scheduler(sys.db, sys.store, sys.queue, sys.schedules, sys.settings);
  await s.start();
  stops.push(() => s.stop());
}
if (roles.includes("worker")) {
  const w = new Worker(sys.db, sys.queue, sys.store, sys.redis.duplicate({ maxRetriesPerRequest: null }));
  await w.start();
  stops.unshift(() => w.stop()); // drain workers first
}
if (!roles.includes("api")) serveMetrics(config.metricsPort, async () => (await sys.db.query("select 1"), true));

let stopping = false;
for (const sig of ["SIGTERM", "SIGINT"] as const)
  process.on(sig, async () => {
    if (stopping) return;
    stopping = true;
    log("info", "shutting down", { signal: sig });
    for (const stop of stops) await stop().catch((e) => log("warn", "stop failed", { error: String(e) }));
    await sys.close();
    process.exit(0);
  });
