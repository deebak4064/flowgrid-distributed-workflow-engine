// Prometheus metrics (per process). The API exposes /metrics; scheduler and worker processes expose it on FLOWGRID_METRICS_PORT.
import http from "node:http";
import client from "prom-client";
import { config, log } from "./config.ts";

export const registry = client.register;
client.collectDefaultMetrics({ prefix: "flowgrid_process_" });
registry.setDefaultLabels({ role: config.role });

const ms = [1, 5, 10, 25, 50, 100, 250, 500, 1000, 2500, 5000, 10_000, 30_000, 60_000];
export const m = {
  taskDuration: new client.Histogram({ name: "flowgrid_task_duration_ms", help: "Handler execution time", labelNames: ["type", "outcome"], buckets: ms }),
  queueWait: new client.Histogram({ name: "flowgrid_task_queue_wait_ms", help: "Dispatched (XADD) to started", buckets: ms }),
  schedulingDelay: new client.Histogram({ name: "flowgrid_scheduling_delay_ms", help: "Runnable to dispatched", buckets: ms }),
  workflowDuration: new client.Histogram({ name: "flowgrid_workflow_duration_ms", help: "Execution created to finished", buckets: [...ms, 120_000, 600_000] }),
  tasks: new client.Counter({ name: "flowgrid_tasks_total", help: "Task attempts by outcome", labelNames: ["type", "outcome"] }),
  retries: new client.Counter({ name: "flowgrid_task_retries_total", help: "Attempts that were scheduled for retry", labelNames: ["type"] }),
  duplicates: new client.Counter({ name: "flowgrid_deliveries_duplicate_total", help: "Stream deliveries dropped by the ownership check" }),
  poison: new client.Counter({ name: "flowgrid_queue_poison_total", help: "Messages moved to the DLQ stream" }),
  dispatched: new client.Counter({ name: "flowgrid_dispatched_total", help: "Tasks dispatched to the stream" }),
  redispatched: new client.Counter({ name: "flowgrid_redispatched_total", help: "QUEUED tasks re-published after a lost message" }),
  recoveredLeases: new client.Counter({ name: "flowgrid_recovered_leases_total", help: "Expired leases reaped by the scheduler" }),
  workersDied: new client.Counter({ name: "flowgrid_workers_died_total", help: "Workers declared DEAD" }),
  inFlight: new client.Gauge({ name: "flowgrid_worker_in_flight", help: "Tasks executing in this worker", labelNames: ["worker"] }),
  utilization: new client.Gauge({ name: "flowgrid_worker_utilization", help: "in_flight / concurrency", labelNames: ["worker"] }),
  queueDepth: new client.Gauge({ name: "flowgrid_queue_depth", help: "Dispatched, not yet started (QUEUED)" }),
  runnable: new client.Gauge({ name: "flowgrid_queue_runnable", help: "Ready, waiting for capacity" }),
  capacity: new client.Gauge({ name: "flowgrid_workers_capacity", help: "Sum of active worker concurrency" }),
};

/** Tiny standalone endpoint for non-API roles: /metrics and /health. */
export function serveMetrics(port: number, health: () => Promise<boolean>) {
  if (!port) return null;
  const srv = http.createServer(async (req, res) => {
    if (req.url === "/metrics") { res.setHeader("Content-Type", registry.contentType); res.end(await registry.metrics()); return; }
    if (req.url === "/health") { const ok = await health().catch(() => false); res.statusCode = ok ? 200 : 503; res.end(JSON.stringify({ status: ok ? "UP" : "DOWN" })); return; }
    res.statusCode = 404; res.end();
  });
  srv.listen(port, () => log("info", "metrics endpoint listening", { port }));
  return srv;
}
