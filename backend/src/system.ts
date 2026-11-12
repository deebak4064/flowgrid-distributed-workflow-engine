// Wiring: one place that builds the services every process role shares.
import type { Redis } from "ioredis";
import { createPool, type Db } from "./db.ts";
import { EventBus } from "./events.ts";
import { ExecutionService } from "./executions.ts";
import { createRedis, StreamQueue } from "./queue.ts";
import { ScheduleService, Settings } from "./schedules.ts";
import { TaskStore } from "./taskStore.ts";
import { WorkflowService } from "./workflows.ts";

export interface System {
  db: Db; redis: Redis; queue: StreamQueue; store: TaskStore; executions: ExecutionService;
  workflows: WorkflowService; schedules: ScheduleService; settings: Settings; close(): Promise<void>;
}

export function createSystem(opts: { dbUrl?: string; redisUrl?: string; dbPool?: number } = {}): System {
  const db = createPool(opts.dbUrl, opts.dbPool);
  const redis = createRedis(opts.redisUrl);
  const queue = new StreamQueue(redis);
  const store = new TaskStore(db, new EventBus(redis));
  const executions = new ExecutionService(db, store);
  return {
    db, redis, queue, store, executions, workflows: new WorkflowService(db), schedules: new ScheduleService(db, executions), settings: new Settings(db),
    async close() { redis.disconnect(); await db.end(); },
  };
}
