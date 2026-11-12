// Redis Streams adapter.
//
// Delivery is AT-LEAST-ONCE: a message can be delivered again after a consumer crash (XAUTOCLAIM) or be
// re-published by the scheduler's redispatch sweep. Correctness never depends on Redis: every message carries
// the attempt_id fencing token, and the worker's claim is a compare-and-set in Postgres, so duplicate and stale
// messages are detected and dropped there. Redis is a dispatch channel, Postgres is the source of truth.
import { Redis } from "ioredis";
import { config, log } from "./config.ts";

export const STREAM = "fg:tasks";
export const GROUP = "fg-workers";
export const DLQ_STREAM = "fg:tasks:dlq";
export const EVENTS_CHANNEL = "fg:events";
const MAX_LEN = 1_000_000;

export interface Dispatch { taskId: string; attemptId: string; orgId: string; executionId: string; taskType: string }
export interface Message { id: string; fields: Record<string, string> }

export function createRedis(url = config.redisUrl): Redis {
  // maxRetriesPerRequest=null on blocking consumers is set by callers; here commands fail fast during an outage
  // so callers can fall back (tasks stay QUEUED in Postgres and are re-published later).
  const r = new Redis(url, { maxRetriesPerRequest: 2, enableOfflineQueue: true, lazyConnect: false,
    retryStrategy: (n) => Math.min(n * 200, 3000) });
  r.on("error", (e) => log("debug", "redis error", { error: e.message }));
  return r;
}

const toFields = (arr: string[]) => {
  const o: Record<string, string> = {};
  for (let i = 0; i + 1 < arr.length; i += 2) o[arr[i]] = arr[i + 1];
  return o;
};

export class StreamQueue {
  readonly redis: Redis;
  constructor(redis: Redis) { this.redis = redis; }

  async ensureGroup() {
    try {
      await this.redis.xgroup("CREATE", STREAM, GROUP, "0", "MKSTREAM");
      log("info", "created consumer group", { stream: STREAM, group: GROUP });
    } catch (e) {
      if (!String((e as Error).message).includes("BUSYGROUP")) throw e;
    }
  }

  /** Pipelined XADD of a dispatch batch. Returns stream ids in input order (null where the add failed). */
  async publish(batch: Dispatch[]): Promise<(string | null)[]> {
    if (batch.length === 0) return [];
    const p = this.redis.pipeline();
    const now = String(Date.now());
    for (const d of batch)
      p.xadd(STREAM, "MAXLEN", "~", MAX_LEN, "*", "t", d.taskId, "a", d.attemptId, "o", d.orgId, "w", d.executionId, "k", d.taskType, "q", now);
    try {
      const res = await p.exec();
      return (res ?? []).map(([err, id]) => (err ? null : (id as string)));
    } catch {
      return batch.map(() => null); // task stays QUEUED in Postgres; the redispatch sweep will publish it again
    }
  }

  /** Blocking group read; each blocking consumer must own its connection (pass a dedicated client). */
  async read(conn: Redis, consumer: string, count: number, blockMs: number): Promise<Message[]> {
    try {
      const res = (await conn.xreadgroup("GROUP", GROUP, consumer, "COUNT", count, "BLOCK", blockMs, "STREAMS", STREAM, ">")) as
        [string, [string, string[]][]][] | null;
      if (!res) return [];
      return res[0][1].map(([id, f]) => ({ id, fields: toFields(f) }));
    } catch (e) {
      if (String((e as Error).message).startsWith("NOGROUP")) {
        log("warn", "consumer group missing (Redis restarted without persistence?), recreating");
        await this.ensureGroup();
        return [];
      }
      throw e;
    }
  }

  /**
   * Take over messages a crashed consumer read but never acknowledged (visibility timeout).
   * Messages delivered more than `poisonDeliveries` times are parked in the DLQ stream instead (poison messages).
   */
  async reclaim(consumer: string, minIdleMs: number, count: number, poisonDeliveries: number): Promise<Message[]> {
    const res = (await this.redis.xautoclaim(STREAM, GROUP, consumer, minIdleMs, "0-0", "COUNT", count)) as [string, [string, string[] | null][], string[]?];
    const msgs: Message[] = [];
    for (const [id, f] of res[1] ?? []) if (f && f.length) msgs.push({ id, fields: toFields(f) });
    if (msgs.length === 0) return msgs;
    const out: Message[] = [];
    for (const m of msgs) {
      const pending = (await this.redis.xpending(STREAM, GROUP, m.id, m.id, 1)) as [string, string, number, number][];
      const deliveries = pending[0]?.[3] ?? 1;
      if (deliveries > poisonDeliveries) await this.deadLetter(m, `DELIVERY_LIMIT_EXCEEDED (${deliveries})`);
      else out.push(m);
    }
    return out;
  }

  /**
   * A worker was declared DEAD: re-publish whatever it had read but not yet claimed (its pending entries) right away,
   * instead of waiting out the visibility timeout, and remove the dead consumer from the group. Copies are harmless:
   * the Postgres ownership check drops whichever delivery comes second. Returns (attemptId, newMessageId) pairs.
   */
  async takeOverConsumer(consumer: string): Promise<[string, string][]> {
    const pending = (await this.redis.xpending(STREAM, GROUP, "-", "+", 1000, consumer)) as [string, string, number, number][];
    const out: [string, string][] = [];
    if (pending.length) {
      const claimed = (await this.redis.xclaim(STREAM, GROUP, "fg-recovery", 0, ...pending.map((p) => p[0]))) as [string, string[] | null][];
      for (const [id, f] of claimed) {
        if (f?.length) {
          const fields = toFields(f);
          const newId = await this.redis.xadd(STREAM, "MAXLEN", "~", MAX_LEN, "*", ...Object.entries({ ...fields, q: String(Date.now()) }).flat());
          if (newId && fields.a) out.push([fields.a, newId]);
        }
        await this.ack(id);
      }
    }
    await this.redis.xgroup("DELCONSUMER", STREAM, GROUP, consumer);
    return out;
  }

  async ack(messageId: string) {
    await this.redis.xack(STREAM, GROUP, messageId);
  }

  /** Poison messages (unparseable / redelivered too often) are parked in a DLQ stream instead of looping forever. */
  async deadLetter(m: Message, reason: string) {
    const f = { ...m.fields, reason, original_id: m.id };
    await this.redis.xadd(DLQ_STREAM, "MAXLEN", "~", 100_000, "*", ...Object.entries(f).flat());
    await this.ack(m.id);
  }

  async groupInfo(): Promise<Record<string, unknown>> {
    const groups = (await this.redis.xinfo("GROUPS", STREAM)) as unknown[][];
    for (const g of groups) {
      const m = toFields(g.map(String));
      if (m.name === GROUP) {
        const raw: Record<string, unknown> = {};
        for (let i = 0; i + 1 < g.length; i += 2) raw[String(g[i])] = g[i + 1];
        return raw;
      }
    }
    return {};
  }

  /**
   * True if the message is still on its way: not yet delivered to the group, or delivered but un-acked
   * (XAUTOCLAIM will recover it). False means it was lost (Redis data loss, failed publish).
   */
  async isInFlight(messageId: string | null, lastDeliveredId: string): Promise<boolean> {
    if (!messageId) return false;
    try {
      if (compareIds(messageId, lastDeliveredId) > 0) return (await this.redis.xrange(STREAM, messageId, messageId)).length > 0;
      return ((await this.redis.xpending(STREAM, GROUP, messageId, messageId, 1)) as unknown[]).length > 0;
    } catch {
      return false;
    }
  }

  async stats() {
    const [length, dlq, pending, g, consumers] = await Promise.all([
      this.redis.xlen(STREAM), this.redis.xlen(DLQ_STREAM),
      this.redis.xpending(STREAM, GROUP) as Promise<[number, ...unknown[]]>, this.groupInfo(),
      this.redis.xinfo("CONSUMERS", STREAM, GROUP) as Promise<unknown[][]>,
    ]);
    return {
      stream: STREAM, group: GROUP, length, deadLetterLength: dlq, pending: Number(pending[0] ?? 0),
      lag: g.lag ?? null, lastDeliveredId: g["last-delivered-id"] ?? null,
      consumers: consumers.map((c) => {
        const o: Record<string, unknown> = {};
        for (let i = 0; i + 1 < c.length; i += 2) o[String(c[i])] = c[i + 1];
        return o;
      }),
    };
  }

  async recentDeadLetters(count: number) {
    const rows = await this.redis.xrevrange(DLQ_STREAM, "+", "-", "COUNT", count);
    return rows.map(([id, f]) => ({ ...toFields(f), id }));
  }
}

export function compareIds(a: string, b: string): number {
  const [x0, x1 = "0"] = a.split("-"), [y0, y1 = "0"] = b.split("-");
  const c = Number(BigInt(x0) - BigInt(y0));
  return c !== 0 ? Math.sign(c) : Math.sign(Number(BigInt(x1) - BigInt(y1)));
}
