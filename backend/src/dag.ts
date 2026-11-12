// Pure workflow logic: DAG validation and topological analysis, edge conditions, retry backoff, strategies.
// No I/O here, so all of it is unit-tested directly.

export type TriggerRule = "ALL_SUCCESS" | "ANY_SUCCESS";
export interface Condition { path: string; op: string; value?: unknown }
export interface NodeDef {
  key: string; name: string; type: string; params?: Record<string, unknown> | null; timeoutMs?: number | null;
  retry?: { maxAttempts?: number | null; backoffMs?: number | null; maxBackoffMs?: number | null } | null;
  priority?: number | null; deadlineMs?: number | null; trigger?: TriggerRule | null; position?: { x: number; y: number } | null;
}
export interface EdgeDef { from: string; to: string; when?: Condition | null }
export interface WorkflowDefinition { nodes: NodeDef[]; edges?: EdgeDef[] }
export interface DagAnalysis { topologicalOrder: string[]; levels: string[][]; errors: string[]; valid: boolean }

const KEY = /^[a-z0-9][a-z0-9_-]{0,63}$/;
const OPS = new Set(["eq", "neq", "gt", "gte", "lt", "lte", "in", "exists", "truthy"]);

/**
 * Structural validation + Kahn's algorithm, level by level: O(V + E). Levels are the sets of tasks that can
 * run in parallel. If Kahn cannot consume every node there is a cycle, and a DFS recovers one concrete cycle.
 */
export function validateDag(def: WorkflowDefinition, knownTypes?: Set<string>): DagAnalysis {
  const errors: string[] = [];
  const nodes = new Map<string, NodeDef>();
  if (!def?.nodes?.length) errors.push("workflow needs at least one task");
  if ((def?.nodes?.length ?? 0) > 200) errors.push("at most 200 tasks per workflow");
  for (const n of def?.nodes ?? []) {
    const k = `node '${n.key}'`;
    if (!KEY.test(n.key ?? "")) errors.push(`${k}: key must be lowercase letters, digits, _ or - (max 64)`);
    if (nodes.has(n.key)) errors.push(`duplicate node key '${n.key}'`);
    nodes.set(n.key, n);
    if (!n.name?.trim()) errors.push(`${k}: name is required`);
    if (knownTypes && !knownTypes.has(n.type)) errors.push(`${k}: unknown task type '${n.type}'`);
    if (n.timeoutMs != null && (n.timeoutMs < 100 || n.timeoutMs > 3_600_000)) errors.push(`${k}: timeoutMs must be between 100 and 3600000`);
    if (n.priority != null && (n.priority < 0 || n.priority > 10)) errors.push(`${k}: priority must be between 0 and 10`);
    if (n.deadlineMs != null && n.deadlineMs < 0) errors.push(`${k}: deadlineMs must be >= 0`);
    const r = n.retry;
    if (r) {
      if (r.maxAttempts != null && (r.maxAttempts < 1 || r.maxAttempts > 20)) errors.push(`${k}: retry.maxAttempts must be between 1 and 20`);
      if (r.backoffMs != null && r.backoffMs < 0) errors.push(`${k}: retry.backoffMs must be >= 0`);
      if (r.maxBackoffMs != null && r.backoffMs != null && r.maxBackoffMs < r.backoffMs) errors.push(`${k}: retry.maxBackoffMs must be >= backoffMs`);
    }
  }
  const out = new Map<string, string[]>([...nodes.keys()].map((k) => [k, []]));
  const indeg = new Map<string, number>([...nodes.keys()].map((k) => [k, 0]));
  const seen = new Set<string>();
  for (const e of def?.edges ?? []) {
    let ok = true;
    if (!nodes.has(e.from)) { errors.push(`edge references unknown node '${e.from}'`); ok = false; }
    if (!nodes.has(e.to)) { errors.push(`edge references unknown node '${e.to}'`); ok = false; }
    if (e.from === e.to) { errors.push(`node '${e.from}' depends on itself`); ok = false; }
    if (seen.has(`${e.from}->${e.to}`)) { errors.push(`duplicate edge ${e.from} -> ${e.to}`); ok = false; }
    if (e.when && !OPS.has(String(e.when.op).toLowerCase())) { errors.push(`edge ${e.from} -> ${e.to}: unknown condition op '${e.when.op}'`); ok = false; }
    seen.add(`${e.from}->${e.to}`);
    if (ok) { out.get(e.from)!.push(e.to); indeg.set(e.to, indeg.get(e.to)! + 1); }
  }
  if (errors.length) return { topologicalOrder: [], levels: [], errors, valid: false };

  for (const n of nodes.values())
    if (n.trigger === "ANY_SUCCESS" && indeg.get(n.key) === 0) errors.push(`node '${n.key}': ANY_SUCCESS requires at least one dependency`);

  const remaining = new Map(indeg);
  const order: string[] = [];
  const levels: string[][] = [];
  let frontier = [...nodes.keys()].filter((k) => remaining.get(k) === 0);
  if (frontier.length === 0) errors.push("workflow has no root task (every task has a dependency)");
  while (frontier.length) {
    levels.push(frontier);
    order.push(...frontier);
    const next: string[] = [];
    for (const k of frontier) for (const d of out.get(k)!) {
      remaining.set(d, remaining.get(d)! - 1);
      if (remaining.get(d) === 0) next.push(d);
    }
    frontier = next;
  }
  if (order.length < nodes.size) errors.push(`cyclic dependency detected: ${findCycle([...nodes.keys()], out).join(" -> ")}`);
  return { topologicalOrder: order, levels, errors, valid: errors.length === 0 };
}

/** Iterative DFS with white/grey/black colouring; returns one cycle as a closed path (a -> b -> a). */
export function findCycle(keys: string[], out: Map<string, string[]>): string[] {
  const color = new Map<string, number>();
  const parent = new Map<string, string>();
  for (const start of keys) {
    if (color.get(start)) continue;
    const stack: [string, number][] = [[start, 0]];
    color.set(start, 1);
    while (stack.length) {
      const frame = stack[stack.length - 1];
      const children = out.get(frame[0]) ?? [];
      if (frame[1] < children.length) {
        const child = children[frame[1]++];
        const c = color.get(child) ?? 0;
        if (c === 1) {
          const path = [child];
          for (let cur = frame[0]; cur !== child; cur = parent.get(cur)!) path.push(cur);
          path.push(child);
          return path.reverse();
        }
        if (c === 0) { color.set(child, 1); parent.set(child, frame[0]); stack.push([child, 0]); }
      } else {
        color.set(frame[0], 2);
        stack.pop();
      }
    }
  }
  return [];
}

// ------------------------------------------------------------------ edge conditions

/**
 * Evaluates an edge guard {"path":"route","op":"eq","value":"AUTO_APPROVE"} against the upstream output.
 * Deliberately tiny and side-effect free: no expression language, nothing a tenant can abuse.
 */
export function evaluate(cond: Condition | null | undefined, output: unknown): boolean {
  if (!cond) return true;
  const actual = resolve(output, cond.path ?? "");
  const expected = cond.value;
  switch (String(cond.op ?? "eq").toLowerCase()) {
    case "exists": return actual !== undefined && actual !== null;
    case "truthy": return truthy(actual);
    case "eq": return equal(actual, expected);
    case "neq": return !equal(actual, expected);
    case "gt": return typeof actual === "number" && typeof expected === "number" && actual > expected;
    case "gte": return typeof actual === "number" && typeof expected === "number" && actual >= expected;
    case "lt": return typeof actual === "number" && typeof expected === "number" && actual < expected;
    case "lte": return typeof actual === "number" && typeof expected === "number" && actual <= expected;
    case "in": return Array.isArray(expected) && expected.some((e) => equal(actual, e));
    default: return false;
  }
}

function resolve(root: unknown, path: string): unknown {
  let cur = root;
  if (!path) return cur;
  for (const p of path.split(".")) cur = cur !== null && typeof cur === "object" ? (cur as Record<string, unknown>)[p] : undefined;
  return cur;
}

function truthy(v: unknown): boolean {
  if (v === undefined || v === null) return false;
  if (typeof v === "object") return Object.keys(v as object).length > 0;
  return Boolean(v);
}

function equal(a: unknown, b: unknown): boolean {
  if (b === undefined || b === null) return a === undefined || a === null;
  if (typeof a === "object" || typeof b === "object") return JSON.stringify(a) === JSON.stringify(b);
  return a === b || String(a) === String(b);
}

// ------------------------------------------------------------------ retry backoff

/**
 * Exponential backoff with "equal jitter": cap = min(max, base * 2^(attempt-1)), delay = cap/2 + rand[0, cap/2].
 * Jitter de-synchronises retries after a shared outage (no thundering herd) while guaranteeing at least half
 * the exponential delay, so a failing dependency still gets breathing room.
 */
export function backoffMs(failedAttempt: number, baseMs: number, maxMs: number, rand = Math.random): number {
  if (baseMs <= 0) return 0;
  const cap = Math.min(maxMs, baseMs * 2 ** Math.min(Math.max(failedAttempt - 1, 0), 30));
  const half = Math.floor(cap / 2);
  return half + Math.floor(rand() * (cap - half + 1));
}

// ------------------------------------------------------------------ scheduling strategies

/**
 * Dispatch ordering over the runnable set (READY/RETRYING). Each order is served by its own partial B-tree
 * index, so picking the next N tasks is an index range scan: O(log n + N) regardless of backlog size.
 *  FIFO      fair and predictable, ignores urgency (head-of-line blocking for urgent work).
 *  PRIORITY  urgent first; starvation risk for low priority, mitigated by aging (effective_priority grows with wait).
 *  EDF       minimises deadline misses when the load is feasible; degrades badly under overload (domino effect).
 */
export const STRATEGIES = {
  FIFO: "ready_at, id",
  PRIORITY: "effective_priority desc, ready_at, id",
  EDF: "deadline_at nulls last, effective_priority desc, ready_at",
} as const;
export type Strategy = keyof typeof STRATEGIES;
