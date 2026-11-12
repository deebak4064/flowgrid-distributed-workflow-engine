// Thin typed client for the FlowGrid REST API. Every screen reads real backend data through this.

const TOKEN_KEY = "fg.token";
const USER_KEY = "fg.user";

export type Role = "ADMIN" | "OPERATOR" | "VIEWER";
export interface User { id: string; email: string; displayName: string; role: Role; orgId: string; orgName: string; createdAt?: string; lastLoginAt?: string }

export class ApiError extends Error {
  constructor(public status: number, message: string, public code?: string, public errors?: string[]) { super(message); }
}

export const session = {
  token: () => localStorage.getItem(TOKEN_KEY),
  user: (): User | null => { try { return JSON.parse(localStorage.getItem(USER_KEY) ?? "null"); } catch { return null; } },
  save(token: string, user: User) { localStorage.setItem(TOKEN_KEY, token); localStorage.setItem(USER_KEY, JSON.stringify(user)); },
  clear() { localStorage.removeItem(TOKEN_KEY); localStorage.removeItem(USER_KEY); },
};

export async function api<T>(path: string, init: RequestInit & { json?: unknown } = {}): Promise<T> {
  const headers = new Headers(init.headers);
  const token = session.token();
  if (token) headers.set("Authorization", `Bearer ${token}`);
  if (init.json !== undefined) headers.set("Content-Type", "application/json");
  const res = await fetch(`/api/v1${path}`, { ...init, headers, body: init.json !== undefined ? JSON.stringify(init.json) : init.body });
  if (res.status === 401 && !path.startsWith("/auth/")) {
    session.clear();
    window.location.assign("/login");
  }
  if (!res.ok) {
    let body: { detail?: string; code?: string; errors?: string[] } = {};
    try { body = await res.json(); } catch { /* not json */ }
    throw new ApiError(res.status, body.detail ?? `Request failed (${res.status})`, body.code, body.errors);
  }
  if (res.status === 204) return undefined as T;
  return res.json() as Promise<T>;
}

export const qs = (o: Record<string, string | number | boolean | undefined | null>) => {
  const p = new URLSearchParams();
  for (const [k, v] of Object.entries(o)) if (v !== undefined && v !== null && v !== "") p.set(k, String(v));
  const s = p.toString();
  return s ? `?${s}` : "";
};

// ---------------------------------------------------------------- types

export interface Page<T> { items: T[]; page: number; size: number; total: number }

export type TaskStatus = "PENDING" | "READY" | "QUEUED" | "RUNNING" | "SUCCEEDED" | "FAILED" | "RETRYING" | "CANCELLED" | "SKIPPED";
export type ExecStatus = "PENDING" | "RUNNING" | "SUCCEEDED" | "FAILED" | "CANCELLED";

export interface Condition { path: string; op: string; value?: unknown }
export interface NodeDef {
  key: string; name: string; type: string; params?: Record<string, unknown>; timeoutMs?: number | null;
  retry?: { maxAttempts?: number | null; backoffMs?: number | null; maxBackoffMs?: number | null } | null;
  priority?: number | null; deadlineMs?: number | null; trigger?: "ALL_SUCCESS" | "ANY_SUCCESS" | null; position?: { x: number; y: number } | null;
}
export interface EdgeDef { from: string; to: string; when?: Condition | null }
export interface WorkflowDefinition { nodes: NodeDef[]; edges: EdgeDef[] }
export interface DagAnalysis { topologicalOrder: string[]; levels: string[][]; errors: string[]; valid: boolean }

export interface WorkflowSummary {
  id: string; name: string; description: string; tags: string[]; currentVersion: number; nodeCount: number; updatedAt: string;
  executions: number; succeeded: number; failed: number; running: number; lastStatus?: ExecStatus; lastRunAt?: string;
}
export interface WorkflowDetail {
  id: string; name: string; description: string; tags: string[]; currentVersion: number; lockVersion: number; createdAt: string; updatedAt: string;
  definition: WorkflowDefinition; versions: { version: number; checksum: string; changeNote: string; createdAt: string }[]; analysis?: DagAnalysis;
}
export interface TaskDefinition { id: string; type: string; description: string; category: string; defaultTimeoutMs: number; defaultMaxAttempts: number; defaultBackoffMs: number }

export interface ExecutionSummary {
  id: string; workflowId: string; workflowName: string; version: number; status: ExecStatus; trigger: string; priority?: number;
  totalTasks: number; remainingTasks: number; completedTasks: number; createdAt: string; startedAt?: string; finishedAt?: string; runAt?: string;
  durationMs?: number; error?: string;
}
export interface Attempt {
  attempt: number; attemptId: string; workerId?: string; status: string; queuedAt?: string; startedAt?: string; finishedAt?: string;
  durationMs?: number; backoffMs?: number; error?: string; errorType?: string;
}
export interface TaskView {
  id: string; nodeKey: string; name: string; taskType: string; status: TaskStatus; priority: number; effectivePriority: number;
  attempt: number; maxAttempts: number; leaseOwner?: string; leaseExpiresAt?: string; readyAt?: string; notBefore?: string; queuedAt?: string;
  startedAt?: string; finishedAt?: string; deadlineAt?: string; durationMs?: number; output?: unknown; error?: string; errorType?: string;
  timeoutMs: number; triggerRule: string; x: number; y: number; attempts: Attempt[];
}
export interface ExecutionDetail {
  execution: ExecutionSummary; input: unknown; deadlineAt?: string; tasks: TaskView[];
  edges: { from: string; to: string; condition?: Condition | null }[];
}
export interface ExecEvent {
  id?: number; executionId: string; taskId?: string; nodeKey?: string; type: string; from?: string; to?: string; attempt?: number;
  workerId?: string; message?: string; at: string;
}

export interface Worker {
  id: string; hostname: string; pid: number; concurrency: number; status: "ACTIVE" | "DRAINING" | "DEAD" | "STOPPED"; started_at: string;
  last_heartbeat_at: string; in_flight: number; completed_count: number; failed_count: number; heartbeat_age_ms: number; utilization: number;
  leased_tasks: number; task_type_count: number;
  recent: { at: string; in_flight: number; capacity: number; cpu_load: number; heap_used_mb: number }[];
}

export interface DeadLetter {
  id: string; taskExecutionId: string; executionId: string; workflowName: string; nodeKey: string; taskType: string; reason: string;
  error: string; attempts: number; createdAt: string; resolvedAt?: string; resolution?: string; executionStatus?: string;
}

export interface Schedule {
  id: string; workflowId: string; workflowName: string; name: string; type: "ONCE" | "CRON" | "INTERVAL"; cron?: string; intervalMs?: number;
  timezone: string; nextFireAt?: string; lastFiredAt?: string; fireCount: number; enabled: boolean; priority?: number; input: unknown;
  createdAt: string; lockVersion: number;
}
export interface CalendarEntry { scheduleId?: string; workflowId: string; title: string; workflowName: string; at: string; kind: "PLANNED" | "FIRED" | "DELAYED"; executionId?: string; status?: string }

export type Num = number | null;
export interface Metrics {
  windowMinutes: number; bucketSeconds: number;
  queue: { queued: number; runnable: number; backing_off: number; delayed: number; running: number; waiting_on_dependencies: number; oldest_runnable_age_ms: number };
  workers: { active: number; capacity: number; in_flight: number; utilization: number };
  tasks: { succeeded: number; failed_attempts: number; retries: number; completed_per_sec: number; failure_rate: number; avg_duration_ms: Num;
    p50_ms: Num; p95_ms: Num; p99_ms: Num; avg_queue_wait_ms: Num; p95_queue_wait_ms: Num };
  scheduling: { avg_delay_ms: Num; p95_delay_ms: Num };
  workflows: { started: number; succeeded: number; failed: number; cancelled: number; active: number; avg_duration_ms: Num; p95_duration_ms: Num };
  deadLetters: number;
  series: { t: string; completed: number; failed: number; retried: number; avg_duration_ms: Num; p95_duration_ms: Num }[];
  byTaskType: { task_type: string; succeeded: number; failed: number; avg_duration_ms: Num; p95_ms: Num }[];
}
