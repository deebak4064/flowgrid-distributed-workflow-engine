// Workflow definitions: immutable, versioned DAG snapshots. Executions pin a version id, so publishing v2
// never changes an in-flight v1 run.
import crypto from "node:crypto";
import { tx, type Db, type Tx } from "./db.ts";
import { validateDag, type WorkflowDefinition } from "./dag.ts";
import { ApiError, pageOf } from "./errors.ts";

export class WorkflowService {
  readonly db: Db;
  constructor(db: Db) { this.db = db; }

  async knownTypes(): Promise<Set<string>> {
    return new Set((await this.db.query("select type from task_definitions")).rows.map((r) => r.type));
  }

  async validate(def: WorkflowDefinition) {
    return validateDag(def, await this.knownTypes());
  }

  async create(orgId: string, userId: string | null, name: string, description: string | undefined, tags: string[] | undefined, def: WorkflowDefinition) {
    await this.requireValid(def);
    const id = await tx(this.db, async (t) => {
      const ins = await t.c.query(`insert into workflows(org_id, name, description, tags, created_by) values ($1, $2, $3, $4, $5)
        on conflict (org_id, name) do nothing returning id`, [orgId, name, description ?? "", tags ?? [], userId]);
      if (ins.rows.length === 0) throw ApiError.conflict(`A workflow named '${name}' already exists`);
      await this.publishVersion(t, orgId, ins.rows[0].id, 0, def, "initial version", userId);
      return ins.rows[0].id as string;
    });
    return this.detail(orgId, id);
  }

  /** Publishes a new immutable version. expectedLockVersion gives editors optimistic concurrency (stale edit -> 409). */
  async update(orgId: string, userId: string | null, id: string, b: { expectedLockVersion?: number | null; description?: string; tags?: string[];
    definition?: WorkflowDefinition; changeNote?: string }) {
    if (b.definition) await this.requireValid(b.definition);
    await tx(this.db, async (t) => {
      const cur = await t.c.query("select current_version, lock_version from workflows where id = $1 and org_id = $2 and not archived for update", [id, orgId]);
      if (cur.rows.length === 0) throw ApiError.notFound("Workflow");
      const { current_version, lock_version } = cur.rows[0];
      if (b.expectedLockVersion != null && Number(b.expectedLockVersion) !== lock_version)
        throw new ApiError(409, "concurrent_modification", `Workflow was modified by someone else (lock version ${lock_version})`);
      await t.c.query(`update workflows set description = coalesce($1, description), tags = coalesce($2, tags), updated_at = now(),
        lock_version = lock_version + 1 where id = $3`, [b.description ?? null, b.tags ?? null, id]);
      if (b.definition) await this.publishVersion(t, orgId, id, current_version, b.definition, b.changeNote ?? "", userId);
    });
    return this.detail(orgId, id);
  }

  async archive(orgId: string, id: string) {
    await tx(this.db, async (t) => {
      const r = await t.c.query(`update workflows set archived = true, name = name || '~archived~' || left(id::text, 8), lock_version = lock_version + 1
        where id = $1 and org_id = $2 and not archived`, [id, orgId]);
      if (!r.rowCount) throw ApiError.notFound("Workflow");
      await t.c.query("update scheduled_jobs set enabled = false where workflow_id = $1", [id]);
    });
  }

  private async requireValid(def: WorkflowDefinition) {
    const r = await this.validate(def);
    if (!r.valid) throw ApiError.badRequest("Invalid workflow definition", r.errors);
  }

  private async publishVersion(t: Tx, orgId: string, workflowId: string, currentVersion: number, def: WorkflowDefinition, note: string, userId: string | null) {
    const json = JSON.stringify({ nodes: def.nodes, edges: def.edges ?? [] });
    const checksum = crypto.createHash("sha256").update(json).digest("hex");
    if (currentVersion > 0) {
      const cur = await t.c.query("select checksum from workflow_versions where workflow_id = $1 and version = $2", [workflowId, currentVersion]);
      if (cur.rows[0]?.checksum === checksum) return; // no-op edit: don't mint a new version
    }
    const v = (await t.c.query(`insert into workflow_versions(workflow_id, version, definition, checksum, change_note, created_by)
      values ($1, $2, $3::jsonb, $4, $5, $6) returning id`, [workflowId, currentVersion + 1, json, checksum, note, userId])).rows[0].id;
    const defaults = new Map((await t.c.query("select type, default_timeout_ms, default_max_attempts, default_backoff_ms from task_definitions where org_id is null or org_id = $1 order by org_id nulls first", [orgId])).rows.map((r) => [r.type, r]));
    const nodeIds = new Map<string, string>();
    for (const n of def.nodes) {
      const td = defaults.get(n.type)!;
      const backoff = n.retry?.backoffMs ?? td.default_backoff_ms;
      const id = (await t.c.query(`insert into workflow_nodes(workflow_version_id, node_key, name, task_type, params, timeout_ms, max_attempts,
          backoff_base_ms, backoff_max_ms, priority, deadline_ms, trigger_rule, position_x, position_y)
        values ($1, $2, $3, $4, $5::jsonb, $6, $7, $8, $9, $10, $11, $12, $13, $14) returning id`,
        [v, n.key, n.name, n.type, JSON.stringify(n.params ?? {}), n.timeoutMs ?? td.default_timeout_ms, n.retry?.maxAttempts ?? td.default_max_attempts,
          backoff, n.retry?.maxBackoffMs ?? Math.max(backoff * 32, 30_000), n.priority ?? 5, n.deadlineMs ?? null, n.trigger ?? "ALL_SUCCESS",
          n.position?.x ?? 0, n.position?.y ?? 0])).rows[0].id;
      nodeIds.set(n.key, id);
    }
    for (const e of def.edges ?? [])
      await t.c.query("insert into workflow_edges(workflow_version_id, from_node_id, to_node_id, condition) values ($1, $2, $3, $4::jsonb)",
        [v, nodeIds.get(e.from), nodeIds.get(e.to), e.when ? JSON.stringify(e.when) : null]);
    await t.c.query("update workflows set current_version = $1, updated_at = now(), lock_version = lock_version + 1 where id = $2", [currentVersion + 1, workflowId]);
  }

  async detail(orgId: string, id: string) {
    const w = (await this.db.query("select * from workflows where id = $1 and org_id = $2", [id, orgId])).rows[0];
    if (!w) throw ApiError.notFound("Workflow");
    const versions = (await this.db.query("select version, checksum, change_note, created_at from workflow_versions where workflow_id = $1 order by version desc", [id])).rows;
    const def = (await this.db.query("select definition from workflow_versions where workflow_id = $1 and version = $2", [id, w.current_version])).rows[0]?.definition ?? null;
    return { id: w.id, name: w.name, description: w.description, tags: w.tags, currentVersion: w.current_version, lockVersion: w.lock_version,
      createdAt: w.created_at, updatedAt: w.updated_at, definition: def,
      versions: versions.map((v) => ({ version: v.version, checksum: v.checksum, changeNote: v.change_note, createdAt: v.created_at })),
      analysis: def ? validateDag(def) : null };
  }

  async version(orgId: string, id: string, version: number) {
    const r = await this.db.query(`select v.definition from workflow_versions v join workflows w on w.id = v.workflow_id
      where w.id = $1 and w.org_id = $2 and v.version = $3`, [id, orgId, version]);
    if (r.rows.length === 0) throw ApiError.notFound("Workflow version");
    return r.rows[0].definition;
  }

  async list(orgId: string, q: { q?: string; page?: unknown; size?: unknown }) {
    const { page, size } = pageOf(q.page, q.size);
    const like = q.q?.trim() ? `%${q.q.trim().toLowerCase().replace(/[%_]/g, (m) => "\\" + m)}%` : "%";
    const total = (await this.db.query("select count(*) as n from workflows where org_id = $1 and not archived and lower(name) like $2", [orgId, like])).rows[0].n;
    const rows = (await this.db.query(`select w.id, w.name, w.description, w.tags, w.current_version, w.updated_at,
        (select count(*) from workflow_nodes n join workflow_versions v on v.id = n.workflow_version_id where v.workflow_id = w.id and v.version = w.current_version) as nodes,
        s.total, s.ok, s.failed, s.running, s.last_run,
        (select status from workflow_executions x where x.workflow_id = w.id order by created_at desc limit 1) as last_status
      from workflows w
      left join lateral (select count(*) as total, count(*) filter (where status = 'SUCCEEDED') as ok, count(*) filter (where status = 'FAILED') as failed,
          count(*) filter (where status in ('PENDING', 'RUNNING')) as running, max(created_at) as last_run
        from workflow_executions e where e.workflow_id = w.id) s on true
      where w.org_id = $1 and not w.archived and lower(w.name) like $2 order by w.updated_at desc limit $3 offset $4`, [orgId, like, size, page * size])).rows;
    return { items: rows.map((r) => ({ id: r.id, name: r.name, description: r.description, tags: r.tags, currentVersion: r.current_version,
      nodeCount: r.nodes, updatedAt: r.updated_at, executions: r.total, succeeded: r.ok, failed: r.failed, running: r.running,
      lastStatus: r.last_status, lastRunAt: r.last_run })), page, size, total };
  }

  async taskDefinitions(orgId: string) {
    const rows = (await this.db.query(`select id, type, description, category, default_timeout_ms, default_max_attempts, default_backoff_ms
      from task_definitions where org_id is null or org_id = $1 order by category, type`, [orgId])).rows;
    return rows.map((r) => ({ id: r.id, type: r.type, description: r.description, category: r.category, defaultTimeoutMs: r.default_timeout_ms,
      defaultMaxAttempts: r.default_max_attempts, defaultBackoffMs: r.default_backoff_ms }));
  }
}
