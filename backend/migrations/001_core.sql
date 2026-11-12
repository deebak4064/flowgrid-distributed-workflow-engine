-- FlowGrid core schema.
-- Postgres is the source of truth for every state transition; Redis Streams only carries dispatch messages.
-- Every task state change is a compare-and-set on (attempt_id, status[, lease_owner]) so a stale or
-- duplicate worker can never commit a result for an attempt it no longer owns.

-- ---------------------------------------------------------------- tenancy & auth
create table organizations (
    id          uuid primary key default gen_random_uuid(),
    name        text not null unique,
    slug        text not null unique,
    created_at  timestamptz not null default now()
);

create table users (
    id             uuid primary key default gen_random_uuid(),
    org_id         uuid not null references organizations(id) on delete cascade,
    email          text not null unique,
    display_name   text not null,
    password_hash  text not null,
    role           text not null check (role in ('ADMIN', 'OPERATOR', 'VIEWER')),
    created_at     timestamptz not null default now(),
    last_login_at  timestamptz
);
create index users_org_idx on users(org_id);

-- ---------------------------------------------------------------- definitions
-- Registry of executable task types ("execution functions"). org_id null = built-in, visible to all orgs.
create table task_definitions (
    id                    uuid primary key default gen_random_uuid(),
    org_id                uuid references organizations(id) on delete cascade,
    type                  text not null,
    description           text not null default '',
    category              text not null default 'core',
    default_timeout_ms    int  not null default 30000 check (default_timeout_ms between 100 and 3600000),
    default_max_attempts  int  not null default 3 check (default_max_attempts between 1 and 20),
    default_backoff_ms    int  not null default 500 check (default_backoff_ms >= 0),
    params_schema         jsonb not null default '{}'::jsonb,
    created_at            timestamptz not null default now(),
    unique nulls not distinct (org_id, type)
);

create table workflows (
    id               uuid primary key default gen_random_uuid(),
    org_id           uuid not null references organizations(id) on delete cascade,
    name             text not null,
    description      text not null default '',
    current_version  int  not null default 0,
    tags             text[] not null default '{}',
    archived         boolean not null default false,
    created_by       uuid references users(id) on delete set null,
    created_at       timestamptz not null default now(),
    updated_at       timestamptz not null default now(),
    lock_version     bigint not null default 0,          -- optimistic lock for concurrent definition edits
    unique (org_id, name)
);

-- Immutable snapshot. Executions pin a version id, so publishing v2 never changes an in-flight v1 run.
create table workflow_versions (
    id           uuid primary key default gen_random_uuid(),
    workflow_id  uuid not null references workflows(id) on delete cascade,
    version      int  not null,
    definition   jsonb not null,
    checksum     text not null,
    change_note  text not null default '',
    created_by   uuid references users(id) on delete set null,
    created_at   timestamptz not null default now(),
    unique (workflow_id, version)
);

create table workflow_nodes (
    id                   uuid primary key default gen_random_uuid(),
    workflow_version_id  uuid not null references workflow_versions(id) on delete cascade,
    node_key             text not null,
    name                 text not null,
    task_type            text not null,
    params               jsonb not null default '{}'::jsonb,
    timeout_ms           int  not null,
    max_attempts         int  not null,
    backoff_base_ms      int  not null,
    backoff_max_ms       int  not null,
    priority             int  not null default 5 check (priority between 0 and 10),
    deadline_ms          bigint,                          -- relative to execution creation, used by EDF
    trigger_rule         text not null default 'ALL_SUCCESS' check (trigger_rule in ('ALL_SUCCESS', 'ANY_SUCCESS')),
    position_x           real not null default 0,
    position_y           real not null default 0,
    unique (workflow_version_id, node_key)
);

create table workflow_edges (
    id                   uuid primary key default gen_random_uuid(),
    workflow_version_id  uuid not null references workflow_versions(id) on delete cascade,
    from_node_id         uuid not null references workflow_nodes(id) on delete cascade,
    to_node_id           uuid not null references workflow_nodes(id) on delete cascade,
    condition            jsonb,                           -- null = always taken
    unique (workflow_version_id, from_node_id, to_node_id),
    check (from_node_id <> to_node_id)
);
create index workflow_edges_from_idx on workflow_edges(from_node_id);

-- ---------------------------------------------------------------- scheduling
create table scheduled_jobs (
    id             uuid primary key default gen_random_uuid(),
    org_id         uuid not null references organizations(id) on delete cascade,
    workflow_id    uuid not null references workflows(id) on delete cascade,
    name           text not null,
    schedule_type  text not null check (schedule_type in ('ONCE', 'CRON', 'INTERVAL')),
    cron           text,
    interval_ms    bigint check (interval_ms is null or interval_ms >= 1000),
    timezone       text not null default 'UTC',
    next_fire_at   timestamptz,
    last_fired_at  timestamptz,
    fire_count     bigint not null default 0,
    input          jsonb not null default '{}'::jsonb,
    priority       int check (priority between 0 and 10),
    enabled        boolean not null default true,
    created_by     uuid references users(id) on delete set null,
    created_at     timestamptz not null default now(),
    lock_version   bigint not null default 0,
    check (schedule_type <> 'CRON' or cron is not null),
    check (schedule_type <> 'INTERVAL' or interval_ms is not null)
);
-- The scheduler polls "what fires next"; partial index keeps that O(log n) and ignores disabled jobs.
create index scheduled_jobs_due_idx on scheduled_jobs(next_fire_at) where enabled;

-- ---------------------------------------------------------------- executions
create table workflow_executions (
    id                   uuid primary key default gen_random_uuid(),
    org_id               uuid not null references organizations(id) on delete cascade,
    workflow_id          uuid not null references workflows(id) on delete cascade,
    workflow_version_id  uuid not null references workflow_versions(id),
    version              int  not null,
    status               text not null check (status in ('PENDING', 'RUNNING', 'SUCCEEDED', 'FAILED', 'CANCELLED')),
    trigger              text not null default 'API' check (trigger in ('API', 'SCHEDULE', 'RETRY', 'BENCHMARK')),
    input                jsonb not null default '{}'::jsonb,
    idempotency_key      text,
    priority             int,
    deadline_at          timestamptz,
    run_at               timestamptz,
    total_tasks          int not null,
    remaining_tasks      int not null check (remaining_tasks >= 0), -- decremented atomically; whoever hits 0 finalizes
    scheduled_job_id     uuid references scheduled_jobs(id) on delete set null,
    retry_of             uuid references workflow_executions(id) on delete set null,
    created_by           uuid references users(id) on delete set null,
    created_at           timestamptz not null default now(),
    started_at           timestamptz,
    finished_at          timestamptz,
    error                text,
    unique (org_id, idempotency_key)                      -- duplicate "execute" calls return the same run
);
create index workflow_executions_org_created_idx on workflow_executions(org_id, created_at desc);
create index workflow_executions_org_status_idx on workflow_executions(org_id, status, created_at desc);
create index workflow_executions_workflow_idx on workflow_executions(workflow_id, created_at desc);

create table task_executions (
    id                     uuid primary key default gen_random_uuid(),
    workflow_execution_id  uuid not null references workflow_executions(id) on delete cascade,
    org_id                 uuid not null,
    node_id                uuid not null references workflow_nodes(id),
    node_key               text not null,
    task_type              text not null,
    status                 text not null check (status in
                              ('PENDING', 'READY', 'QUEUED', 'RUNNING', 'SUCCEEDED', 'FAILED', 'RETRYING', 'CANCELLED', 'SKIPPED')),
    priority               int  not null,
    effective_priority     int  not null,                 -- priority + aging bonus (anti-starvation)
    deadline_at            timestamptz,
    ready_at               timestamptz,
    not_before             timestamptz,                   -- delayed start / retry backoff
    total_deps             int not null,
    pending_deps           int not null,
    satisfied_deps         int not null default 0,
    trigger_rule           text not null,
    attempt                int not null default 0,
    max_attempts           int not null,
    timeout_ms             int not null,
    backoff_base_ms        int not null,
    backoff_max_ms         int not null,
    attempt_id             uuid unique,                   -- fencing token for the current attempt
    lease_owner            text,
    lease_expires_at       timestamptz,
    stream_message_id      text,
    queued_at              timestamptz,
    started_at             timestamptz,
    finished_at            timestamptz,
    output                 jsonb,
    error                  text,
    error_type             text,
    idempotency_key        text not null,                 -- stable across attempts: <execution>:<node>
    created_at             timestamptz not null default now(),
    unique (workflow_execution_id, node_key)
);
create index task_executions_wf_idx on task_executions(workflow_execution_id);
-- Dispatch indexes: one per strategy, all partial on the runnable set so they stay small even with
-- millions of historical rows. The B-tree is the durable priority queue; it survives scheduler crashes.
create index task_ready_priority_idx on task_executions(effective_priority desc, ready_at, id)
    where status in ('READY', 'RETRYING');
create index task_ready_fifo_idx on task_executions(ready_at, id)
    where status in ('READY', 'RETRYING');
create index task_ready_edf_idx on task_executions(deadline_at nulls last, effective_priority desc, ready_at)
    where status in ('READY', 'RETRYING');
-- Recovery indexes.
create index task_running_lease_idx on task_executions(lease_expires_at) where status = 'RUNNING';
create index task_running_owner_idx on task_executions(lease_owner) where status = 'RUNNING';
create index task_queued_idx on task_executions(queued_at) where status = 'QUEUED';
create index task_org_finished_idx on task_executions(org_id, finished_at);

-- One row per attempt (first run included): the execution history behind timelines and retry metrics.
create table retry_attempts (
    id                 uuid primary key default gen_random_uuid(),
    task_execution_id  uuid not null references task_executions(id) on delete cascade,
    org_id             uuid not null,
    attempt            int  not null,
    attempt_id         uuid not null unique,
    worker_id          text,
    status             text not null check (status in
                          ('QUEUED', 'RUNNING', 'SUCCEEDED', 'FAILED', 'TIMED_OUT', 'LOST', 'CANCELLED', 'DISCARDED')),
    queued_at          timestamptz not null default now(),
    started_at         timestamptz,
    finished_at        timestamptz,
    duration_ms        int,
    backoff_ms         int,
    error              text,
    error_type         text,
    unique (task_execution_id, attempt)
);
create index retry_attempts_org_finished_idx on retry_attempts(org_id, finished_at);

create table dead_letter_tasks (
    id                     uuid primary key default gen_random_uuid(),
    org_id                 uuid,
    task_execution_id      uuid references task_executions(id) on delete cascade,
    workflow_execution_id  uuid references workflow_executions(id) on delete cascade,
    node_key               text,
    task_type              text,
    reason                 text not null check (reason in
                              ('RETRIES_EXHAUSTED', 'NON_RETRYABLE', 'POISON_MESSAGE', 'LEASE_LOST_EXHAUSTED', 'UNKNOWN_TASK_TYPE')),
    error                  text,
    attempts               int not null default 0,
    stream_message_id      text,
    payload                jsonb not null default '{}'::jsonb,
    created_at             timestamptz not null default now(),
    resolved_at            timestamptz,
    resolution             text check (resolution in ('REPLAYED', 'DISCARDED'))
);
create index dead_letter_open_idx on dead_letter_tasks(org_id, created_at desc) where resolved_at is null;

-- ---------------------------------------------------------------- workers
create table worker_nodes (
    id                 text primary key,
    hostname           text not null,
    pid                bigint,
    concurrency        int not null,
    status             text not null check (status in ('ACTIVE', 'DRAINING', 'DEAD', 'STOPPED')),
    task_types         text[] not null default '{}',
    started_at         timestamptz not null default now(),
    last_heartbeat_at  timestamptz not null default now(),
    in_flight          int not null default 0,
    completed_count    bigint not null default 0,
    failed_count       bigint not null default 0,
    app_version        text not null default 'dev'
);
create index worker_nodes_active_idx on worker_nodes(last_heartbeat_at) where status = 'ACTIVE';

create table worker_heartbeats (
    id            bigserial primary key,
    worker_id     text not null references worker_nodes(id) on delete cascade,
    at            timestamptz not null default now(),
    in_flight     int not null,
    capacity      int not null,
    cpu_load      real,
    heap_used_mb  int
);
create index worker_heartbeats_worker_idx on worker_heartbeats(worker_id, at desc);
create index worker_heartbeats_at_brin on worker_heartbeats using brin(at);   -- cheap range deletes for retention

-- ---------------------------------------------------------------- events, idempotency, audit
create table execution_events (
    id                     bigserial primary key,
    org_id                 uuid not null,
    workflow_execution_id  uuid not null,
    task_execution_id      uuid,
    node_key               text,
    type                   text not null,
    from_status            text,
    to_status              text,
    attempt                int,
    worker_id              text,
    message                text,
    created_at             timestamptz not null default now()
);
create index execution_events_exec_idx on execution_events(workflow_execution_id, id);
create index execution_events_org_idx on execution_events(org_id, id desc);
create index execution_events_created_brin on execution_events using brin(created_at);

-- Logical-operation dedupe: the row is inserted in the SAME transaction as the business side effect,
-- so a retried task either sees the prior result or performs the effect once.
create table idempotency_records (
    scope       text not null,
    key         text not null,
    org_id      uuid,
    result      jsonb not null default '{}'::jsonb,
    created_at  timestamptz not null default now(),
    expires_at  timestamptz not null default now() + interval '30 days',
    primary key (scope, key)
);
create index idempotency_expires_idx on idempotency_records(expires_at);

-- Cluster-wide runtime settings (e.g. active scheduling strategy). Schedulers re-read every few seconds.
create table system_settings (
    key         text primary key,
    value       jsonb not null,
    updated_at  timestamptz not null default now()
);
insert into system_settings(key, value) values
    ('scheduler.strategy', '"PRIORITY"'),
    ('scheduler.aging_seconds', '30');

create table audit_events (
    id             bigserial primary key,
    org_id         uuid,
    user_id        uuid,
    actor_email    text,
    action         text not null,
    resource_type  text not null,
    resource_id    text,
    details        jsonb not null default '{}'::jsonb,
    ip             text,
    created_at     timestamptz not null default now()
);
create index audit_events_org_idx on audit_events(org_id, created_at desc);
