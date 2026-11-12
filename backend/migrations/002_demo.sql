-- Business tables for the demo applications (insurance claims, financial reporting).
-- They are NOT part of the engine: handlers write here, and every side-effect table carries a
-- unique idempotency_key so a retried or duplicated task can never create a second business record.

create table customers (
    id              text not null,
    org_id          uuid not null references organizations(id) on delete cascade,
    name            text not null,
    tier            text not null check (tier in ('BRONZE', 'SILVER', 'GOLD', 'PLATINUM')),
    policy_number   text not null,
    policy_limit    numeric(12, 2) not null,
    deductible      numeric(12, 2) not null,
    customer_since  date not null,
    prior_claims    int not null default 0,
    primary key (org_id, id)
);

create table claims (
    id               uuid primary key default gen_random_uuid(),
    org_id           uuid not null references organizations(id) on delete cascade,
    claim_number     text not null,
    customer_id      text not null,
    claim_type       text not null,
    amount           numeric(12, 2) not null check (amount > 0),
    incident_date    date not null,
    description      text not null default '',
    documents        jsonb not null default '[]'::jsonb,
    status           text not null default 'SUBMITTED',
    workflow_execution_id uuid,
    idempotency_key  text not null unique,
    submitted_at     timestamptz not null default now(),
    unique (org_id, claim_number)
);

create table claim_decisions (
    id               uuid primary key default gen_random_uuid(),
    org_id           uuid not null,
    claim_id         uuid not null references claims(id) on delete cascade,
    decision         text not null check (decision in ('AUTO_APPROVED', 'MANUAL_REVIEW', 'REJECTED')),
    risk_score       numeric(5, 2) not null,
    payable_amount   numeric(12, 2) not null default 0,
    reason           text not null,
    idempotency_key  text not null unique,
    created_at       timestamptz not null default now()
);

create table notifications (
    id               uuid primary key default gen_random_uuid(),
    org_id           uuid not null,
    claim_id         uuid references claims(id) on delete cascade,
    channel          text not null,
    recipient        text not null,
    subject          text not null,
    body             text not null,
    idempotency_key  text not null unique,
    created_at       timestamptz not null default now()
);

create table financial_reports (
    id               uuid primary key default gen_random_uuid(),
    org_id           uuid not null,
    period           text not null,
    summary          jsonb not null,
    idempotency_key  text not null unique,
    created_at       timestamptz not null default now()
);
