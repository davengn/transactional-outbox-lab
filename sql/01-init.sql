-- sql/01-init.sql

create table scan_jobs (
                           id uuid primary key default gen_random_uuid(),
                           tenant_id uuid not null,
                           source_name text not null,
                           status text not null default 'PENDING',
                           created_at timestamptz not null default now()
);

-- Shape matches the cdc_outbox_events design from the CDC evaluation doc
create table cdc_outbox_events (
                                   id uuid primary key default gen_random_uuid(),
                                   aggregate_type text not null,
                                   aggregate_id text not null,
                                   event_type text not null,
                                   schema_version int not null default 1,
                                   tenant_id uuid not null,
                                   occurred_at timestamptz not null default now(),
                                   payload jsonb not null
);

-- Comparison path only (Step 9): the "old" app-polling model, kept deliberately separate
-- from cdc_outbox_events so the two paths never double-publish the same row.
create table legacy_outbox_events (
                                      id uuid primary key default gen_random_uuid(),
                                      aggregate_type text not null,
                                      aggregate_id text not null,
                                      event_type text not null,
                                      tenant_id uuid not null,
                                      occurred_at timestamptz not null default now(),
                                      payload jsonb not null,
                                      published boolean not null default false,
                                      published_at timestamptz
);

create table heartbeat_ping (
                                pinged_at timestamptz not null default now()
);

-- Critical: publish only inserts. Cleanup DELETEs and any future status UPDATEs
-- never generate WAL traffic Debezium has to filter through. See Step 12.
create publication cdc_outbox_pub for table cdc_outbox_events, heartbeat_ping with (publish = 'insert');