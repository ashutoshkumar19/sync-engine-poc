-- Database Initialization

CREATE EXTENSION IF NOT EXISTS "pgcrypto";

-- Table: sync_metadata
CREATE TABLE IF NOT EXISTS sync_metadata (
    tenant_id         TEXT NOT NULL,
    pipeline_id       TEXT NOT NULL,
    record_id         TEXT NOT NULL,
    field_state       JSONB NOT NULL DEFAULT '{}',
    src_sync_event_id TEXT,
    created_at        TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at        TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (tenant_id, pipeline_id, record_id)
);

-- Table: sync_events
CREATE TABLE IF NOT EXISTS sync_events (
    id                UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    event_id          TEXT NOT NULL,
    tenant_id         TEXT NOT NULL,
    pipeline_id       TEXT NOT NULL,
    record_id         TEXT NOT NULL,
    destination_id    TEXT NOT NULL,
    operation         TEXT NOT NULL,
    payload           JSONB NOT NULL,
    status            TEXT NOT NULL DEFAULT 'PENDING', -- PENDING, PROCESSING, SUCCESS, FAILED, SUPERSEDED
    echo_status       TEXT,                            -- NULL, RESOLVED
    attempt_count     INT NOT NULL DEFAULT 0,
    max_retries       INT NOT NULL DEFAULT 3,
    next_attempt_at   TIMESTAMPTZ,
    lease_expires_at  TIMESTAMPTZ,
    error_message     TEXT,
    created_at        TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at        TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT uq_pipeline_event UNIQUE (pipeline_id, event_id)
);

CREATE INDEX IF NOT EXISTS idx_sync_events_claim 
ON sync_events (pipeline_id, status, next_attempt_at) 
WHERE status = 'PENDING';

CREATE INDEX IF NOT EXISTS idx_sync_events_record 
ON sync_events (tenant_id, record_id);

CREATE INDEX IF NOT EXISTS idx_sync_events_echo 
ON sync_events (event_id, echo_status);

-- Table: system_a_accounts (System A mock table)
CREATE TABLE IF NOT EXISTS system_a_accounts (
    id           TEXT PRIMARY KEY,
    first_name   TEXT,
    last_name    TEXT,
    email        TEXT,
    phone        TEXT,
    company      TEXT,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at   TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Table: system_b_crm_records (System B mock table)
CREATE TABLE IF NOT EXISTS system_b_crm_records (
    id               TEXT PRIMARY KEY,
    full_name        TEXT,
    email            TEXT,
    phone            TEXT,
    account_name     TEXT,
    sync_event_id    TEXT,
    created_at       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    last_modified_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
