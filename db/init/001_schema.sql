CREATE EXTENSION IF NOT EXISTS pgcrypto;
CREATE TABLE IF NOT EXISTS jobs (
 id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
 idempotency_key TEXT UNIQUE,
 queue_name TEXT NOT NULL,
 job_type TEXT NOT NULL,
 payload JSONB NOT NULL,
 status TEXT NOT NULL CHECK (status IN ('pending','running','succeeded','failed','cancelled')),
 priority INTEGER NOT NULL DEFAULT 0,
 attempts INTEGER NOT NULL DEFAULT 0,
 available_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
 locked_by TEXT,
 locked_at TIMESTAMPTZ,
 lease_expires_at TIMESTAMPTZ,
 created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
 updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_jobs_queue_status_available ON jobs(queue_name,status,available_at);
CREATE INDEX IF NOT EXISTS idx_jobs_lease_expiry ON jobs(status,lease_expires_at) WHERE status='running';