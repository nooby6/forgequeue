DROP INDEX IF EXISTS idx_jobs_lease_expiry;

ALTER TABLE jobs
    DROP COLUMN IF EXISTS lease_expires_at,
    DROP COLUMN IF EXISTS locked_at,
    DROP COLUMN IF EXISTS locked_by;
