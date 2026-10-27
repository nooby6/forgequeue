DROP INDEX IF EXISTS idx_jobs_cancellation_requested;
ALTER TABLE jobs DROP COLUMN IF EXISTS cancellation_requested_at;
