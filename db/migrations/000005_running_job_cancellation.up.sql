ALTER TABLE jobs
    ADD COLUMN cancellation_requested_at TIMESTAMPTZ;

CREATE INDEX idx_jobs_cancellation_requested
    ON jobs(queue_name, cancellation_requested_at)
    WHERE status = 'running' AND cancellation_requested_at IS NOT NULL;
