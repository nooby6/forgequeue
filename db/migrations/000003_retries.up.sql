ALTER TABLE jobs
    ADD COLUMN IF NOT EXISTS max_attempts INTEGER NOT NULL DEFAULT 3;

ALTER TABLE jobs
    ADD CONSTRAINT jobs_valid_max_attempts CHECK (max_attempts BETWEEN 1 AND 20);
