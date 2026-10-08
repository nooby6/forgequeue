ALTER TABLE jobs DROP CONSTRAINT IF EXISTS jobs_valid_max_attempts;
ALTER TABLE jobs DROP COLUMN IF EXISTS max_attempts;
