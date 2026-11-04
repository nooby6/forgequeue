CREATE OR REPLACE FUNCTION prevent_job_dependency_cycles()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    -- Serialize graph mutations so two concurrent, opposite edges cannot
    -- both pass the recursive cycle check before either transaction commits.
    PERFORM pg_advisory_xact_lock(748392616);

    IF EXISTS (
        WITH RECURSIVE dependency_path(job_id) AS (
            SELECT NEW.depends_on_job_id
            UNION
            SELECT dependency.depends_on_job_id
            FROM job_dependencies dependency
            JOIN dependency_path path ON dependency.job_id = path.job_id
            WHERE TG_OP <> 'UPDATE'
               OR dependency.job_id <> OLD.job_id
               OR dependency.depends_on_job_id <> OLD.depends_on_job_id
        )
        SELECT 1
        FROM dependency_path
        WHERE job_id = NEW.job_id
    ) THEN
        RAISE EXCEPTION 'job dependency would create a cycle'
            USING ERRCODE = '23514',
                  CONSTRAINT = 'job_dependencies_acyclic';
    END IF;

    RETURN NEW;
END;
$$;

CREATE TRIGGER prevent_job_dependency_cycles
BEFORE INSERT OR UPDATE ON job_dependencies
FOR EACH ROW
EXECUTE FUNCTION prevent_job_dependency_cycles();
