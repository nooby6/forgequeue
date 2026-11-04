DROP TRIGGER IF EXISTS prevent_job_dependency_cycles ON job_dependencies;
DROP FUNCTION IF EXISTS prevent_job_dependency_cycles();
