-- Optional per-job scheduler config.
-- NULL means "no per-job config — use the controller's global scheduler config".
ALTER TABLE job_configs ADD COLUMN scheduler_config JSONB;
