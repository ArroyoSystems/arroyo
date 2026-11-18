-- Per-job scheduler config overlay. Same shape as the controller's
-- global scheduler config (e.g. kubernetes-scheduler.*); an empty
-- object means "no overrides, use the global config as-is".
ALTER TABLE job_configs ADD COLUMN scheduler_config JSONB NOT NULL DEFAULT '{}'::jsonb;
