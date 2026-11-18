-- Per-job pipeline config overlay. Same shape as the controller's
-- global pipeline config; an empty object means "no overrides, use
-- the global config as-is".
ALTER TABLE job_configs ADD COLUMN pipeline_config JSONB NOT NULL DEFAULT '{}'::jsonb;
