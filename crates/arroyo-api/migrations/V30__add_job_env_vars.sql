-- Per-job environment variables forwarded to workers at scheduling time.
-- Stored as a JSON object of the form {"KEY": "VALUE", ...}.
ALTER TABLE job_configs ADD COLUMN env_vars JSONB NOT NULL DEFAULT '{}'::jsonb;
