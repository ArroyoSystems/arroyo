ALTER TABLE job_statuses ADD COLUMN state_context JSONB DEFAULT '{"version": 1}' NOT NULL;
