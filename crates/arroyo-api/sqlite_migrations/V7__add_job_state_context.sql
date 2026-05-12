ALTER TABLE job_statuses ADD COLUMN state_context TEXT DEFAULT '{"version": 1}' NOT NULL;
