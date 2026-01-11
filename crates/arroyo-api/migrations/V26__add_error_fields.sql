ALTER TABLE job_log_messages ADD COLUMN error_domain TEXT DEFAULT 'Internal';
ALTER TABLE job_log_messages ADD COLUMN retry_hint TEXT DEFAULT 'WithBackoff';

ALTER TABLE job_statuses ADD COLUMN failure_domain TEXT;