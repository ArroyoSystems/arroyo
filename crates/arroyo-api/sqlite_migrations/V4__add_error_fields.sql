ALTER TABLE job_log_messages ADD COLUMN error_domain TEXT DEFAULT 'internal';
ALTER TABLE job_log_messages ADD COLUMN retry_hint TEXT DEFAULT 'with_backoff';

ALTER TABLE job_statuses ADD COLUMN failure_domain TEXT;