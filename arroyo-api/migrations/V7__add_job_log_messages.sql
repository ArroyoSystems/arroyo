create type log_level as ENUM ('info', 'warn', 'error');

CREATE TABLE job_log_messages (
    id BIGSERIAL PRIMARY KEY,
    created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP NOT NULL,
    job_id VARCHAR(8) REFERENCES job_configs(id) ON DELETE CASCADE,
    operator_id TEXT,
    task_index BIGINT,
    log_level log_level DEFAULT 'info',
    message TEXT NOT NULL,
    details TEXT
);
