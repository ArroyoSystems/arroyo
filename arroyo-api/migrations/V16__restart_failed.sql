CREATE TYPE restart_mode as ENUM (
    'safe', 'force');

ALTER TABLE job_configs
ADD COLUMN restart_nonce int not null default 0,
ADD COLUMN restart_mode restart_mode not null default 'safe';

ALTER TABLE job_statuses
ADD COLUMN restart_nonce int not null default 0;