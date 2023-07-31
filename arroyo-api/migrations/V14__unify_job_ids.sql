-- going forward the 'id' column will be used to store the public id

ALTER TABLE job_configs
DROP COLUMN pub_id;

ALTER TABLE job_configs
ALTER COLUMN id TYPE VARCHAR;

ALTER TABLE job_configs
ADD CONSTRAINT job_configs_unique_id UNIQUE (id);

ALTER TABLE checkpoints
ALTER COLUMN job_id TYPE VARCHAR;

ALTER TABLE job_statuses
ALTER COLUMN id TYPE VARCHAR;

ALTER TABLE job_log_messages
ALTER COLUMN job_id TYPE VARCHAR;
