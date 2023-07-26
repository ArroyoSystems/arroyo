UPDATE api_keys
SET pub_id = COALESCE(pub_id, CAST(id as VARCHAR));

ALTER TABLE api_keys
ALTER COLUMN pub_id SET NOT NULL;

UPDATE connections
SET pub_id = COALESCE(pub_id, CAST(id as VARCHAR));

ALTER TABLE connections
ALTER COLUMN pub_id SET NOT NULL;

UPDATE schemas
SET pub_id = COALESCE(pub_id, CAST(id as VARCHAR));

ALTER TABLE schemas
ALTER COLUMN pub_id SET NOT NULL;

UPDATE pipelines
SET pub_id = COALESCE(pub_id, CAST(id as VARCHAR));

ALTER TABLE pipelines
ALTER COLUMN pub_id SET NOT NULL;

UPDATE job_configs
SET pub_id = COALESCE(pub_id, CAST(id as VARCHAR));

ALTER TABLE job_configs
ALTER COLUMN pub_id SET NOT NULL;

UPDATE checkpoints
SET pub_id = COALESCE(pub_id, CAST(id as VARCHAR));

ALTER TABLE checkpoints
ALTER COLUMN pub_id SET NOT NULL;

UPDATE job_statuses
SET pub_id = COALESCE(pub_id, CAST(id as VARCHAR));

ALTER TABLE job_statuses
ALTER COLUMN pub_id SET NOT NULL;

UPDATE cluster_info
SET pub_id = COALESCE(pub_id, CAST(id as VARCHAR));

ALTER TABLE cluster_info
ALTER COLUMN pub_id SET NOT NULL;

UPDATE job_log_messages
SET pub_id = COALESCE(pub_id, CAST(id as VARCHAR));

ALTER TABLE job_log_messages
ALTER COLUMN pub_id SET NOT NULL;

UPDATE connection_tables
SET pub_id = COALESCE(pub_id, CAST(id as VARCHAR));

ALTER TABLE connection_tables
ALTER COLUMN pub_id SET NOT NULL;

UPDATE connection_table_pipelines
SET pub_id = COALESCE(pub_id, CAST(id as VARCHAR));

ALTER TABLE connection_table_pipelines
ALTER COLUMN pub_id SET NOT NULL;
