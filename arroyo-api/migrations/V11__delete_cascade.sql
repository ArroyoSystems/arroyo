-- add missing ON DELETE CASCADE to connection_table_pipelines
ALTER TABLE connection_table_pipelines RENAME COLUMN pipeline_id to pipeline_id_old;
ALTER TABLE connection_table_pipelines
    ADD COLUMN pipeline_id BIGINT REFERENCES pipelines(id) ON DELETE CASCADE;
UPDATE connection_table_pipelines SET pipeline_id = pipeline_id_old;
ALTER TABLE connection_table_pipelines DROP COLUMN pipeline_id_old;

-- add missing ON DELETE CASCADE to job_configs
ALTER TABLE job_configs RENAME COLUMN pipeline_id to pipeline_id_old;
ALTER TABLE job_configs
    ADD COLUMN pipeline_id BIGINT REFERENCES pipelines(id) ON DELETE CASCADE;
UPDATE job_configs SET pipeline_id = pipeline_id_old;
ALTER TABLE job_configs DROP COLUMN pipeline_id_old;
