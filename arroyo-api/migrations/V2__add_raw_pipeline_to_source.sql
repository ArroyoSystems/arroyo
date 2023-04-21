-- add column to sources for the raw_pipeline_id
-- which is a foreign key to the pipelines table
ALTER TABLE sources
-- add string column
ADD COLUMN raw_pipeline_job_id VARCHAR(8);
REFERENCES pipelines(id);