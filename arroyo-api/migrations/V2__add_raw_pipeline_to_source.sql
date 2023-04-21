ALTER TABLE sources
ADD COLUMN raw_pipeline_job_id VARCHAR(8);
REFERENCES pipelines(id);