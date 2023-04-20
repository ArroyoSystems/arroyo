-- add column to sources for the raw_pipeline_id
-- which is a foreign key to the pipelines table
ALTER TABLE sources
ADD COLUMN raw_pipeline_id INTEGER 
REFERENCES pipelines(id);