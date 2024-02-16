ALTER TABLE pipeline_definitions
ADD COLUMN udfs JSONB NOT NULL DEFAULT '[]';