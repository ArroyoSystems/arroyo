-- We have two tables, pipelines and pipeline_definitions, that are
-- conceptually one table, and which generally have a 1-1 relationship.
-- pipeline_definitions stores the history of pipeline definitions, and
-- pipelines references the current version.
--
-- This distinction has not proved useful, and so this migration is
-- intended to merge the two tables into one. At the end of this,

ALTER TABLE pipelines ADD COLUMN textual_repr TEXT;
ALTER TABLE pipelines ADD COLUMN program BYTEA;
ALTER TABLE pipelines ADD COLUMN udfs JSONB NOT NULL DEFAULT '[]';

-- fill in the new fields on pipelines
UPDATE pipelines
SET textual_repr = pipeline_definitions.textual_repr,
    program = pipeline_definitions.program,
    udfs = pipeline_definitions.udfs
FROM pipeline_definitions
WHERE pipelines.id = pipeline_definitions.pipeline_id
AND pipelines.current_version = pipeline_definitions.version;

ALTER TABLE pipelines DROP COLUMN current_version;
ALTER TABLE pipelines ALTER COLUMN textual_repr SET NOT NULL;
ALTER TABLE pipelines ALTER COLUMN program SET NOT NULL;

-- modify connection_table_pipelines so that it references the pipeline, instead of pipeline_definition
ALTER TABLE connection_table_pipelines RENAME COLUMN pipeline_id to pipeline_definition_id;
ALTER TABLE connection_table_pipelines ADD COLUMN pipeline_id BIGINT REFERENCES pipelines(id);

UPDATE connection_table_pipelines
SET pipeline_id = pipeline_definition_id;

ALTER TABLE connection_table_pipelines ALTER COLUMN pipeline_id SET NOT NULL;
ALTER TABLE connection_table_pipelines DROP COLUMN pipeline_definition_id;

-- modify job_configs to reference the pipeline instead of pipeline_definition
ALTER TABLE job_configs ADD COLUMN pipeline_id BIGINT REFERENCES pipelines(id);

UPDATE job_configs
SET pipeline_id = pipeline_definition;

ALTER TABLE job_configs ALTER COLUMN pipeline_id SET NOT NULL;
ALTER TABLE job_configs DROP COLUMN pipeline_definition;

-- drop pipeline_Definitions
DROP TABLE pipeline_definitions;
