----------- api keys -------------------
--! get_api_key
SELECT user_id, organization_id
FROM api_keys
WHERE api_key = :api_key;

----------- connections ----------------
--! create_connection
INSERT INTO connections (organization_id, created_by, name, type, config)
VALUES (:organization_id, :created_by, :name, :type, :config)
RETURNING id;

--! get_connections : DbConnection()
SELECT
    id,
    name,
    type,
    config
FROM connections
WHERE connections.organization_id = :organization_id
ORDER BY COALESCE(connections.updated_at, connections.created_at) DESC;

--! get_connection : DbConnection()
SELECT
    id,
    name,
    type,
    config
FROM connections
WHERE connections.organization_id = :organization_id AND connections.name = :name
ORDER BY COALESCE(connections.updated_at, connections.created_at) DESC;

--! get_connection_by_id: DbConnection()
SELECT
    id,
    name,
    type,
    config
FROM connections
WHERE connections.organization_id = :organization_id AND connections.id = :id
ORDER BY COALESCE(connections.updated_at, connections.created_at) DESC;

--! delete_connection
DELETE FROM connections
WHERE organization_id = :organization_id AND name = :name;

----------- schemas --------------------

--! create_schema
INSERT INTO schemas (organization_id, created_by, name, kafka_schema_registry, type, config)
VALUES (:organization_id, :created_by, :name, :kafka_schema_registry, :type, :config) RETURNING id;


------- connection tables -------------
--! create_connection_table(connection_id?, schema?)
INSERT INTO connection_tables
(organization_id, created_by, name, table_type, connector, connection_id, config, schema)
VALUES (:organization_id, :created_by, :name, :table_type, :connector, :connection_id, :config, :schema);

--! get_connection_tables: (connection_id?, connection_name?, connection_type?, connection_config?, schema?)
SELECT connection_tables.id as id,
    connection_tables.name as name,
    connection_tables.connector as connector,
    connection_tables.table_type as table_type,
    connection_tables.config as config,
    connection_tables.schema as schema,
    connection_tables.connection_id as connection_id,
    connections.name as connection_name,
    connections.type as connection_type,
    connections.config as connection_config,
    (SELECT count(*) as pipeline_count
        FROM connection_table_pipelines
        WHERE connection_table_pipelines.connection_table_id = connection_tables.id
    ) as consumer_count

FROM connection_tables
LEFT JOIN connections ON connections.id = connection_tables.connection_id
WHERE connection_tables.organization_id = :organization_id;

--! delete_connection_table
DELETE FROM connection_tables
WHERE organization_id = :organization_id AND id = :id;


----------- pipelines -------------------

--! create_pipeline
INSERT INTO pipelines (organization_id, created_by, name, type, current_version)
VALUES (:organization_id, :created_by, :name, :type, :current_version)
RETURNING id;

--! create_pipeline_definition (textual_repr?, udfs?)
INSERT INTO pipeline_definitions (organization_id, created_by, pipeline_id, version, textual_repr, udfs, program)
VALUES  (:organization_id, :created_by, :pipeline_id, :version, :textual_repr, :udfs, :program)
RETURNING id;

--! get_pipeline: DbPipeline(textual_repr?, udfs?)
SELECT pipelines.id as id, name, type, textual_repr, udfs, program FROM pipelines
INNER JOIN pipeline_definitions as d ON pipelines.id = d.pipeline_id AND pipelines.current_version = d.version
WHERE pipelines.id = :pipeline_id AND pipelines.organization_id = :organization_id;

--! add_pipeline_connection_table
INSERT INTO connection_table_pipelines(pipeline_id, connection_table_id)
VALUES (:pipeline_id, :connection_table_id);

--! delete_pipeline
DELETE FROM pipelines
WHERE id = :pipeline_id AND organization_id = :organization_id;


----------- jobs -----------------------

--! update_job(checkpoint_interval_micros?, stop?, parallelism_overrides?)
UPDATE job_configs
SET
   updated_at = :updated_at,
   updated_by = :updated_by,

   stop = COALESCE(:stop, stop),
   checkpoint_interval_micros = COALESCE(:checkpoint_interval_micros, checkpoint_interval_micros),
   parallelism_overrides = COALESCE(:parallelism_overrides, parallelism_overrides)
WHERE id = :job_id AND organization_id = :organization_id;

--! create_job(ttl_micros?)
INSERT INTO job_configs
    (id, organization_id, pipeline_name, created_by, pipeline_definition, checkpoint_interval_micros, ttl_micros)
VALUES (:id, :organization_id, :pipeline_name, :created_by, :pipeline_definition, :checkpoint_interval_micros, :ttl_micros);

--! create_job_status
INSERT INTO job_statuses (id, organization_id) VALUES (:id, :organization_id);

--! get_jobs: (start_time?, finish_time?, state?, tasks?, textual_repr?, failure_message?, run_id?, udfs?)
SELECT job_configs.id as id, pipeline_name, stop, textual_repr, start_time, finish_time, state, tasks, pipeline_definition, failure_message, run_id, udfs
FROM job_configs
         LEFT JOIN job_statuses ON job_configs.id = job_statuses.id
         INNER JOIN pipeline_definitions ON pipeline_definition = pipeline_definitions.id
WHERE job_configs.organization_id = :organization_id AND ttl_micros IS NULL
ORDER BY COALESCE(job_configs.updated_at, job_configs.created_at) DESC;

--! get_job_details: (start_time?, finish_time?, state?, tasks?, textual_repr?, udfs?, failure_message?, run_id?)
SELECT pipeline_name, stop, parallelism_overrides, state, start_time, finish_time, tasks, textual_repr, program, pipeline_definition, udfs, failure_message, run_id
FROM job_configs
         LEFT JOIN job_statuses ON job_configs.id = job_statuses.id
         INNER JOIN pipeline_definitions ON pipeline_definition = pipeline_definitions.id
WHERE job_configs.organization_id = :organization_id AND job_configs.id = :job_id;

--! get_job_checkpoints: (finish_time?)
SELECT epoch, state_backend, start_time, finish_time FROM checkpoints
WHERE job_id = :job_id
    AND organization_id = :organization_id
    AND state != 'compacted'
    AND state != 'failed'
ORDER BY epoch;

--! get_checkpoint_details: (finish_time?, operators?)
SELECT epoch, state_backend, start_time, finish_time, operators FROM checkpoints
WHERE job_id = :job_id
    AND organization_id = :organization_id
    AND epoch = :epoch
    AND state != 'failed';

--! delete_pipeline_for_job
DELETE FROM pipelines WHERE pipelines.id = (
    SELECT pipelines.id as pipeline_id
    FROM job_configs
    INNER JOIN pipeline_definitions as pd ON job_configs.pipeline_definition = pd.id
    INNER JOIN pipelines ON pipelines.id = pd.pipeline_id
    WHERE job_configs.id = :job_id AND job_configs.organization_id = :organization_id);

--! get_operator_errors
SELECT jlm.job_id, jlm.operator_id, jlm.task_index, jlm.created_at, jlm.log_level, jlm.message, jlm.details
FROM job_log_messages jlm
INNER JOIN (
    SELECT operator_id, task_index, MAX(created_at) AS max_created_at
    FROM job_log_messages
    WHERE job_id = :job_id
    GROUP BY operator_id, task_index
) jlm_max ON jlm.operator_id = jlm_max.operator_id AND jlm.task_index = jlm_max.task_index AND jlm.created_at = jlm_max.max_created_at
WHERE jlm.job_id = :job_id
  AND jlm.log_level = 'error';
