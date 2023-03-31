----------- api keys -------------------
--! get_api_key
SELECT user_id, organization_id
FROM api_keys
WHERE api_key = :api_key;

----------- connections ----------------
--! create_connection
INSERT INTO connections (organization_id, created_by, name, type, config)
VALUES (:organization_id, :created_by, :name, :type, :config);

--! get_connections : DbConnection()
SELECT connections.id as id, connections.name as name, connections.type as type, connections.config as config,
    (SELECT count(*) as source_count FROM sources where sources.connection_id = connections.id) as source_count,
    (SELECT count(*) as sink_count FROM sinks where sinks.connection_id = connections.id) as sink_count
FROM connections
WHERE connections.organization_id = :organization_id
ORDER BY COALESCE(connections.updated_at, connections.created_at) DESC;

--! get_connection : DbConnection()
SELECT connections.id as id, connections.name as name, connections.type as type, connections.config as config,
    (SELECT count(*) as source_count FROM sources where sources.connection_id = connections.id) as source_count,
    (SELECT count(*) as sink_count FROM sinks where sinks.connection_id = connections.id) as sink_count
FROM connections
WHERE connections.organization_id = :organization_id AND connections.name = :name
GROUP BY connections.id
ORDER BY COALESCE(connections.updated_at, connections.created_at) DESC;

--! delete_connection
DELETE FROM connections
WHERE organization_id = :organization_id AND name = :name;

----------- schemas --------------------

--! create_schema
INSERT INTO schemas (organization_id, created_by, name, kafka_schema_registry, type, config)
VALUES (:organization_id, :created_by, :name, :kafka_schema_registry, :type, :config) RETURNING id;


----------- sources --------------------

--! create_source(connection_id?)
INSERT INTO sources
(organization_id, created_by, name, type, config, schema_id, connection_id)
VALUES (:organization_id, :created_by, :name, :type, :config, :schema_id, :connection_id);

--! get_sources: (schema_config?, connection_name?, connection_type?, connection_config?, source_config?)
SELECT
    schemas.id as schema_id,
    schemas.type as schema_type,
    schemas.config as schema_config,
    schemas.kafka_schema_registry as kafka_schema_registry,
    sd.id as source_id,
    sd.name as source_name,
    sd.type as source_type,
    sd.config as source_config,
    connections.name as connection_name,
    connections.type as connection_type,
    connections.config as connection_config,
    (SELECT count(*) as consumer_count
        FROM pipeline_sources
        WHERE pipeline_sources.source_id = sd.id
    ) as consumer_count
FROM sources as sd
INNER JOIN schemas ON schema_id = schemas.id
LEFT JOIN connections ON connection_id = connections.id
WHERE sd.organization_id = :organization_id;

--! delete_source
DELETE FROM sources
WHERE organization_id = :organization_id AND name = :name;

----------- sinks ----------------------

--! create_sink(connection_id?)
INSERT INTO sinks
(organization_id, created_by, name, type, connection_id, config)
VALUES (:organization_id, :created_by, :name, :type, :connection_id, :config);

--! get_sinks: (connection_id?)
SELECT
    id,
    name,
    type,
    connection_id,
    config,
    (SELECT count(*) as producer_count FROM pipeline_sinks WHERE pipeline_sinks.sink_id = sinks.id) as producers
FROM sinks
WHERE organization_id = :organization_id;

--! delete_sink
DELETE FROM sinks
WHERE organization_id = :organization_id AND name = :name;

----------- pipelines -------------------

--! create_pipeline
INSERT INTO pipelines (organization_id, created_by, name, type, current_version)
VALUES (:organization_id, :created_by, :name, :type, :current_version)
RETURNING id;

--! create_pipeline_definition (textual_repr?)
INSERT INTO pipeline_definitions (organization_id, created_by, pipeline_id, version, textual_repr, program)
VALUES  (:organization_id, :created_by, :pipeline_id, :version, :textual_repr, :program)
RETURNING id;

--! get_pipeline: DbPipeline(textual_repr?)
SELECT pipelines.id as id, name, type, textual_repr, program FROM pipelines
INNER JOIN pipeline_definitions as d ON pipelines.id = d.pipeline_id AND pipelines.current_version = d.version
WHERE pipelines.id = :pipeline_id AND pipelines.organization_id = :organization_id;

--! add_pipeline_source
INSERT INTO pipeline_sources(pipeline_id, source_id)
VALUES (:pipeline_id, :source_id);

--! add_pipeline_sink
INSERT INTO pipeline_sinks(pipeline_id, sink_id)
VALUES (:pipeline_id, :sink_id);

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

--! get_jobs: (start_time?, finish_time?, state?, tasks?, textual_repr?, failure_message?, run_id?)
SELECT job_configs.id as id, pipeline_name, stop, textual_repr, start_time, finish_time, state, tasks, pipeline_definition, failure_message, run_id
FROM job_configs
         LEFT JOIN job_statuses ON job_configs.id = job_statuses.id
         INNER JOIN pipeline_definitions ON pipeline_definition = pipeline_definitions.id
WHERE job_configs.organization_id = :organization_id AND ttl_micros IS NULL
ORDER BY COALESCE(job_configs.updated_at, job_configs.created_at) DESC;

--! get_job_details: (start_time?, finish_time?, state?, tasks?, textual_repr?, failure_message?, run_id?)
SELECT pipeline_name, stop, parallelism_overrides, state, start_time, finish_time, tasks, textual_repr, program, pipeline_definition, failure_message, run_id
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
