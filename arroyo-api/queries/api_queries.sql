----------- api keys -------------------
--! get_api_key
SELECT user_id, organization_id
FROM api_keys
WHERE api_key = :api_key;

----------- connection profiles ----------------
--! create_connection_profile
INSERT INTO connection_profiles (pub_id, organization_id, created_by, name, type, config)
VALUES (:pub_id, :organization_id, :created_by, :name, :type, :config)
RETURNING id;

--! get_connection_profiles : DbConnectionProfile()
SELECT
    id,
    pub_id,
    name,
    type,
    config
FROM connection_profiles
WHERE connection_profiles.organization_id = :organization_id
ORDER BY COALESCE(connection_profiles.updated_at, connection_profiles.created_at) DESC;

--! get_connection_profile_by_pub_id: DbConnectionProfile()
SELECT
    id,
    pub_id,
    name,
    type,
    config
FROM connection_profiles
WHERE connection_profiles.organization_id = :organization_id AND connection_profiles.pub_id = :pub_id
ORDER BY COALESCE(connection_profiles.updated_at, connection_profiles.created_at) DESC;

--! delete_connection_profile
DELETE FROM connection_profiles
WHERE organization_id = :organization_id AND name = :name;

----------- schemas --------------------

--! create_schema
INSERT INTO schemas (pub_id, organization_id, created_by, name, kafka_schema_registry, type, config)
VALUES (:pub_id, :organization_id, :created_by, :name, :kafka_schema_registry, :type, :config) RETURNING id;


------- connection tables -------------
--! create_connection_table(profile_id?, schema?)
INSERT INTO connection_tables
(pub_id, organization_id, created_by, name, table_type, connector, connection_id, config, schema)
VALUES (:pub_id, :organization_id, :created_by, :name, :table_type, :connector, :profile_id, :config, :schema);

--: DbConnectionTable (profile_id?, profile_name?, profile_type?, profile_config?, schema?)

--! get_connection_tables: DbConnectionTable
SELECT connection_tables.id as id,
    connection_tables.pub_id as pub_id,
    connection_tables.name as name,
    connection_tables.created_at as created_at,
    connection_tables.connector as connector,
    connection_tables.table_type as table_type,
    connection_tables.config as config,
    connection_tables.schema as schema,
    connection_profiles.pub_id as profile_id,
    connection_profiles.name as profile_name,
    connection_profiles.type as profile_type,
    connection_profiles.config as profile_config,
    (SELECT count(*) as pipeline_count
        FROM connection_table_pipelines
        WHERE connection_table_pipelines.connection_table_id = connection_tables.id
    ) as consumer_count
FROM connection_tables
LEFT JOIN connection_profiles ON connection_profiles.id = connection_tables.connection_id
WHERE connection_tables.organization_id = :organization_id
    AND (connection_tables.created_at < (
        SELECT created_at FROM connection_tables
        WHERE pub_id = :starting_after
    ) OR :starting_after = '')
ORDER BY connection_tables.created_at DESC
LIMIT CASE WHEN :limit > 0 THEN :limit ELSE NULL END;

--! get_all_connection_tables: DbConnectionTable
SELECT connection_tables.id as id,
    connection_tables.pub_id as pub_id,
    connection_tables.name as name,
    connection_tables.created_at as created_at,
    connection_tables.connector as connector,
    connection_tables.table_type as table_type,
    connection_tables.config as config,
    connection_tables.schema as schema,
    connection_profiles.pub_id as profile_id,
    connection_profiles.name as profile_name,
    connection_profiles.type as profile_type,
    connection_profiles.config as profile_config,
    (SELECT count(*) as pipeline_count
        FROM connection_table_pipelines
        WHERE connection_table_pipelines.connection_table_id = connection_tables.id
    ) as consumer_count
FROM connection_tables
LEFT JOIN connection_profiles ON connection_profiles.id = connection_tables.connection_id
WHERE connection_tables.organization_id = :organization_id
ORDER BY connection_tables.created_at DESC;

--! get_connection_table: DbConnectionTable
SELECT connection_tables.id as id,
    connection_tables.pub_id as pub_id,
    connection_tables.name as name,
    connection_tables.created_at as created_at,
    connection_tables.connector as connector,
    connection_tables.table_type as table_type,
    connection_tables.config as config,
    connection_tables.schema as schema,
    connection_profiles.pub_id as profile_id,
    connection_profiles.name as profile_name,
    connection_profiles.type as profile_type,
    connection_profiles.config as profile_config,
    (SELECT count(*) as pipeline_count
        FROM connection_table_pipelines
        WHERE connection_table_pipelines.connection_table_id = connection_tables.id
    ) as consumer_count
FROM connection_tables
LEFT JOIN connection_profiles ON connection_profiles.id = connection_tables.connection_id
WHERE connection_tables.organization_id = :organization_id AND connection_tables.pub_id = :pub_id;

--! delete_connection_table
DELETE FROM connection_tables
WHERE organization_id = :organization_id AND pub_id = :pub_id;


----------- pipelines -------------------

--: DbPipeline (state?, ttl_micros?)

--! create_pipeline(udfs?, textual_repr?)
INSERT INTO pipelines (pub_id, organization_id, created_by, name, type, textual_repr, udfs, program)
VALUES (:pub_id, :organization_id, :created_by, :name, :type, :textual_repr, :udfs, :program)
RETURNING id;

--! get_pipelines : DbPipeline
SELECT pipelines.pub_id, name, type, textual_repr, udfs, program, checkpoint_interval_micros, stop, pipelines.created_at, state, parallelism_overrides, ttl_micros
FROM pipelines
    INNER JOIN job_configs on pipelines.id = job_configs.pipeline_id
    LEFT JOIN job_statuses ON job_configs.id = job_statuses.id
WHERE pipelines.organization_id = :organization_id
    AND pipelines.pub_id IS NOT NULL
    AND ttl_micros IS NULL
    AND (pipelines.created_at < (
        SELECT created_at FROM pipelines
        WHERE pub_id = :starting_after
    ) OR :starting_after = '')
ORDER BY pipelines.created_at DESC
LIMIT :limit::integer;

--! get_pipeline: DbPipeline
SELECT pipelines.pub_id, name, type, textual_repr, udfs, program, checkpoint_interval_micros, stop, pipelines.created_at, state, parallelism_overrides, ttl_micros
FROM pipelines
    INNER JOIN job_configs on pipelines.id = job_configs.pipeline_id
    LEFT JOIN job_statuses ON job_configs.id = job_statuses.id
WHERE pipelines.pub_id = :pub_id AND pipelines.organization_id = :organization_id;

--! add_pipeline_connection_table
INSERT INTO connection_table_pipelines(pub_id, pipeline_id, connection_table_id)
VALUES (:pub_id, :pipeline_id, :connection_table_id);

--! delete_pipeline
DELETE FROM pipelines
WHERE pub_id = :pub_id AND organization_id = :organization_id;


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

--! restart_job(mode)
UPDATE job_configs
SET
   updated_at = :updated_at,
   updated_by = :updated_by,
   restart_nonce = restart_nonce + 1,
   restart_mode = :mode
WHERE id = :job_id AND organization_id = :organization_id;

--! create_job(ttl_micros?)
INSERT INTO job_configs
(id, organization_id, pipeline_name, created_by, pipeline_id, checkpoint_interval_micros, ttl_micros)
VALUES (:id, :organization_id, :pipeline_name, :created_by, :pipeline_id, :checkpoint_interval_micros, :ttl_micros);

--! create_job_status
INSERT INTO job_statuses (pub_id, id, organization_id) VALUES (:pub_id, :id, :organization_id);

--! get_jobs: (start_time?, finish_time?, state?, tasks?, textual_repr?, failure_message?, run_id?, udfs)
SELECT job_configs.id as id, pipeline_name, stop, textual_repr, start_time, finish_time, state, tasks, pipeline_id, failure_message, run_id, udfs
FROM job_configs
         LEFT JOIN job_statuses ON job_configs.id = job_statuses.id
         INNER JOIN pipelines ON pipeline_id = pipelines.id
WHERE job_configs.organization_id = :organization_id AND ttl_micros IS NULL
ORDER BY COALESCE(job_configs.updated_at, job_configs.created_at) DESC;

--! get_pipeline_jobs : DbPipelineJob(start_time?, finish_time?, state?, tasks?, failure_message?, run_id?)
SELECT job_configs.id, stop, start_time, finish_time, state, tasks, failure_message, run_id, checkpoint_interval_micros, job_configs.created_at
FROM job_configs
         LEFT JOIN job_statuses ON job_configs.id = job_statuses.id
         INNER JOIN pipelines ON pipelines.id = job_configs.pipeline_id
WHERE job_configs.organization_id = :organization_id AND pipelines.pub_id = :pub_id
ORDER BY job_configs.created_at DESC;

--! get_all_jobs : DbPipelineJob(start_time?, finish_time?, state?, tasks?, failure_message?, run_id?)
SELECT job_configs.id, stop, start_time, finish_time, state, tasks, failure_message, run_id, checkpoint_interval_micros, job_configs.created_at
FROM job_configs
         LEFT JOIN job_statuses ON job_configs.id = job_statuses.id
         INNER JOIN pipelines ON pipelines.id = job_configs.pipeline_id
WHERE job_configs.organization_id = :organization_id AND ttl_micros IS NULL
ORDER BY job_configs.created_at DESC;

--! get_pipeline_job : DbPipelineJob(start_time?, finish_time?, state?, tasks?, failure_message?, run_id?)
SELECT job_configs.id, stop, start_time, finish_time, state, tasks, failure_message, run_id, checkpoint_interval_micros, job_configs.created_at
FROM job_configs
         LEFT JOIN job_statuses ON job_configs.id = job_statuses.id
         INNER JOIN pipelines ON pipelines.id = job_configs.pipeline_id
WHERE job_configs.organization_id = :organization_id AND job_configs.id = :job_id
ORDER BY job_configs.created_at DESC;


--! get_job_details: (start_time?, finish_time?, state?, tasks?, textual_repr?, udfs, failure_message?, run_id?)
SELECT pipeline_name, stop, parallelism_overrides, state, start_time, finish_time, tasks, textual_repr, program, pipeline_id, udfs, failure_message, run_id
FROM job_configs
         LEFT JOIN job_statuses ON job_configs.id = job_statuses.id
         INNER JOIN pipelines ON pipeline_id = pipelines.id
WHERE job_configs.organization_id = :organization_id AND job_configs.id = :job_id;

--: DbCheckpoint (finish_time?, operators?)

--! get_job_checkpoints: DbCheckpoint
SELECT epoch, state_backend, start_time, finish_time, operators FROM checkpoints
JOIN job_configs ON checkpoints.job_id = job_configs.id
WHERE job_configs.id = :job_id
    AND checkpoints.organization_id = :organization_id
    AND state != 'compacted'
    AND state != 'failed'
ORDER BY epoch;

--! get_job_checkpoint: DbCheckpoint
SELECT epoch, state_backend, start_time, finish_time, operators FROM checkpoints
JOIN job_configs ON checkpoints.job_id = job_configs.id
WHERE job_configs.id = :job_id
    AND checkpoints.organization_id = :organization_id
    AND state != 'compacted'
    AND state != 'failed'
    AND checkpoints.pub_id = :checkpoint_pub_id;

--! get_checkpoint_details: (finish_time?, operators?)
SELECT epoch, state_backend, start_time, finish_time, operators FROM checkpoints
WHERE job_id = :job_id
    AND organization_id = :organization_id
    AND epoch = :epoch
    AND state != 'failed';

--! delete_pipeline_for_job
DELETE FROM pipelines WHERE pipelines.id = (
    SELECT pipeline_id
    FROM job_configs
    WHERE job_configs.id = :job_id AND job_configs.organization_id = :organization_id);

--: DbLogMessage (operator_id?, task_index?)

--! get_operator_errors : DbLogMessage
SELECT jlm.pub_id, jlm.job_id, jlm.operator_id, jlm.task_index, jlm.created_at, jlm.log_level, jlm.message, jlm.details
FROM job_log_messages jlm
JOIN public.job_configs ON job_configs.id = jlm.job_id
WHERE job_configs.organization_id = :organization_id AND job_configs.id = :job_id
  AND jlm.log_level = 'error'
  AND (jlm.created_at < (
    SELECT created_at FROM job_log_messages
    WHERE pub_id = :starting_after
) OR :starting_after = '')
ORDER BY jlm.created_at DESC
LIMIT :limit::integer;

----------- udfs -----------------------

--: DbUdf (description?)

--! create_udf
INSERT INTO udfs (pub_id, organization_id, created_by, prefix, name, language, definition, description)
VALUES (:pub_id, :organization_id, :created_by, :prefix, :name, :language, :definition, :description);

--! get_udf: DbUdf
SELECT pub_id, prefix, name, definition, created_at, updated_at, language, description
FROM udfs
WHERE organization_id = :organization_id AND pub_id = :pub_id;

--! get_udf_by_name: DbUdf
SELECT pub_id, prefix, name, definition, created_at, updated_at, language, description
FROM udfs
WHERE organization_id = :organization_id AND name = :name;

--! get_udfs: DbUdf
SELECT pub_id, prefix, name, definition, created_at, updated_at, language, description
FROM udfs
WHERE organization_id = :organization_id;

--! delete_udf
DELETE FROM udfs
WHERE organization_id = :organization_id AND pub_id = :pub_id;
