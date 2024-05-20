--! all_jobs : Job(ttl_micros?, state?, start_time?, finish_time?, tasks?, failure_message?, run_id?, pipeline_path?, wasm_path?)
SELECT
    job_configs.id as id,
    job_configs.organization_id as org_id,
    pipeline_name,
    pipeline_id,
    checkpoint_interval_micros,
    ttl_micros,
    parallelism_overrides,
    stop,
    state,
    start_time,
    finish_time,
    tasks,
    failure_message,
    restarts,
    run_id,
    pipeline_path,
    wasm_path,
    job_configs.restart_nonce as config_restart_nonce,
    job_statuses.restart_nonce as status_restart_nonce,
    restart_mode
FROM job_configs
LEFT JOIN job_statuses ON job_configs.id = job_statuses.id;

--! update_job_status (start_time?, finish_time?, tasks?, failure_message?, pipeline_path?, wasm_path?)
UPDATE job_statuses
SET state = :state,
    start_time = :start_time,
    finish_time = :finish_time,
    tasks = :tasks,
    failure_message = :failure_message,
    restarts = :restarts,
    pipeline_path = :pipeline_path,
    wasm_path = :wasm_path,
    run_id = :run_id,
    restart_nonce = :restart_nonce
WHERE id = :job_id;

--! get_program
SELECT program, proto_version FROM pipelines WHERE id = :id;

--! mark_checkpoints_compacted
UPDATE checkpoints
    set state = 'compacted'
WHERE job_id = :job_id AND epoch < :epoch;

--! drop_old_checkpoint_rows
DELETE FROM checkpoints
WHERE job_id = :job_id AND epoch < :epoch;

--! create_checkpoint
INSERT INTO checkpoints
(pub_id, organization_id, job_id, state_backend, epoch, min_epoch, start_time)
VALUES (:pub_id, :organization_id, :job_id, :state_backend, :epoch, :min_epoch, :start_time);

--! update_checkpoint (finish_time?)
UPDATE checkpoints
SET
    operators = :operators,
    finish_time = :finish_time,
    state = :state
WHERE pub_id = :pub_id;

--! commit_checkpoint
UPDATE checkpoints
SET
    finish_time = :finish_time,
    state = 'ready'
WHERE pub_id = :pub_id;

--! mark_compacting
UPDATE checkpoints
SET
    state = 'compacting'
WHERE job_id = :job_id AND epoch >= :min_epoch AND epoch < :epoch;

--! mark_failed
UPDATE checkpoints
SET
    state = 'failed'
WHERE job_id = :job_id AND epoch >= :epoch;

--! last_successful_checkpoint
SELECT pub_id, epoch, min_epoch, state = 'committing' as needs_commits
FROM checkpoints
WHERE job_id = :job_id AND (state = 'ready' or state = 'committing')
ORDER BY epoch DESC
LIMIT 1;

--! create_job_log_message
INSERT INTO job_log_messages (pub_id, job_id, operator_id, task_index, log_level, message, details)
VALUES (:pub_id, :job_id, :operator_id, :task_index, :log_level, :message, :details);
