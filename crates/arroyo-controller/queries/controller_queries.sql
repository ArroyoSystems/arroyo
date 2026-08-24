--! all_jobs : Job(ttl_micros?, state?, start_time?, finish_time?, tasks?, failure_message?, failure_domain?, run_id?, pipeline_path?, wasm_path?)
SELECT
    c.id as id,
    c.organization_id as org_id,
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
    failure_domain,
    restarts,
    run_id,
    pipeline_path,
    wasm_path,
    c.restart_nonce as config_restart_nonce,
    s.restart_nonce as status_restart_nonce,
    restart_mode,
    state_context,
    env_vars,
    scheduler_config
FROM job_configs c
INNER JOIN job_statuses s ON c.id = s.id;

--! update_job_status (start_time?, finish_time?, tasks?, failure_message?, failure_domain?, pipeline_path?, wasm_path?)
UPDATE job_statuses
SET state = :state,
    start_time = :start_time,
    finish_time = :finish_time,
    tasks = :tasks,
    failure_message = :failure_message,
    failure_domain = :failure_domain,
    restarts = :restarts,
    pipeline_path = :pipeline_path,
    wasm_path = :wasm_path,
    run_id = :run_id,
    restart_nonce = :restart_nonce,
    state_context = :state_context
WHERE id = :job_id;

--! get_program : PipelineRow(state_url?)
SELECT program, pub_id as pipeline_id, proto_version, state_url, tags FROM pipelines WHERE id = :id;

--! create_job_log_message
INSERT INTO job_log_messages (pub_id, job_id, operator_id, task_index, log_level, message, details, error_domain, retry_hint)
VALUES (:pub_id, :job_id, :operator_id, :task_index, :log_level, :message, :details, :error_domain, :retry_hint);

--! clean_preview_pipelines
DELETE FROM pipelines WHERE id in (
  SELECT jc.pipeline_id
  FROM job_configs jc
  INNER JOIN job_statuses js ON jc.id = js.id
  WHERE (js.state = 'Finished' OR js.state = 'Stopped' OR js.state = 'Failed')
    AND jc.ttl_micros > 0
    AND jc.created_at < :created_at);
