use crate::queries::api_queries::{
    DbCheckpoint, DbLogMessage, DbPipelineJob, GetOperatorErrorsParams,
};
use arroyo_rpc::api_types::checkpoints::{
    Checkpoint, CheckpointEventSpan, CheckpointSpanType, OperatorCheckpointGroup,
    SubtaskCheckpointGroup,
};
use arroyo_rpc::api_types::pipelines::{JobLogLevel, JobLogMessage, OutputData, StopType};
use arroyo_rpc::api_types::{
    CheckpointCollection, JobCollection, JobLogMessageCollection,
    OperatorCheckpointGroupCollection, PaginationQueryParams,
};
use arroyo_rpc::grpc;
use arroyo_rpc::grpc::api::{
    CreateJobReq, JobStatus, OperatorCheckpointDetail, TaskCheckpointDetail,
    TaskCheckpointEventType,
};
use arroyo_rpc::grpc::controller_grpc_client::ControllerGrpcClient;
use arroyo_rpc::public_ids::{generate_id, IdTypes};
use axum::extract::{Path, Query, State};
use axum::response::sse::{Event, Sse};
use axum::Json;
use cornucopia_async::GenericClient;
use cornucopia_async::Params;
use deadpool_postgres::Transaction;
use futures_util::stream::Stream;
use std::convert::Infallible;
use std::{collections::HashMap, time::Duration};
use tokio_stream::wrappers::ReceiverStream;
use tokio_stream::StreamExt as _;
use tonic::Request;
use tracing::info;

const PREVIEW_TTL: Duration = Duration::from_secs(60);

use crate::pipelines::{query_job_by_pub_id, query_pipeline_by_pub_id};
use crate::rest::AppState;
use crate::rest_utils::{
    authenticate, bad_request, client, log_and_map, not_found, paginate_results,
    validate_pagination_params, BearerAuth, ErrorResp,
};
use crate::types::public::LogLevel;
use crate::{queries::api_queries, to_micros, types::public, AuthData};

pub(crate) async fn create_job<'a>(
    request: CreateJobReq,
    pipeline_name: &str,
    pipeline_id: &i64,
    auth: &AuthData,
    client: &Transaction<'a>,
) -> Result<String, ErrorResp> {
    let checkpoint_interval = if request.preview {
        Duration::from_secs(24 * 60 * 60)
    } else {
        Duration::from_micros(request.checkpoint_interval_micros)
    };

    if checkpoint_interval < Duration::from_secs(1)
        || checkpoint_interval > Duration::from_secs(24 * 60 * 60)
    {
        return Err(bad_request(
            "Checkpoint_interval_micros must be between 1 second and 1 day.".to_string(),
        ));
    }

    let running_jobs = get_job_statuses(&auth, client)
        .await?
        .iter()
        .filter(|j| j.running_desired && j.state != "Failed" && j.state != "Finished")
        .count();

    if running_jobs > auth.org_metadata.max_running_jobs as usize {
        let message = format!("You have exceeded the maximum number
            of running jobs in your plan ({}). Stop an existing job or contact support@arroyo.systems for
            an increase", auth.org_metadata.max_running_jobs);

        return Err(bad_request(message));
    }

    let job_id = generate_id(IdTypes::JobConfig);

    // TODO: handle chance of collision in ids
    api_queries::create_job()
        .bind(
            client,
            &job_id,
            &auth.organization_id,
            &pipeline_name,
            &auth.user_id,
            &pipeline_id,
            &(checkpoint_interval.as_micros() as i64),
            &(if request.preview {
                Some(PREVIEW_TTL.as_micros() as i64)
            } else {
                None
            }),
        )
        .await
        .map_err(log_and_map)?;

    api_queries::create_job_status()
        .bind(
            client,
            &generate_id(IdTypes::JobStatus),
            &job_id,
            &auth.organization_id,
        )
        .await
        .map_err(log_and_map)?;

    Ok(job_id)
}

pub(crate) async fn get_job_statuses(
    auth: &AuthData,
    client: &impl GenericClient,
) -> Result<Vec<JobStatus>, ErrorResp> {
    let res = api_queries::get_jobs()
        .bind(client, &auth.organization_id)
        .all()
        .await
        .map_err(log_and_map)?;

    res.into_iter()
        .map(|rec| {
            Ok(JobStatus {
                job_id: rec.id,
                pipeline_name: rec.pipeline_name,
                running_desired: rec.stop == public::StopMode::none,
                state: rec.state.unwrap_or_else(|| "Created".to_string()),
                run_id: rec.run_id.unwrap_or(0) as u64,
                start_time: rec.start_time.map(to_micros),
                finish_time: rec.finish_time.map(to_micros),
                tasks: rec.tasks.map(|t| t as u64),
                definition: rec.textual_repr,
                udfs: serde_json::from_value(rec.udfs).map_err(log_and_map)?,
                pipeline_id: format!("{}", rec.pipeline_id),
                failure_message: rec.failure_message,
            })
        })
        .collect()
}

pub(crate) fn get_action(state: &str, running_desired: &bool) -> (String, Option<StopType>, bool) {
    enum Progress {
        InProgress,
        Stable,
    }

    use Progress::*;
    use StopType::*;

    let (a, s, p) = match (state.as_ref(), running_desired) {
        ("Created", true) => ("Stop", Some(Checkpoint), InProgress),
        ("Created", false) => ("Start", Some(None), Stable),

        ("Compiling", true) => ("Stop", Some(Checkpoint), InProgress),
        ("Compiling", false) => ("Stopping", Option::None, InProgress),

        ("Scheduling", true) => ("Stop", Some(Checkpoint), InProgress),
        ("Scheduling", false) => ("Stopping", Option::None, InProgress),

        ("Running", true) => ("Stop", Some(Checkpoint), Stable),
        ("Running", false) => ("Stopping", Option::None, InProgress),

        ("Rescaling", true) => ("Stop", Some(Checkpoint), InProgress),
        ("Rescaling", false) => ("Stopping", Option::None, InProgress),

        ("CheckpointStopping", true) => ("Force Stop", Some(Immediate), InProgress),
        ("CheckpointStopping", false) => ("Force Stop", Some(Immediate), InProgress),

        ("Recovering", true) => ("Stop", Some(Checkpoint), InProgress),
        ("Recovering", false) => ("Stopping", Option::None, InProgress),

        ("Restarting", true) => ("Stop", Some(Checkpoint), InProgress),
        ("Restarting", false) => ("Stopping", Option::None, InProgress),

        ("Stopping", true) => ("Stopping", Some(Checkpoint), InProgress),
        ("Stopping", false) => ("Stopping", Option::None, InProgress),

        ("Stopped", true) => ("Starting", Option::None, InProgress),
        ("Stopped", false) => ("Start", Some(None), Stable),

        ("Finishing", true) => ("Finishing", Option::None, InProgress),
        ("Finishing", false) => ("Finishing", Option::None, InProgress),

        ("Finished", true) => ("Finished", Option::None, Stable),
        ("Finished", false) => ("Finished", Option::None, Stable),

        ("Failed", true) => ("Failed", Option::None, Stable),
        ("Failed", false) => ("Start", Some(None), Stable),

        _ => panic!("unhandled state {}", state),
    };

    let in_progress = match p {
        InProgress => true,
        Stable => false,
    };

    (a.to_string(), s, in_progress)
}

/// List a job's error messages
#[utoipa::path(
    get,
    path = "/v1/pipelines/{pipeline_id}/jobs/{job_id}/errors",
    tag = "jobs",
    params(
        ("pipeline_id" = String, Path, description = "Pipeline id"),
        ("job_id" = String, Path, description = "Job id"),
        ("starting_after" = Option<String>, Query, description = "Starting after"),
        ("limit" = Option<u32>, Query, description = "Limit"),
    ),
    responses(
        (status = 200, description = "Got job's error messages", body = JobLogMessageCollection),
    ),
)]
pub async fn get_job_errors(
    State(state): State<AppState>,
    bearer_auth: BearerAuth,
    Path((pipeline_pub_id, job_pub_id)): Path<(String, String)>,
    query_params: Query<PaginationQueryParams>,
) -> Result<Json<JobLogMessageCollection>, ErrorResp> {
    let client = client(&state.pool).await?;
    let auth_data = authenticate(&state.pool, bearer_auth).await?;

    let (starting_after, limit) =
        validate_pagination_params(query_params.starting_after.clone(), query_params.limit)?;

    query_job_by_pub_id(&pipeline_pub_id, &job_pub_id, &client, &auth_data).await?;

    let errors = api_queries::get_operator_errors()
        .params(
            &client,
            &GetOperatorErrorsParams {
                organization_id: &auth_data.organization_id,
                job_id: &job_pub_id,
                starting_after: starting_after.unwrap_or_default(),
                limit: limit as i32,
            },
        )
        .all()
        .await
        .map_err(log_and_map)?
        .into_iter()
        .map(|m| m.into())
        .collect();

    let (errors, has_more) = paginate_results(errors, limit);

    Ok(Json(JobLogMessageCollection {
        data: errors,
        has_more,
    }))
}

impl Into<JobLogMessage> for DbLogMessage {
    fn into(self) -> JobLogMessage {
        let level: JobLogLevel = match self.log_level {
            LogLevel::info => JobLogLevel::Info,
            LogLevel::warn => JobLogLevel::Warn,
            LogLevel::error => JobLogLevel::Error,
        };

        JobLogMessage {
            id: self.pub_id,
            created_at: to_micros(self.created_at),
            operator_id: self.operator_id,
            task_index: self.task_index.map(|i| i as u64),
            level,
            message: self.message,
            details: self.details,
        }
    }
}

/// List a job's checkpoints
#[utoipa::path(
    get,
    path = "/v1/pipelines/{pipeline_id}/jobs/{job_id}/checkpoints",
    tag = "jobs",
    params(
        ("pipeline_id" = String, Path, description = "Pipeline id"),
        ("job_id" = String, Path, description = "Job id")
    ),
    responses(
        (status = 200, description = "Got job's checkpoints", body = CheckpointCollection),
    ),
)]
pub async fn get_job_checkpoints(
    State(state): State<AppState>,
    bearer_auth: BearerAuth,
    Path((pipeline_pub_id, job_pub_id)): Path<(String, String)>,
) -> Result<Json<CheckpointCollection>, ErrorResp> {
    let client = client(&state.pool).await?;
    let auth_data = authenticate(&state.pool, bearer_auth).await?;

    query_job_by_pub_id(&pipeline_pub_id, &job_pub_id, &client, &auth_data).await?;

    let checkpoints = api_queries::get_job_checkpoints()
        .bind(&client, &job_pub_id, &auth_data.organization_id)
        .all()
        .await
        .map_err(log_and_map)?
        .into_iter()
        .map(|m| m.into())
        .collect();

    Ok(Json(CheckpointCollection { data: checkpoints }))
}

fn get_event_spans(subtask_details: &TaskCheckpointDetail) -> Vec<CheckpointEventSpan> {
    let alignment_started = subtask_details
        .events
        .iter()
        .find(|e| e.event_type == TaskCheckpointEventType::AlignmentStarted as i32);

    let checkpoint_started = subtask_details
        .events
        .iter()
        .find(|e| e.event_type == TaskCheckpointEventType::CheckpointStarted as i32);

    let operator_finished = subtask_details
        .events
        .iter()
        .find(|e| e.event_type == TaskCheckpointEventType::CheckpointOperatorFinished as i32);

    let sync_finished = subtask_details
        .events
        .iter()
        .find(|e| e.event_type == TaskCheckpointEventType::CheckpointSyncFinished as i32);

    let pre_commit = subtask_details
        .events
        .iter()
        .find(|e| e.event_type == TaskCheckpointEventType::CheckpointPreCommit as i32);

    let mut event_spans = vec![];

    if let (Some(alignment_started), Some(checkpoint_started)) =
        (alignment_started, checkpoint_started)
    {
        event_spans.push(CheckpointEventSpan {
            span_type: CheckpointSpanType::Alignment,
            description: "Time spent waiting for alignment barriers".to_string(),
            start_time: alignment_started.time,
            finish_time: checkpoint_started.time,
        });
    }

    if let (Some(checkpoint_started), Some(sync_finished)) = (checkpoint_started, sync_finished) {
        event_spans.push(CheckpointEventSpan {
            span_type: CheckpointSpanType::Sync,
            description: "The synchronous part of checkpointing.".to_string(),
            start_time: checkpoint_started.time,
            finish_time: sync_finished.time,
        });
    }

    if let (Some(checkpoint_started), Some(operator_finished)) =
        (checkpoint_started, operator_finished)
    {
        event_spans.push(CheckpointEventSpan {
            span_type: CheckpointSpanType::Async,
            description: "The asynchronous part of checkpointing.".to_string(),
            start_time: checkpoint_started.time,
            finish_time: operator_finished.time,
        });
    }

    if let (Some(operator_finished), Some(pre_commit)) = (operator_finished, pre_commit) {
        event_spans.push(CheckpointEventSpan {
            span_type: CheckpointSpanType::Committing,
            description: "Committing the checkpoint.".to_string(),
            start_time: operator_finished.time,
            finish_time: pre_commit.time,
        });
    }

    event_spans
}

/// Get a checkpoint's details
#[utoipa::path(
    get,
    path = "/v1/pipelines/{pipeline_id}/jobs/{job_id}/checkpoints/{epoch}/operator_checkpoint_groups",
    tag = "jobs",
    params(
        ("pipeline_id" = String, Path, description = "Pipeline id"),
        ("job_id" = String, Path, description = "Job id"),
        ("epoch" = u32, Path, description = "Epoch")
    ),
    responses(
        (status = 200, description = "Got checkpoint's details", body = OperatorCheckpointGroupCollection),
    ),
)]
pub async fn get_checkpoint_details(
    State(state): State<AppState>,
    bearer_auth: BearerAuth,
    Path((pipeline_pub_id, job_pub_id, epoch)): Path<(String, String, u32)>,
) -> Result<Json<OperatorCheckpointGroupCollection>, ErrorResp> {
    let client = client(&state.pool).await?;
    let auth_data = authenticate(&state.pool, bearer_auth).await?;

    query_job_by_pub_id(&pipeline_pub_id, &job_pub_id, &client, &auth_data).await?;

    let checkpoint_details = api_queries::get_checkpoint_details()
        .bind(
            &client,
            &job_pub_id,
            &auth_data.organization_id,
            &(epoch as i32),
        )
        .opt()
        .await
        .map_err(log_and_map)?
        .ok_or_else(|| {
            not_found(&format!(
                "Checkpoint with epoch {} for job '{}'",
                epoch, job_pub_id
            ))
        })?;

    let mut operators: Vec<OperatorCheckpointGroup> = vec![];

    checkpoint_details
        .operators
        .map(|o| serde_json::from_value(o).unwrap())
        .unwrap_or_else(HashMap::<String, OperatorCheckpointDetail>::new)
        .iter()
        .for_each(|(operator_id, operator_details)| {
            let mut operator_bytes = 0;
            let mut subtasks = vec![];

            operator_details
                .tasks
                .iter()
                .for_each(|(subtask_index, subtask_details)| {
                    operator_bytes += subtask_details.bytes.unwrap_or(0);
                    subtasks.push(SubtaskCheckpointGroup {
                        index: subtask_index.clone(),
                        bytes: subtask_details.bytes.unwrap_or(0),
                        event_spans: get_event_spans(&subtask_details),
                    });
                });

            operators.push(OperatorCheckpointGroup {
                operator_id: operator_id.to_string(),
                bytes: operator_bytes,
                subtasks,
            });
        });

    Ok(Json(OperatorCheckpointGroupCollection { data: operators }))
}

/// Subscribe to a job's output
#[utoipa::path(
    get,
    path = "/v1/pipelines/{pipeline_id}/jobs/{job_id}/output",
    tag = "jobs",
    params(
        ("pipeline_id" = String, Path, description = "Pipeline id"),
        ("job_id" = String, Path, description = "Job id")
    ),
    responses(
        (status = 200, description = "Job output as 'text/event-stream'"),
    ),
)]
pub async fn get_job_output(
    State(state): State<AppState>,
    bearer_auth: BearerAuth,
    Path((pipeline_pub_id, job_pub_id)): Path<(String, String)>,
) -> Result<Sse<impl Stream<Item = Result<Event, Infallible>>>, ErrorResp> {
    let client = client(&state.pool).await?;
    let auth_data = authenticate(&state.pool, bearer_auth).await?;

    // validate that the job exists, the user has access, and the graph has a GrpcSink
    query_job_by_pub_id(&pipeline_pub_id, &job_pub_id, &client, &auth_data).await?;
    let pipeline = query_pipeline_by_pub_id(&pipeline_pub_id, &client, &auth_data).await?;
    if !pipeline
        .graph
        .nodes
        .iter()
        .any(|n| n.operator.contains("WebSink"))
    {
        // TODO: make this check more robust
        return Err(bad_request("Job does not have a web sink".to_string()));
    }
    let (tx, rx) = tokio::sync::mpsc::channel(32);

    let mut controller = ControllerGrpcClient::connect(state.controller_addr.clone())
        .await
        .unwrap();

    let mut stream = controller
        .subscribe_to_output(Request::new(grpc::GrpcOutputSubscription {
            job_id: job_pub_id.clone(),
        }))
        .await
        .unwrap()
        .into_inner();

    info!("Subscribed to output");
    tokio::spawn(async move {
        let _controller = controller;
        let mut message_count = 0;
        while let Some(d) = stream.next().await {
            if d.as_ref().map(|t| t.done).unwrap_or(false) {
                info!("Stream done for {}", job_pub_id);
                break;
            }

            let v = match d {
                Ok(d) => d,
                Err(_) => break,
            };

            let output_data: OutputData = v.into();
            let e = Ok(Event::default()
                .json_data(output_data)
                .unwrap()
                .id(message_count.to_string()));

            if tx.send(e).await.is_err() {
                break;
            }

            message_count += 1;
        }

        info!("Closing watch stream for {}", job_pub_id);
    });

    Ok(Sse::new(ReceiverStream::new(rx)))
}

/// Get all jobs
#[utoipa::path(
    get,
    path = "/v1/jobs",
    tag = "jobs",
    responses(
        (status = 200, description = "Get all jobs", body = JobCollection),
    ),
)]
pub async fn get_jobs(
    State(state): State<AppState>,
    bearer_auth: BearerAuth,
) -> Result<Json<JobCollection>, ErrorResp> {
    let client = client(&state.pool).await?;
    let auth_data = authenticate(&state.pool, bearer_auth).await?;

    let jobs: Vec<DbPipelineJob> = api_queries::get_all_jobs()
        .bind(&client, &auth_data.organization_id)
        .all()
        .await
        .map_err(log_and_map)?;

    Ok(Json(JobCollection {
        data: jobs.into_iter().map(|p| p.into()).collect(),
    }))
}

impl Into<Checkpoint> for DbCheckpoint {
    fn into(self) -> Checkpoint {
        Checkpoint {
            epoch: self.epoch as u32,
            backend: self.state_backend,
            start_time: to_micros(self.start_time),
            finish_time: self.finish_time.map(to_micros),
        }
    }
}
