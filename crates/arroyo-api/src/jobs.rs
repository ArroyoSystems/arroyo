use crate::queries::api_queries::{DbCheckpoint, DbLogMessage, DbPipelineJob};
use arroyo_rpc::api_types::checkpoints::{
    Checkpoint, JobCheckpointSpan, OperatorCheckpointGroup, SubtaskCheckpointGroup,
};
use arroyo_rpc::api_types::pipelines::{JobLogLevel, JobLogMessage, OutputData, StopType};
use arroyo_rpc::api_types::{
    CheckpointCollection, JobCollection, JobLogMessageCollection,
    OperatorCheckpointGroupCollection, PaginationQueryParams,
};
use arroyo_rpc::grpc::api::OperatorCheckpointDetail;
use arroyo_rpc::public_ids::{generate_id, IdTypes};
use arroyo_rpc::{get_event_spans, grpc};
use axum::extract::{Path, Query, State};
use axum::response::sse::{Event, Sse};
use axum::Json;
use futures_util::stream::Stream;
use std::convert::Infallible;
use std::{collections::HashMap, time::Duration};
use tokio_stream::wrappers::ReceiverStream;
use tokio_stream::StreamExt as _;
use tonic::{Code, Request};
use tracing::info;

const PREVIEW_TTL: Duration = Duration::from_secs(60);

use crate::pipelines::{query_job_by_pub_id, query_pipeline_by_pub_id};
use crate::rest::AppState;
use crate::rest_utils::{
    authenticate, bad_request, log_and_map, not_found, paginate_results,
    validate_pagination_params, BearerAuth, ErrorResp,
};
use crate::types::public::LogLevel;
use crate::{queries::api_queries, to_micros, types::public, AuthData};
use arroyo_rpc::config::config;
use arroyo_rpc::controller_client;
use cornucopia_async::DatabaseSource;

pub(crate) async fn create_job(
    pipeline_name: &str,
    pipeline_id: i64,
    checkpoint_interval: Duration,
    preview: bool,
    auth: &AuthData,
    db: &DatabaseSource,
) -> Result<String, ErrorResp> {
    let checkpoint_interval = if preview {
        Duration::from_secs(24 * 60 * 60)
    } else {
        checkpoint_interval
    };

    if checkpoint_interval < Duration::from_secs(1)
        || checkpoint_interval > Duration::from_secs(24 * 60 * 60)
    {
        return Err(bad_request(
            "Checkpoint_interval_micros must be between 1 second and 1 day.".to_string(),
        ));
    }

    let running_jobs = api_queries::fetch_get_jobs(&db.client().await?, &auth.organization_id)
        .await?
        .iter()
        .filter(|j| {
            j.stop == public::StopMode::none
                && !j
                    .state
                    .as_ref()
                    .map(|s| s == "Failed" || s == "Finished")
                    .unwrap_or(false)
        })
        .count();

    if running_jobs > auth.org_metadata.max_running_jobs as usize {
        let message = format!("You have exceeded the maximum number
            of running jobs in your plan ({}). Stop an existing job or contact support@arroyo.systems for
            an increase", auth.org_metadata.max_running_jobs);

        return Err(bad_request(message));
    }

    let job_id = generate_id(IdTypes::JobConfig);

    // TODO: handle chance of collision in ids
    api_queries::execute_create_job(
        &db.client().await?,
        &job_id,
        &auth.organization_id,
        &pipeline_name,
        &auth.user_id,
        &pipeline_id,
        &(checkpoint_interval.as_micros() as i64),
        &(if preview {
            Some(PREVIEW_TTL.as_micros() as i64)
        } else {
            None
        }),
    )
    .await?;

    api_queries::execute_create_job_status(
        &db.client().await?,
        &generate_id(IdTypes::JobStatus),
        &job_id,
        &auth.organization_id,
    )
    .await?;

    Ok(job_id)
}

pub(crate) fn get_action(state: &str, running_desired: &bool) -> (String, Option<StopType>, bool) {
    enum Progress {
        InProgress,
        Stable,
    }

    use Progress::*;
    use StopType::*;

    let (a, s, p) = match (state, running_desired) {
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

        _ => panic!("unhandled state {state}"),
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
    let auth_data = authenticate(&state.database, bearer_auth).await?;
    let db = state.database.client().await?;

    let (starting_after, limit) =
        validate_pagination_params(query_params.starting_after.clone(), query_params.limit)?;

    query_job_by_pub_id(&pipeline_pub_id, &job_pub_id, &db, &auth_data).await?;

    let errors = api_queries::fetch_get_operator_errors(
        &db,
        &auth_data.organization_id,
        &job_pub_id,
        &starting_after.unwrap_or_default(),
        &(limit as i32),
    )
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

impl From<DbLogMessage> for JobLogMessage {
    fn from(val: DbLogMessage) -> Self {
        let level: JobLogLevel = match val.log_level {
            LogLevel::info => JobLogLevel::Info,
            LogLevel::warn => JobLogLevel::Warn,
            LogLevel::error => JobLogLevel::Error,
        };

        JobLogMessage {
            id: val.pub_id,
            created_at: to_micros(val.created_at),
            operator_id: val.operator_id,
            task_index: val.task_index.map(|i| i as u64),
            level,
            message: val.message,
            details: val.details,
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
    let db = state.database.client().await?;
    let auth_data = authenticate(&state.database, bearer_auth).await?;

    query_job_by_pub_id(&pipeline_pub_id, &job_pub_id, &db, &auth_data).await?;

    let checkpoints =
        api_queries::fetch_get_job_checkpoints(&db, &job_pub_id, &auth_data.organization_id)
            .await
            .map_err(log_and_map)?
            .into_iter()
            .filter_map(|m| m.try_into().ok())
            .collect();

    Ok(Json(CheckpointCollection { data: checkpoints }))
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
    let db = state.database.client().await?;
    let auth_data = authenticate(&state.database, bearer_auth).await?;

    query_job_by_pub_id(&pipeline_pub_id, &job_pub_id, &db, &auth_data).await?;

    let checkpoint_details = api_queries::fetch_get_checkpoint_details(
        &db,
        &job_pub_id,
        &auth_data.organization_id,
        &(epoch as i32),
    )
    .await
    .map_err(log_and_map)?
    .into_iter()
    .next()
    .ok_or_else(|| {
        not_found(&format!(
            "Checkpoint with epoch {epoch} for job '{job_pub_id}'"
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
                        index: *subtask_index,
                        bytes: subtask_details.bytes.unwrap_or(0),
                        event_spans: get_event_spans(subtask_details).into(),
                    });
                });

            operators.push(OperatorCheckpointGroup {
                operator_id: operator_id.to_string(),
                bytes: operator_bytes,
                started_metadata_write: operator_details.started_metadata_write,
                finish_time: operator_details.finish_time,
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
    let db = state.database.client().await?;
    let auth_data = authenticate(&state.database, bearer_auth).await?;

    // validate that the job exists, the user has access, and the graph has a GrpcSink
    query_job_by_pub_id(&pipeline_pub_id, &job_pub_id, &db, &auth_data).await?;

    let pipeline = query_pipeline_by_pub_id(&pipeline_pub_id, &db, &auth_data).await?;

    if !pipeline
        .graph
        .nodes
        .iter()
        .any(|n| n.operator.contains("preview"))
    {
        // TODO: make this check more robust
        return Err(bad_request("Job does not have a preview sink".to_string()));
    }
    let (tx, rx) = tokio::sync::mpsc::channel(32);

    let mut controller = controller_client("api", &config().api.tls)
        .await
        .map_err(log_and_map)?;

    let mut stream = controller
        .subscribe_to_output(Request::new(grpc::rpc::GrpcOutputSubscription {
            job_id: job_pub_id.clone(),
        }))
        .await
        .map_err(|e| match e.code() {
            Code::FailedPrecondition | Code::NotFound => bad_request(e.message().to_string()),
            _ => log_and_map(e),
        })?
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
    let auth_data = authenticate(&state.database, bearer_auth).await?;

    let jobs: Vec<DbPipelineJob> = api_queries::fetch_get_all_jobs(
        &state.database.client().await?,
        &auth_data.organization_id,
    )
    .await?;

    Ok(Json(JobCollection {
        data: jobs.into_iter().map(|p| p.into()).collect(),
    }))
}

impl TryFrom<DbCheckpoint> for Checkpoint {
    type Error = anyhow::Error;

    fn try_from(val: DbCheckpoint) -> anyhow::Result<Self> {
        let events: Vec<JobCheckpointSpan> = serde_json::from_value(val.event_spans)?;

        Ok(Checkpoint {
            epoch: val.epoch as u32,
            backend: val.state_backend,
            start_time: to_micros(val.start_time),
            finish_time: val.finish_time.map(to_micros),
            events: events.into_iter().map(|e| e.into()).collect(),
        })
    }
}
