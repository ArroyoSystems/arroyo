use crate::queries::api_queries::{DbCheckpoint, DbLogMessage, DbPipelineJob};
use arroyo_datastream::Program;
use arroyo_rpc::grpc::api::{
    CheckpointDetailsResp, CheckpointOverview, CreateJobReq, JobDetailsResp, JobStatus,
    PipelineProgram, StopType,
};
use arroyo_rpc::public_ids::{generate_id, IdTypes};
use axum::extract::{Path, State};
use axum::Json;
use cornucopia_async::GenericClient;
use deadpool_postgres::Transaction;
use http::StatusCode;
use prost::Message;
use std::{collections::HashMap, time::Duration};
use tonic::Status;

const PREVIEW_TTL: Duration = Duration::from_secs(60);

use crate::pipelines::query_job_by_pub_id;
use crate::rest::AppState;
use crate::rest_types::{
    Checkpoint, CheckpointCollection, JobCollection, JobLogMessage, JobLogMessageCollection,
};
use crate::rest_utils::{authenticate, client, log_and_map_rest, BearerAuth, ErrorResp};
use crate::{log_and_map, queries::api_queries, to_micros, types::public, AuthData};

pub(crate) async fn create_job<'a>(
    request: CreateJobReq,
    pipeline_name: &str,
    pipeline_id: &i64,
    auth: AuthData,
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
        return Err(ErrorResp {
            status_code: StatusCode::BAD_REQUEST,
            message: "checkpoint_interval_micros must be between 1 second and 1 day".to_string(),
        });
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

        return Err(ErrorResp {
            status_code: StatusCode::BAD_REQUEST,
            message,
        });
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
) -> Result<Vec<JobStatus>, Status> {
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

pub(crate) async fn get_job_details(
    job_id: &str,
    auth: &AuthData,
    client: &impl GenericClient,
) -> Result<JobDetailsResp, Status> {
    let res = api_queries::get_job_details()
        .bind(client, &auth.organization_id, &job_id)
        .opt()
        .await
        .map_err(log_and_map)?
        .ok_or_else(|| Status::not_found(format!("There is no job with id '{}'", job_id)))?;

    let mut program: Program = PipelineProgram::decode(&res.program[..])
        .map_err(log_and_map)?
        .try_into()
        .map_err(log_and_map)?;

    program.update_parallelism(
        &res.parallelism_overrides
            .as_object()
            .unwrap()
            .into_iter()
            .map(|(k, v)| (k.clone(), v.as_u64().unwrap() as usize))
            .collect(),
    );

    let state = res.state.unwrap_or_else(|| "Created".to_string());
    let running_desired = res.stop == public::StopMode::none;

    let (action_text, action, in_progress) = get_action(&state, &running_desired);

    let status = JobStatus {
        job_id: job_id.to_string(),
        pipeline_name: res.pipeline_name,
        running_desired,
        state,
        run_id: res.run_id.unwrap_or(0) as u64,
        start_time: res.start_time.map(to_micros),
        finish_time: res.finish_time.map(to_micros),
        tasks: res.tasks.map(|t| t as u64),
        definition: res.textual_repr,
        pipeline_id: format!("{}", res.pipeline_id),
        udfs: serde_json::from_value(res.udfs).map_err(log_and_map)?,
        failure_message: res.failure_message,
    };

    Ok(JobDetailsResp {
        job_status: Some(status),
        job_graph: Some(program.as_job_graph()),

        action: action.map(|action| action as i32),
        action_text: action_text.to_string(),
        in_progress,
    })
}

pub(crate) async fn checkpoint_details(
    job_pub_id: &str,
    epoch: u32,
    auth: AuthData,
    client: &impl GenericClient,
) -> Result<CheckpointDetailsResp, Status> {
    let job_id = api_queries::get_pipeline_job()
        .bind(client, &auth.organization_id, &job_pub_id)
        .one()
        .await
        .map_err(log_and_map)?
        .id;

    let res = api_queries::get_checkpoint_details()
        .bind(client, &job_id, &auth.organization_id, &(epoch as i32))
        .opt()
        .await
        .map_err(log_and_map)?
        .ok_or_else(|| {
            Status::not_found(format!(
                "There is no checkpoint with epoch {} for job '{}'",
                epoch, job_id
            ))
        })?;

    Ok(CheckpointDetailsResp {
        overview: Some(CheckpointOverview {
            epoch,
            backend: res.state_backend,
            start_time: to_micros(res.start_time),
            finish_time: res.finish_time.map(to_micros),
        }),
        operators: res
            .operators
            .map(|o| serde_json::from_value(o).unwrap())
            .unwrap_or_else(HashMap::new),
    })
}

/// List a job's error messages
#[utoipa::path(
    get,
    path = "/v1/pipelines/{pipeline_id}/jobs/{job_id}/errors",
    tag = "jobs",
    params(
        ("pipeline_id" = String, Path, description = "Pipeline id"),
        ("job_id" = String, Path, description = "Job id")
    ),
    responses(
        (status = 200, description = "Got job's error messages", body = JobLogMessageCollection),
    ),
)]
pub async fn get_job_errors(
    State(state): State<AppState>,
    bearer_auth: BearerAuth,
    Path((pipeline_pub_id, job_pub_id)): Path<(String, String)>,
) -> Result<Json<JobLogMessageCollection>, ErrorResp> {
    let client = client(&state.pool).await?;
    let auth_data = authenticate(&state.pool, bearer_auth).await?;

    query_job_by_pub_id(&pipeline_pub_id, &job_pub_id, &client, &auth_data).await?;

    let errors = api_queries::get_operator_errors()
        .bind(&client, &auth_data.organization_id, &job_pub_id)
        .all()
        .await
        .map_err(log_and_map_rest)?
        .into_iter()
        .map(|m| m.into())
        .collect();

    Ok(Json(JobLogMessageCollection {
        data: errors,
        has_more: false,
    }))
}

impl Into<JobLogMessage> for DbLogMessage {
    fn into(self) -> JobLogMessage {
        JobLogMessage {
            created_at: to_micros(self.created_at),
            operator_id: self.operator_id,
            task_index: self.task_index.map(|i| i as u64),
            level: self.log_level.into(),
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
        .map_err(log_and_map_rest)?
        .into_iter()
        .map(|m| m.into())
        .collect();

    Ok(Json(CheckpointCollection {
        data: checkpoints,
        has_more: false,
    }))
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
        .map_err(log_and_map_rest)?;

    Ok(Json(JobCollection {
        has_more: false,
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
