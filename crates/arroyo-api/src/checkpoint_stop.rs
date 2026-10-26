use crate::AuthData;
use crate::queries::api_queries::{self, DbPipelineJob};
use crate::rest::AppState;
use crate::rest_utils::{
    BearerAuth, ErrorResp, PipelinePath, authenticate, log_and_map, not_found,
};
use crate::types::public::StopMode;
use arroyo_rpc::api_types::pipelines::{CheckpointStopResult, CheckpointStopStatus};
use arroyo_state::{StorageProviderFor, get_storage_provider};
use arroyo_state_protocol::ProtocolPaths;
use arroyo_state_protocol::store::StoreError;
use arroyo_state_protocol::types::{Generation, ProtocolError};
use arroyo_state_protocol::workflow::verify_stop_checkpoint;
use arroyo_types::{JobId, PipelineId};
use axum::Json;
use axum::extract::{Path, State};
use axum::response::IntoResponse;
use cornucopia_async::DatabaseSource;
use http::header;
use tracing::debug;

#[utoipa::path(
    get,
    path = "/v1/pipelines/{id}/checkpoint-stop-status",
    tag = "pipelines",
    params(
        ("id" = String, Path, description = "Pipeline id")
    ),
    responses(
        (status = 200, description = "Point-in-time verification of checkpoint-stop for the current execution. status is verified only when the job is stopped and its stop checkpoint is canonical and fully committed where required; otherwise it is unverified. This endpoint does not stop the job or wait for completion.", body = CheckpointStopStatus),
        (status = 404, description = "Pipeline or job not found", body = ErrorResp),
        (status = 500, description = "Database or checkpoint storage could not be read or validated; completion is unverified", body = ErrorResp),
    ),
)]
pub async fn get_checkpoint_stop_status(
    State(state): State<AppState>,
    bearer_auth: BearerAuth,
    Path(PipelinePath { id: pipeline_id }): Path<PipelinePath>,
) -> Result<impl IntoResponse, ErrorResp> {
    let auth = authenticate(&state.database, bearer_auth).await?;
    let database = &state.database;
    let unverified = (
        [(header::CACHE_CONTROL, "no-store")],
        Json(CheckpointStopStatus::Unverified),
    );
    let pipeline = api_queries::fetch_get_pipeline(
        &database.client().await?,
        &pipeline_id,
        &auth.organization_id,
    )
    .await?
    .into_iter()
    .next()
    .ok_or_else(|| not_found("Pipeline"))?;
    let job = read_job(database, &auth, &pipeline_id).await?;
    if job.stop != StopMode::checkpoint || job.state.as_deref() != Some("Stopped") {
        debug!(pipeline_id, state = ?job.state, stop = ?job.stop, "Job is not stopped in checkpoint mode");
        return Ok(unverified);
    }
    let Some(run_id) = job.run_id.and_then(|run_id| u64::try_from(run_id).ok()) else {
        debug!(pipeline_id, run_id = ?job.run_id, "Stopped job has no valid execution generation");
        return Ok(unverified);
    };
    let storage = get_storage_provider(&StorageProviderFor::Controller {
        storage_url: pipeline.state_url,
    })
    .await
    .map_err(|error| map_verification_error(error.into()))?;
    let paths = ProtocolPaths::new(PipelineId::new(&pipeline_id), JobId::new(&job.id));
    let Some(checkpoint) = verify_stop_checkpoint(storage.as_ref(), &paths, Generation(run_id))
        .await
        .map_err(map_verification_error)?
    else {
        return Ok(unverified);
    };

    let confirmed = read_job(database, &auth, &pipeline_id).await?;
    if confirmed.id != job.id
        || confirmed.run_id != job.run_id
        || confirmed.state.as_deref() != Some("Stopped")
        || confirmed.stop != StopMode::checkpoint
    {
        debug!(
            pipeline_id,
            "Execution changed during checkpoint verification"
        );
        return Ok(unverified);
    }
    Ok((
        [(header::CACHE_CONTROL, "no-store")],
        Json(CheckpointStopStatus::Verified {
            checkpoint: CheckpointStopResult {
                job_id: job.id,
                run_id,
                checkpoint_epoch: *checkpoint.epoch,
                checkpoint_ref: checkpoint.checkpoint_ref.to_string(),
            },
        }),
    ))
}

async fn read_job(
    database: &DatabaseSource,
    auth: &AuthData,
    pipeline_id: &str,
) -> Result<DbPipelineJob, ErrorResp> {
    api_queries::fetch_get_pipeline_jobs(
        &database.client().await?,
        &auth.organization_id,
        &pipeline_id,
    )
    .await?
    .into_iter()
    .next()
    .ok_or_else(|| not_found("Job for pipeline"))
}

fn map_verification_error(error: StoreError) -> ErrorResp {
    let message = match &error {
        StoreError::Protocol(ProtocolError::MetadataMismatch { field, .. }) => {
            format!("Checkpoint metadata field '{field}' does not match the expected value")
        }
        StoreError::Protocol(_) => {
            "Checkpoint metadata violates the checkpoint protocol".to_string()
        }
        StoreError::DecodeJson { .. }
        | StoreError::DecodeProtobuf { .. }
        | StoreError::InvalidProtobuf { .. } => {
            "Checkpoint metadata could not be decoded".to_string()
        }
        _ => "Checkpoint storage could not be read".to_string(),
    };
    let mut response = log_and_map(error);
    response.message = message;
    response
}
