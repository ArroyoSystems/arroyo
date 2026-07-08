use anyhow::Context;
use axum::Json;
use axum::extract::{Path, State};

use crate::queries::api_queries;
use crate::rest::AppState;
use crate::rest_utils::{
    BearerAuth, ErrorResp, PipelineJobPath, authenticate, log_and_map, not_found,
    service_unavailable,
};
use arroyo_rpc::api_types::OperatorMetricGroupCollection;
use arroyo_rpc::api_types::metrics::OperatorMetricGroup;
use arroyo_rpc::config::config;
use arroyo_rpc::grpc::rpc::JobMetricsReq;
use arroyo_rpc::{StateContext, job_controller_client};
use tonic::Code;
use tonic::codec::CompressionEncoding;
use tracing::error;

/// Get a job's metrics
#[utoipa::path(
    get,
    path = "/v1/pipelines/{pipeline_id}/jobs/{job_id}/operator_metric_groups",
    tag = "jobs",
    params(
        ("pipeline_id" = String, Path, description = "Pipeline id"),
        ("job_id" = String, Path, description = "Job id"),
    ),
    responses(
        (status = 200, description = "Got metric groups", body = OperatorMetricGroupCollection),
    ),
)]
pub async fn get_operator_metric_groups(
    State(state): State<AppState>,
    bearer_auth: BearerAuth,
    Path(PipelineJobPath {
        id: pipeline_pub_id,
        job_id: job_pub_id,
    }): Path<PipelineJobPath>,
) -> Result<Json<OperatorMetricGroupCollection>, ErrorResp> {
    let auth_data = authenticate(&state.database, bearer_auth).await?;

    let job = api_queries::fetch_get_pipeline_job(
        &state.database.client().await?,
        &auth_data.organization_id,
        &pipeline_pub_id,
        &job_pub_id,
    )
    .await?
    .into_iter()
    .next()
    .ok_or_else(|| not_found("Job"))?;

    let state_context: Option<StateContext> = job
        .state_context
        .map(serde_json::from_value)
        .transpose()
        .with_context(|| format!("converting state context for job {}", job_pub_id))
        .map_err(log_and_map)?;

    let mut controller = if let Some(ctx) = state_context
        && let Some(leader) = ctx.leader
    {
        job_controller_client("api", &config().api.tls, leader.rpc_address, true).await
    } else {
        job_controller_client(
            "api",
            &config().api.tls,
            config().controller_endpoint(),
            false,
        )
        .await
    }
    .map_err(|e| {
        error!("Failed to connect to controller service: {}", e);
        service_unavailable("controller-service")
    })?
    .accept_compressed(CompressionEncoding::Zstd)
    .send_compressed(CompressionEncoding::Zstd);

    let data = match controller
        .job_metrics(JobMetricsReq { job_id: job.id })
        .await
    {
        Ok(resp) => {
            let metrics: Vec<OperatorMetricGroup> =
                serde_json::from_str(&resp.into_inner().metrics).map_err(log_and_map)?;

            metrics
        }
        Err(e) => {
            if e.code() == Code::NotFound {
                vec![]
            } else {
                return Err(log_and_map(e));
            }
        }
    };

    Ok(Json(OperatorMetricGroupCollection { data }))
}
