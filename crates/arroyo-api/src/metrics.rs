use axum::Json;
use axum::extract::{Path, State};

use crate::pipelines::query_job_by_pub_id;
use crate::rest::AppState;
use crate::rest_utils::{BearerAuth, ErrorResp, authenticate, log_and_map, service_unavailable};
use arroyo_rpc::api_types::OperatorMetricGroupCollection;
use arroyo_rpc::api_types::metrics::OperatorMetricGroup;
use arroyo_rpc::config::config;
use arroyo_rpc::grpc::rpc::JobMetricsReq;
use arroyo_rpc::grpc::rpc::controller_grpc_client::ControllerGrpcClient;
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
    Path((pipeline_pub_id, job_pub_id)): Path<(String, String)>,
) -> Result<Json<OperatorMetricGroupCollection>, ErrorResp> {
    let auth_data = authenticate(&state.database, bearer_auth).await?;

    let job = query_job_by_pub_id(
        &pipeline_pub_id,
        &job_pub_id,
        &state.database.client().await?,
        &auth_data,
    )
    .await?;

    let channel = arroyo_rpc::connect_grpc(
        "api",
        config().controller_endpoint(),
        &config().api.tls,
        &config().controller.tls,
    )
    .await
    .map_err(|e| {
        error!("Failed to connect to controller service: {}", e);
        service_unavailable("controller-service")
    })?;

    let mut controller = ControllerGrpcClient::new(channel)
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
