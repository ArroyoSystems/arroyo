use axum::extract::{Path, State};
use axum::Json;

use crate::pipelines::query_job_by_pub_id;
use crate::rest::AppState;
use crate::rest_utils::{authenticate, log_and_map, BearerAuth, ErrorResp};
use arroyo_rpc::api_types::metrics::OperatorMetricGroup;
use arroyo_rpc::api_types::OperatorMetricGroupCollection;
use arroyo_rpc::grpc::controller_grpc_client::ControllerGrpcClient;
use arroyo_rpc::grpc::JobMetricsReq;
use tonic::Code;

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

    let mut controller = ControllerGrpcClient::connect(state.controller_addr)
        .await
        .map_err(log_and_map)?;

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
