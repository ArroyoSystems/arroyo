use axum::extract::{Path, State};
use axum::Json;
use base64::engine::general_purpose;
use base64::Engine;
use std::str::FromStr;
use std::{collections::HashMap, env, time::SystemTime};

use crate::pipelines::query_job_by_pub_id;
use crate::rest::AppState;
use crate::rest_utils::{authenticate, client, BearerAuth, ErrorResp};
use arroyo_rpc::types::{
    Metric, MetricGroup, MetricNames, OperatorMetricGroup, OperatorMetricGroupCollection,
    SubtaskMetrics,
};
use arroyo_types::{
    to_millis, API_METRICS_RATE_ENV, BYTES_RECV, BYTES_SENT, MESSAGES_RECV, MESSAGES_SENT,
    TX_QUEUE_REM, TX_QUEUE_SIZE,
};
use http::StatusCode;
use http::{header::AUTHORIZATION, HeaderMap, HeaderValue};
use once_cell::sync::Lazy;
use prometheus_http_query::Client;

const METRICS_GRANULARITY_SECS: f64 = 5.0;

static METRICS_CLIENT: Lazy<Client> = Lazy::new(|| {
    let mut headers = HeaderMap::new();
    if let Ok(basic_auth) = std::env::var("PROM_AUTH") {
        headers.append(
            AUTHORIZATION,
            HeaderValue::from_str(
                &("Basic ".to_owned() + &general_purpose::STANDARD.encode(basic_auth)),
            )
            .unwrap(),
        );
    }
    let client = reqwest::Client::builder()
        .default_headers(headers)
        .build()
        .unwrap();

    let prometheus_endpoint =
        std::env::var("PROM_ENDPOINT").unwrap_or_else(|_| "http://localhost:9090".to_string());
    prometheus_http_query::Client::from(client, &prometheus_endpoint).unwrap()
});

fn simple_query(metric: &str, job_id: &str, run_id: &u64, rate: &str) -> String {
    format!(
        "rate({}{{job_id=\"{}\",run_id=\"{}\"}}[{}])",
        metric, job_id, run_id, rate
    )
}

fn backpressure_query(job_id: &str, run_id: &u64) -> String {
    let tx_queue_size: String = format!(
        "{}{{job_id=\"{}\",run_id=\"{}\"}}",
        TX_QUEUE_SIZE, job_id, run_id
    );
    let tx_queue_rem: String = format!(
        "{}{{job_id=\"{}\",run_id=\"{}\"}}",
        TX_QUEUE_REM, job_id, run_id
    );
    // add 1 to each value to account for uninitialized values (which report 0); this can happen when a task
    // never reads any data
    format!("1 - (({} + 1) / ({} + 1))", tx_queue_rem, tx_queue_size)
}

fn get_query(metric_name: MetricNames, job_id: &str, run_id: &u64, rate: &str) -> String {
    match metric_name {
        MetricNames::BytesRecv => simple_query(BYTES_RECV, job_id, run_id, rate),
        MetricNames::BytesSent => simple_query(BYTES_SENT, job_id, run_id, rate),
        MetricNames::MessagesRecv => simple_query(MESSAGES_RECV, job_id, run_id, rate),
        MetricNames::MessagesSent => simple_query(MESSAGES_SENT, job_id, run_id, rate),
        MetricNames::Backpressure => backpressure_query(job_id, run_id),
    }
}

/// Get a job's metrics
#[utoipa::path(
    get,
    path = "/v1/pipelines/{pipeline_id}/jobs/{job_id}/operator_metric_groups",
    tag = "metrics",
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
    let client = client(&state.pool).await?;
    let auth_data = authenticate(&state.pool, bearer_auth).await?;

    let job = query_job_by_pub_id(&pipeline_pub_id, &job_pub_id, &client, &auth_data).await?;
    let rate = env::var(API_METRICS_RATE_ENV).unwrap_or_else(|_| "15s".to_string());
    let end = (to_millis(SystemTime::now()) / 1000) as i64;
    let start = end - 5 * 60;

    let result = tokio::try_join!(
        METRICS_CLIENT
            .query_range(
                get_query(MetricNames::BytesRecv, &job.id, &job.run_id, &rate),
                start,
                end,
                METRICS_GRANULARITY_SECS
            )
            .get(),
        METRICS_CLIENT
            .query_range(
                get_query(MetricNames::BytesSent, &job.id, &job.run_id, &rate),
                start,
                end,
                METRICS_GRANULARITY_SECS
            )
            .get(),
        METRICS_CLIENT
            .query_range(
                get_query(MetricNames::MessagesRecv, &job.id, &job.run_id, &rate),
                start,
                end,
                METRICS_GRANULARITY_SECS
            )
            .get(),
        METRICS_CLIENT
            .query_range(
                get_query(MetricNames::MessagesSent, &job.id, &job.run_id, &rate),
                start,
                end,
                METRICS_GRANULARITY_SECS
            )
            .get(),
        METRICS_CLIENT
            .query_range(
                get_query(MetricNames::Backpressure, &job.id, &job.run_id, &rate),
                start,
                end,
                METRICS_GRANULARITY_SECS
            )
            .get(),
    );

    let mut collection = OperatorMetricGroupCollection {
        data: vec![],
        has_more: false,
    };

    match result {
        Ok((r1, r2, r3, r4, r5)) => {
            let mut metrics = HashMap::new();

            for (metric_name, query_result) in [
                (MetricNames::BytesRecv, r1),
                (MetricNames::BytesSent, r2),
                (MetricNames::MessagesRecv, r3),
                (MetricNames::MessagesSent, r4),
                (MetricNames::Backpressure, r5),
            ] {
                // for each metric query

                for v in query_result.data().as_matrix().unwrap() {
                    // for each operator/subtask pair

                    let operator_id = v.metric().get("operator_id").unwrap().clone();
                    let subtask_idx =
                        u32::from_str(v.metric().get("subtask_idx").unwrap()).unwrap();

                    let data = v
                        .samples()
                        .iter()
                        .map(|s| Metric {
                            time: (s.timestamp() * 1000.0 * 1000.0) as u64,
                            value: s.value(),
                        })
                        .collect();

                    metrics
                        .entry(operator_id)
                        .or_insert(HashMap::new())
                        .entry(metric_name.clone())
                        .or_insert(vec![])
                        .push(SubtaskMetrics {
                            idx: subtask_idx,
                            metrics: data,
                        });
                }
            }

            for (operator_id, metric_groups) in metrics.iter_mut() {
                let mut o = OperatorMetricGroup {
                    operator_id: operator_id.clone(),
                    metric_groups: vec![],
                };

                for (metric_name, subtask_metrics) in metric_groups.iter_mut() {
                    let m = MetricGroup {
                        name: metric_name.clone(),
                        subtasks: subtask_metrics.clone(),
                    };

                    if !m.subtasks.is_empty() {
                        o.metric_groups.push(m);
                    }
                }

                if !o.metric_groups.is_empty() {
                    collection.data.push(o);
                }
            }

            Ok(Json(collection))
        }
        Err(_) => Err(ErrorResp {
            status_code: StatusCode::INTERNAL_SERVER_ERROR,
            message: "Failed to query Prometheus".to_string(),
        }),
    }
}
