use cornucopia_async::GenericClient;
use std::str::FromStr;
use std::{collections::HashMap, env, time::SystemTime};

use arroyo_rpc::grpc::api::{job_metrics_resp::OperatorMetrics, JobMetricsResp};
use arroyo_rpc::grpc::api::{Metric, SubtaskMetrics};
use arroyo_types::{
    to_millis, API_METRICS_RATE_ENV, BYTES_RECV, BYTES_SENT, MESSAGES_RECV, MESSAGES_SENT,
    TX_QUEUE_REM, TX_QUEUE_SIZE,
};
use http::{header::AUTHORIZATION, HeaderMap, HeaderValue};
use once_cell::sync::Lazy;
use prometheus_http_query::Client;
use tonic::Status;

use crate::{jobs, AuthData};

const METRICS_GRANULARITY_SECS: f64 = 5.0;

static METRICS_CLIENT: Lazy<Client> = Lazy::new(|| {
    let mut headers = HeaderMap::new();
    if let Ok(basic_auth) = std::env::var("PROM_AUTH") {
        headers.append(
            AUTHORIZATION,
            HeaderValue::from_str(&("Basic ".to_owned() + &base64::encode(basic_auth))).unwrap(),
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

pub(crate) async fn get_metrics(
    job_id: String,
    auth: AuthData,
    client: &impl GenericClient,
) -> Result<JobMetricsResp, Status> {
    // validate that the job exists and user can access it
    let job_details = jobs::get_job_details(&job_id, &auth, client).await?;

    let rate = env::var(API_METRICS_RATE_ENV).unwrap_or_else(|_| "15s".to_string());

    #[derive(Copy, Clone)]
    enum QueryMetrics {
        BytesRecv,
        BytesSent,
        MessagesRecv,
        MessagesSent,
        Backpressure,
    }

    impl QueryMetrics {
        fn simple_query(&self, metric: &str, job_id: &str, run_id: u64, rate: &str) -> String {
            format!(
                "rate({}{{job_id=\"{}\",run_id=\"{}\"}}[{}])",
                metric, job_id, run_id, rate
            )
        }

        fn backpressure_query(&self, job_id: &str, run_id: u64) -> String {
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

        fn get_query(&self, job_id: &str, run_id: u64, rate: &str) -> String {
            match self {
                BytesRecv => self.simple_query(BYTES_RECV, job_id, run_id, rate),
                BytesSent => self.simple_query(BYTES_SENT, job_id, run_id, rate),
                MessagesRecv => self.simple_query(MESSAGES_RECV, job_id, run_id, rate),
                MessagesSent => self.simple_query(MESSAGES_SENT, job_id, run_id, rate),
                Backpressure => self.backpressure_query(job_id, run_id),
            }
        }
    }

    use QueryMetrics::*;

    let end = (to_millis(SystemTime::now()) / 1000) as i64;
    let start = end - 5 * 60;
    let run_id = job_details.job_status.unwrap().run_id;

    let result = tokio::try_join!(
        METRICS_CLIENT
            .query_range(
                BytesRecv.get_query(&job_id, run_id, &rate),
                start,
                end,
                METRICS_GRANULARITY_SECS
            )
            .get(),
        METRICS_CLIENT
            .query_range(
                BytesSent.get_query(&job_id, run_id, &rate),
                start,
                end,
                METRICS_GRANULARITY_SECS
            )
            .get(),
        METRICS_CLIENT
            .query_range(
                MessagesRecv.get_query(&job_id, run_id, &rate),
                start,
                end,
                METRICS_GRANULARITY_SECS
            )
            .get(),
        METRICS_CLIENT
            .query_range(
                MessagesSent.get_query(&job_id, run_id, &rate),
                start,
                end,
                METRICS_GRANULARITY_SECS
            )
            .get(),
        METRICS_CLIENT
            .query_range(
                Backpressure.get_query(&job_id, run_id, &rate),
                start,
                end,
                METRICS_GRANULARITY_SECS
            )
            .get(),
    );

    match result {
        Ok((r1, r2, r3, r4, r5)) => {
            let mut metrics = HashMap::new();

            for (q, r) in [
                (BytesRecv, r1),
                (BytesSent, r2),
                (MessagesRecv, r3),
                (MessagesSent, r4),
                (Backpressure, r5),
            ] {
                for v in r.data().as_matrix().unwrap() {
                    let operator_id = v.metric().get("operator_id").unwrap().clone();
                    let subtask_idx =
                        u32::from_str(v.metric().get("subtask_idx").unwrap()).unwrap();
                    let op = metrics.entry(operator_id).or_insert(OperatorMetrics {
                        subtasks: HashMap::new(),
                    });

                    let data = v
                        .samples()
                        .iter()
                        .map(|s| Metric {
                            time: (s.timestamp() * 1000.0 * 1000.0) as u64,
                            value: s.value(),
                        })
                        .collect();

                    let entry = op.subtasks.entry(subtask_idx).or_insert(SubtaskMetrics {
                        bytes_recv: vec![],
                        bytes_sent: vec![],
                        messages_recv: vec![],
                        messages_sent: vec![],
                        backpressure: vec![],
                    });

                    match q {
                        BytesRecv => entry.bytes_recv = data,
                        BytesSent => entry.bytes_sent = data,
                        MessagesRecv => entry.messages_recv = data,
                        MessagesSent => entry.messages_sent = data,
                        Backpressure => entry.backpressure = data,
                    };
                }
            }
            Ok(JobMetricsResp {
                job_id,
                start_time: start as u64 * 1000,
                end_time: end as u64 * 1000,
                metrics,
            })
        }
        Err(err) => Err(Status::internal(format!(
            "Failed to query prometheus: {}",
            err
        ))),
    }
}
