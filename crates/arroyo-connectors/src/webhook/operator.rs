use arrow::array::RecordBatch;
use async_trait::async_trait;
use std::collections::HashMap;
use std::{
    sync::Arc,
    time::{Duration, SystemTime},
};

use arroyo_types::CheckpointBarrier;

use tokio::sync::{Mutex, Semaphore};
use tracing::warn;

use crate::webhook::MAX_INFLIGHT;
use arroyo_formats::ser::ArrowSerializer;
use arroyo_operator::context::{Collector, OperatorContext};
use arroyo_operator::operator::ArrowOperator;
use arroyo_rpc::grpc::rpc::TableConfig;
use arroyo_rpc::ControlResp;
use arroyo_state::global_table_config;

pub struct WebhookSinkFunc {
    pub url: Arc<String>,
    pub semaphore: Arc<Semaphore>,
    pub client: reqwest::Client,
    pub serializer: ArrowSerializer,
    pub last_reported_error_at: Arc<Mutex<SystemTime>>,
}

#[async_trait]
impl ArrowOperator for WebhookSinkFunc {
    fn name(&self) -> String {
        "WebhookSink".to_string()
    }

    fn tables(&self) -> HashMap<String, TableConfig> {
        global_table_config("s", "webhook sink state")
    }

    async fn process_batch(
        &mut self,
        record: RecordBatch,
        ctx: &mut OperatorContext,
        _: &mut dyn Collector,
    ) {
        for body in self.serializer.serialize(&record) {
            let permit = self
                .semaphore
                .clone()
                .acquire_owned()
                .await
                .expect("websink semaphore closed");

            let body: bytes::Bytes = body.into();

            let client = self.client.clone();
            let control_tx = ctx.control_tx.clone();
            let error_lock = self.last_reported_error_at.clone();
            let url = self.url.clone();

            // these are just used for (potential) error reporting and we don't need to clone them
            let operator_id = ctx.task_info.operator_id.clone();
            let task_index = ctx.task_info.task_index as usize;
            let node_id = ctx.task_info.node_id;

            tokio::task::spawn(async move {
                // move the permit into the task
                let _permit = permit;
                let mut retries = 0;
                loop {
                    let req = client
                        .post(&*url)
                        .body(body.clone())
                        .build()
                        .expect("failed to build request");

                    match client.execute(req).await {
                        Ok(_) => break,
                        Err(e) => {
                            if let Ok(mut last_reported) = error_lock.try_lock() {
                                if last_reported.elapsed().unwrap_or_default()
                                    > Duration::from_secs(1)
                                {
                                    warn!("websink request failed: {:?}", e);

                                    let details = if let Some(status) = e.status() {
                                        format!(
                                            "server responded with error code: {}",
                                            status.as_u16()
                                        )
                                    } else {
                                        e.to_string()
                                    };

                                    control_tx
                                        .send(ControlResp::Error {
                                            node_id,
                                            operator_id: operator_id.clone(),
                                            task_index,
                                            message: format!("webhook failed (retry {})", retries),
                                            details,
                                        })
                                        .await
                                        .unwrap();

                                    *last_reported = SystemTime::now();
                                }
                            }

                            retries += 1;

                            tokio::time::sleep(Duration::from_millis(
                                (50 * (1 << retries)).min(5_000),
                            ))
                            .await
                        }
                    }
                }
            });
        }
    }

    async fn handle_checkpoint(
        &mut self,
        _: CheckpointBarrier,
        _ctx: &mut OperatorContext,
        _: &mut dyn Collector,
    ) {
        // wait to acquire all of the permits (effectively blocking until all inflight requests are done)
        let _permits = self.semaphore.acquire_many(MAX_INFLIGHT).await.unwrap();

        // TODO: instead of blocking checkpoints on in-progress (or failing) requests, we should store them to state
    }
}
