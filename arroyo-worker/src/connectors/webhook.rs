use std::{
    marker::PhantomData,
    sync::Arc,
    time::{Duration, SystemTime},
};

use arroyo_macro::process_fn;
use arroyo_rpc::ControlResp;
use arroyo_rpc::{grpc::TableDescriptor, var_str::VarStr, OperatorConfig};
use arroyo_types::{CheckpointBarrier, Key, Record};

use serde::{Deserialize, Serialize};
use tokio::sync::{Mutex, Semaphore};

use arroyo_formats::old::DataSerializer;
use arroyo_formats::SchemaData;
use tracing::warn;
use typify::import_types;

use crate::engine::StreamNode;
use crate::header_map;
use crate::old::Context;

import_types!(schema = "../connector-schemas/webhook/table.json", convert = { {type = "string", format = "var-str"} = VarStr });

const MAX_INFLIGHT: u32 = 50;

#[derive(StreamNode)]
pub struct WebhookSinkFunc<K, T>
where
    K: Key,
    T: Serialize + SchemaData,
{
    url: Arc<String>,
    semaphore: Arc<Semaphore>,
    client: reqwest::Client,
    serializer: DataSerializer<T>,
    last_reported_error_at: Arc<Mutex<SystemTime>>,
    _t: PhantomData<K>,
}

#[process_fn(in_k = K, in_t = T)]
impl<K, T> WebhookSinkFunc<K, T>
where
    K: Key,
    T: Serialize + SchemaData,
{
    pub fn from_config(config: &str) -> Self {
        let config: OperatorConfig =
            serde_json::from_str(config).expect("Invalid config for WebhookSink");
        let table: WebhookTable =
            serde_json::from_value(config.table).expect("Invalid table config for WebhookSink");

        let headers = header_map(table.headers)
            .into_iter()
            .map(|(k, v)| {
                (
                    (&k).try_into()
                        .expect(&format!("invalid header name {}", k)),
                    (&v).try_into()
                        .expect(&format!("invalid header value {}", v)),
                )
            })
            .collect();

        Self {
            url: Arc::new(table.endpoint),
            client: reqwest::ClientBuilder::new()
                .default_headers(headers)
                .timeout(Duration::from_secs(5))
                .build()
                .expect("could not construct reqwest client"),
            semaphore: Arc::new(Semaphore::new(MAX_INFLIGHT as usize)),
            serializer: DataSerializer::new(
                config
                    .format
                    .expect("No format configured for webhook sink"),
            ),
            last_reported_error_at: Arc::new(Mutex::new(SystemTime::UNIX_EPOCH)),
            _t: PhantomData,
        }
    }

    fn name(&self) -> String {
        "WebhookSink".to_string()
    }

    fn tables(&self) -> Vec<TableDescriptor> {
        vec![arroyo_state::global_table("s", "webhook sink state")]
    }

    async fn process_element(&mut self, record: &Record<K, T>, ctx: &mut Context<(), ()>) {
        let permit = self
            .semaphore
            .clone()
            .acquire_owned()
            .await
            .expect("websink semaphore closed");

        let Some(body) = self.serializer.to_vec(&record.value) else {
            return;
        };

        let body: bytes::Bytes = body.into();

        let client = self.client.clone();
        let control_tx = ctx.control_tx.clone();
        let error_lock = self.last_reported_error_at.clone();
        let url = self.url.clone();

        // these are just used for (potential) error reporting and we don't need to clone them
        let operator_id = ctx.task_info.operator_id.clone();
        let task_index = ctx.task_info.task_index;

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
                            if last_reported.elapsed().unwrap_or_default() > Duration::from_secs(1)
                            {
                                warn!("websink request failed: {:?}", e);

                                let details = if let Some(status) = e.status() {
                                    format!("server responded with error code: {}", status.as_u16())
                                } else {
                                    e.to_string()
                                };

                                control_tx
                                    .send(ControlResp::Error {
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

                        tokio::time::sleep(Duration::from_millis((50 * (1 << retries)).min(5_000)))
                            .await
                    }
                }
            }
        });
    }

    async fn handle_checkpoint(&mut self, _: &CheckpointBarrier, _ctx: &mut Context<(), ()>) {
        // wait to acquire all of the permits (effectively blocking until all inflight requests are done)
        let _permits = self.semaphore.acquire_many(MAX_INFLIGHT).await.unwrap();

        // TODO: instead of blocking checkpoints on in-progress (or failing) requests, we should store them to state
    }
}
