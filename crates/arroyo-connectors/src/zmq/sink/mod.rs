use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use std::sync::atomic::AtomicBool;
use std::sync::Arc;
use std::time::Duration;

use crate::zmq::{ConnectionPattern, ZmqConfig};
use arroyo_formats::ser::ArrowSerializer;
use arroyo_operator::context::{Collector, OperatorContext};
use arroyo_operator::operator::ArrowOperator;
use arroyo_rpc::formats::Format;
use zmq::Context;

#[cfg(test)]
mod test;

pub struct ZmqSinkFunc {
    pub config: ZmqConfig,
    pub serializer: ArrowSerializer,
    pub socket: Option<zmq::Socket>,
    pub stopped: Arc<AtomicBool>,
}

impl ZmqSinkFunc {
    pub fn new(config: ZmqConfig, format: Format) -> Self {
        Self {
            config,
            serializer: ArrowSerializer::new(format),
            socket: None,
            stopped: Arc::new(AtomicBool::new(false)),
        }
    }
}

#[async_trait]
impl ArrowOperator for ZmqSinkFunc {
    fn name(&self) -> String {
        let url = self.config.url.sub_env_vars().unwrap();
        format!("zmq-sink-{url}")
    }

    async fn on_start(&mut self, ctx: &mut OperatorContext) {
        let url = match self.config.url.sub_env_vars() {
            Ok(url) => url,
            Err(e) => {
                ctx.report_error(
                    "Failed to resolve ZMQ URL",
                    format!("Environment variable substitution failed: {e}"),
                )
                .await;
                panic!("Failed to resolve ZMQ URL: {e}");
            }
        };

        let mut attempts = 0;
        while attempts < 20 {
            let context = Context::new();

            match context.socket(zmq::SocketType::PUSH) {
                Ok(socket) => {
                    let result = match self.config.connection_pattern {
                        ConnectionPattern::Bind => socket.bind(&url),
                        ConnectionPattern::Connect => socket.connect(&url),
                    };

                    match result {
                        Ok(_) => {
                            self.socket = Some(socket);
                            return;
                        }
                        Err(e) => {
                            ctx.report_error("Failed to connect to ZMQ", e.to_string())
                                .await;
                        }
                    }
                }
                Err(e) => {
                    ctx.report_error("Failed to create ZMQ socket", e.to_string())
                        .await;
                }
            }

            tokio::time::sleep(Duration::from_millis((50 * (1 << attempts)).min(5_000))).await;
            attempts += 1;
        }

        panic!("Failed to establish connection to ZMQ after 20 retries");
    }

    async fn process_batch(
        &mut self,
        batch: RecordBatch,
        ctx: &mut OperatorContext,
        _: &mut dyn Collector,
    ) {
        let socket = self.socket.as_ref().unwrap();

        for data in self.serializer.serialize(&batch) {
            match socket.send(data, 0) {
                Ok(_) => (),
                Err(e) => {
                    ctx.report_error("Could not write to ZMQ", format!("{e:?}"))
                        .await;
                    panic!("Could not write to ZMQ: {e:?}");
                }
            }
        }
    }
}

impl Drop for ZmqSinkFunc {
    fn drop(&mut self) {
        self.stopped
            .store(true, std::sync::atomic::Ordering::SeqCst);
    }
}
