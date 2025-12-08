use anyhow::bail;
use arroyo_rpc::{connector_err, ControlMessage};
use arroyo_types::{SignalMessage, Watermark};
use async_trait::async_trait;
use bincode::{Decode, Encode};
use bytes::Bytes;
use futures::StreamExt;
use std::borrow::Cow;
use std::collections::HashMap;
use std::time::Duration;
use std::time::SystemTime;

use tokio::select;
use tokio::time::MissedTickBehavior;

use crate::polling_http::EmitBehavior;
use arroyo_operator::context::{SourceCollector, SourceContext};
use arroyo_operator::operator::SourceOperator;
use arroyo_operator::SourceFinishType;
use arroyo_rpc::errors::DataflowResult;
use arroyo_rpc::formats::{BadData, Format, Framing};
use arroyo_rpc::grpc::rpc::{StopMode, TableConfig};
use arroyo_state::tables::global_keyed_map::GlobalKeyedView;
use tracing::{debug, info, warn};

const MAX_BODY_SIZE: usize = 5 * 1024 * 1024; // 5M ought to be enough for anybody

pub struct PollingHttpSourceFunc {
    pub state: PollingHttpSourceState,
    pub client: reqwest::Client,
    pub endpoint: url::Url,
    pub method: reqwest::Method,
    pub body: Option<Bytes>,
    pub polling_interval: Duration,
    pub emit_behavior: EmitBehavior,
    pub format: Format,
    pub framing: Option<Framing>,
    pub bad_data: Option<BadData>,
}

#[derive(Clone, Debug, Encode, Decode, PartialEq, PartialOrd, Default)]
pub struct PollingHttpSourceState {
    last_message: Option<Vec<u8>>,
}

#[async_trait]
impl SourceOperator for PollingHttpSourceFunc {
    fn name(&self) -> String {
        "PollingHttpSource".to_string()
    }

    fn tables(&self) -> HashMap<String, TableConfig> {
        arroyo_state::global_table_config("s", "polling http source state")
    }

    async fn on_start(&mut self, ctx: &mut SourceContext) -> DataflowResult<()> {
        let s: &mut GlobalKeyedView<(), PollingHttpSourceState> =
            ctx.table_manager.get_global_keyed_state("s").await?;

        if let Some(state) = s.get(&()) {
            self.state = state.clone();
        }
        Ok(())
    }

    async fn run(
        &mut self,
        ctx: &mut SourceContext,
        collector: &mut SourceCollector,
    ) -> DataflowResult<SourceFinishType> {
        self.run_int(ctx, collector).await
    }
}

impl PollingHttpSourceFunc {
    async fn our_handle_control_message(
        &mut self,
        ctx: &mut SourceContext,
        collector: &mut SourceCollector,
        msg: Option<ControlMessage>,
    ) -> DataflowResult<Option<SourceFinishType>> {
        let Some(msg) = msg else {
            return Ok(None);
        };
        match msg {
            ControlMessage::Checkpoint(c) => {
                debug!("starting checkpointing {}", ctx.task_info.task_index);
                let state = self.state.clone();
                let s = ctx.table_manager.get_global_keyed_state("s").await?;
                s.insert((), state).await;

                if self.start_checkpoint(c, ctx, collector).await {
                    return Ok(Some(SourceFinishType::Immediate));
                }
            }
            ControlMessage::Stop { mode } => {
                info!("Stopping polling http source: {:?}", mode);

                match mode {
                    StopMode::Graceful => {
                        return Ok(Some(SourceFinishType::Graceful));
                    }
                    StopMode::Immediate => {
                        return Ok(Some(SourceFinishType::Immediate));
                    }
                }
            }
            ControlMessage::Commit { .. } => {
                unreachable!("sources shouldn't receive commit messages");
            }
            ControlMessage::LoadCompacted { compacted } => {
                ctx.load_compacted(compacted).await;
            }
            ControlMessage::NoOp => {}
        }
        Ok(None)
    }

    async fn request(&mut self) -> anyhow::Result<Vec<u8>> {
        let mut request = self
            .client
            .request(self.method.clone(), self.endpoint.clone());

        if let Some(body) = self.body.clone() {
            request = request.body(body);
        }

        let resp = self.client.execute(request.build()?).await?;

        if resp.status().is_success() {
            let content_len = resp.content_length().unwrap_or(0);
            if content_len > MAX_BODY_SIZE as u64 {
                bail!("content length sent by server exceeds maximum limit ({content_len} > {MAX_BODY_SIZE})");
            }

            let mut buf = Vec::with_capacity(content_len as usize);

            let mut bytes_stream = resp.bytes_stream();
            while let Some(chunk) = bytes_stream.next().await {
                buf.extend_from_slice(&chunk?);

                if buf.len() > MAX_BODY_SIZE {
                    bail!("response body exceeds max length {MAX_BODY_SIZE}");
                }
            }

            Ok(buf)
        } else {
            let status = resp.status();
            let bytes = resp.bytes().await;
            let error_body = bytes
                .as_ref()
                .map(|b| String::from_utf8_lossy(b))
                .unwrap_or_else(|_| Cow::from("<no body>"));

            warn!(
                "HTTP request to {} failed with {}: {}",
                self.endpoint,
                status.as_u16(),
                error_body
            );

            bail!("http server responded with {}", status.as_u16());
        }
    }

    async fn run_int(
        &mut self,
        ctx: &mut SourceContext,
        collector: &mut SourceCollector,
    ) -> DataflowResult<SourceFinishType> {
        collector.initialize_deserializer(
            self.format.clone(),
            self.framing.clone(),
            self.bad_data.clone(),
            &[],
        );

        // since there's no way to partition across an http source, only read on the first task
        let mut timer = tokio::time::interval(self.polling_interval);
        timer.set_missed_tick_behavior(MissedTickBehavior::Delay);

        if ctx.task_info.task_index == 0 {
            loop {
                select! {
                    _ = timer.tick()  => {
                        match self.request().await {
                            Ok(buf) => {
                                if self.emit_behavior == EmitBehavior::Changed && Some(&buf) == self.state.last_message.as_ref() {
                                    continue;
                                }

                                collector.deserialize_slice(&buf, SystemTime::now(), None).await?;

                                if collector.should_flush() {
                                    collector.flush_buffer().await?;
                                }

                                self.state.last_message = Some(buf);
                            }
                            Err(e) => {
                                ctx.report_nonfatal_error(connector_err!(User, WithBackoff, "HTTP request failed: {}", e)).await;
                            }
                        }
                    }
                    control_message = ctx.control_rx.recv() => {
                        if let Some(r) = self.our_handle_control_message(ctx, collector, control_message).await? {
                            return Ok(r);
                        }
                    }
                }
            }
        } else {
            // otherwise set idle and just process control messages
            collector
                .broadcast(SignalMessage::Watermark(Watermark::Idle))
                .await;
            loop {
                let msg = ctx.control_rx.recv().await;
                if let Some(r) = self.our_handle_control_message(ctx, collector, msg).await? {
                    return Ok(r);
                }
            }
        }
    }
}
