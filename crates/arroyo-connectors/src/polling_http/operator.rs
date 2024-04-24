use async_trait::async_trait;
use bincode::{Decode, Encode};
use bytes::Bytes;
use futures::StreamExt;
use std::borrow::Cow;
use std::collections::HashMap;
use std::time::Duration;
use std::time::SystemTime;

use arroyo_rpc::ControlMessage;
use arroyo_types::{ArrowMessage, SignalMessage, UserError, Watermark};

use tokio::select;
use tokio::time::MissedTickBehavior;

use crate::polling_http::EmitBehavior;
use arroyo_operator::context::ArrowContext;
use arroyo_operator::operator::SourceOperator;
use arroyo_operator::SourceFinishType;
use arroyo_rpc::formats::{BadData, Format, Framing};
use arroyo_rpc::grpc::{StopMode, TableConfig};
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

    async fn on_start(&mut self, ctx: &mut ArrowContext) {
        let s: &mut GlobalKeyedView<(), PollingHttpSourceState> = ctx
            .table_manager
            .get_global_keyed_state("s")
            .await
            .expect("should be able to read http state");

        if let Some(state) = s.get(&()) {
            self.state = state.clone();
        }
    }

    async fn run(&mut self, ctx: &mut ArrowContext) -> SourceFinishType {
        match self.run_int(ctx).await {
            Ok(r) => r,
            Err(e) => {
                ctx.report_error(e.name.clone(), e.details.clone()).await;

                panic!("{}: {}", e.name, e.details);
            }
        }
    }
}

impl PollingHttpSourceFunc {
    async fn our_handle_control_message(
        &mut self,
        ctx: &mut ArrowContext,
        msg: Option<ControlMessage>,
    ) -> Option<SourceFinishType> {
        match msg? {
            ControlMessage::Checkpoint(c) => {
                debug!("starting checkpointing {}", ctx.task_info.task_index);
                let state = self.state.clone();
                let s = ctx
                    .table_manager
                    .get_global_keyed_state("s")
                    .await
                    .expect("should be able to get http state");
                s.insert((), state).await;

                if self.start_checkpoint(c, ctx).await {
                    return Some(SourceFinishType::Immediate);
                }
            }
            ControlMessage::Stop { mode } => {
                info!("Stopping polling http source: {:?}", mode);

                match mode {
                    StopMode::Graceful => {
                        return Some(SourceFinishType::Graceful);
                    }
                    StopMode::Immediate => {
                        return Some(SourceFinishType::Immediate);
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
        None
    }

    async fn request(&mut self) -> Result<Vec<u8>, UserError> {
        let mut request = self
            .client
            .request(self.method.clone(), self.endpoint.clone());

        if let Some(body) = self.body.clone() {
            request = request.body(body);
        }

        let resp = self
            .client
            .execute(request.build().expect("building request failed"))
            .await
            .map_err(|e| {
                UserError::new(
                    "request failed",
                    format!("failed to execute HTTP request: {}", e),
                )
            })?;

        if resp.status().is_success() {
            let content_len = resp.content_length().unwrap_or(0);
            if content_len > MAX_BODY_SIZE as u64 {
                return Err(UserError::new(
                    "error reading from http endpoint",
                    format!(
                        "content length sent by server exceeds maximum limit ({} > {})",
                        content_len, MAX_BODY_SIZE
                    ),
                ));
            }

            let mut buf = Vec::with_capacity(content_len as usize);

            let mut bytes_stream = resp.bytes_stream();
            while let Some(chunk) = bytes_stream.next().await {
                buf.extend_from_slice(&chunk.map_err(|e| {
                    UserError::new(
                        "request failed",
                        format!("failed while reading body: {}", e),
                    )
                })?);

                if buf.len() > MAX_BODY_SIZE {
                    return Err(UserError::new(
                        "error reading from http endpoint",
                        format!("response body exceeds max length {}", MAX_BODY_SIZE),
                    ));
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

            Err(UserError::new(
                "server responded with error",
                format!("http server responded with {}", status.as_u16()),
            ))
        }
    }

    async fn run_int(&mut self, ctx: &mut ArrowContext) -> Result<SourceFinishType, UserError> {
        ctx.initialize_deserializer(
            self.format.clone(),
            self.framing.clone(),
            self.bad_data.clone(),
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

                                ctx.deserialize_slice(&buf, SystemTime::now()).await?;

                                if ctx.should_flush() {
                                    ctx.flush_buffer().await?;
                                }

                                self.state.last_message = Some(buf);
                            }
                            Err(e) => {
                                ctx.report_user_error(e).await;
                            }
                        }
                    }
                    control_message = ctx.control_rx.recv() => {
                        if let Some(r) = self.our_handle_control_message(ctx, control_message).await {
                            return Ok(r);
                        }
                    }
                }
            }
        } else {
            // otherwise set idle and just process control messages
            ctx.broadcast(ArrowMessage::Signal(SignalMessage::Watermark(
                Watermark::Idle,
            )))
            .await;
            loop {
                let msg = ctx.control_rx.recv().await;
                if let Some(r) = self.our_handle_control_message(ctx, msg).await {
                    return Ok(r);
                }
            }
        }
    }
}
