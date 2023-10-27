use bincode::{Decode, Encode};
use bytes::Bytes;
use futures::StreamExt;
use std::borrow::Cow;
use std::str::FromStr;
use std::time::SystemTime;
use std::{marker::PhantomData, time::Duration};

use arroyo_macro::source_fn;
use arroyo_rpc::ControlMessage;
use arroyo_rpc::{grpc::TableDescriptor, OperatorConfig};
use arroyo_types::{string_to_map, Message, Record, UserError, Watermark};

use serde::{Deserialize, Serialize};
use tokio::select;
use tokio::time::MissedTickBehavior;

use arroyo_rpc::grpc::StopMode;
use arroyo_state::tables::global_keyed_map::GlobalKeyedState;
use tracing::{debug, info, warn};
use typify::import_types;

use crate::formats::DataDeserializer;
use crate::{
    engine::{Context, StreamNode},
    SchemaData, SourceFinishType,
};

import_types!(schema = "../connector-schemas/polling_http/table.json");

const DEFAULT_POLLING_INTERVAL: Duration = Duration::from_secs(1);
const MAX_BODY_SIZE: usize = 5 * 1024 * 1024; // 5M ought to be enough for anybody

#[derive(StreamNode)]
pub struct PollingHttpSourceFunc<K, T>
where
    K: Send + 'static,
    T: SchemaData,
{
    state: PollingHttpSourceState,
    client: reqwest::Client,
    endpoint: url::Url,
    method: reqwest::Method,
    body: Option<Bytes>,
    polling_interval: Duration,
    emit_behavior: EmitBehavior,
    deserializer: DataDeserializer<T>,

    _t: PhantomData<(K, T)>,
}

#[derive(Clone, Debug, Encode, Decode, PartialEq, PartialOrd)]
pub struct PollingHttpSourceState {
    last_message: Option<Vec<u8>>,
}

#[source_fn(out_k = (), out_t = T)]
impl<K, T> PollingHttpSourceFunc<K, T>
where
    K: Send + 'static,
    T: SchemaData,
{
    pub fn from_config(config: &str) -> Self {
        let config: OperatorConfig =
            serde_json::from_str(config).expect("Invalid config for WebhookSink");
        let table: PollingHttpTable =
            serde_json::from_value(config.table).expect("Invalid table config for WebhookSink");

        let headers = string_to_map(table.headers.as_ref().map(|t| t.0.as_str()).unwrap_or(""))
            .expect("Invalid header map")
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

        let deserializer = DataDeserializer::new(
            config
                .format
                .expect("polling http source must have a format configured"),
            config.framing,
        );

        Self {
            state: PollingHttpSourceState { last_message: None },
            client: reqwest::ClientBuilder::new()
                .default_headers(headers)
                .timeout(Duration::from_secs(5))
                .build()
                .expect("could not construct http client"),
            endpoint: url::Url::from_str(&table.endpoint).expect("invalid endpoint"),
            method: match table.method {
                None | Some(Method::Get) => reqwest::Method::GET,
                Some(Method::Post) => reqwest::Method::POST,
                Some(Method::Put) => reqwest::Method::PUT,
                Some(Method::Patch) => reqwest::Method::PATCH,
            },
            body: table.body.map(|b| b.into()),
            polling_interval: table
                .poll_interval_ms
                .map(|d| Duration::from_millis(d as u64))
                .unwrap_or(DEFAULT_POLLING_INTERVAL),
            emit_behavior: table.emit_behavior.unwrap_or(EmitBehavior::All),
            deserializer,
            _t: PhantomData,
        }
    }

    fn name(&self) -> String {
        "PollingHttpSource".to_string()
    }

    fn tables(&self) -> Vec<TableDescriptor> {
        vec![arroyo_state::global_table("s", "polling http source state")]
    }

    async fn on_start(&mut self, ctx: &mut Context<(), T>) {
        let s: GlobalKeyedState<(), PollingHttpSourceState, _> =
            ctx.state.get_global_keyed_state('s').await;

        if let Some(state) = s.get(&()) {
            self.state = state.clone();
        }
    }

    async fn our_handle_control_message(
        &mut self,
        ctx: &mut Context<(), T>,
        msg: Option<ControlMessage>,
    ) -> Option<SourceFinishType> {
        match msg? {
            ControlMessage::Checkpoint(c) => {
                debug!("starting checkpointing {}", ctx.task_info.task_index);
                let state = self.state.clone();
                let mut s: GlobalKeyedState<(), PollingHttpSourceState, _> =
                    ctx.state.get_global_keyed_state('s').await;
                s.insert((), state).await;

                if self.checkpoint(c, ctx).await {
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
            ControlMessage::Commit { epoch: _ } => {
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

    async fn run(&mut self, ctx: &mut Context<(), T>) -> SourceFinishType {
        // since there's no way to partition across an http source, only read on the first task
        let mut timer = tokio::time::interval(self.polling_interval);
        timer.set_missed_tick_behavior(MissedTickBehavior::Delay);

        if ctx.task_info.task_index == 0 {
            loop {
                select! {
                    _ = timer.tick()  => {
                        match self.request().await {
                            Ok(buf) => {
                                if self.emit_behavior == EmitBehavior::Changed {
                                    if Some(&buf) == self.state.last_message.as_ref() {
                                        continue;
                                    }
                                }

                                let iter = self.deserializer.deserialize_slice(&buf).await;

                                for record in iter {
                                    match record {
                                        Ok(value) => {
                                            ctx.collect(Record {
                                                timestamp: SystemTime::now(),
                                                key: None,
                                                value,
                                            }).await;
                                        }
                                        Err(e) => {
                                            ctx.report_user_error(e).await;
                                        }
                                    }
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
                            return r;
                        }
                    }
                }
            }
        } else {
            // otherwise set idle and just process control messages
            ctx.broadcast(Message::Watermark(Watermark::Idle)).await;
            loop {
                let msg = ctx.control_rx.recv().await;
                if let Some(r) = self.our_handle_control_message(ctx, msg).await {
                    return r;
                }
            }
        }
    }
}
