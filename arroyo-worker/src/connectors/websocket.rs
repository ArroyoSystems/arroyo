use std::{
    marker::PhantomData,
    time::{Duration, Instant, SystemTime},
};

use arroyo_macro::source_fn;
use arroyo_rpc::types::Format;
use arroyo_rpc::{
    grpc::{StopMode, TableDescriptor},
    ControlMessage, OperatorConfig,
};
use arroyo_state::tables::GlobalKeyedState;
use arroyo_types::{Data, Message, Record, UserError, Watermark};
use bincode::{Decode, Encode};
use futures::{SinkExt, StreamExt};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use tokio::select;
use tokio_tungstenite::{connect_async, tungstenite};
use tracing::{debug, info};
use typify::import_types;

use crate::{
    engine::{Context, StreamNode},
    formats, SourceFinishType,
};

import_types!(schema = "../connector-schemas/websocket/table.json");

#[derive(Clone, Debug, Encode, Decode, PartialEq, PartialOrd, Default)]
pub struct WebsocketSourceState {}

#[derive(StreamNode, Clone)]
pub struct WebsocketSourceFunc<K, T>
where
    K: DeserializeOwned + Data,
    T: DeserializeOwned + Data,
{
    url: String,
    subscription_message: Option<String>,
    format: Format,
    state: WebsocketSourceState,
    _t: PhantomData<(K, T)>,
}

#[source_fn(out_k = (), out_t = T)]
impl<K, T> WebsocketSourceFunc<K, T>
where
    K: DeserializeOwned + Data,
    T: DeserializeOwned + Data,
{
    pub fn from_config(config: &str) -> Self {
        let config: OperatorConfig =
            serde_json::from_str(config).expect("Invalid config for WebsocketSource");
        let table: WebsocketTable =
            serde_json::from_value(config.table).expect("Invalid table config for WebsocketSource");

        Self {
            url: table.endpoint,
            subscription_message: table.subscription_message.map(|s| s.into()),
            format: config.format.expect("WebsocketSource requires a format"),
            state: WebsocketSourceState::default(),
            _t: PhantomData,
        }
    }

    fn name(&self) -> String {
        "WebsocketSource".to_string()
    }

    fn tables(&self) -> Vec<TableDescriptor> {
        vec![arroyo_state::global_table("e", "websocket source state")]
    }

    async fn on_start(&mut self, ctx: &mut Context<(), T>) {
        let s: GlobalKeyedState<(), WebsocketSourceState, _> =
            ctx.state.get_global_keyed_state('e').await;

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
                let mut s: GlobalKeyedState<(), WebsocketSourceState, _> =
                    ctx.state.get_global_keyed_state('e').await;
                s.insert((), self.state.clone()).await;

                if self.checkpoint(c, ctx).await {
                    return Some(SourceFinishType::Immediate);
                }
            }
            ControlMessage::Stop { mode } => {
                info!("Stopping websocket source: {:?}", mode);

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
        }
        None
    }

    async fn run(&mut self, ctx: &mut Context<(), T>) -> SourceFinishType {
        let ws_stream = match connect_async(&self.url).await {
            Ok((ws_stream, _)) => ws_stream,
            Err(e) => {
                ctx.report_error(
                    "Failed to connect to websocket server".to_string(),
                    e.to_string(),
                )
                .await;
                panic!("{}", e);
            }
        };

        let mut last_reported_error = Instant::now();
        let mut errors = 0;

        let (mut tx, mut rx) = ws_stream.split();

        if let Some(msg) = &self.subscription_message {
            if let Err(e) = tx.send(tungstenite::Message::Text(msg.clone())).await {
                ctx.report_error(
                    "Failed to send subscription message to websocket server".to_string(),
                    e.to_string(),
                )
                .await;
                panic!(
                    "Failed to send subscription message to websocket server: {:?}",
                    e
                );
            }
        }

        // since there's no way to partition across a websocket source, only read on the first task
        if ctx.task_info.task_index == 0 {
            loop {
                select! {
                    message = rx.next()  => {
                        match message {
                            Some(Ok(msg)) => {
                                let data = match msg {
                                    tungstenite::Message::Text(t) => {
                                        formats::deserialize_slice(&self.format, &t.as_bytes()).map(|t| Some(t))
                                    },
                                    tungstenite::Message::Binary(bs) => {
                                        formats::deserialize_slice(&self.format, &bs).map(|t| Some(t))
                                    },
                                    tungstenite::Message::Ping(d) => {
                                        tx.send(tungstenite::Message::Pong(d)).await
                                            .map(|_| None)
                                            .map_err(|e| UserError::new("Failed to send pong to websocket server", e.to_string()))
                                    },
                                    tungstenite::Message::Pong(_) => {
                                        // ignore
                                        Ok(None)
                                    },
                                    tungstenite::Message::Close(_) => {
                                        ctx.report_error("Received close frame from server".to_string(), "".to_string()).await;
                                        return SourceFinishType::Final;
                                    },
                                    tungstenite::Message::Frame(_) => {
                                        // this should be captured by tungstenite
                                        Ok(None)
                                    },
                                };

                                match data {
                                    Ok(Some(t)) => {
                                        ctx.collector.collect(Record {
                                            timestamp: SystemTime::now(),
                                            key: None,
                                            value: t,
                                        }).await;
                                    }
                                    Ok(None) => {}
                                    Err(e) => {
                                        errors += 1;
                                        if last_reported_error.elapsed() > Duration::from_secs(30) {
                                            ctx.report_error(format!("{} x {}", e.name, errors),
                                                e.details).await;
                                            errors = 0;
                                            last_reported_error = Instant::now();
                                        }
                                    }
                                };
                            }
                        Some(Err(e)) => {
                            ctx.report_error("Error while reading from websocket".to_string(), format!("{:?}", e)).await;
                            panic!("Error while reading from websocket: {:?}", e);
                        }
                        None => {
                            info!("Socket closed");
                            return SourceFinishType::Final;
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
