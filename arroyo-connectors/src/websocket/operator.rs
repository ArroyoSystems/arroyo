use async_trait::async_trait;
use std::collections::HashMap;
use std::str::FromStr;
use std::time::SystemTime;

use arroyo_operator::context::ArrowContext;
use arroyo_operator::operator::SourceOperator;
use arroyo_operator::SourceFinishType;
use arroyo_rpc::formats::{BadData, Format, Framing};
use arroyo_rpc::grpc::TableConfig;
use arroyo_rpc::{grpc::StopMode, ControlMessage};
use arroyo_state::global_table_config;
use arroyo_state::tables::global_keyed_map::GlobalKeyedView;
use arroyo_types::{ArrowMessage, SignalMessage, UserError, Watermark};
use bincode::{Decode, Encode};
use futures::{SinkExt, StreamExt};
use tokio::select;
use tokio_tungstenite::tungstenite::handshake::client::generate_key;
use tokio_tungstenite::tungstenite::http::Uri;
use tokio_tungstenite::{connect_async, tungstenite};
use tracing::{debug, info};
use tungstenite::http::Request;

#[derive(Clone, Debug, Encode, Decode, PartialEq, PartialOrd, Default)]
pub struct WebsocketSourceState {}

pub struct WebsocketSourceFunc {
    pub url: String,
    pub headers: Vec<(String, String)>,
    pub subscription_messages: Vec<String>,
    pub format: Format,
    pub framing: Option<Framing>,
    pub bad_data: Option<BadData>,
    pub state: WebsocketSourceState,
}

#[async_trait]
impl SourceOperator for WebsocketSourceFunc {
    fn name(&self) -> String {
        "WebsocketSource".to_string()
    }

    fn tables(&self) -> HashMap<String, TableConfig> {
        global_table_config("e", "websocket source state")
    }

    async fn on_start(&mut self, ctx: &mut ArrowContext) {
        let s: &mut GlobalKeyedView<(), WebsocketSourceState> = ctx
            .table_manager
            .get_global_keyed_state("e")
            .await
            .expect("couldn't get state for websocket");

        if let Some(state) = s.get(&()) {
            self.state = state.clone();
        }

        ctx.initialize_deserializer(
            self.format.clone(),
            self.framing.clone(),
            self.bad_data.clone(),
        );
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

impl WebsocketSourceFunc {
    async fn our_handle_control_message(
        &mut self,
        ctx: &mut ArrowContext,
        msg: Option<ControlMessage>,
    ) -> Option<SourceFinishType> {
        match msg? {
            ControlMessage::Checkpoint(c) => {
                debug!("starting checkpointing {}", ctx.task_info.task_index);
                let s: &mut GlobalKeyedView<(), WebsocketSourceState> = ctx
                    .table_manager
                    .get_global_keyed_state("e")
                    .await
                    .expect("couldn't get state for websocket");
                s.insert((), self.state.clone()).await;

                if self.start_checkpoint(c, ctx).await {
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

    async fn handle_message(
        &mut self,
        msg: &[u8],
        ctx: &mut ArrowContext,
    ) -> Result<(), UserError> {
        ctx.deserialize_slice(msg, SystemTime::now()).await?;

        if ctx.should_flush() {
            ctx.flush_buffer().await?;
        }

        Ok(())
    }

    async fn run_int(&mut self, ctx: &mut ArrowContext) -> Result<SourceFinishType, UserError> {
        let uri = match Uri::from_str(&self.url.to_string()) {
            Ok(uri) => uri,
            Err(e) => {
                ctx.report_error("Failed to parse endpoint".to_string(), format!("{:?}", e))
                    .await;
                panic!("Failed to parse endpoint: {:?}", e);
            }
        };

        let host = match uri.host() {
            Some(host) => host,
            None => {
                ctx.report_error("Endpoint must have a host".to_string(), "".to_string())
                    .await;
                panic!("Endpoint must have a host");
            }
        };

        let mut request_builder = Request::builder().uri(&self.url);

        for (k, v) in &self.headers {
            request_builder = request_builder.header(k, v);
        }

        let request = match request_builder
            .header("Host", host)
            .header("Sec-WebSocket-Key", generate_key())
            .header("Sec-WebSocket-Version", "13")
            .header("Connection", "Upgrade")
            .header("Upgrade", "websocket")
            .body(())
        {
            Ok(request) => request,
            Err(e) => {
                ctx.report_error("Failed to build request".to_string(), format!("{:?}", e))
                    .await;
                panic!("Failed to build request: {:?}", e);
            }
        };

        let ws_stream = match connect_async(request).await {
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

        let (mut tx, mut rx) = ws_stream.split();

        for msg in &self.subscription_messages {
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
                                match msg {
                                    tungstenite::Message::Text(t) => {
                                        self.handle_message(&t.as_bytes(), ctx).await?
                                    },
                                    tungstenite::Message::Binary(bs) => {
                                        self.handle_message(&bs, ctx).await?
                                    },
                                    tungstenite::Message::Ping(d) => {
                                        tx.send(tungstenite::Message::Pong(d)).await
                                            .map(|_| ())
                                            .map_err(|e| UserError::new("Failed to send pong to websocket server", e.to_string()))?
                                    },
                                    tungstenite::Message::Pong(_) => {
                                        // ignore
                                    },
                                    tungstenite::Message::Close(_) => {
                                        ctx.report_error("Received close frame from server".to_string(), "".to_string()).await;
                                        panic!("Received close frame from server");
                                    },
                                    tungstenite::Message::Frame(_) => {
                                        // this should be captured by tungstenite
                                    },
                                };
                            }
                        Some(Err(e)) => {
                            ctx.report_error("Error while reading from websocket".to_string(), format!("{:?}", e)).await;
                            panic!("Error while reading from websocket: {:?}", e);
                        }
                        None => {
                            info!("Socket closed");
                            return Ok(SourceFinishType::Final);
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
