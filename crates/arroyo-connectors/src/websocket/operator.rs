use async_trait::async_trait;
use std::collections::HashMap;
use std::str::FromStr;
use std::time::SystemTime;

use arroyo_operator::context::{SourceCollector, SourceContext};
use arroyo_operator::operator::SourceOperator;
use arroyo_operator::SourceFinishType;
use arroyo_rpc::connector_err;
use arroyo_rpc::errors::DataflowResult;
use arroyo_rpc::formats::{BadData, Format, Framing};
use arroyo_rpc::grpc::rpc::TableConfig;
use arroyo_rpc::{grpc::rpc::StopMode, ControlMessage};
use arroyo_state::global_table_config;
use arroyo_state::tables::global_keyed_map::GlobalKeyedView;
use arroyo_types::{SignalMessage, Watermark};
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

    async fn on_start(&mut self, ctx: &mut SourceContext) -> DataflowResult<()> {
        let s: &mut GlobalKeyedView<(), WebsocketSourceState> = ctx
            .table_manager
            .get_global_keyed_state("e")
            .await?;

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
        collector.initialize_deserializer(
            self.format.clone(),
            self.framing.clone(),
            self.bad_data.clone(),
            &[],
        );

        self.run_int(ctx, collector).await
    }
}

impl WebsocketSourceFunc {
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
                let s: &mut GlobalKeyedView<(), WebsocketSourceState> = ctx
                    .table_manager
                    .get_global_keyed_state("e")
                    .await?;
                s.insert((), self.state.clone()).await;

                if self.start_checkpoint(c, ctx, collector).await {
                    return Ok(Some(SourceFinishType::Immediate));
                }
            }
            ControlMessage::Stop { mode } => {
                info!("Stopping websocket source: {:?}", mode);

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

    async fn handle_message(
        &mut self,
        msg: &[u8],
        collector: &mut SourceCollector,
    ) -> DataflowResult<()> {
        collector
            .deserialize_slice(msg, SystemTime::now(), None)
            .await?;

        if collector.should_flush() {
            collector.flush_buffer().await?;
        }

        Ok(())
    }

    async fn run_int(
        &mut self,
        ctx: &mut SourceContext,
        collector: &mut SourceCollector,
    ) -> DataflowResult<SourceFinishType> {
        let uri = Uri::from_str(&self.url.to_string())
            .map_err(|e| connector_err!(User, NoRetry, "invalid websocket URL '{}': {}", self.url, e))?;

        let host = uri
            .host()
            .ok_or_else(|| connector_err!(User, NoRetry, "websocket endpoint must have a host"))?;

        let mut request_builder = Request::builder().uri(&self.url);

        for (k, v) in &self.headers {
            request_builder = request_builder.header(k, v);
        }

        let request = request_builder
            .header("Host", host)
            .header("Sec-WebSocket-Key", generate_key())
            .header("Sec-WebSocket-Version", "13")
            .header("Connection", "Upgrade")
            .header("Upgrade", "websocket")
            .body(())
            .map_err(|e| connector_err!(Internal, NoRetry, "failed to build websocket request: {}", e))?;

        let (ws_stream, _) = connect_async(request)
            .await
            .map_err(|e| connector_err!(External, WithBackoff, "failed to connect to websocket '{}': {}", self.url, e))?;

        let (mut tx, mut rx) = ws_stream.split();

        for msg in &self.subscription_messages {
            tx.send(tungstenite::Message::Text(msg.clone()))
                .await
                .map_err(|e| connector_err!(External, WithBackoff, "failed to send subscription message: {}", e))?;
        }

        let mut flush_ticker = tokio::time::interval(std::time::Duration::from_millis(50));
        flush_ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

        // since there's no way to partition across a websocket source, only read on the first task
        if ctx.task_info.task_index == 0 {
            loop {
                select! {
                    message = rx.next()  => {
                        match message {
                            Some(Ok(msg)) => {
                                match msg {
                                    tungstenite::Message::Text(t) => {
                                        self.handle_message(t.as_bytes(), collector).await?
                                    },
                                    tungstenite::Message::Binary(bs) => {
                                        self.handle_message(&bs, collector).await?
                                    },
                                    tungstenite::Message::Ping(d) => {
                                        tx.send(tungstenite::Message::Pong(d))
                                            .await
                                            .map_err(|e| connector_err!(External, WithBackoff, "failed to send pong: {}", e))?;
                                    },
                                    tungstenite::Message::Pong(_) => {
                                        // ignore
                                    },
                                    tungstenite::Message::Close(_) => {
                                        return Err(connector_err!(External, WithBackoff, "websocket server closed the connection"));
                                    },
                                    tungstenite::Message::Frame(_) => {
                                        // this should be captured by tungstenite
                                    },
                                };
                            }
                        Some(Err(e)) => {
                            return Err(connector_err!(External, WithBackoff, "error reading from websocket: {}", e));
                        }
                        None => {
                            info!("Socket closed");
                            return Ok(SourceFinishType::Final);
                        }
                    }
                    }
                    _ = flush_ticker.tick() => {
                        if collector.should_flush() {
                            collector.flush_buffer().await?;
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
