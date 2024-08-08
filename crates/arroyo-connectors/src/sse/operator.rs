use crate::sse::SseTable;
use arroyo_operator::context::ArrowContext;
use arroyo_operator::operator::{OperatorNode, SourceOperator};
use arroyo_operator::SourceFinishType;
use arroyo_rpc::formats::{BadData, Format, Framing};
use arroyo_rpc::grpc::rpc::{StopMode, TableConfig};
use arroyo_rpc::{ControlMessage, ControlResp, OperatorConfig};
use arroyo_state::tables::global_keyed_map::GlobalKeyedView;
use arroyo_types::{string_to_map, ArrowMessage, SignalMessage, UserError, Watermark};
use async_trait::async_trait;
use bincode::{Decode, Encode};
use eventsource_client::{Client, Error, SSE};
use futures::StreamExt;
use std::collections::{HashMap, HashSet};
use std::time::{Duration, Instant, SystemTime};
use tokio::select;
use tokio::time::MissedTickBehavior;
use tracing::{debug, info};

#[derive(Clone, Debug, Encode, Decode, PartialEq, PartialOrd, Default)]
pub struct SSESourceState {
    last_id: Option<String>,
}

pub struct SSESourceFunc {
    url: String,
    headers: Vec<(String, String)>,
    events: Vec<String>,
    format: Format,
    framing: Option<Framing>,
    bad_data: Option<BadData>,
    state: SSESourceState,
}

impl SSESourceFunc {
    pub fn new_operator(table: SseTable, config: OperatorConfig) -> anyhow::Result<OperatorNode> {
        let headers = table
            .headers
            .as_ref()
            .map(|s| s.sub_env_vars().expect("Failed to substitute env vars"));

        Ok(OperatorNode::from_source(Box::new(SSESourceFunc {
            url: table.endpoint,
            headers: string_to_map(&headers.unwrap_or("".to_string()), ':')
                .expect("Invalid header map")
                .into_iter()
                .collect(),
            events: table
                .events
                .map(|e| e.split(',').map(|e| e.to_string()).collect())
                .unwrap_or_default(),
            format: config.format.expect("SSE requires a format"),
            framing: config.framing,
            bad_data: config.bad_data,
            state: SSESourceState::default(),
        })))
    }
}

#[async_trait]
impl SourceOperator for SSESourceFunc {
    fn name(&self) -> String {
        "SSESource".to_string()
    }

    fn tables(&self) -> HashMap<String, TableConfig> {
        arroyo_state::global_table_config("e", "sse source state")
    }

    async fn run(&mut self, ctx: &mut ArrowContext) -> SourceFinishType {
        let s: &mut GlobalKeyedView<(), SSESourceState> = ctx
            .table_manager
            .get_global_keyed_state("e")
            .await
            .expect("should be able to read SSE state");

        if let Some(state) = s.get(&()) {
            self.state = state.clone();
        }

        match self.run_int(ctx).await {
            Ok(r) => r,
            Err(e) => {
                ctx.report_error(e.name.clone(), e.details.clone()).await;

                panic!("{}: {}", e.name, e.details);
            }
        }
    }
}

impl SSESourceFunc {
    async fn our_handle_control_message(
        &mut self,
        ctx: &mut ArrowContext,
        msg: Option<ControlMessage>,
    ) -> Option<SourceFinishType> {
        match msg? {
            ControlMessage::Checkpoint(c) => {
                debug!("starting checkpointing {}", ctx.task_info.task_index);
                let s = ctx
                    .table_manager
                    .get_global_keyed_state("e")
                    .await
                    .expect("should be able to get SSE state");
                s.insert((), self.state.clone()).await;

                if self.start_checkpoint(c, ctx).await {
                    return Some(SourceFinishType::Immediate);
                }
            }
            ControlMessage::Stop { mode } => {
                info!("Stopping eventsource source: {:?}", mode);

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

    async fn run_int(&mut self, ctx: &mut ArrowContext) -> Result<SourceFinishType, UserError> {
        ctx.initialize_deserializer(
            self.format.clone(),
            self.framing.clone(),
            self.bad_data.clone(),
        );

        let mut client = eventsource_client::ClientBuilder::for_url(&self.url).unwrap();

        if let Some(id) = &self.state.last_id {
            client = client.last_event_id(id.clone());
        }

        for (k, v) in &self.headers {
            client = client.header(k, v).unwrap();
        }

        let mut stream = client.build().stream();
        let events: HashSet<_> = self.events.iter().cloned().collect();

        let mut flush_ticker = tokio::time::interval(Duration::from_millis(50));
        flush_ticker.set_missed_tick_behavior(MissedTickBehavior::Delay);

        let mut last_eof = Instant::now();

        // since there's no way to partition across an event source, only read on the first task
        if ctx.task_info.task_index == 0 {
            loop {
                select! {
                    message = stream.next()  => {
                        match message {
                            Some(Ok(msg)) => {
                                match msg {
                                    SSE::Event(event) => {
                                        if let Some(id) = event.id {
                                            self.state.last_id = Some(id);
                                        }

                                        if events.is_empty() || events.contains(&event.event_type) {
                                            ctx.deserialize_slice(
                                                event.data.as_bytes(), SystemTime::now()).await?;

                                            if ctx.should_flush() {
                                                ctx.flush_buffer().await?;
                                            }
                                        }
                                    }
                                    SSE::Comment(s) => {
                                        debug!("Received comment {:?}", s);
                                    }
                                }
                            }
                            Some(Err(Error::Eof)) => {
                                // Many SSE servers will periodically send an EOF; just reconnect
                                // and continue on unless we immediately get another
                                if last_eof.elapsed() < Duration::from_secs(5) {
                                    ctx.report_user_error(UserError::new("Error while reading from EventSource",
                                    "Received repeated EOF from EventSource server")).await;
                                    panic!("Error while reading from EventSource: EOF");
                                }
                                last_eof = Instant::now();
                            }
                            Some(Err(e)) => {
                                ctx.control_tx.send(
                                    ControlResp::Error {
                                        operator_id: ctx.task_info.operator_id.clone(),
                                        task_index: ctx.task_info.task_index,
                                        message: "Error while reading from EventSource".to_string(),
                                        details: format!("{:?}", e)}
                                ).await.unwrap();
                                panic!("Error while reading from EventSource: {:?}", e);
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
                    _ = flush_ticker.tick() => {
                        if ctx.should_flush() {
                            ctx.flush_buffer().await?;
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
