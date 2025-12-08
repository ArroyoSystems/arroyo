use crate::sse::SseTable;
use arroyo_operator::context::{SourceCollector, SourceContext};
use arroyo_operator::operator::{ConstructedOperator, SourceOperator};
use arroyo_operator::SourceFinishType;
use arroyo_rpc::connector_err;
use arroyo_rpc::errors::DataflowResult;
use arroyo_rpc::formats::{BadData, Format, Framing};
use arroyo_rpc::grpc::rpc::{StopMode, TableConfig};
use arroyo_rpc::{ControlMessage, OperatorConfig};
use arroyo_state::tables::global_keyed_map::GlobalKeyedView;
use arroyo_types::{string_to_map, SignalMessage, Watermark};
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
    pub fn new_operator(
        table: SseTable,
        config: OperatorConfig,
    ) -> anyhow::Result<ConstructedOperator> {
        let headers = table
            .headers
            .as_ref()
            .map(|s| s.sub_env_vars().expect("Failed to substitute env vars"));

        Ok(ConstructedOperator::from_source(Box::new(SSESourceFunc {
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

    async fn run(
        &mut self,
        ctx: &mut SourceContext,
        collector: &mut SourceCollector,
    ) -> DataflowResult<SourceFinishType> {
        let s: &mut GlobalKeyedView<(), SSESourceState> =
            ctx.table_manager.get_global_keyed_state("e").await?;

        if let Some(state) = s.get(&()) {
            self.state = state.clone();
        }

        self.run_int(ctx, collector).await
    }
}

impl SSESourceFunc {
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
                let s = ctx.table_manager.get_global_keyed_state("e").await?;
                s.insert((), self.state.clone()).await;

                if self.start_checkpoint(c, ctx, collector).await {
                    return Ok(Some(SourceFinishType::Immediate));
                }
            }
            ControlMessage::Stop { mode } => {
                info!("Stopping eventsource source: {:?}", mode);

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

        let mut client = eventsource_client::ClientBuilder::for_url(&self.url)
            .map_err(|e| connector_err!(User, NoRetry, "invalid SSE URL '{}': {}", self.url, e))?;

        if let Some(id) = &self.state.last_id {
            client = client.last_event_id(id.clone());
        }

        for (k, v) in &self.headers {
            client = client
                .header(k, v)
                .map_err(|e| connector_err!(User, NoRetry, "invalid header '{}': {}", k, e))?;
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
                                            collector.deserialize_slice(
                                                event.data.as_bytes(), SystemTime::now(), None).await?;

                                            if collector.should_flush() {
                                                collector.flush_buffer().await?;
                                            }
                                        }
                                    }
                                    SSE::Comment(s) => {
                                        debug!("Received comment {:?}", s);
                                    }
                                    SSE::Connected(_) => {}
                                }
                            }
                            Some(Err(Error::Eof)) => {
                                // Many SSE servers will periodically send an EOF; just reconnect
                                // and continue on unless we immediately get another
                                if last_eof.elapsed() < Duration::from_secs(5) {
                                    return Err(connector_err!(External, WithBackoff, "received repeated EOF from SSE server '{}'", self.url));
                                }
                                last_eof = Instant::now();
                            }
                            Some(Err(e)) => {
                                return Err(connector_err!(External, WithBackoff, "error reading from SSE server '{}': {:?}", self.url, e));
                            }
                            None => {
                                info!("Socket closed");
                                return Ok(SourceFinishType::Final);
                            }
                        }
                    }
                    control_message = ctx.control_rx.recv() => {
                        if let Some(r) = self.our_handle_control_message(ctx, collector, control_message).await? {
                            return Ok(r);
                        }
                    }
                    _ = flush_ticker.tick() => {
                        if collector.should_flush() {
                            collector.flush_buffer().await?;
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
