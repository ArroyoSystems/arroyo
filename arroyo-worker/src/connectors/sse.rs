use crate::old::Context;
use crate::{RateLimiter, SourceFinishType};
use arroyo_formats::{DataDeserializer, SchemaData};
use arroyo_macro::{source_fn, StreamNode};
use arroyo_rpc::formats::{BadData, Format, Framing};
use arroyo_rpc::grpc::{StopMode, TableDescriptor};
use arroyo_rpc::{var_str::VarStr, ControlMessage, ControlResp, OperatorConfig};
use arroyo_state::tables::global_keyed_map::GlobalKeyedState;
use arroyo_types::{string_to_map, Data, Message, UserError, Watermark};
use bincode::{Decode, Encode};
use eventsource_client::{Client, SSE};
use futures::StreamExt;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use std::marker::PhantomData;
use std::time::SystemTime;
use tokio::select;
use tracing::{debug, info};
use typify::import_types;

import_types!(
    schema = "../connector-schemas/sse/table.json",
    convert = { {type = "string", format = "var-str"} = VarStr });

#[derive(Clone, Debug, Encode, Decode, PartialEq, PartialOrd, Default)]
pub struct SSESourceState {
    last_id: Option<String>,
}

#[derive(StreamNode)]
pub struct SSESourceFunc<K, T>
where
    K: DeserializeOwned + Data,
    T: SchemaData,
{
    url: String,
    headers: Vec<(String, String)>,
    events: Vec<String>,
    deserializer: DataDeserializer<T>,
    bad_data: Option<BadData>,
    rate_limiter: RateLimiter,
    state: SSESourceState,
    _t: PhantomData<K>,
}

#[source_fn(out_k = (), out_t = T)]
impl<K, T> SSESourceFunc<K, T>
where
    K: DeserializeOwned + Data,
    T: SchemaData,
{
    pub fn new(
        url: &str,
        headers: Vec<(&str, &str)>,
        events: Vec<&str>,
        format: Format,
        bad_data: Option<BadData>,
        framing: Option<Framing>,
    ) -> Self {
        SSESourceFunc {
            url: url.to_string(),
            headers: headers
                .into_iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect(),
            events: events.into_iter().map(|s| s.to_string()).collect(),
            deserializer: DataDeserializer::new(format, framing),
            bad_data,
            rate_limiter: RateLimiter::new(),
            state: SSESourceState::default(),
            _t: PhantomData,
        }
    }

    pub fn from_config(config: &str) -> Self {
        let config: OperatorConfig =
            serde_json::from_str(config).expect("Invalid config for SSESource");
        let table: SseTable =
            serde_json::from_value(config.table).expect("Invalid table config for SSESource");

        let headers = table
            .headers
            .as_ref()
            .map(|s| s.sub_env_vars().expect("Failed to substitute env vars"));

        Self {
            url: table.endpoint,
            headers: string_to_map(&headers.unwrap_or("".to_string()))
                .expect("Invalid header map")
                .into_iter()
                .collect(),
            events: table
                .events
                .map(|e| e.split(',').map(|e| e.to_string()).collect())
                .unwrap_or_else(std::vec::Vec::new),
            deserializer: DataDeserializer::new(
                config.format.expect("SSESource requires a format"),
                config.framing,
            ),
            bad_data: config.bad_data,
            rate_limiter: RateLimiter::new(),
            state: SSESourceState::default(),
            _t: PhantomData,
        }
    }

    fn name(&self) -> String {
        "SSESource".to_string()
    }

    fn tables(&self) -> Vec<TableDescriptor> {
        vec![arroyo_state::global_table("e", "sse source state")]
    }

    async fn on_start(&mut self, ctx: &mut Context<(), T>) {
        let s: GlobalKeyedState<(), SSESourceState, _> =
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
                let mut s: GlobalKeyedState<(), SSESourceState, _> =
                    ctx.state.get_global_keyed_state('e').await;
                s.insert((), self.state.clone()).await;

                if self.checkpoint(c, ctx).await {
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

    async fn run(&mut self, ctx: &mut Context<(), T>) -> SourceFinishType {
        match self.run_int(ctx).await {
            Ok(r) => r,
            Err(e) => {
                ctx.report_error(e.name.clone(), e.details.clone()).await;

                panic!("{}: {}", e.name, e.details);
            }
        }
    }

    async fn run_int(&mut self, ctx: &mut Context<(), T>) -> Result<SourceFinishType, UserError> {
        let mut client = eventsource_client::ClientBuilder::for_url(&self.url).unwrap();

        if let Some(id) = &self.state.last_id {
            client = client.last_event_id(id.clone());
        }

        for (k, v) in &self.headers {
            client = client.header(k, v).unwrap();
        }

        let mut stream = client.build().stream();
        let events: HashSet<_> = self.events.iter().cloned().collect();

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
                                            let iter = self.deserializer.deserialize_slice(&event.data.as_bytes()).await;

                                            for v in iter {
                                                ctx.collect_source_record(SystemTime::now(), v, &self.bad_data, &mut self.rate_limiter).await?;
                                            }
                                        }
                                    }
                                    SSE::Comment(s) => {
                                        debug!("Received comment {:?}", s);
                                    }
                                }
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
                }
            }
        } else {
            // otherwise set idle and just process control messages
            ctx.broadcast(Message::Watermark(Watermark::Idle)).await;

            loop {
                let msg = ctx.control_rx.recv().await;
                if let Some(r) = self.our_handle_control_message(ctx, msg).await {
                    return Ok(r);
                }
            }
        }
    }
}
