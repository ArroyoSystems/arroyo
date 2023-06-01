use crate::engine::Context;
use crate::SourceFinishType;
use arroyo_macro::{source_fn, StreamNode};
use arroyo_rpc::grpc::{StopMode, TableDescriptor};
use arroyo_rpc::ControlMessage;
use arroyo_state::tables::GlobalKeyedState;
use arroyo_types::{Data, Record};
use bincode::{Decode, Encode};
use eventsource_client::{Client, SSE};
use futures::StreamExt;
use serde::de::DeserializeOwned;
use std::collections::HashSet;
use std::marker::PhantomData;
use std::time::SystemTime;
use tokio::select;
use tracing::{debug, info, warn};

use crate::operators::SerializationMode;

#[derive(Clone, Debug, Encode, Decode, PartialEq, PartialOrd, Default)]
pub struct EventSourceState {
    last_id: Option<String>,
}

#[derive(StreamNode, Clone)]
pub struct EventSourceSourceFunc<T>
where
    T: DeserializeOwned + Data,
{
    url: String,
    headers: Vec<(String, String)>,
    events: Vec<String>,
    serialization_mode: SerializationMode,
    state: EventSourceState,
    _t: PhantomData<T>,
}

#[source_fn(out_k = (), out_t = T)]
impl<T> EventSourceSourceFunc<T>
where
    T: DeserializeOwned + Data,
{
    pub fn new(
        url: &str,
        headers: Vec<(&str, &str)>,
        events: Vec<&str>,
        serialization_mode: SerializationMode,
    ) -> EventSourceSourceFunc<T> {
        EventSourceSourceFunc {
            url: url.to_string(),
            headers: headers
                .into_iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect(),
            events: events.into_iter().map(|s| s.to_string()).collect(),
            serialization_mode,
            state: EventSourceState::default(),
            _t: PhantomData,
        }
    }

    fn name(&self) -> String {
        "EventSourceSource".to_string()
    }

    fn tables(&self) -> Vec<TableDescriptor> {
        vec![arroyo_state::global_table("e", "event source state")]
    }

    async fn on_start(&mut self, ctx: &mut Context<(), T>) {
        let s: GlobalKeyedState<(), EventSourceState, _> =
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
                let mut s: GlobalKeyedState<(), EventSourceState, _> =
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
        }
        None
    }

    async fn run(&mut self, ctx: &mut Context<(), T>) -> SourceFinishType {
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
                                            match self.serialization_mode.deserialize_str(&event.data) {
                                                Ok(value) => {
                                                    ctx.collector.collect(Record {
                                                        timestamp: SystemTime::now(),
                                                        key: None,
                                                        value,
                                                    }).await;
                                                }
                                                Err(e) => {
                                                    warn!("Invalid message from EventSource: {:}", e);
                                                }
                                            }

                                        }
                                    }
                                    SSE::Comment(s) => {
                                        debug!("Received comment {:?}", s);
                                    }
                                }
                            }
                            Some(Err(e)) => {
                                panic!("Error while reading from EventSource: {:?}", e);
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
            // otherwise just process control messages
            loop {
                let msg = ctx.control_rx.recv().await;
                if let Some(r) = self.our_handle_control_message(ctx, msg).await {
                    return r;
                }
            }
        }
    }
}
