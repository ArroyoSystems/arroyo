use std::fmt::Debug;
use std::marker::PhantomData;

use crate::engine::{Context, StreamNode};
use crate::SourceFinishType;
use arroyo_macro::source_fn;
use arroyo_rpc::grpc::{StopMode, TableDescriptor};
use arroyo_rpc::{ControlMessage, OperatorConfig};
use arroyo_types::*;
use bincode::{Decode, Encode};
use serde::{Deserialize, Serialize};
use std::time::{Duration, SystemTime};
use tracing::{debug, info};
use typify::import_types;

import_types!(schema = "../connector-schemas/impulse/table.json");

#[derive(Encode, Decode, Debug, Copy, Clone, Eq, PartialEq)]
pub struct ImpulseSourceState {
    counter: usize,
    start_time: SystemTime,
}

#[derive(Debug, Clone, Copy)]
pub enum ImpulseSpec {
    Delay(Duration),
    EventsPerSecond(f32),
}

#[derive(StreamNode, Debug)]
pub struct ImpulseSourceFunc<K: Data, T: Data> {
    interval: Option<Duration>,
    spec: ImpulseSpec,
    limit: usize,
    state: ImpulseSourceState,
    _t: PhantomData<(K, T)>,
}

#[source_fn(out_t = ImpulseEvent)]
impl<K: Data, T: Data> ImpulseSourceFunc<K, T> {
    /* This method is mainly useful for testing,
      so you can always ensure it starts at the beginning of an interval.
    */
    pub fn new_aligned(
        interval: Duration,
        spec: ImpulseSpec,
        alignment_duration: Duration,
        limit: usize,
        start_time: SystemTime,
    ) -> Self {
        let start_millis = to_millis(start_time);
        let truncated_start_millis =
            start_millis - (start_millis % alignment_duration.as_millis() as u64);
        let start_time = from_millis(truncated_start_millis);
        Self::new(Some(interval), spec, limit, start_time)
    }

    pub fn new(
        interval: Option<Duration>,
        spec: ImpulseSpec,
        limit: usize,
        start_time: SystemTime,
    ) -> Self {
        Self {
            interval,
            spec,
            limit,
            state: ImpulseSourceState {
                counter: 0,
                start_time,
            },
            _t: PhantomData,
        }
    }

    pub fn from_config(config: &str) -> Self {
        let config: OperatorConfig =
            serde_json::from_str(config).expect("Invalid config for ImpulseSource");
        let table: ImpulseTable =
            serde_json::from_value(config.table).expect("Invalid table config for ImpulseSource");

        Self::new(
            table
                .event_time_interval
                .map(|i| Duration::from_micros(i as u64)),
            ImpulseSpec::EventsPerSecond(table.event_rate as f32),
            table
                .message_count
                .map(|n| n as usize)
                .unwrap_or(usize::MAX),
            SystemTime::now(),
        )
    }

    fn name(&self) -> String {
        "impulse-source".to_string()
    }

    fn tables(&self) -> Vec<TableDescriptor> {
        vec![arroyo_state::global_table("i", "impulse source state")]
    }

    async fn on_start(&mut self, ctx: &mut Context<(), ImpulseEvent>) {
        let s = ctx
            .state
            .get_global_keyed_state::<usize, ImpulseSourceState>('i')
            .await;

        if let Some(state) = s.get(&ctx.task_info.task_index) {
            self.state = *state;
        }
    }

    async fn run(&mut self, ctx: &mut Context<(), ImpulseEvent>) -> SourceFinishType {
        let delay = match self.spec {
            ImpulseSpec::Delay(d) => d,
            ImpulseSpec::EventsPerSecond(eps) => {
                Duration::from_secs_f32(1.0 / (eps / ctx.task_info.parallelism as f32))
            }
        };
        info!(
            "Starting impulse source with delay {:?} and limit {}",
            delay, self.limit
        );

        let start_time = SystemTime::now() - delay * self.state.counter as u32;

        while self.state.counter < self.limit {
            let timestamp = self
                .interval
                .map(|d| self.state.start_time + d * self.state.counter as u32)
                .unwrap_or_else(SystemTime::now);
            ctx.collect(Record {
                timestamp,
                key: None,
                value: ImpulseEvent {
                    counter: self.state.counter as u64,
                    subtask_index: ctx.task_info.task_index as u64,
                },
            })
            .await;

            self.state.counter += 1;

            match ctx.control_rx.try_recv() {
                Ok(ControlMessage::Checkpoint(c)) => {
                    // checkpoint our state
                    debug!("starting checkpointing {}", ctx.task_info.task_index);
                    ctx.state
                        .get_global_keyed_state('i')
                        .await
                        .insert(ctx.task_info.task_index, self.state)
                        .await;
                    if self.checkpoint(c, ctx).await {
                        return SourceFinishType::Immediate;
                    }
                }
                Ok(ControlMessage::Stop { mode }) => {
                    info!("Stopping impulse source {:?}", mode);

                    match mode {
                        StopMode::Graceful => {
                            return SourceFinishType::Graceful;
                        }
                        StopMode::Immediate => {
                            return SourceFinishType::Immediate;
                        }
                    }
                }
                Ok(ControlMessage::Commit { .. }) => {
                    unreachable!("sources shouldn't receive commit messages");
                }
                Ok(ControlMessage::LoadCompacted { compacted }) => {
                    ctx.load_compacted(compacted).await;
                }
                Ok(ControlMessage::NoOp) => {}
                Err(_) => {
                    // no messages
                }
            }

            if !delay.is_zero() {
                let next_sleep = start_time + delay * self.state.counter as u32;
                if let Ok(sleep_time) = next_sleep.duration_since(SystemTime::now()) {
                    tokio::time::sleep(sleep_time).await;
                }
            }
        }

        SourceFinishType::Final
    }
}
