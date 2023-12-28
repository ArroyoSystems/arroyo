use std::fmt::Debug;
use std::marker::PhantomData;
use std::sync::Arc;

use crate::engine::{ArrowContext, StreamNode};
use crate::operator::{ArrowOperator, ArrowOperatorConstructor, BaseOperator};
use crate::SourceFinishType;
use arrow_array::{ArrayRef, RecordBatch, TimestampNanosecondArray, UInt64Array};
use arroyo_rpc::grpc::{api, StopMode, TableDescriptor};
use arroyo_rpc::{ControlMessage, OperatorConfig};
use arroyo_types::*;
use async_trait::async_trait;
use bincode::{Decode, Encode};
use serde::{Deserialize, Serialize};
use std::time::{Duration, SystemTime};
use tokio::sync::mpsc::Receiver;
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

#[derive(Debug)]
pub struct ImpulseSourceFunc {
    interval: Option<Duration>,
    spec: ImpulseSpec,
    limit: usize,
    state: ImpulseSourceState,
}

impl ImpulseSourceFunc {
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
        }
    }

    async fn run(&mut self, ctx: &mut ArrowContext) -> SourceFinishType {
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

        let schema = ctx.out_schema.as_ref().unwrap().schema.clone();

        while self.state.counter < self.limit {
            let timestamp = self
                .interval
                .map(|d| self.state.start_time + d * self.state.counter as u32)
                .unwrap_or_else(SystemTime::now);

            let columns: Vec<ArrayRef> = vec![
                Arc::new(UInt64Array::from(vec![self.state.counter as u64])),
                Arc::new(UInt64Array::from(vec![ctx.task_info.task_index as u64])),
                Arc::new(TimestampNanosecondArray::from(vec![
                    to_nanos(timestamp) as i64
                ])),
            ];

            ctx.collect(RecordBatch::try_new(schema.clone(), columns).unwrap())
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

impl ArrowOperatorConstructor<api::ConnectorOp, Self> for ImpulseSourceFunc {
    fn from_config(config: api::ConnectorOp) -> anyhow::Result<Self> {
        let config: OperatorConfig =
            serde_json::from_str(&config.config).expect("Invalid config for ImpulseSource");
        let table: ImpulseTable =
            serde_json::from_value(config.table).expect("Invalid table config for ImpulseSource");

        Ok(ImpulseSourceFunc {
            interval: table
                .event_time_interval
                .map(|i| Duration::from_micros(i as u64)),
            spec: ImpulseSpec::EventsPerSecond(table.event_rate as f32),
            limit: table
                .message_count
                .map(|n| n as usize)
                .unwrap_or(usize::MAX),
            state: ImpulseSourceState {
                counter: 0,
                start_time: SystemTime::now(),
            },
        })
    }
}

#[async_trait]
impl BaseOperator for ImpulseSourceFunc {
    fn name(&self) -> String {
        "impulse-source".to_string()
    }

    fn tables(&self) -> Vec<TableDescriptor> {
        vec![arroyo_state::global_table("i", "impulse source state")]
    }

    async fn run_behavior(
        mut self: Box<Self>,
        ctx: &mut ArrowContext,
        _: Vec<Receiver<ArrowMessage>>,
    ) -> Option<ArrowMessage> {
        let s = ctx
            .state
            .get_global_keyed_state::<usize, ImpulseSourceState>('i')
            .await;

        if let Some(state) = s.get(&ctx.task_info.task_index) {
            self.state = *state;
        }

        self.run(ctx).await.into()
    }
}
