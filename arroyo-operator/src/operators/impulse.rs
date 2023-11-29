use crate::operator::{ArrowContext, ArrowOperator, ArrowOperatorConstructor};
use crate::SourceFinishType;
use arrow::datatypes::DataType::Time64;
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use arrow_array::{
    ArrayRef, RecordBatch, Time64NanosecondArray, TimestampNanosecondArray, UInt64Array,
};
use arroyo_rpc::grpc::{StopMode, TableDescriptor};
use arroyo_rpc::{ControlMessage, OperatorConfig};
use arroyo_types::{to_nanos, ArrowRecord};
use prost::Message;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::sync::Arc;
use std::time::{Duration, SystemTime};
use tracing::{debug, info};
use typify::import_types;

import_types!(schema = "../connector-schemas/impulse/table.json");

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub struct ImpulseSourceState {
    counter: usize,
    start_time: SystemTime,
}

#[derive(Debug, Clone, Copy)]
pub enum ImpulseSpec {
    Delay(Duration),
    EventsPerSecond(f32),
}

pub struct ImpulseSource {
    interval: Option<Duration>,
    spec: ImpulseSpec,
    limit: usize,
    state: ImpulseSourceState,
}

impl ImpulseSource {
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

        // let schema = Arc::new(Schema::new(vec![
        //     Field::new("time", DataType::Time64(TimeUnit::Nanosecond), false),
        //     Field::new("counter", DataType::UInt64, false),
        //     Field::new("subtask_index", DataType::UInt64, false),
        // ]));

        while self.state.counter < self.limit {
            let timestamp = self
                .interval
                .map(|d| self.state.start_time + d * self.state.counter as u32)
                .unwrap_or_else(SystemTime::now);

            let columns: Vec<ArrayRef> = vec![
                Arc::new(TimestampNanosecondArray::from(vec![
                    to_nanos(timestamp) as i64
                ])),
                Arc::new(UInt64Array::from(vec![self.state.counter as u64])),
                Arc::new(UInt64Array::from(vec![ctx.task_info.task_index as u64])),
            ];

            let record = ArrowRecord { columns, count: 1 };

            ctx.collect(record).await;

            self.state.counter += 1;

            match ctx.control_rx.try_recv() {
                Ok(ControlMessage::Checkpoint(c)) => {
                    // checkpoint our state
                    // debug!("starting checkpointing {}", ctx.task_info.task_index);
                    // ctx.state
                    //     .get_global_keyed_state('i')
                    //     .await
                    //     .insert(ctx.task_info.task_index, self.state)
                    //     .await;
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
                Ok(ControlMessage::Commit { epoch: _ }) => {
                    unreachable!("sources shouldn't receive commit messages");
                }
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

impl ArrowOperatorConstructor for ImpulseSource {
    fn from_config(config: Vec<u8>) -> Box<dyn ArrowOperator> {
        let mut buf = config.as_slice();
        let op = arroyo_rpc::grpc::api::ConnectorOp::decode(&mut buf).unwrap();

        let config: OperatorConfig =
            serde_json::from_str(&op.config).expect("Invalid config for ImpulseSource");
        let table: ImpulseTable =
            serde_json::from_value(config.table).expect("Invalid table config for ImpulseSource");

        Box::new(ImpulseSource {
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

#[async_trait::async_trait]
impl ArrowOperator for ImpulseSource {
    fn name(&self) -> String {
        "impulse-source".to_string()
    }

    fn tables(&self) -> Vec<TableDescriptor> {
        vec![arroyo_state::global_table("i", "impulse source state")]
    }

    async fn on_start(&mut self, ctx: &mut ArrowContext) {
        // let s = ctx
        //     .state
        //     .get_global_keyed_state::<usize, ImpulseSourceState>('i')
        //     .await;
        //
        // if let Some(state) = s.get(&ctx.task_info.task_index) {
        //     self.state = *state;
        // }

        self.run(ctx).await;
    }

    async fn process_batch(&mut self, _: ArrowRecord, _: &mut ArrowContext) {
        unreachable!("process batch should not be called on source");
    }
}
