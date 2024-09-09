use std::collections::HashMap;
use std::fmt::Debug;
use std::sync::Arc;

use arrow::array::builder::{TimestampNanosecondBuilder, UInt64Builder};
use arrow::array::RecordBatch;
use arroyo_rpc::grpc::rpc::{StopMode, TableConfig};
use arroyo_rpc::ControlMessage;
use async_trait::async_trait;
use bincode::{Decode, Encode};
use datafusion::common::ScalarValue;
use std::time::{Duration, SystemTime};

use arroyo_operator::context::ArrowContext;
use arroyo_operator::operator::SourceOperator;
use arroyo_operator::SourceFinishType;
use arroyo_types::{from_millis, print_time, to_millis, to_nanos};
use tracing::{debug, info};

#[derive(Encode, Decode, Debug, Copy, Clone, Eq, PartialEq)]
pub struct ImpulseSourceState {
    pub counter: usize,
    pub start_time: SystemTime,
}

#[derive(Debug, Clone, Copy)]
pub enum ImpulseSpec {
    #[allow(unused)]
    Delay(Duration),
    EventsPerSecond(f32),
}

#[derive(Debug)]
pub struct ImpulseSourceFunc {
    pub interval: Option<Duration>,
    pub spec: ImpulseSpec,
    pub limit: usize,
    pub state: ImpulseSourceState,
}

impl ImpulseSourceFunc {
    /* This method is mainly useful for testing,
      so you can always ensure it starts at the beginning of an interval.
    */
    #[allow(unused)]
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

    fn batch_size(&self, ctx: &mut ArrowContext) -> usize {
        let duration_micros = self.delay(ctx).as_micros();
        if duration_micros == 0 {
            return 8192;
        }
        let batch_size = Duration::from_millis(100).as_micros() / duration_micros;
        batch_size.clamp(1, 8192) as usize
    }

    fn delay(&self, ctx: &mut ArrowContext) -> Duration {
        match self.spec {
            ImpulseSpec::Delay(d) => d,
            ImpulseSpec::EventsPerSecond(eps) => {
                Duration::from_secs_f32(1.0 / (eps / ctx.task_info.parallelism as f32))
            }
        }
    }

    async fn run(&mut self, ctx: &mut ArrowContext) -> SourceFinishType {
        let delay = self.delay(ctx);
        info!(
            "Starting impulse source with start {} delay {:?} and limit {}",
            print_time(self.state.start_time),
            delay,
            self.limit
        );
        if let Some(state) = ctx
            .table_manager
            .get_global_keyed_state("i")
            .await
            .unwrap()
            .get(&ctx.task_info.task_index)
        {
            self.state = *state;
            info!(
                "state {:?} restored, start time {:?}",
                self.state, self.state.start_time
            );
        }

        let start_time = SystemTime::now() - delay * self.state.counter as u32;

        let schema = ctx.out_schema.as_ref().unwrap().schema.clone();

        let batch_size = self.batch_size(ctx);

        let mut items = 0;
        let mut counter_builder = UInt64Builder::with_capacity(batch_size);
        let task_index_scalar = ScalarValue::UInt64(Some(ctx.task_info.task_index as u64));
        let mut timestamp_builder = TimestampNanosecondBuilder::with_capacity(batch_size);

        while self.state.counter < self.limit {
            let timestamp = self
                .interval
                .map(|d| self.state.start_time + d * self.state.counter as u32)
                .unwrap_or_else(SystemTime::now);

            counter_builder.append_value(self.state.counter as u64);
            timestamp_builder.append_value(to_nanos(timestamp) as i64);
            items += 1;
            if items == batch_size {
                let counter_column = counter_builder.finish();
                let task_index_column = task_index_scalar.to_array_of_size(items).unwrap();
                let timestamp_column = timestamp_builder.finish();
                ctx.collect(
                    RecordBatch::try_new(
                        schema.clone(),
                        vec![
                            Arc::new(counter_column),
                            Arc::new(task_index_column),
                            Arc::new(timestamp_column),
                        ],
                    )
                    .unwrap(),
                )
                .await;
                items = 0;
            }

            self.state.counter += 1;

            match ctx.control_rx.try_recv() {
                Ok(ControlMessage::Checkpoint(c)) => {
                    // checkpoint our state
                    debug!("starting checkpointing {}", ctx.task_info.task_index);
                    if items > 0 {
                        let counter_column = counter_builder.finish();
                        let task_index_column = task_index_scalar.to_array_of_size(items).unwrap();
                        let timestamp_column = timestamp_builder.finish();
                        ctx.collect(
                            RecordBatch::try_new(
                                schema.clone(),
                                vec![
                                    Arc::new(counter_column),
                                    Arc::new(task_index_column),
                                    Arc::new(timestamp_column),
                                ],
                            )
                            .unwrap(),
                        )
                        .await;
                        items = 0;
                    }
                    ctx.table_manager
                        .get_global_keyed_state("i")
                        .await
                        .unwrap()
                        .insert(ctx.task_info.task_index, self.state)
                        .await;
                    if self.start_checkpoint(c, ctx).await {
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
        if items > 0 {
            let counter_column = counter_builder.finish();
            let task_index_column = task_index_scalar.to_array_of_size(items).unwrap();
            let timestamp_column = timestamp_builder.finish();
            ctx.collect(
                RecordBatch::try_new(
                    schema.clone(),
                    vec![
                        Arc::new(counter_column),
                        Arc::new(task_index_column),
                        Arc::new(timestamp_column),
                    ],
                )
                .unwrap(),
            )
            .await;
        }

        SourceFinishType::Final
    }
}

#[async_trait]
impl SourceOperator for ImpulseSourceFunc {
    fn name(&self) -> String {
        "impulse-source".to_string()
    }

    fn tables(&self) -> HashMap<String, TableConfig> {
        arroyo_state::global_table_config("i", "impulse source state")
    }

    async fn on_start(&mut self, ctx: &mut ArrowContext) {
        let s = ctx
            .table_manager
            .get_global_keyed_state("i")
            .await
            .expect("should have table i in impulse source");

        if let Some(state) = s.get(&ctx.task_info.task_index) {
            self.state = *state;
        }
    }

    async fn run(&mut self, ctx: &mut ArrowContext) -> SourceFinishType {
        self.run(ctx).await
    }
}
