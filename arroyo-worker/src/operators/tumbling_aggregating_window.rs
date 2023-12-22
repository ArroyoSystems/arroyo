use std::time::SystemTime;

use crate::engine::StreamNode;
use arroyo_macro::process_fn;
use arroyo_rpc::grpc::{TableDeleteBehavior, TableDescriptor, TableType, TableWriteBehavior};
use arroyo_state::tables::time_key_map::TimeKeyMap;
use arroyo_types::*;
use std::time::Duration;
use tracing::debug;
use crate::old::Context;

#[derive(StreamNode)]
pub struct TumblingAggregatingWindowFunc<K: Key, T: Data, BinA: Data, OutT: Data> {
    width: Duration,
    aggregator: fn(&K, Window, &BinA) -> OutT,
    bin_merger: fn(&T, Option<&BinA>) -> BinA,
    state: TumblingWindowState,
}

#[derive(Debug)]
enum TumblingWindowState {
    // We haven't received any data.
    NoData,
    // We've received data, but don't have any data in the memory_view.
    BufferedData { earliest_bin_time: SystemTime },
}

#[process_fn(in_k = K, in_t = T, out_k = K, out_t = OutT)]
impl<K: Key, T: Data, BinA: Data, OutT: Data> TumblingAggregatingWindowFunc<K, T, BinA, OutT> {
    fn name(&self) -> String {
        "TumblingAggregatingWindow".to_string()
    }

    pub fn new(
        width: Duration,
        // TODO: this can consume the bin, as we drop it right after.
        aggregator: fn(&K, Window, &BinA) -> OutT,
        bin_merger: fn(&T, Option<&BinA>) -> BinA,
    ) -> Self {
        TumblingAggregatingWindowFunc {
            width,
            aggregator,
            bin_merger,
            state: TumblingWindowState::NoData,
        }
    }

    fn bin_start(&self, timestamp: SystemTime) -> SystemTime {
        if self.width == Duration::ZERO {
            return timestamp;
        }
        let mut nanos = to_nanos(timestamp);
        nanos -= nanos % self.width.as_nanos();
        let result = from_nanos(nanos);
        debug!("bin start for {:?} is {:?}", timestamp, result);
        result
    }

    fn tables(&self) -> Vec<TableDescriptor> {
        vec![TableDescriptor {
            name: "a".to_string(),
            description: "window state".to_string(),
            table_type: TableType::TimeKeyMap as i32,
            delete_behavior: TableDeleteBehavior::NoReadsBeforeWatermark as i32,
            write_behavior: TableWriteBehavior::NoWritesBeforeWatermark as i32,
            retention_micros: self.width.as_micros() as u64,
        }]
    }

    async fn process_element(&mut self, record: &Record<K, T>, ctx: &mut Context<K, OutT>) {
        let bin_start = self.bin_start(record.timestamp);

        if let Some(watermark) = ctx.last_present_watermark() {
            if bin_start < self.bin_start(watermark) {
                return;
            }
        }

        self.state = match self.state {
            TumblingWindowState::NoData => TumblingWindowState::BufferedData {
                earliest_bin_time: bin_start,
            },
            TumblingWindowState::BufferedData { earliest_bin_time } => {
                TumblingWindowState::BufferedData {
                    earliest_bin_time: earliest_bin_time.min(bin_start),
                }
            }
        };

        let mut aggregating_map: TimeKeyMap<K, BinA, _> = ctx
            .state
            .get_time_key_map('a', ctx.last_present_watermark())
            .await;

        let mut key = record.key.clone().unwrap();
        let bin_aggregate = aggregating_map.get(bin_start, &mut key);
        let new_value = (self.bin_merger)(&record.value, bin_aggregate);
        aggregating_map.insert(bin_start, key, new_value);
    }

    async fn on_start(&mut self, ctx: &mut Context<K, OutT>) {
        let map = ctx
            .state
            .get_time_key_map::<K, BinA>('a', ctx.last_present_watermark())
            .await;

        self.state = match map.get_min_time() {
            Some(min_time) => TumblingWindowState::BufferedData {
                earliest_bin_time: self.bin_start(min_time),
            },
            None => TumblingWindowState::NoData,
        };
    }
    fn should_advance(&self, watermark: SystemTime) -> bool {
        let watermark_bin = self.bin_start(watermark);
        match self.state {
            TumblingWindowState::BufferedData { earliest_bin_time } => {
                earliest_bin_time + self.width <= watermark_bin
            }
            TumblingWindowState::NoData => false,
        }
    }

    fn window_end(&self, bin_start: SystemTime) -> SystemTime {
        if self.width == Duration::ZERO {
            bin_start
        } else {
            bin_start + self.width - Duration::from_nanos(1)
        }
    }

    async fn advance(&mut self, ctx: &mut Context<K, OutT>) {
        debug!("advancing with state {:?}", self.state);
        let bin_start = match self.state {
            TumblingWindowState::BufferedData { earliest_bin_time } => {
                self.bin_start(earliest_bin_time)
            }
            TumblingWindowState::NoData => unreachable!(),
        };
        let mut aggregating_map: TimeKeyMap<K, BinA, _> = ctx
            .state
            .get_time_key_map('a', ctx.last_present_watermark())
            .await;

        let window_end = self.window_end(bin_start);
        let mut records = vec![];
        for (key, value) in aggregating_map.evict_for_timestamp(bin_start) {
            records.push(Record {
                timestamp: window_end,
                key: Some(key.clone()),
                value: (self.aggregator)(
                    &key,
                    Window::new(bin_start, bin_start + self.width),
                    &value,
                ),
            });
        }
        self.state = match aggregating_map.get_min_time() {
            Some(min_time) => TumblingWindowState::BufferedData {
                earliest_bin_time: self.bin_start(min_time),
            },
            None => TumblingWindowState::NoData,
        };

        for record in records {
            debug!("emitting {:?}", record);
            ctx.collect(record).await;
        }
    }

    async fn handle_watermark(&mut self, watermark: Watermark, ctx: &mut Context<K, OutT>) {
        debug!(
            "watermark {:?}
            state {:?}",
            watermark, self.state
        );

        match watermark {
            Watermark::EventTime(t) => {
                while self.should_advance(t) {
                    self.advance(ctx).await;
                }
            }
            Watermark::Idle => (),
        }

        ctx.broadcast(arroyo_types::Message::Watermark(watermark))
            .await;
    }

    async fn handle_checkpoint(
        &mut self,
        _checkpoint_barrier: &arroyo_types::CheckpointBarrier,
        ctx: &mut Context<K, OutT>,
    ) {
        let mut aggregating_map: TimeKeyMap<K, BinA, _> = ctx
            .state
            .get_time_key_map('a', ctx.last_present_watermark())
            .await;
        aggregating_map.flush().await;
    }
}
