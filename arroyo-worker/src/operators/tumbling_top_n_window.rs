use std::{
    cmp::Ordering,
    collections::{BTreeMap, BinaryHeap, HashMap},
    time::SystemTime,
};

use crate::engine::{Context, StreamNode};
use arroyo_macro::process_fn;
use arroyo_rpc::grpc::{TableDeleteBehavior, TableDescriptor, TableType, TableWriteBehavior};
use arroyo_state::tables::TimeKeyMap;

use arroyo_types::*;
use std::time::Duration;
use tracing::debug;
#[derive(StreamNode)]
pub struct TumblingTopNWindowFunc<K: Key, T: Data, SK: Ord + Send + 'static, OutT: Data> {
    width: Duration,
    max_elements: usize,
    extractor: fn(&T) -> SK,
    converter: fn(T, usize) -> OutT,
    buffering_max_heaps: BTreeMap<SystemTime, HashMap<K, BinaryHeap<PartitioningElement<SK, T>>>>,
    state: TumblingWindowState,
}
pub struct PartitioningElement<SK: Ord, T>(SK, SystemTime, T);

impl<SK: Ord, T> Ord for PartitioningElement<SK, T> {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.0.cmp(&other.0)
    }
}
impl<SK: Ord, T> PartialOrd for PartitioningElement<SK, T> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl<SK: Ord, T> Eq for PartitioningElement<SK, T> {}

impl<SK: Ord, T> PartialEq for PartitioningElement<SK, T> {
    fn eq(&self, other: &Self) -> bool {
        self.0 == other.0
    }
}

#[derive(Debug)]
enum TumblingWindowState {
    // We haven't received any data.
    NoData,
    // We've received data, but don't have any data in the memory_view.
    BufferedData { earliest_bin_time: SystemTime },
}

#[process_fn(in_k = K, in_t = T, out_k = K, out_t = OutT)]
impl<K: Key, T: Data, SK: Ord + Send + 'static, OutT: Data> TumblingTopNWindowFunc<K, T, SK, OutT> {
    fn name(&self) -> String {
        "TumblingTopNWindow".to_string()
    }

    pub fn new(
        width: Duration,
        max_elements: usize,
        extractor: fn(&T) -> SK,
        converter: fn(T, usize) -> OutT,
    ) -> Self {
        Self {
            width,
            max_elements,
            extractor,
            converter,
            buffering_max_heaps: BTreeMap::new(),
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
            name: "w".to_string(),
            description: "window state".to_string(),
            table_type: TableType::TimeKeyMap as i32,
            delete_behavior: TableDeleteBehavior::NoReadsBeforeWatermark as i32,
            write_behavior: TableWriteBehavior::NoWritesBeforeWatermark as i32,
            retention_micros: self.width.as_micros() as u64,
        }]
    }

    async fn process_element(&mut self, record: &Record<K, T>, ctx: &mut Context<K, OutT>) {
        self.insert(
            record.key.clone().unwrap(),
            record.timestamp,
            record.value.clone(),
            ctx.watermark(),
        )
    }

    fn insert(&mut self, key: K, timestamp: SystemTime, value: T, watermark: Option<SystemTime>) {
        let bin_start = self.bin_start(timestamp);

        if let Some(watermark) = watermark {
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
        let heap = self
            .buffering_max_heaps
            .entry(bin_start)
            .or_default()
            .entry(key)
            .or_insert_with(|| {
                let heap: BinaryHeap<PartitioningElement<SK, T>> = BinaryHeap::new();
                heap
            });

        let entry = PartitioningElement::<SK, T>((self.extractor)(&value), timestamp, value);
        heap.push(entry);

        if heap.len() > self.max_elements {
            heap.pop();
        }
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

    async fn advance(&mut self, ctx: &mut Context<K, OutT>) {
        debug!("advancing with state {:?}", self.state);
        let bin_start = match self.state {
            TumblingWindowState::BufferedData { earliest_bin_time } => {
                self.bin_start(earliest_bin_time)
            }
            TumblingWindowState::NoData => unreachable!(),
        };
        let Some(results) = self.buffering_max_heaps.remove(&bin_start) else {
            unreachable!();
        };
        self.state = match self.buffering_max_heaps.first_key_value() {
            Some((k, _v)) => TumblingWindowState::BufferedData {
                earliest_bin_time: *k,
            },
            None => TumblingWindowState::NoData,
        };

        let mut records = vec![];

        for (key, mut values) in results {
            let mut rank = values.len();
            while let Some(entry) = values.pop() {
                let timestamp = entry.1;
                let value = entry.2;
                records.push(Record {
                    timestamp,
                    key: Some(key.clone()),
                    value: (self.converter)(value, rank),
                });
                rank -= 1;
            }
        }

        for record in records {
            debug!("emitting {:?}", record);
            ctx.collect(record).await;
        }
    }
    async fn on_start(&mut self, ctx: &mut Context<K, OutT>) {
        let watermark = ctx.watermark();
        let mut state: TimeKeyMap<(K, u64), (SystemTime, T), _> =
            ctx.state.get_time_key_map('w', ctx.watermark()).await;
        for (_bin_time, (key, _rank), (timestamp, value)) in state.get_all().await {
            self.insert(key.clone(), *timestamp, value.clone(), watermark);
        }
    }

    async fn handle_watermark(
        &mut self,
        _watermark: std::time::SystemTime,
        ctx: &mut Context<K, OutT>,
    ) {
        let Some(watermark) = ctx.watermark() else {return};
        debug!(
            "watermark {:?}
        state {:?}",
            watermark, self.state
        );
        while self.should_advance(watermark) {
            self.advance(ctx).await;
        }
        ctx.broadcast(arroyo_types::Message::Watermark(Watermark::EventTime(
            watermark,
        )))
        .await;
    }

    async fn handle_checkpoint(
        &mut self,
        _checkpoint_barrier: &arroyo_types::CheckpointBarrier,
        ctx: &mut Context<K, OutT>,
    ) {
        let mut state = ctx.state.get_time_key_map('w', ctx.watermark()).await;
        for (bucket_timestamp, values) in &self.buffering_max_heaps {
            for (key, heap) in values {
                let values: Vec<_> = heap
                    .iter()
                    .map(|entry| (entry.1, entry.2.clone()))
                    .collect();
                let mut rank: u64 = 0;
                for (timestamp, value) in values {
                    state.insert(*bucket_timestamp, (key.clone(), rank), (timestamp, value));
                    rank += 1;
                }
            }
        }
        state.flush().await;
    }
}
