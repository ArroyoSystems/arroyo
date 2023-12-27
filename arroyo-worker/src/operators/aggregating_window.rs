use std::{
    collections::{BTreeMap, HashMap},
    ops::{Add, Sub},
    time::SystemTime,
};

use crate::engine::StreamNode;
use crate::old::Context;
use arroyo_macro::process_fn;
use arroyo_rpc::grpc::{TableDeleteBehavior, TableDescriptor, TableType, TableWriteBehavior};
use arroyo_state::tables::time_key_map::TimeKeyMap;
use arroyo_types::*;
use std::time::Duration;
use tracing::warn;

#[derive(StreamNode)]
pub struct AggregatingWindowFunc<K: Key, T: Data, BinA: Data, MemA: Data, OutT: Data> {
    width: Duration,
    slide: Duration,
    aggregator: fn(&K, Window, &MemA) -> OutT,
    bin_merger: fn(&T, Option<&BinA>) -> BinA,
    in_memory_add: fn(Option<MemA>, BinA) -> MemA,
    in_memory_remove: fn(MemA, BinA) -> Option<MemA>,
    memory_view: HashMap<K, MemA>,
    state: SlidingWindowState,
}
#[derive(Debug)]
enum SlidingWindowState {
    // We haven't received any data.
    NoData,
    // We've received data, but don't have any data in the memory_view.
    OnlyBufferedData { earliest_bin_time: SystemTime },
    // There is data in memory_view waiting to be emitted.
    // will trigger on a watermark after next_window_start + self.slide
    InMemoryData { next_window_start: SystemTime },
}

#[process_fn(in_k = K, in_t = T, out_k = K, out_t = OutT)]
impl<K: Key, T: Data, BinA: Data, MemA: Data, OutT: Data>
    AggregatingWindowFunc<K, T, BinA, MemA, OutT>
{
    fn name(&self) -> String {
        "KeyWindow".to_string()
    }

    pub fn new(
        width: Duration,
        slide: Duration,
        aggregator: fn(&K, Window, &MemA) -> OutT,
        bin_merger: fn(&T, Option<&BinA>) -> BinA,
        in_memory_add: fn(Option<MemA>, BinA) -> MemA,
        in_memory_remove: fn(MemA, BinA) -> Option<MemA>,
    ) -> Self {
        AggregatingWindowFunc {
            width,
            slide,
            aggregator,
            bin_merger,
            in_memory_add,
            in_memory_remove,
            memory_view: HashMap::new(),
            state: SlidingWindowState::NoData,
        }
    }

    fn bin_start(&self, timestamp: SystemTime) -> SystemTime {
        let mut nanos = to_nanos(timestamp);
        nanos -= nanos % self.slide.as_nanos();
        from_nanos(nanos)
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

        let watermark = ctx.last_present_watermark();
        if watermark.is_some() && bin_start < self.bin_start(watermark.unwrap()) {
            return;
        }
        self.state = match self.state {
            SlidingWindowState::NoData => SlidingWindowState::OnlyBufferedData {
                earliest_bin_time: bin_start,
            },
            SlidingWindowState::OnlyBufferedData { earliest_bin_time } => {
                SlidingWindowState::OnlyBufferedData {
                    earliest_bin_time: earliest_bin_time.min(bin_start),
                }
            }
            SlidingWindowState::InMemoryData { next_window_start } => {
                SlidingWindowState::InMemoryData { next_window_start }
            }
        };
        let mut aggregating_map = ctx.state.get_time_key_map('a', watermark).await;
        let mut key = record.key.clone().unwrap();
        let bin_aggregate = aggregating_map.get(bin_start, &mut key);
        let new_value = (self.bin_merger)(&record.value, bin_aggregate);
        aggregating_map.insert(bin_start, key, new_value);
    }

    async fn on_start(&mut self, ctx: &mut Context<K, OutT>) {
        let watermark = ctx.last_present_watermark();
        let map = ctx.state.get_time_key_map::<K, BinA>('a', watermark).await;

        let Some(map_min_time) = map.get_min_time() else {
            self.state = SlidingWindowState::NoData;
            return;
        };
        let map_min_bin = self.bin_start(map_min_time);
        let Some(watermark) = watermark else {
            self.state = SlidingWindowState::OnlyBufferedData {
                earliest_bin_time: map_min_bin,
            };
            return;
        };
        let watermark_bin = self.bin_start(watermark);
        if watermark_bin <= map_min_bin {
            self.state = SlidingWindowState::OnlyBufferedData {
                earliest_bin_time: map_min_bin,
            };
            return;
        }
        let mut bin = map_min_bin;
        while bin < watermark_bin {
            for (key, bin_value) in map.get_all_for_time(bin) {
                self.add_data(key, bin_value.clone());
            }
            bin += self.slide;
        }
        self.state = SlidingWindowState::InMemoryData {
            next_window_start: watermark_bin,
        };
    }

    fn add_data(&mut self, key: &K, bin_value: BinA) {
        // TODO: probably more efficient ways to do this,
        // in particular modifying entries in place.
        let current = self.memory_view.remove(key);
        self.memory_view
            .insert(key.clone(), (self.in_memory_add)(current, bin_value));
    }

    fn remove_data(&mut self, key: &K, bin_value: BinA) {
        let entry = self.memory_view.remove_entry(key);
        if entry.is_none() {
            warn!("no memory data for {:?}", key);
            return;
        }
        let (key, current) = entry.unwrap();
        if let Some(new_value) = (self.in_memory_remove)(current, bin_value) {
            self.memory_view.insert(key, new_value);
        }
    }

    fn should_advance(&self, watermark: SystemTime) -> bool {
        let watermark_bin = self.bin_start(watermark);
        match self.state {
            SlidingWindowState::NoData => false,
            SlidingWindowState::OnlyBufferedData { earliest_bin_time } => {
                earliest_bin_time + self.slide <= watermark_bin
            }
            SlidingWindowState::InMemoryData { next_window_start } => {
                next_window_start + self.slide <= watermark_bin
            }
        }
    }
    async fn advance(&mut self, ctx: &mut Context<K, OutT>) {
        let bin_start = match self.state {
            SlidingWindowState::NoData => unreachable!(),
            SlidingWindowState::OnlyBufferedData { earliest_bin_time } => earliest_bin_time,
            SlidingWindowState::InMemoryData { next_window_start } => next_window_start,
        };

        let bin_end = bin_start + self.slide;
        let mut aggregating_map: TimeKeyMap<K, BinA, _> = ctx
            .state
            .get_time_key_map('a', ctx.last_present_watermark())
            .await;

        // flush the new bin.
        aggregating_map.flush_at_watermark(bin_end).await;

        // add the next bin data to the in memory store.
        for (key, bin) in aggregating_map.get_all_for_time(bin_start) {
            self.add_data(key, bin.clone());
        }

        // remove the leaving bin data from memory
        for (key, bin) in aggregating_map.evict_for_timestamp(bin_start - self.width) {
            self.remove_data(&key, bin);
        }

        let window_end = bin_end - Duration::from_nanos(1);
        let mut records = vec![];
        for (key, in_memory) in self.memory_view.iter() {
            let value = (self.aggregator)(
                key,
                Window {
                    start: bin_start,
                    end: bin_end,
                },
                in_memory,
            );
            records.push(Record {
                timestamp: window_end,
                key: Some(key.clone()),
                value,
            });
        }
        self.state = if self.memory_view.is_empty() {
            match aggregating_map.get_min_time() {
                None => SlidingWindowState::NoData,
                Some(earliest_time) => SlidingWindowState::OnlyBufferedData {
                    earliest_bin_time: self.bin_start(earliest_time),
                },
            }
        } else {
            SlidingWindowState::InMemoryData {
                next_window_start: bin_end,
            }
        };
        for record in records {
            ctx.collect(record).await;
        }
    }

    async fn handle_watermark(&mut self, watermark: Watermark, ctx: &mut Context<K, OutT>) {
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

pub fn count_add(current: Option<(i64, i64)>, bin_value: i64) -> (i64, i64) {
    match current {
        Some((bins, count)) => (bins + 1, count + bin_value),
        None => (1, bin_value),
    }
}

pub fn count_remove(current: (i64, i64), bin_value: i64) -> Option<(i64, i64)> {
    if current.0 == 1 {
        None
    } else {
        Some((current.0 - 1, current.1 - bin_value))
    }
}

pub fn count_aggregate(memory: &(i64, i64)) -> i64 {
    memory.1
}

pub fn nullable_sum_add<T: Add<Output = T>>(
    current: Option<(i64, i64, Option<T>)>,
    bin_value: Option<T>,
) -> (i64, i64, Option<T>) {
    match (current, bin_value) {
        (Some((bins, present_bins, Some(mem_value))), Some(bin_value)) => {
            (bins + 1, present_bins + 1, Some(mem_value + bin_value))
        }
        (Some((bins, present_bins, None)), Some(bin_value)) => {
            (bins + 1, present_bins + 1, Some(bin_value))
        }
        (Some((bins, present_bins, mem_value)), None) => (bins + 1, present_bins, mem_value),
        (None, bin_value) => (1, 1, bin_value),
    }
}
pub fn nullable_sum_remove<T: Sub<Output = T>>(
    current: (i64, i64, Option<T>),
    bin_value: Option<T>,
) -> Option<(i64, i64, Option<T>)> {
    if current.0 == 1 {
        return None;
    }
    match bin_value {
        Some(bin_value) => {
            if current.1 == 1 {
                Some((current.0 - 1, 0, None))
            } else {
                Some((
                    current.0 - 1,
                    current.1 - 1,
                    current.2.map(|val| val - bin_value),
                ))
            }
        }
        None => Some((current.0 - 1, current.1, current.2)),
    }
}

pub fn nullable_sum_aggregate<T: Clone>(memory: &(i64, i64, Option<T>)) -> Option<T> {
    memory.2.clone()
}

pub fn non_nullable_sum_add<T: Add<Output = T>>(
    current: Option<(i64, T)>,
    bin_value: T,
) -> (i64, T) {
    match current {
        Some((bins, value)) => (bins + 1, value + bin_value),
        None => (1, bin_value),
    }
}

pub fn non_nullable_sum_remove<T: Sub<Output = T>>(
    current: (i64, T),
    bin_value: T,
) -> Option<(i64, T)> {
    if current.0 == 1 {
        None
    } else {
        Some((current.0 - 1, current.1 - bin_value))
    }
}

pub fn non_nullable_sum_aggregate<T: Clone>(memory: &(i64, T)) -> T {
    memory.1.clone()
}

pub fn nullable_heap_add<T: Ord>(
    current: Option<(i64, BTreeMap<T, usize>)>,
    bin_value: Option<T>,
) -> (i64, BTreeMap<T, usize>) {
    match (current, bin_value) {
        (None, None) => (1, BTreeMap::new()),
        (None, Some(bin_value)) => {
            let mut map = BTreeMap::new();
            map.insert(bin_value, 1);
            (1, map)
        }
        (Some((bin_count, map)), None) => (bin_count + 1, map),
        (Some((bin_count, mut map)), Some(bin_value)) => {
            map.entry(bin_value)
                .and_modify(|val| {
                    *val += 1;
                })
                .or_insert(1);
            (bin_count + 1, map)
        }
    }
}

pub fn nullable_heap_remove<T: Ord>(
    current: (i64, BTreeMap<T, usize>),
    bin_value: Option<T>,
) -> Option<(i64, BTreeMap<T, usize>)> {
    if current.0 == 1 {
        None
    } else {
        match bin_value {
            Some(bin_value) => {
                let mut map = current.1;
                let value_count = *map.get(&bin_value).unwrap();
                if value_count == 1 {
                    map.remove(&bin_value);
                } else {
                    map.insert(bin_value, value_count - 1);
                }
                Some((current.0 - 1, map))
            }
            None => Some((current.0 - 1, current.1)),
        }
    }
}

pub fn nullable_min_heap_aggregate<T: Ord + Clone>(
    memory: &(i64, BTreeMap<T, usize>),
) -> Option<T> {
    memory.1.first_key_value().map(|(key, _value)| key.clone())
}

pub fn nullable_max_heap_aggregate<T: Ord + Clone>(
    memory: &(i64, BTreeMap<T, usize>),
) -> Option<T> {
    memory.1.last_key_value().map(|(key, _value)| key.clone())
}

pub fn non_nullable_heap_add<T: Ord>(
    current: Option<BTreeMap<T, usize>>,
    bin_value: T,
) -> BTreeMap<T, usize> {
    match current {
        Some(mut map) => {
            let value = map.get(&bin_value).cloned().unwrap_or_default() + 1;
            map.insert(bin_value, value);
            map
        }
        None => {
            let mut map = BTreeMap::new();
            map.insert(bin_value, 1);
            map
        }
    }
}

pub fn non_nullable_heap_remove<T: Ord>(
    current: BTreeMap<T, usize>,
    bin_value: T,
) -> Option<BTreeMap<T, usize>> {
    let mut map = current;
    let value_count = *map.get(&bin_value).unwrap();
    if value_count == 1 {
        map.remove(&bin_value);
    } else {
        map.insert(bin_value, value_count - 1);
    }
    if map.is_empty() {
        None
    } else {
        Some(map)
    }
}

pub fn non_nullable_min_heap_aggregate<T: Ord + Clone>(memory: BTreeMap<T, usize>) -> T {
    memory
        .first_key_value()
        .map(|(key, _value)| key.clone())
        .unwrap()
}

pub fn non_nullable_max_heap_aggregate<T: Ord + Clone>(memory: &BTreeMap<T, usize>) -> T {
    memory
        .last_key_value()
        .map(|(key, _value)| key.clone())
        .unwrap()
}

pub fn nullable_average_add<T: Add<Output = T>>(
    current: Option<(i64, i64, Option<(i64, T)>)>,
    bin_value: Option<(i64, T)>,
) -> (i64, i64, Option<(i64, T)>) {
    let (bins, non_null_bins, stats) = current.unwrap_or((0, 0, None));

    match (stats, bin_value) {
        (None, None) => (bins + 1, non_null_bins, None),
        (None, Some(new_value)) => (bins + 1, non_null_bins + 1, Some(new_value)),
        (Some(memory_value), None) => (bins + 1, non_null_bins, Some(memory_value)),
        (Some((memory_count, memory_sums)), Some((bin_count, bin_sums))) => (
            bins + 1,
            non_null_bins + 1,
            Some((memory_count + bin_count, memory_sums + bin_sums)),
        ),
    }
}

pub fn nullable_average_remove<T: Sub<Output = T>>(
    current: (i64, i64, Option<(i64, T)>),
    bin_value: Option<(i64, T)>,
) -> Option<(i64, i64, Option<(i64, T)>)> {
    let (bins, non_null_bins, stats) = current;
    if bins == 1 {
        return None;
    }

    if non_null_bins == 1 {
        return Some((bins - 1, 0, None));
    }
    match bin_value {
        Some((bin_count, bin_sum)) => {
            if non_null_bins == 1 {
                Some((bins - 1, 0, None))
            } else {
                let (memory_count, memory_sum) = stats.unwrap();
                Some((
                    bins - 1,
                    non_null_bins - 1,
                    Some((memory_count - bin_count, memory_sum - bin_sum)),
                ))
            }
        }
        None => Some((bins - 1, non_null_bins, stats)),
    }
}

pub fn non_nullable_average_add<T: Add<Output = T>>(
    current: Option<(i64, T)>,
    bin_value: (i64, T),
) -> (i64, T) {
    let (bin_count, bin_sum) = bin_value;
    match current {
        Some((current_count, current_sum)) => (current_count + bin_count, current_sum + bin_sum),
        None => (bin_count, bin_sum),
    }
}

pub fn non_nullable_average_remove<T: Sub<Output = T>>(
    current: (i64, T),
    bin_value: (i64, T),
) -> Option<(i64, T)> {
    let (current_count, current_sum) = current;
    let (bin_count, bin_sum) = bin_value;
    if current_count == bin_count {
        return None;
    }
    Some((current_count - bin_count, current_sum - bin_sum))
}
