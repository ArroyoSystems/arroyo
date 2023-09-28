use std::{
    collections::{BTreeMap, BTreeSet, HashMap},
    ops::{Add, Sub},
    time::SystemTime,
};

use crate::engine::{Context, StreamNode};
use arroyo_macro::process_fn;
use arroyo_rpc::grpc::{TableDeleteBehavior, TableDescriptor, TableType, TableWriteBehavior};
use arroyo_state::tables::time_key_map::TimeKeyMap;
use arroyo_types::*;
use std::time::Duration;
use tracing::warn;

#[derive(StreamNode)]
pub struct SlidingAggregatingTopNWindowFunc<
    K: Key + Ord,
    T: Data,
    BinA: Data,
    MemA: Data,
    PK: Key,
    SK: Ord + Send + Clone + 'static,
    OutT: Data,
> {
    width: Duration,
    slide: Duration,
    bin_merger: fn(&T, Option<&BinA>) -> BinA,
    in_memory_add: fn(Option<MemA>, BinA) -> MemA,
    in_memory_remove: fn(MemA, BinA) -> Option<MemA>,
    partitioning_func: fn(&K) -> PK,
    extractor: fn(&K, &MemA) -> SK,
    aggregator: fn(&K, Window, &MemA) -> OutT,
    max_elements: usize,
    partition_heaps: HashMap<PK, PartitionSortedStore<K, SK, MemA>>,
    state: SlidingWindowState,
}

struct PartitionSortedStore<K: Ord, SK: Ord + Clone, MemA> {
    memory_view: HashMap<K, (SK, MemA)>,
    elements: BTreeSet<(SK, K)>,
}

impl<K: Ord, SK: Ord + Clone, MemA> Default for PartitionSortedStore<K, SK, MemA> {
    fn default() -> Self {
        Self {
            memory_view: Default::default(),
            elements: Default::default(),
        }
    }
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

#[process_fn(in_k = K, in_t = T, out_k = PK, out_t = OutT)]
impl<
        K: Key + Ord,
        T: Data,
        BinA: Data,
        MemA: Data,
        PK: Key,
        SK: Ord + Send + Clone + 'static,
        OutT: Data,
    > SlidingAggregatingTopNWindowFunc<K, T, BinA, MemA, PK, SK, OutT>
{
    fn name(&self) -> String {
        "SlidingTopNAggregatingWindow".to_string()
    }

    pub fn new(
        width: Duration,
        slide: Duration,
        bin_merger: fn(&T, Option<&BinA>) -> BinA,
        in_memory_add: fn(Option<MemA>, BinA) -> MemA,
        in_memory_remove: fn(MemA, BinA) -> Option<MemA>,
        partitioning_func: fn(&K) -> PK,
        extractor: fn(&K, &MemA) -> SK,
        aggregator: fn(&K, Window, &MemA) -> OutT,
        max_elements: usize,
    ) -> Self {
        Self {
            width,
            slide,
            bin_merger,
            in_memory_add,
            in_memory_remove,
            partitioning_func,
            extractor,
            aggregator,
            max_elements,
            partition_heaps: HashMap::new(),
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

    async fn process_element(&mut self, record: &Record<K, T>, ctx: &mut Context<PK, OutT>) {
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
        let mut aggregating_map: TimeKeyMap<K, BinA, _> = ctx
            .state
            .get_time_key_map('a', ctx.last_present_watermark())
            .await;

        let mut key = record.key.clone().unwrap();
        let bin_aggregate = aggregating_map.get(bin_start, &mut key);
        let new_value = (self.bin_merger)(&record.value, bin_aggregate);
        aggregating_map.insert(bin_start, key, new_value);
    }

    async fn on_start(&mut self, ctx: &mut Context<PK, OutT>) {
        let watermark = ctx.last_present_watermark();
        let map: TimeKeyMap<K, BinA, _> = ctx.state.get_time_key_map('a', watermark).await;

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
        let partition_key = (self.partitioning_func)(key);
        let partition_set = self.partition_heaps.entry(partition_key).or_default();
        let current = partition_set.memory_view.remove_entry(key);
        match current {
            Some((old_key, (current_sort_key, current_mem))) => {
                let new_mem = (self.in_memory_add)(Some(current_mem), bin_value);
                let new_sort_key = (self.extractor)(key, &new_mem);
                // borrow checker was being weird when I just had &(current_sort_key, old_key) as the argument to take.
                let pair = (current_sort_key, old_key);
                if pair.0 != new_sort_key {
                    let (_, popped_key) = partition_set
                        .elements
                        .take(&pair)
                        .expect("error for the element to be missing");
                    partition_set
                        .elements
                        .insert((new_sort_key.clone(), popped_key));
                }
                partition_set
                    .memory_view
                    .insert(pair.1, (new_sort_key, new_mem));
            }
            None => {
                let new_mem = (self.in_memory_add)(None, bin_value);
                let sort_key = (self.extractor)(key, &new_mem);
                partition_set
                    .elements
                    .insert((sort_key.clone(), key.clone()));
                partition_set
                    .memory_view
                    .insert(key.clone(), (sort_key, new_mem));
            }
        }
    }

    fn remove_data(&mut self, key: &K, bin_value: BinA) {
        let partition_key = (self.partitioning_func)(key);
        let partition_set = self.partition_heaps.entry(partition_key).or_default();
        let entry = partition_set.memory_view.remove_entry(key);
        if entry.is_none() {
            warn!("no memory data for {:?}", key);
            return;
        }
        let (old_key, (current_sort_key, current_mem)) = entry.unwrap();
        let pair = (current_sort_key, old_key);
        if let Some(new_value) = (self.in_memory_remove)(current_mem, bin_value) {
            let new_sort_key = (self.extractor)(key, &new_value);
            if pair.0 != new_sort_key {
                let (_, popped_key) = partition_set
                    .elements
                    .take(&pair)
                    .expect("error for the element to be missing");
                partition_set
                    .elements
                    .insert((new_sort_key.clone(), popped_key));
            }
            partition_set
                .memory_view
                .insert(pair.1, (new_sort_key, new_value));
        } else {
            partition_set.elements.remove(&pair);
            if partition_set.elements.is_empty() {
                self.partition_heaps.remove(&(self.partitioning_func)(key));
            }
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
    async fn advance(&mut self, ctx: &mut Context<PK, OutT>) {
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
            self.remove_data(&key, bin.clone());
        }
        let window_end = bin_end - Duration::from_nanos(1);
        let mut records = vec![];

        for (partition, partition_store) in &self.partition_heaps {
            for (_sort_key, key) in partition_store.elements.iter().take(self.max_elements) {
                let (_, in_memory) = partition_store.memory_view.get(key).unwrap();
                let value = (self.aggregator)(
                    key,
                    Window {
                        start: bin_end - self.width,
                        end: bin_end,
                    },
                    in_memory,
                );
                records.push(Record {
                    timestamp: window_end,
                    key: Some(partition.clone()),
                    value,
                });
            }
        }
        self.state = if self.partition_heaps.is_empty() {
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

    async fn handle_watermark(&mut self, watermark: Watermark, ctx: &mut Context<PK, OutT>) {
        match watermark {
            Watermark::EventTime(watermark) => {
                while self.should_advance(watermark) {
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
        ctx: &mut Context<PK, OutT>,
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
