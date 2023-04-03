//use crate::parquet::ParquetBackend;
use crate::{BackingStore, StateBackend};
use arroyo_rpc::grpc::{CheckpointMetadata, TableDescriptor, TableType};
use arroyo_types::{from_micros, Data, Key, TaskInfo};
use std::collections::{BTreeMap, HashMap};
use std::time::{Duration, SystemTime};
use tracing::warn;

pub struct TimeKeyMap<'a, K: Key, V: Data, S: BackingStore> {
    table: char,
    parquet: &'a mut S,
    cache: &'a mut TimeKeyMapCache<K, V>,
}

impl<'a, K: Key, V: Data + PartialEq, S: BackingStore> TimeKeyMap<'a, K, V, S> {
    pub fn new(table: char, parquet: &'a mut S, cache: &'a mut TimeKeyMapCache<K, V>) -> Self {
        Self {
            table,
            parquet,
            cache,
        }
    }
    pub fn get(&self, timestamp: SystemTime, key: &mut K) -> Option<&V> {
        let buffered_value = self
            .cache
            .buffered_values
            .get(&timestamp)
            .and_then(|map| map.get(key));
        if buffered_value.is_some() {
            buffered_value
        } else {
            self.cache
                .persisted_values
                .get(&timestamp)
                .and_then(|map| map.get(key))
        }
    }

    pub fn insert(&mut self, event_time: SystemTime, key: K, value: V) {
        if let Some(map_for_time) = self.cache.persisted_values.get(&event_time) {
            if let Some(stored_value) = map_for_time.get(&key) {
                if value == *stored_value {
                    return;
                }
            }
        }
        self.cache
            .buffered_values
            .entry(event_time)
            .or_default()
            .insert(key, value);
    }

    pub fn get_all_for_time(&self, timestamp: SystemTime) -> Vec<(&K, &V)> {
        match (
            self.cache.buffered_values.get(&timestamp),
            self.cache.persisted_values.get(&timestamp),
        ) {
            (None, None) => {
                vec![]
            }
            (None, Some(map)) | (Some(map), None) => map.iter().collect(),
            (Some(buffered_map), Some(persisted_map)) => persisted_map
                .iter()
                .filter(|(k, _v)| !buffered_map.contains_key(k))
                .chain(buffered_map.iter())
                .collect(),
        }
    }

    pub async fn get_all(&mut self) -> Vec<(SystemTime, &K, &V)> {
        if !self.cache.buffered_values.is_empty() {
            warn!("buffered values present, flushing them before returning all values");
            self.flush().await;
        }
        self.cache
            .persisted_values
            .iter()
            .flat_map(|(timestamp, map)| {
                let tuples: Vec<_> = map
                    .iter()
                    .map(|(key, value)| (*timestamp, key, value))
                    .collect();
                tuples
            })
            .collect()
    }

    pub fn get_min_time(&self) -> Option<SystemTime> {
        let persisted_time = self.cache.persisted_values.keys().min();
        let buffered_time = self.cache.buffered_values.keys().min();
        match (persisted_time, buffered_time) {
            (None, None) => None,
            (None, Some(time)) | (Some(time), None) => Some(*time),
            (Some(persisted_time), Some(buffered_time)) => Some(*persisted_time.min(buffered_time)),
        }
    }

    pub fn evict_all_before_watermark(&mut self, watermark: SystemTime) -> Vec<(K, V)> {
        let mut result = vec![];
        loop {
            let persisted_time = self.cache.persisted_values.keys().min();
            let buffered_time = self.cache.buffered_values.keys().min();
            let min_time = match (persisted_time, buffered_time) {
                (None, None) => break,
                (None, Some(time)) | (Some(time), None) => time,
                (Some(persisted_time), Some(buffered_time)) => persisted_time.min(buffered_time),
            };
            if *min_time <= watermark {
                result.append(&mut self.evict_for_timestamp(*min_time))
            } else {
                break;
            }
        }
        result
    }

    pub fn evict_for_timestamp(&mut self, timestamp: SystemTime) -> Vec<(K, V)> {
        match (
            self.cache.persisted_values.remove(&timestamp),
            self.cache.buffered_values.remove(&timestamp),
        ) {
            (None, None) => vec![],
            (None, Some(map_at_time)) | (Some(map_at_time), None) => {
                map_at_time.into_iter().collect()
            }
            (Some(persisted_values), Some(buffered_values)) => {
                let mut results: Vec<_> = persisted_values
                    .into_iter()
                    .filter(|(k, _v)| !buffered_values.contains_key(k))
                    .collect();
                results.append(&mut buffered_values.into_iter().collect());
                results
            }
        }
    }

    pub async fn flush_at_watermark(&mut self, watermark: SystemTime) {
        loop {
            let Some(&earliest) = self.cache.buffered_values.keys().next() else {
                break;
            };
            if watermark < earliest {
                break;
            }
            let (time, mut values) = self.cache.buffered_values.pop_first().unwrap();
            let persisted_map = self.cache.persisted_values.entry(time).or_default();
            let drained = values.drain();
            for (mut key, mut value) in drained {
                self.parquet
                    .write_data_triple(
                        self.table,
                        TableType::TimeKeyMap,
                        time,
                        &mut key,
                        &mut value,
                    )
                    .await;
                persisted_map.insert(key, value);
            }
        }
    }

    pub async fn flush(&mut self) {
        let Some(timestamp) = self.cache.buffered_values
        .keys().max() else {
            return;
        };
        self.flush_at_watermark(*timestamp).await;
    }
}
pub struct TimeKeyMapCache<K: Key, V: Data> {
    persisted_values: BTreeMap<SystemTime, HashMap<K, V>>,
    buffered_values: BTreeMap<SystemTime, HashMap<K, V>>,
}

impl<K: Key, V: Data> TimeKeyMapCache<K, V> {
    pub async fn from_checkpoint<S: BackingStore>(
        backing_store: &S,
        _task_info: &TaskInfo,
        table: char,
        table_descriptor: &TableDescriptor,
        watermark: Option<SystemTime>,
    ) -> Self {
        let mut persisted_values: BTreeMap<SystemTime, HashMap<K, V>> = BTreeMap::new();
        let min_valid_time = watermark.map_or(SystemTime::UNIX_EPOCH, |watermark| {
            watermark - Duration::from_micros(table_descriptor.retention_micros)
        });
        for (timestamp, key, value) in backing_store.get_data_triples(table).await {
            if timestamp < min_valid_time {
                continue;
            }
            persisted_values
                .entry(timestamp)
                .or_default()
                .insert(key, value);
        }

        Self {
            persisted_values,
            buffered_values: BTreeMap::default(),
        }
    }
}
impl<K: Key, V: Data> Default for TimeKeyMapCache<K, V> {
    fn default() -> Self {
        Self {
            persisted_values: BTreeMap::default(),
            buffered_values: BTreeMap::default(),
        }
    }
}

pub struct KeyTimeMultiMap<'a, K: Key, V: Data, S: BackingStore> {
    table: char,
    backing_store: &'a mut S,
    cache: &'a mut KeyTimeMultiMapCache<K, V>,
}

impl<'a, K: Key, V: Data, S: BackingStore> KeyTimeMultiMap<'a, K, V, S> {
    pub fn new(
        table: char,
        backing_store: &'a mut S,
        cache: &'a mut KeyTimeMultiMapCache<K, V>,
    ) -> Self {
        Self {
            table,
            backing_store,
            cache,
        }
    }
    pub async fn insert(&mut self, timestamp: SystemTime, mut key: K, mut value: V) {
        self.backing_store
            .write_data_triple(
                self.table,
                TableType::KeyTimeMultiMap,
                timestamp,
                &mut key,
                &mut value,
            )
            .await;

        self.cache
            .values
            .entry(key)
            .or_default()
            .entry(timestamp)
            .or_default()
            .push(value);
    }

    pub async fn get_time_range(
        &mut self,
        key: &mut K,
        start: SystemTime,
        end: SystemTime,
    ) -> Vec<&V> {
        let Some(key_map) = self.cache.values.get(key) else {
            return vec![];
        };
        key_map
            .range(start..end)
            .flat_map(|(_time, values)| values)
            .collect()
    }

    pub async fn clear_time_range(&mut self, key: &mut K, start: SystemTime, end: SystemTime) {
        if let Some(key_map) = self.cache.values.get_mut(key) {
            let times_to_remove = key_map.range(start..end);
            let times: Vec<_> = times_to_remove.map(|(time, _values)| *time).collect();
            for time in times {
                key_map.remove(&time);
            }
        };
    }
}

pub struct KeyTimeMultiMapCache<K: Key, V: Data> {
    values: HashMap<K, BTreeMap<SystemTime, Vec<V>>>,
}
impl<K: Key, V: Data> KeyTimeMultiMapCache<K, V> {
    pub async fn from_checkpoint<S: BackingStore>(
        backing_store: &S,
        task_info: &TaskInfo,
        table: char,
        table_descriptor: &TableDescriptor,
        checkpoint_metadata: &CheckpointMetadata,
    ) -> Self {
        let mut values: HashMap<K, BTreeMap<SystemTime, Vec<V>>> = HashMap::new();
        // TODO: there may be a race here, as the initial checkpoint_metadata might get stale.
        // This is unlikely as this method is only called on start, but should probably be the domain of the backing store.
        let operator_metadata = StateBackend::load_operator_metadata(
            &task_info.job_id,
            &task_info.operator_id,
            checkpoint_metadata.epoch,
        )
        .await;
        let min_valid_time = operator_metadata
            .min_watermark
            .map_or(SystemTime::UNIX_EPOCH, |min_watermark| {
                from_micros(min_watermark - table_descriptor.retention_micros)
            });

        for (timestamp, key, value) in backing_store.get_data_triples(table).await {
            if timestamp < min_valid_time {
                continue;
            }
            values
                .entry(key)
                .or_default()
                .entry(timestamp)
                .or_default()
                .push(value);
        }
        Self { values }
    }
}

impl<K: Key, V: Data> Default for KeyTimeMultiMapCache<K, V> {
    fn default() -> Self {
        Self {
            values: Default::default(),
        }
    }
}

pub struct GlobalKeyedState<'a, K: Key, V: Data, S: BackingStore> {
    table: char,
    parquet: &'a mut S,
    cache: &'a mut GlobalKeyedStateCache<K, V>,
}

impl<'a, K: Key, V: Data, S: BackingStore> GlobalKeyedState<'a, K, V, S> {
    pub fn new(
        table: char,
        backing_store: &'a mut S,
        cache: &'a mut GlobalKeyedStateCache<K, V>,
    ) -> Self {
        Self {
            table,
            parquet: backing_store,
            cache,
        }
    }
    pub async fn insert(&mut self, mut key: K, mut value: V) {
        self.parquet
            .write_key_value(self.table, &mut key, &mut value)
            .await;
        self.cache.values.insert(key, value);
    }

    pub fn get_all(&mut self) -> Vec<&V> {
        self.cache.values.values().collect()
    }

    pub fn get(&self, key: &K) -> Option<&V> {
        self.cache.values.get(key)
    }
}

pub struct GlobalKeyedStateCache<K: Key, V: Data> {
    values: HashMap<K, V>,
}
impl<K: Key, V: Data> GlobalKeyedStateCache<K, V> {
    pub async fn from_checkpoint<S: BackingStore>(backing_store: &S, table: char) -> Self {
        let mut values = HashMap::new();
        for (key, value) in backing_store.get_key_values(table).await {
            values.insert(key, value);
        }
        Self { values }
    }
}

impl<K: Key, V: Data> Default for GlobalKeyedStateCache<K, V> {
    fn default() -> Self {
        Self {
            values: Default::default(),
        }
    }
}
