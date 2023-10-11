use crate::metrics::TABLE_SIZE_GAUGE;
use crate::{BackingStore, DataOperation, BINCODE_CONFIG};
use arroyo_rpc::grpc::{TableDescriptor, TableType};
use arroyo_types::{Data, Key, TaskInfo};
use std::collections::{BTreeMap, HashMap};
use std::time::{Duration, SystemTime};

pub struct TimeKeyMap<'a, K: Key, V: Data, S: BackingStore> {
    table: char,
    store: &'a mut S,
    cache: &'a mut TimeKeyMapCache<K, V>,
}

impl<'a, K: Key, V: Data + PartialEq, S: BackingStore> TimeKeyMap<'a, K, V, S> {
    pub fn new(table: char, parquet: &'a mut S, cache: &'a mut TimeKeyMapCache<K, V>) -> Self {
        Self {
            table,
            store: parquet,
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

        TABLE_SIZE_GAUGE
            .with_label_values(&[
                &self.store.task_info().operator_id,
                &self.store.task_info().task_index.to_string(),
                &self.table.to_string(),
            ])
            .set(self.cache.buffered_values.len() as f64);
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

    pub async fn remove(&mut self, event_time: SystemTime, k: &mut K) -> Option<V> {
        if let Some(m) = self.cache.buffered_values.get_mut(&event_time) {
            m.remove(k);
        }

        if let Some(m) = self.cache.persisted_values.get_mut(&event_time) {
            self.store
                .delete_time_key(self.table, TableType::TimeKeyMap, event_time, k)
                .await;
            m.remove(k)
        } else {
            None
        }
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
                self.store
                    .write_data_tuple(
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
        let Some(timestamp) = self.cache.buffered_values.keys().max() else {
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
        for tuple in backing_store.get_data_tuples(table).await {
            if tuple.timestamp < min_valid_time {
                continue;
            }
            match tuple.operation {
                DataOperation::Insert => {
                    persisted_values
                        .entry(tuple.timestamp)
                        .or_default()
                        .insert(tuple.key, tuple.value.unwrap());
                }
                DataOperation::DeleteTimeKey(op) => {
                    let key = bincode::decode_from_slice(&op.key, BINCODE_CONFIG)
                        .unwrap()
                        .0;
                    persisted_values
                        .entry(op.timestamp)
                        .or_default()
                        .remove(&key);
                }
                DataOperation::DeleteKey(..) => {
                    panic!("Not supported")
                }
                DataOperation::DeleteValue(..) => {
                    panic!("Not supported")
                }
                DataOperation::DeleteTimeRange(..) => {
                    panic!("Not supported")
                }
            }
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
