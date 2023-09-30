use crate::judy::InMemoryJudyNode;
use crate::parquet::ParquetBackend;
//use crate::parquet::ParquetBackend;
use crate::{BackingStore, StateBackend};
use anyhow::{Context, Result};
use arroyo_rpc::grpc::{CheckpointMetadata, TableDescriptor, TableType};
use arroyo_types::{from_micros, Data, Key, TaskInfo};
use bincode::config;
use futures::{Future, TryFutureExt};
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::io::Cursor;
use std::sync::Arc;
use std::time::{Duration, SystemTime};
use tokio::task::JoinHandle;

use super::backend::JudyBackend;
use super::{BytesWithOffset, JudyNode, JudyWriter};
pub mod global_keyed_map;
pub mod key_time_multimap;
pub mod keyed_map;
pub mod reader;
pub mod time_key_map;

pub struct TimeKeyMap<'a, K: Key, V: Data, S: BackingStore> {
    table: char,
    store: &'a mut S,
    cache: &'a mut TimeKeyMapCache<K, V>,
    // this is so we can return a pointer
    last_value: Option<V>,
}

impl<'a, K: Key, V: Data + PartialEq> TimeKeyMap<'a, K, V, JudyBackend> {
    pub fn new(
        table: char,
        store: &'a mut JudyBackend,
        cache: &'a mut TimeKeyMapCache<K, V>,
    ) -> Self {
        Self {
            table,
            store,
            cache,
            last_value: None,
        }
    }
    pub async fn get(&mut self, timestamp: SystemTime, key: &mut K) -> Option<&V> {
        {
            let cached_value = self.cache.get(timestamp, key);
            if cached_value.is_some() {
                return cached_value;
            }
        }
        if let Some(value) = self
            .store
            .get_last_value_for_key(self.table, key, timestamp)
            .await
        {
            self.last_value = Some(value);
            self.last_value.as_ref()
        } else {
            None
        }
    }

    pub fn insert(&mut self, event_time: SystemTime, mut key: K, value: V) {
        self.cache.insert(event_time, &mut key, value);
    }

    pub async fn get_all_for_time(&mut self, timestamp: SystemTime) -> Vec<(K, V)> {
        let files = self
            .store
            .get_epoch_ordered_values_at_time(self.table, timestamp)
            .await;
        // TODO: we could traverse N bitmaps in parallel, selecting the first one.
        // Instead we just add them to the map in order, new values overwriting old ones.
        let mut result_map = BTreeMap::new();
        for bytes in files {
            let mut index_reader = Cursor::new(bytes.clone());
            let mut data_reader = BytesWithOffset::new(bytes);
            JudyNode::fill_btree_map(&mut index_reader, &mut data_reader, &mut result_map, vec![])
                .await
                .unwrap();
        }
        result_map
            .into_iter()
            .map(|(k, v)| {
                (
                    bincode::decode_from_slice(&k, config::standard())
                        .unwrap()
                        .0,
                    bincode::decode_from_slice(&v, config::standard())
                        .unwrap()
                        .0,
                )
            })
            .collect()
    }

    pub async fn get_all(&mut self) -> Vec<(SystemTime, K, V)> {
        todo!()
    }

    pub fn remove(&mut self, event_time: SystemTime, k: &mut K) -> Option<V> {
        self.cache.remove(event_time, k)
    }

    pub async fn get_min_time(&mut self) -> Option<SystemTime> {
        let min_live_timestamp = self.cache.min_live_timestamp();
        let backing_store_min_time = self
            .store
            .get_earliest_timestamp(self.table, min_live_timestamp)
            .await;
        let cache_min_timestamp = self.cache.earliest_live_timestamp();
        match (backing_store_min_time, cache_min_timestamp) {
            (Some(backing_store_min_time), Some(cache_min_timestamp)) => {
                Some(backing_store_min_time.min(cache_min_timestamp))
            }
            (Some(backing_store_min_time), None) => Some(backing_store_min_time),
            (None, Some(cache_min_timestamp)) => Some(cache_min_timestamp),
            (None, None) => None,
        }
    }

    pub fn evict_all_before_watermark(&mut self, watermark: SystemTime) -> Vec<(K, V)> {
        let mut result = vec![];
        while let Some(earliest_timestamp) = self.cache.earliest_live_timestamp() {
            if earliest_timestamp <= watermark {
                result.extend(self.evict_for_timestamp(earliest_timestamp));
            } else {
                break;
            }
        }
        result
    }

    pub fn evict_for_timestamp(&mut self, timestamp: SystemTime) -> Vec<(K, V)> {
        self.cache.evict_for_timestamp(timestamp)
    }

    pub async fn flush_at_watermark(&mut self, _watermark: SystemTime) {}

    pub async fn flush(&mut self) -> Option<JoinHandle<Result<Vec<u8>>>> {
        let frozen_map = self.cache.get_map_to_flush();
        if let Some(map) = frozen_map {
            let map_to_put_in_future = Arc::new(map);
            let fut = tokio::spawn(async move {
                let mut writer = JudyWriter::new();
                let mut sorted_bytes = BTreeMap::new();
                for (timestamp, judy) in map_to_put_in_future.iter() {
                    sorted_bytes.insert(
                        bincode::encode_to_vec(timestamp, config::standard()).unwrap(),
                        judy.to_judy_bytes().await?,
                    );
                }
                for (key, value) in sorted_bytes {
                    writer.insert(&key, &value).await?;
                }
                let mut write_bytes = Cursor::new(vec![]);
                writer.serialize(&mut write_bytes).await?;
                Ok(write_bytes.into_inner())
            });
            return Some(fut);
        }
        None
    }
}

pub struct TimeKeyMapCache<K: Key, V: Data> {
    current_watermark: Option<SystemTime>,
    max_lookback: Option<Duration>,
    prior_active_epoch: Option<Arc<BTreeMap<SystemTime, InMemoryJudyNode<K, V>>>>,
    active_epoch_judy: BTreeMap<SystemTime, InMemoryJudyNode<K, V>>,
}

impl<K: Key, V: Data> TimeKeyMapCache<K, V> {
    pub async fn from_checkpoint(
        backing_store: &JudyBackend,
        _task_info: &TaskInfo,
        table: char,
        table_descriptor: &TableDescriptor,
        watermark: Option<SystemTime>,
    ) -> Self {
        Self {
            current_watermark: watermark,
            max_lookback: match table_descriptor.delete_behavior() {
                arroyo_rpc::grpc::TableDeleteBehavior::None => None,
                arroyo_rpc::grpc::TableDeleteBehavior::NoReadsBeforeWatermark => {
                    Some(Duration::from_micros(table_descriptor.retention_micros))
                }
            },
            prior_active_epoch: None,
            active_epoch_judy: BTreeMap::default(),
        }
    }

    fn min_live_timestamp(&self) -> SystemTime {
        if let (Some(max_lookback), Some(watermark)) = (self.max_lookback, self.current_watermark) {
            watermark - max_lookback
        } else {
            SystemTime::UNIX_EPOCH
        }
    }

    pub fn get(&self, timestamp: SystemTime, key: &mut K) -> Option<&V> {
        let active_value = self
            .active_epoch_judy
            .get(&timestamp)
            .and_then(|map| map.get(key));
        if active_value.is_some() {
            return active_value;
        }
        self.prior_active_epoch
            .as_ref()
            .and_then(|map| map.get(&timestamp))
            .and_then(|map| map.get(key))
    }

    fn insert(&mut self, event_time: SystemTime, key: &mut K, value: V) {
        self.active_epoch_judy
            .entry(event_time)
            .or_default()
            .insert(key, value)
            .unwrap();
    }

    fn update_watermark(&mut self, watermark: SystemTime) {
        self.current_watermark = Some(watermark);
        if let Some(duration) = self.max_lookback {
            self.active_epoch_judy = self.active_epoch_judy.split_off(&(watermark - duration));
        }
    }

    fn evict_for_timestamp(&mut self, timestamp: SystemTime) -> Vec<(K, V)> {
        let Some(removed_judy) = self.active_epoch_judy.remove(&timestamp) else {
            return vec![];
        };
        removed_judy.to_vec()
    }

    fn get_map_to_flush(&mut self) -> Option<Arc<BTreeMap<SystemTime, InMemoryJudyNode<K, V>>>> {
        if self.active_epoch_judy.is_empty() {
            self.prior_active_epoch = None;
            return None;
        }
        let frozen_map = Arc::new(std::mem::take(&mut self.active_epoch_judy));
        self.prior_active_epoch = Some(frozen_map.clone());
        Some(frozen_map)
    }

    fn remove(&mut self, event_time: SystemTime, k: &mut K) -> Option<V> {
        let judy_node = self.active_epoch_judy.get_mut(&event_time)?;
        let removed_value = judy_node.remove(k);
        if judy_node.is_empty() {
            self.active_epoch_judy.remove(&event_time);
        }
        // TODO: track remove tombstones.
        removed_value
    }

    fn get_all(&mut self) -> Vec<(SystemTime, K, &V)> {
        let mut active_keys = HashSet::new();
        let mut active_epoch_data: Vec<_> = self
            .active_epoch_judy
            .iter()
            .flat_map(|(timestamp, judy)| {
                judy.as_vec()
                    .into_iter()
                    .map(|(key, value)| (*timestamp, key, value))
            })
            .collect();
        active_epoch_data
            .iter()
            .for_each(|(timestamp, key, _value)| {
                active_keys.insert((*timestamp, key.clone()));
            });
        if let Some(prior_epoch) = &self.prior_active_epoch {
            active_epoch_data.extend(
                prior_epoch
                    .iter()
                    .filter(|(timestamp, _judy)| {
                        if let (Some(max_lookback), Some(watermark)) =
                            (self.max_lookback, self.current_watermark)
                        {
                            **timestamp >= watermark - max_lookback
                        } else {
                            true
                        }
                    })
                    .flat_map(|(timestamp, judy)| {
                        judy.as_vec()
                            .into_iter()
                            .map(move |(key, value)| (*timestamp, key, value))
                    })
                    .filter(|(timestamp, key, _value)| {
                        !active_keys.contains(&(*timestamp, key.clone()))
                    }),
            );
        }
        active_epoch_data
    }

    fn earliest_live_timestamp(&self) -> Option<SystemTime> {
        self.active_epoch_judy.keys().min().copied()
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
        /* self.backing_store
        .get_data_tuples(
            self.table,
            TableType::KeyTimeMultiMap,
            timestamp,
            &mut key,
            &mut value,
        )
        .await;*/
        self.cache.insert(timestamp, key, value);
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

    pub fn expire_entries_before(&mut self, expiration_time: SystemTime) {
        self.cache.expire_entries_before(expiration_time);
    }

    pub async fn get_all_values_with_timestamps(
        &mut self,
        key: &mut K,
    ) -> Option<impl Iterator<Item = (SystemTime, &V)>> {
        self.cache.get_all_values_with_timestamps(key)
    }
}

pub struct KeyTimeMultiMapCache<K: Key, V: Data> {
    values: HashMap<K, BTreeMap<SystemTime, Vec<V>>>,
    expirations: BTreeMap<SystemTime, HashSet<K>>,
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
        .await
        .expect("expect metadata for restoring from checkpoint");
        let min_valid_time = operator_metadata
            .min_watermark
            .map_or(SystemTime::UNIX_EPOCH, |min_watermark| {
                from_micros(min_watermark - table_descriptor.retention_micros)
            });

        /*for (timestamp, key, value) in backing_store.get_data_tuples(table).await {
            if timestamp < min_valid_time {
                continue;
            }
            values
                .entry(key)
                .or_default()
                .entry(timestamp)
                .or_default()
                .push(value);
        }*/
        let mut expirations: BTreeMap<SystemTime, HashSet<K>> = BTreeMap::new();
        for (time, key) in values.iter().map(|(key, map)| {
            let time = map.keys().next().unwrap();
            (*time, key.clone())
        }) {
            expirations.entry(time).or_default().insert(key);
        }
        Self {
            values,
            expirations,
        }
    }

    fn get_all_values_with_timestamps(
        &mut self,
        key: &mut K,
    ) -> Option<impl Iterator<Item = (SystemTime, &V)>> {
        if let Some(key_map) = self.values.get(key) {
            let result = key_map
                .iter()
                .flat_map(|(time, values)| values.iter().map(move |value| (*time, value)));
            Some(result)
        } else {
            None
        }
    }

    fn expire_entries_before(&mut self, time: SystemTime) {
        let times_to_remove = self.expirations.range(..time);
        let keys_to_remove: HashSet<_> = times_to_remove
            .flat_map(|(_time, keys)| keys.clone())
            .collect();
        for key in keys_to_remove {
            let key_data = self.values.get_mut(&key).unwrap();
            if *key_data.last_key_value().unwrap().0 <= time {
                self.values.remove(&key);
            } else {
                let retained_data = key_data.split_off(&time);
                let earliest_key = retained_data.first_key_value().unwrap().0;
                self.expirations
                    .entry(*earliest_key)
                    .or_default()
                    .insert(key);
                *key_data = retained_data;
            }
        }
    }

    // Insert a new value for a key at a given timestamp.
    // This potentially updates the earliest timestamp for the key.
    fn insert(&mut self, timestamp: SystemTime, key: K, value: V) {
        let current_entries = self.values.entry(key.clone()).or_default();
        // If there are no entries for this key, insert the new value.
        // the expiration is the timestamp of the new value.
        if current_entries.is_empty() {
            current_entries.insert(timestamp, vec![value]);
            self.expirations.entry(timestamp).or_default().insert(key);
        } else {
            // If there are entries for this key, check if the new value is earlier than the earliest value.
            let current_earliest = *current_entries.first_key_value().unwrap().0;
            if timestamp < current_earliest {
                // there definitely aren't any values at the new timestamp.
                current_entries.insert(timestamp, vec![value]);
                // remove the key from the previous earliest timestamp. If that map is empty also drop it.
                let current_earliest_keys = self.expirations.entry(current_earliest).or_default();
                current_earliest_keys.remove(&key);
                if current_earliest_keys.is_empty() {
                    self.expirations.remove(&current_earliest);
                }
                self.expirations.entry(timestamp).or_default().insert(key);
            } else {
                current_entries.entry(timestamp).or_default().push(value);
            }
        }
    }
}

impl<K: Key, V: Data> Default for KeyTimeMultiMapCache<K, V> {
    fn default() -> Self {
        Self {
            values: Default::default(),
            expirations: Default::default(),
        }
    }
}

pub struct KeyedState<'a, K: Key, V: Data, S: BackingStore> {
    table: char,
    backing_state: &'a mut S,
    cache: &'a mut KeyedStateCache<K, V>,
}

impl<'a, K: Key, V: Data, S: BackingStore> KeyedState<'a, K, V, S> {
    pub fn new(
        table: char,
        backing_store: &'a mut S,
        cache: &'a mut KeyedStateCache<K, V>,
    ) -> Self {
        Self {
            table,
            backing_state: backing_store,
            cache,
        }
    }

    pub async fn insert(&mut self, timestamp: SystemTime, mut key: K, value: V) {
        let mut wrapped = Some(value);
        /*self.backing_state
        .write_data_triple(
            self.table,
            TableType::TimeKeyMap,
            timestamp,
            &mut key,
            &mut wrapped,
        )
        .await;*/
        self.cache.insert(key, wrapped.unwrap());
    }

    pub async fn remove(&mut self, key: &mut K) {
        self.cache.remove(&key);
        self.backing_state
            .write_key_value::<K, Option<V>>(self.table, key, &mut None)
            .await;
    }

    pub fn get(&self, key: &K) -> Option<&V> {
        self.cache.values.get(key)
    }
}

pub struct KeyedStateCache<K: Key, V: Data> {
    values: HashMap<K, V>,
}

impl<K: Key, V: Data> KeyedStateCache<K, V> {
    pub async fn from_checkpoint<S: BackingStore>(backing_store: &S, table: char) -> Self {
        let mut values = HashMap::new();
        for (key, value) in backing_store.get_key_values(table).await {
            match value {
                Some(value) => values.insert(key, value),
                None => values.remove(&key),
            };
        }
        Self { values }
    }

    pub fn insert(&mut self, key: K, value: V) {
        self.values.insert(key, value);
    }
    pub fn remove(&mut self, key: &K) {
        self.values.remove(key);
    }
}

impl<K: Key, V: Data> Default for KeyedStateCache<K, V> {
    fn default() -> Self {
        Self {
            values: Default::default(),
        }
    }
}
