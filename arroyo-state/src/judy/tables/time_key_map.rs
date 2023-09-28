use crate::judy::backend::JudyBackend;
use crate::judy::{BytesWithOffset, InMemoryJudyNode, JudyNode, JudyWriter};
use crate::BackingStore;
use arroyo_rpc::grpc::TableDescriptor;
use arroyo_types::{Data, Key, TaskInfo};
use bincode::config;
use std::collections::{BTreeMap, HashSet};
use std::io::Cursor;
use std::sync::Arc;
use std::time::{Duration, SystemTime};
use tokio::task::JoinHandle;

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
                .await;
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

    pub async fn flush(&mut self) -> Option<JoinHandle<anyhow::Result<Vec<u8>>>> {
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
                writer.serialize(&mut write_bytes);
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
        let mut active_epoch_data: Vec<_> = self
            .active_epoch_judy
            .iter()
            .flat_map(|(timestamp, judy)| {
                judy.as_vec()
                    .into_iter()
                    .map(move |(key, value)| (*timestamp, key, value))
            })
            .collect();
        if let Some(prior_epoch) = &self.prior_active_epoch {
            let mut active_keys = HashSet::new();
            active_epoch_data
                .iter()
                .for_each(|(timestamp, key, _value)| {
                    active_keys.insert((*timestamp, key.clone()));
                });
            active_epoch_data.extend(
                prior_epoch
                    .iter()
                    .filter(|(timestamp, judy)| {
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
