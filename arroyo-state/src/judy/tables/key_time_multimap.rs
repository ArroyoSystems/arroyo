use crate::judy::backend::BytesOrFutureBytes;
use crate::judy::tables::JudyBackend;
use crate::judy::InMemoryJudyNode;
use crate::{BackingStore, StateBackend};
use anyhow::bail;
use anyhow::Result;
use arroyo_rpc::grpc::{
    CheckpointMetadata, ParquetStoreData, TableDeleteBehavior, TableDescriptor,
};
use arroyo_types::{from_micros, Data, Key, TaskInfo};
use bytes::Bytes;
use std::borrow::Cow;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use super::reader::JudyReader;

pub struct KeyTimeMultiMap<'a, K: Key, V: Data, S: BackingStore + KeyTimeMultiMapBackend<K, V>> {
    table: char,
    backing_store: &'a mut S,
    cache: &'a mut KeyTimeMultiMapCache<K, V>,
}

pub struct JudyKeyTimeMultiMap<K: Key, V: Data> {
    table_descriptor: TableDescriptor,
    current_epoch: u32,
    data_by_epoch: BTreeMap<u32, EpochData<K, V>>,
}

enum EpochData<K: Key, V: Data> {
    InMemory(InMemoryJudyNode<K, InMemoryJudyNode<SystemTime, Vec<V>>>),
    Frozen(Arc<InMemoryJudyNode<K, InMemoryJudyNode<SystemTime, Vec<V>>>>),
    BytesReader(Vec<JudyReader<K, JudyReader<SystemTime, Vec<V>>>>),
}

impl<K: Key, V: Data> Default for EpochData<K, V> {
    fn default() -> Self {
        Self::InMemory(Default::default())
    }
}

struct InMemoryData<K: Key, V: Data> {
    data: InMemoryJudyNode<K, InMemoryJudyNode<SystemTime, Vec<V>>>,
    min_hash_key: u64,
    max_hash_key: u64,
    min_timestamp: SystemTime,
    max_timestamp: SystemTime,
}

impl<K: Key, V: Data> EpochData<K, V> {
    fn get_all_values_with_timestamps(&mut self, key: &mut K) -> Vec<(SystemTime, Cow<V>)> {
        match self {
            EpochData::InMemory(data) => data
                .get(key)
                .map(|submap| {
                    submap
                        .as_vec()
                        .into_iter()
                        .flat_map(|(time, values)| {
                            values.iter().map(move |value| (time, Cow::Borrowed(value)))
                        })
                        .collect()
                })
                .unwrap_or_default(),
            EpochData::Frozen(data) => data
                .get(key)
                .map(|submap| {
                    submap
                        .as_vec()
                        .into_iter()
                        .flat_map(|(time, values)| {
                            values.iter().map(move |value| (time, Cow::Borrowed(value)))
                        })
                        .collect()
                })
                .unwrap_or_default(),
            EpochData::BytesReader(readers) => readers
                .iter_mut()
                .filter_map(|reader| reader.get_bytes(key).unwrap())
                .flat_map(|bytes| {
                    let mut reader: JudyReader<SystemTime, Vec<V>> =
                        JudyReader::new(bytes).unwrap();
                    reader
                        .as_vec()
                        .unwrap()
                        .into_iter()
                        .flat_map(|(time, values)| {
                            values
                                .into_iter()
                                .map(move |value| (time, Cow::Owned(value)))
                        })
                })
                .collect(),
        }
    }

    fn insert(&mut self, timestamp: SystemTime, key: K, value: V) -> Result<()> {
        match self {
            EpochData::InMemory(data) => {
                data.get_mut_or_insert(&key, || InMemoryJudyNode::default())
                    .get_mut_or_insert(&timestamp, || vec![])
                    .push(value);
                Ok(())
            }
            EpochData::Frozen(_) => bail!("Cannot insert into frozen epoch data"),
            EpochData::BytesReader(_) => bail!("Cannot insert into byte data"),
        }
    }
    pub async fn get_time_range(
        &mut self,
        key: &mut K,
        start: SystemTime,
        end: SystemTime,
    ) -> Vec<Cow<V>> {
        match self {
            EpochData::InMemory(data) => data
                .get(key)
                .map(|submap| {
                    submap
                        .as_vec()
                        .into_iter()
                        .filter(|(time, _values)| start <= *time && *time < end)
                        .flat_map(|(_time, values)| {
                            values.iter().map(move |value| (Cow::Borrowed(value)))
                        })
                        .collect()
                })
                .unwrap_or_default(),
            EpochData::Frozen(data) => data
                .get(key)
                .map(|submap| {
                    submap
                        .as_vec()
                        .into_iter()
                        .filter(|(time, _values)| start <= *time && *time < end)
                        .flat_map(|(time, values)| {
                            values.iter().map(move |value| (Cow::Borrowed(value)))
                        })
                        .collect()
                })
                .unwrap_or_default(),
            EpochData::BytesReader(data) => data
                .iter_mut()
                .filter_map(|reader| reader.get_bytes(key).unwrap())
                .flat_map(|bytes| {
                    let mut key_reader: JudyReader<SystemTime, Vec<V>> =
                        JudyReader::new(bytes).unwrap();
                    key_reader
                        .as_vec()
                        .unwrap()
                        .into_iter()
                        .filter(|(time, _values)| start <= *time && *time < end)
                        .flat_map(|(_time, values)| {
                            values.into_iter().map(move |value| (Cow::Owned(value)))
                        })
                })
                .collect(),
        }
    }
}

impl<K: Key, V: Data> JudyKeyTimeMultiMap<K, V> {
    pub fn new() -> Self {
        Self {
            table_descriptor: TableDescriptor::default(),
            current_epoch: 1,
            data_by_epoch: BTreeMap::new(),
        }
    }
    pub async fn from_files(
        table_descriptor: TableDescriptor,
        task_info: &TaskInfo,
        current_watermark: Option<SystemTime>,
        files: &mut BTreeMap<u32, BTreeMap<u64, (ParquetStoreData, BytesOrFutureBytes)>>,
    ) -> Self {
        let mut data_by_epoch = BTreeMap::new();
        for (epoch, files) in files {
            let mut readers = Vec::new();
            for (_hash_key, (metadata, bytes_or_future_bytes)) in files {
                // check hash range, watermark
                if table_descriptor.delete_behavior() == TableDeleteBehavior::NoReadsBeforeWatermark
                {
                    if let Some(current_watermark) = current_watermark {
                        if from_micros(
                            metadata.max_timestamp_micros + table_descriptor.retention_micros,
                        ) < current_watermark
                        {
                            continue;
                        }
                    }
                }
                if metadata.min_routing_key > *task_info.key_range.end() {
                    continue;
                }
                if metadata.max_routing_key < *task_info.key_range.start() {
                    continue;
                }
                readers.push(JudyReader::new(bytes_or_future_bytes.bytes().await).unwrap());
            }
            if !readers.is_empty() {
                data_by_epoch.insert(*epoch, EpochData::BytesReader(readers));
            }
        }

        Self {
            table_descriptor,
            current_epoch: 0,
            data_by_epoch,
        }
    }

    fn get_all_values_with_timestamps(&mut self, key: &mut K) -> Vec<(SystemTime, Cow<V>)> {
        self.data_by_epoch
            .values_mut()
            .flat_map(|epoch_data| epoch_data.get_all_values_with_timestamps(key))
            .collect()
    }

    fn insert(&mut self, timestamp: SystemTime, key: K, value: V) {
        self.data_by_epoch
            .entry(self.current_epoch)
            .or_default()
            .insert(timestamp, key, value);
    }
}

#[async_trait::async_trait]
pub trait KeyTimeMultiMapBackend<K: Key, V: Data> {
    async fn get_values_for_key(&mut self, key: &mut K) -> Vec<(SystemTime, &V)>;
    async fn clear_time_range(&mut self, key: &mut K, start: SystemTime, end: SystemTime);
    async fn insert(&mut self, timestamp: SystemTime, key: K, value: V);
    async fn expire_entries_before(&mut self, expiration_time: SystemTime);
    async fn get_all_values_with_timestamps(&mut self, key: &mut K) -> Vec<(SystemTime, &V)>;
}

#[async_trait::async_trait]
impl<K: Key, V: Data> KeyTimeMultiMapBackend<K, V> for JudyBackend {
    async fn get_values_for_key(&mut self, key: &mut K) -> Vec<(SystemTime, &V)> {
        todo!()
    }

    async fn clear_time_range(&mut self, key: &mut K, start: SystemTime, end: SystemTime) {
        todo!()
    }

    async fn insert(&mut self, timestamp: SystemTime, key: K, value: V) {
        todo!()
    }

    async fn expire_entries_before(&mut self, expiration_time: SystemTime) {
        todo!()
    }

    async fn get_all_values_with_timestamps(&mut self, key: &mut K) -> Vec<(SystemTime, &V)> {
        todo!()
    }
}

impl<'a, K: Key, V: Data> KeyTimeMultiMap<'a, K, V, JudyBackend> {
    pub fn new(
        table: char,
        backing_store: &'a mut JudyBackend,
        cache: &'a mut KeyTimeMultiMapCache<K, V>,
    ) -> Self {
        Self {
            table,
            backing_store,
            cache,
        }
    }
    pub async fn insert(&mut self, timestamp: SystemTime, mut key: K, mut value: V) {
        self.cache.insert(timestamp, key, value);
    }

    pub async fn get_time_range(
        &mut self,
        key: &mut K,
        start: SystemTime,
        end: SystemTime,
    ) -> Vec<&V> {
        let mut backing_data = self
            .backing_store
            .get_values_for_key(key)
            .await
            .into_iter()
            .map(|(_time, value)| value)
            .collect();
        let Some(key_map) = self.cache.current_epoch_values.get(key) else {
            return backing_data;
        };
        key_map
            .as_vec()
            .iter()
            .filter(|(time, _values)| start <= *time && *time < end)
            .flat_map(|(_time, values)| values.iter())
            .collect()
    }

    pub async fn get_all_values_with_timestamps(&mut self, key: &mut K) -> Vec<(SystemTime, &V)> {
        self.cache.get_all_values_with_timestamps(key)
    }
}

pub struct KeyTimeMultiMapCache<K: Key, V: Data> {
    prior_epoch_values: Option<Arc<InMemoryJudyNode<K, InMemoryJudyNode<SystemTime, Vec<V>>>>>,
    current_epoch_values: InMemoryJudyNode<K, InMemoryJudyNode<SystemTime, Vec<V>>>,
}

impl<K: Key, V: Data> KeyTimeMultiMapCache<K, V> {
    pub async fn from_checkpoint<S: BackingStore>(
        backing_store: &S,
        task_info: &TaskInfo,
        table: char,
        table_descriptor: &TableDescriptor,
        checkpoint_metadata: &CheckpointMetadata,
    ) -> Self {
        Self {
            prior_epoch_values: None,
            current_epoch_values: InMemoryJudyNode::default(),
        }
    }

    fn get_all_values_with_timestamps(&mut self, key: &mut K) -> Vec<(SystemTime, &V)> {
        let mut result = match self.current_epoch_values.get(key) {
            Some(key_map) => key_map
                .as_vec()
                .into_iter()
                .flat_map(|(time, values)| values.iter().map(move |value| (time, value)))
                .collect(),
            None => vec![],
        };
        if let Some(prior_epoch) = &self.prior_epoch_values {
            if let Some(prior_key_map) = prior_epoch.get(key) {
                result.extend(
                    prior_key_map
                        .as_vec()
                        .into_iter()
                        .flat_map(|(time, values)| values.iter().map(move |value| (time, value))),
                );
            }
        }
        result
    }

    // Insert a new value for a key at a given timestamp.
    // This potentially updates the earliest timestamp for the key.
    fn insert(&mut self, timestamp: SystemTime, key: K, value: V) {
        let key_map = self
            .current_epoch_values
            .get_mut_or_insert(&key, || InMemoryJudyNode::default())
            .get_mut_or_insert(&timestamp, || vec![]);
        key_map.push(value);
    }
}

impl<K: Key, V: Data> Default for KeyTimeMultiMapCache<K, V> {
    fn default() -> Self {
        Self {
            prior_epoch_values: None,
            current_epoch_values: Default::default(),
        }
    }
}
