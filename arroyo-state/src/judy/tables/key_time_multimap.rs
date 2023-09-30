use crate::judy::backend::{BytesOrFutureBytes, Checkpointing, JudyMessage, JudyQueueItem};
use crate::judy::tables::JudyBackend;
use crate::judy::InMemoryJudyNode;
use crate::{hash_key, BackingStore, StateBackend};
use anyhow::bail;
use anyhow::Result;
use arroyo_rpc::grpc::{
    CheckpointMetadata, ParquetStoreData, TableDeleteBehavior, TableDescriptor,
};
use arroyo_types::{from_micros, to_micros, CheckpointBarrier, Data, Key, TaskInfo};
use bytes::Bytes;
use std::borrow::Cow;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::default;
use std::sync::Arc;
use std::time::{Duration, SystemTime};
use tokio::sync::oneshot;

use super::reader::JudyReader;

pub struct JudyKeyTimeMultiMap<K: Key, V: Data> {
    table_descriptor: TableDescriptor,
    current_epoch: u32,
    data_by_epoch: BTreeMap<u32, EpochData<K, V>>,
}

enum EpochData<K: Key, V: Data> {
    InMemory {
        stats: FileStats,
        tree: InMemoryJudyNode<K, InMemoryJudyNode<SystemTime, Vec<V>>>,
    },
    Frozen {
        stats: FileStats,
        tree: Arc<InMemoryJudyNode<K, InMemoryJudyNode<SystemTime, Vec<V>>>>,
    },
    BytesReader(Vec<(FileStats, JudyReader<K, JudyReader<SystemTime, Vec<V>>>)>),
}

#[derive(Clone)]
struct FileStats {
    min_timestamp: SystemTime,
    max_timestamp: SystemTime,
    min_routing_key: u64,
    max_routing_key: u64,
}

impl FileStats {
    fn from_parquet_metadata(metadata: &ParquetStoreData) -> Self {
        Self {
            min_timestamp: from_micros(metadata.min_required_timestamp_micros()),
            max_timestamp: from_micros(metadata.max_timestamp_micros),
            min_routing_key: metadata.min_routing_key,
            max_routing_key: metadata.max_routing_key,
        }
    }

    fn to_metadata(&self, epoch: u32, table: char) -> ParquetStoreData {
        ParquetStoreData {
            epoch,
            table: table.into(),
            min_routing_key: self.min_routing_key,
            max_routing_key: self.max_routing_key,
            max_timestamp_micros: to_micros(self.max_timestamp),
            min_required_timestamp_micros: Some(to_micros(self.min_timestamp)),
            generation: 0,
            ..Default::default()
        }
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
            EpochData::InMemory { stats: _, tree } => tree
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
            EpochData::Frozen { stats: _, tree } => tree
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
                .filter_map(|(_stats, reader)| reader.get_bytes(key).unwrap())
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
            EpochData::InMemory { stats, tree } => {
                stats.min_timestamp = stats.min_timestamp.min(timestamp);
                stats.max_timestamp = stats.max_timestamp.max(timestamp);
                let key_hash = hash_key(&key);
                stats.max_routing_key = stats.max_routing_key.max(key_hash);
                stats.min_routing_key = stats.min_routing_key.min(key_hash);
                tree.get_mut_or_insert(&key, || InMemoryJudyNode::default())
                    .get_mut_or_insert(&timestamp, || vec![])
                    .push(value);
                Ok(())
            }
            EpochData::Frozen { .. } => bail!("Cannot insert into frozen epoch data"),
            EpochData::BytesReader(_) => bail!("Cannot insert into byte data"),
        }
    }
    pub fn get_time_range(
        &mut self,
        key: &mut K,
        start: SystemTime,
        end: SystemTime,
    ) -> Vec<Cow<V>> {
        match self {
            EpochData::InMemory { stats, tree } => {
                if end < stats.min_timestamp || start > stats.max_timestamp {
                    return vec![];
                }
                tree.get(key)
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
                    .unwrap_or_default()
            }
            EpochData::Frozen { stats, tree } => {
                if end < stats.min_timestamp || start > stats.max_timestamp {
                    return vec![];
                }
                tree.get(key)
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
                    .unwrap_or_default()
            }
            EpochData::BytesReader(data) => data
                .iter_mut()
                .filter_map(|(stats, reader)| {
                    if end < stats.min_timestamp || start > stats.max_timestamp {
                        return None;
                    }
                    reader.get_bytes(key).unwrap()
                })
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

    fn as_frozen(
        self,
    ) -> (
        FileStats,
        Arc<InMemoryJudyNode<K, InMemoryJudyNode<SystemTime, Vec<V>>>>,
    ) {
        match self {
            EpochData::InMemory { stats, tree } => (stats, Arc::new(tree)),
            EpochData::Frozen { .. } => unreachable!("shouldn't be freezing already frozen data"),
            EpochData::BytesReader(_) => unreachable!("shouldn't be freezing bytes reader"),
        }
    }
}

impl<K: Key, V: Data> JudyKeyTimeMultiMap<K, V> {
    pub fn new(table_descriptor: TableDescriptor) -> Self {
        Self {
            table_descriptor,
            current_epoch: 1,
            data_by_epoch: BTreeMap::new(),
        }
    }
    pub async fn from_files(
        table_descriptor: TableDescriptor,
        current_epoch: u32,
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
                readers.push((
                    FileStats::from_parquet_metadata(metadata),
                    JudyReader::new(bytes_or_future_bytes.bytes().await).unwrap(),
                ));
            }
            if !readers.is_empty() {
                data_by_epoch.insert(*epoch, EpochData::BytesReader(readers));
            }
        }

        Self {
            table_descriptor,
            current_epoch,
            data_by_epoch,
        }
    }

    pub fn get_all_values_with_timestamps(&mut self, key: &mut K) -> Vec<(SystemTime, Cow<V>)> {
        self.data_by_epoch
            .values_mut()
            .flat_map(|epoch_data| epoch_data.get_all_values_with_timestamps(key))
            .collect()
    }

    pub fn get_time_range(
        &mut self,
        key: &mut K,
        start: SystemTime,
        end: SystemTime,
    ) -> Vec<Cow<V>> {
        self.data_by_epoch
            .values_mut()
            .flat_map(|epoch_data| epoch_data.get_time_range(key, start, end))
            .collect()
    }

    pub fn insert(&mut self, timestamp: SystemTime, key: K, value: V) {
        let current = self.data_by_epoch.get_mut(&self.current_epoch);
        match current {
            Some(current) => {
                current.insert(timestamp, key, value).unwrap();
            }
            None => {
                let initial_stats = FileStats {
                    min_timestamp: timestamp,
                    max_timestamp: timestamp,
                    min_routing_key: hash_key(&key),
                    max_routing_key: hash_key(&key),
                };
                let mut new_epoch_data = EpochData::InMemory {
                    stats: initial_stats,
                    tree: InMemoryJudyNode::default(),
                };
                new_epoch_data.insert(timestamp, key, value).unwrap();
                self.data_by_epoch
                    .insert(self.current_epoch, new_epoch_data);
            }
        }
    }
}
#[async_trait::async_trait]
impl<K: Key, V: Data> Checkpointing for JudyKeyTimeMultiMap<K, V> {
    async fn checkpoint(
        &mut self,
        barrier: &CheckpointBarrier,
        watermark: Option<SystemTime>,
    ) -> Vec<JudyQueueItem> {
        assert_eq!(barrier.epoch, self.current_epoch);
        let Some(current_map) = self.data_by_epoch.remove(&barrier.epoch) else {
            return vec![];
        };
        let (stats, frozen_epoch) = current_map.as_frozen();
        let table_char = self.table_descriptor.name.chars().next().unwrap();
        let metadata = stats.to_metadata(barrier.epoch, table_char);

        self.data_by_epoch.insert(
            barrier.epoch,
            EpochData::Frozen {
                stats,
                tree: frozen_epoch.clone(),
            },
        );
        self.current_epoch += 1;

        let (tx, rx) = oneshot::channel();
        tokio::spawn(async move {
            let bytes = frozen_epoch.to_judy_bytes().await.unwrap();
            // TODO: calculate bytes
            tx.send(JudyMessage::new(table_char, bytes, metadata))
                .unwrap();
        });
        return vec![JudyQueueItem::OneShot(rx)];
    }

    fn as_any(&mut self) -> &mut dyn std::any::Any {
        self
    }
}
