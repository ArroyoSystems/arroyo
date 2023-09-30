use crate::{
    judy::{
        backend::{BytesOrFutureBytes, Checkpointing, JudyMessage, JudyQueueItem},
        InMemoryJudyNode,
    },
    BackingStore,
};
use arroyo_rpc::grpc::{ParquetStoreData, TableDescriptor};
use arroyo_types::{CheckpointBarrier, Data, Key};
use std::{borrow::Cow, collections::HashMap, time::SystemTime};
use tracing::info;

use super::reader::JudyReader;

pub struct GlobalKeyedState<K: Key, V: Data> {
    table_descriptor: TableDescriptor,
    current_epoch: u32,
    data: InMemoryJudyNode<K, V>,
}

impl<K: Key, V: Data> GlobalKeyedState<K, V> {
    pub fn new(table_descriptor: TableDescriptor) -> Self {
        Self {
            table_descriptor,
            current_epoch: 1,
            data: InMemoryJudyNode::new(),
        }
    }

    pub async fn from_files(
        table_descriptor: TableDescriptor,
        current_epoch: u32,
        files_for_epoch: Vec<&mut (ParquetStoreData, BytesOrFutureBytes)>,
    ) -> Self {
        let mut data = InMemoryJudyNode::new();
        for (parquet_store_data, ref mut bytes_or_future_bytes) in files_for_epoch {
            let mut reader: JudyReader<K, V> =
                JudyReader::new(bytes_or_future_bytes.bytes().await).unwrap();
            for (key, value) in reader.as_vec().unwrap() {
                data.insert(&key, value).unwrap();
            }
        }
        Self {
            table_descriptor,
            current_epoch,
            data,
        }
    }

    pub fn insert(&mut self, key: K, mut value: V) {
        self.data.insert(&key, value).unwrap();
    }

    pub fn get_all(&mut self) -> Vec<Cow<V>> {
        self.data
            .as_vec()
            .into_iter()
            .map(|(_, v)| Cow::Borrowed(v))
            .collect()
    }

    pub fn get(&self, key: &K) -> Option<&V> {
        self.data.get(key)
    }
}

#[async_trait::async_trait]
impl<K: Key, V: Data> Checkpointing for GlobalKeyedState<K, V> {
    async fn checkpoint(
        &mut self,
        barrier: &CheckpointBarrier,
        watermark: Option<SystemTime>,
    ) -> Vec<JudyQueueItem> {
        info!("checkpointing in memory data {:?}", self.data.as_vec());
        let table_char = self.table_descriptor.name.chars().next().unwrap();
        vec![JudyQueueItem::Message(JudyMessage::new(
            table_char.clone(),
            self.data.to_judy_bytes().await.unwrap(),
            ParquetStoreData {
                epoch: barrier.epoch,
                table: table_char.to_string(),
                ..Default::default()
            },
        ))]
    }

    fn as_any(&mut self) -> &mut dyn std::any::Any {
        self
    }
}
