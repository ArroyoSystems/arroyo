use crate::{CheckpointMessage, DataOperation, TableData};
use anyhow::{bail, Result};
use arroyo_rpc::grpc::{
    SubtaskCheckpointMetadata, TableCheckpointMetadata, TableConfig, TableEnum,
    TableSubtaskCheckpointMetadata, TableType,
};
use arroyo_storage::StorageProviderRef;
use arroyo_types::{TaskInfo, TaskInfoRef};
use prost::Message;
use std::any::Any;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::fmt::Display;
use std::time::SystemTime;
use tracing::info;

use self::global_keyed_map::GlobalKeyedTable;

pub mod expiring_time_key_map;
pub mod global_keyed_map;
pub mod key_time_multi_map;
pub mod keyed_map;
pub mod table_manager;
pub mod time_key_map;

pub enum Compactor {
    TimeKeyMap,
    KeyTimeMultiMap,
}

pub(crate) fn table_checkpoint_path(
    task_info: &TaskInfo,
    table: impl Display,
    epoch: u32,
    compacted: bool,
) -> String {
    format!(
        "{}/table-{}-{:0>3}{}",
        operator_path(&task_info.job_id, epoch, &task_info.operator_id),
        table,
        task_info.task_index,
        if compacted { "-compacted" } else { "" }
    )
}

fn operator_path(job_id: &str, epoch: u32, operator: &str) -> String {
    format!("{}/operator-{}", base_path(job_id, epoch), operator)
}

fn base_path(job_id: &str, epoch: u32) -> String {
    format!("{}/checkpoints/checkpoint-{:0>7}", job_id, epoch)
}

pub struct DataTuple<K, V> {
    pub timestamp: SystemTime,
    pub key: K,
    pub value: Option<V>,
    pub operation: DataOperation,
}

/// BlindDataTuple's key and value are not decoded
#[derive(Debug, PartialEq, Eq, Clone)]
pub struct BlindDataTuple {
    pub key_hash: u64,
    pub timestamp: SystemTime,
    pub key: Vec<u8>,
    pub value: Vec<u8>,
    pub operation: DataOperation,
}

impl Compactor {
    pub(crate) fn for_table_type(table_type: TableType) -> Self {
        match table_type {
            TableType::Global | TableType::TimeKeyMap => Compactor::TimeKeyMap,
            TableType::KeyTimeMultiMap => Compactor::KeyTimeMultiMap,
        }
    }

    pub(crate) fn compact_tuples(&self, tuples: Vec<BlindDataTuple>) -> Vec<BlindDataTuple> {
        match self {
            Compactor::TimeKeyMap => {
                // keep only the latest entry for each key
                let mut reduced = BTreeMap::new();
                for tuple in tuples.into_iter() {
                    match tuple.operation {
                        DataOperation::Insert | DataOperation::DeleteTimeKey(_) => {}
                        DataOperation::DeleteKey(_)
                        | DataOperation::DeleteValue(_)
                        | DataOperation::DeleteTimeRange(_) => {
                            panic!("Not supported")
                        }
                    }

                    let memory_key = (tuple.timestamp, tuple.key.clone());
                    reduced.insert(memory_key, tuple);
                }

                reduced.into_values().collect()
            }
            Compactor::KeyTimeMultiMap => {
                // Build a values map similar to KeyTimeMultiMap,
                // but with the values being the actual tuples.
                // Then flatten the map to get the compacted inserts.
                let mut values: HashMap<Vec<u8>, BTreeMap<SystemTime, Vec<BlindDataTuple>>> =
                    HashMap::new();
                let mut deletes = HashMap::new();
                for tuple in tuples.into_iter() {
                    let keep_deletes_key =
                        (tuple.timestamp, tuple.key.clone(), tuple.value.clone());
                    match tuple.operation.clone() {
                        DataOperation::Insert => {
                            values
                                .entry(tuple.key.clone())
                                .or_default()
                                .entry(tuple.timestamp)
                                .or_default()
                                .push(tuple);
                        }
                        DataOperation::DeleteTimeKey(_) => panic!("Not supported"),
                        DataOperation::DeleteKey(op) => {
                            // Remove a key. Timestamp is not considered.
                            values.remove(op.key.as_slice());
                            deletes.insert(keep_deletes_key, tuple);
                        }
                        DataOperation::DeleteValue(op) => {
                            // Remove a single value from a (key -> time -> values).
                            values.entry(op.key.clone()).and_modify(|map| {
                                map.entry(op.timestamp).and_modify(|values| {
                                    let position = values.iter().position(|stored_tuple| {
                                        stored_tuple.value == op.value.clone()
                                    });
                                    if let Some(position) = position {
                                        values.remove(position);
                                    }
                                });
                            });
                            deletes.insert(keep_deletes_key, tuple);
                        }
                        DataOperation::DeleteTimeRange(op) => {
                            // Remove range of times from a key.
                            // Timestamp of tuple is not considered (the range is in the DataOperation).
                            if let Some(key_map) = values.get_mut(op.key.as_slice()) {
                                key_map.retain(|time, _values| !(op.start..op.end).contains(time))
                            }
                            deletes.insert(keep_deletes_key, tuple);
                        }
                    }
                }

                let mut reduced: Vec<BlindDataTuple> = vec![];

                // first add the deletes
                for (_, tuple) in deletes.into_iter() {
                    reduced.push(tuple);
                }

                // then flatten values to get the compacted inserts
                for (_, map) in values.into_iter() {
                    for (_, tuples) in map.into_iter() {
                        for tuple in tuples.into_iter() {
                            reduced.push(tuple);
                        }
                    }
                }

                reduced
            }
        }
    }
}

#[cfg(test)]
mod test {
    use crate::tables::{BlindDataTuple, Compactor};
    use crate::{DataOperation, DeleteTimeKeyOperation, DeleteTimeRangeOperation};
    use std::time::{Duration, SystemTime};

    #[tokio::test]
    async fn test_time_key_map_compaction() {
        let t1 = SystemTime::now();
        let t2 = t1 + Duration::from_secs(1);

        let k1 = "k1".as_bytes().to_vec();
        let k2 = "k2".as_bytes().to_vec();
        let v1 = "v1".as_bytes().to_vec();

        let insert_1 = BlindDataTuple {
            key_hash: 123,
            timestamp: t1,
            key: k1.clone(),
            value: v1.clone(),
            operation: DataOperation::Insert,
        };

        let insert_2 = BlindDataTuple {
            key_hash: 123,
            timestamp: t2,
            key: k2.clone(),
            value: v1.clone(),
            operation: DataOperation::Insert,
        };

        let delete = BlindDataTuple {
            key_hash: 123,
            timestamp: t1,
            key: k1.clone(),
            value: v1.clone(),
            operation: DataOperation::DeleteTimeKey(DeleteTimeKeyOperation {
                timestamp: t1,
                key: k1.clone(),
            }),
        };

        let tuples_in = vec![insert_1.clone(), insert_2.clone(), delete.clone()];

        let tuples_out = Compactor::TimeKeyMap.compact_tuples(tuples_in);
        assert_eq!(vec![delete, insert_2], tuples_out);

        // test idempotence
        assert_eq!(
            tuples_out.clone(),
            Compactor::TimeKeyMap.compact_tuples(tuples_out),
        );
    }

    #[tokio::test]
    async fn test_key_time_multi_map_compaction() {
        let t1 = SystemTime::now();
        let t2 = t1 + Duration::from_secs(1);
        let t3 = t2 + Duration::from_secs(1);

        let k1 = "k1".as_bytes().to_vec();
        let v1 = "v1".as_bytes().to_vec();

        let insert_1 = BlindDataTuple {
            key_hash: 123,
            timestamp: t1,
            key: k1.clone(),
            value: v1.clone(),
            operation: DataOperation::Insert,
        };

        let insert_2 = BlindDataTuple {
            key_hash: 123,
            timestamp: t2,
            key: k1.clone(),
            value: v1.clone(),
            operation: DataOperation::Insert,
        };

        let delete_all = BlindDataTuple {
            key_hash: 123,
            timestamp: t2,
            key: k1.clone(),
            value: v1.clone(),
            operation: DataOperation::DeleteTimeRange(DeleteTimeRangeOperation {
                key: k1.clone(),
                start: t1,
                end: t3,
            }),
        };

        let insert_3 = BlindDataTuple {
            key_hash: 123,
            timestamp: t1,
            key: k1.clone(),
            value: v1.clone(),
            operation: DataOperation::Insert,
        };

        let tuples_in = vec![
            insert_1.clone(),
            insert_2.clone(),
            delete_all.clone(),
            insert_3.clone(),
        ];

        let tuples_out = Compactor::KeyTimeMultiMap.compact_tuples(tuples_in);
        assert_eq!(vec![delete_all, insert_3], tuples_out);

        // test idempotence
        assert_eq!(
            tuples_out.clone(),
            Compactor::KeyTimeMultiMap.compact_tuples(tuples_out),
        );
    }
}

pub(crate) trait Table: Send + Sync + 'static + Clone {
    // A stateful struct responsible for taking the checkpoint for a single epoch
    // contains an associated type that is a protobuf for the subtask checkpoint metadata.
    type Checkpointer: TableEpochCheckpointer<
        SubTableCheckpointMessage = Self::TableSubtaskCheckpointMetadata,
    >;
    // Protobuf message containing any configuration for the table.
    type ConfigMessage: prost::Message + Default;
    // A protobuf holding all necessary data for restoring from a specific epoch.
    // Will be produced by the controller checkpointing logic and read by subtasks when restoring from checkpoint.
    type TableCheckpointMessage: prost::Message + Default;

    type TableSubtaskCheckpointMetadata: prost::Message + Default;

    // produce the Table based on the
    // * config: (table specific configuration, such as retention duration),
    // * task_info: subtask specific info, including job_id, operator_id, and subtask_index
    // * checkpoint_message: If restoring from a checkpoint, the checkpoint data for that checkpoint's epoch.
    fn from_config(
        config: Self::ConfigMessage,
        task_info: TaskInfoRef,
        storage_provider: StorageProviderRef,
        checkpoint_message: Option<Self::TableCheckpointMessage>,
    ) -> anyhow::Result<Self>
    where
        Self: Sized;
    // Returns a stateful struct that processes new data to checkpoint,
    // finishes said data, and returns a metadata protobuf.
    // the metadata protobuf should be sufficient to know all checkpoint data for the table that
    // the subtask cares about at that epoch, including previously written data,
    // which will be determined from the previous metadata.
    fn epoch_checkpointer(
        &self,
        epoch: u32,
        previous_metadata: Option<Self::TableSubtaskCheckpointMetadata>,
    ) -> Result<Self::Checkpointer>;
    // A controller method to merge the metadata from each subtask into a single Table metadata.
    // Will do things like dedup files and compute overall min and max watermarks.
    fn merge_checkpoint_metadata(
        config: Self::ConfigMessage,
        subtask_metadata: HashMap<u32, Self::TableSubtaskCheckpointMetadata>,
    ) -> Result<Option<Self::TableCheckpointMessage>>;
    // compute the subtask metadata from the overall table metadata.
    // This is needed because of repartitioning, which means a subtask might need to read data "owned" by other subtasks in the previous epoch.
    fn subtask_metadata_from_table(
        &self,
        table_metadata: Self::TableCheckpointMessage,
    ) -> Result<Option<Self::TableSubtaskCheckpointMetadata>>;

    fn table_type() -> TableEnum;

    fn task_info(&self) -> TaskInfoRef;

    fn files_to_keep(
        config: Self::ConfigMessage,
        checkpoint: Self::TableCheckpointMessage,
    ) -> Result<HashSet<String>>;
}

pub trait ErasedTable: Send + Sync + 'static {
    // produce the Table based on the
    // * config: (table specific configuration, such as retention duration),
    // * task_info: subtask specific info, including job_id, operator_id, and subtask_index
    // * checkpoint_message: If restoring from a checkpoint, the checkpoint data for that checkpoint's epoch.
    fn from_config(
        config: TableConfig,
        task_info: TaskInfoRef,
        storage_provider: StorageProviderRef,
        checkpoint_message: Option<TableCheckpointMetadata>,
    ) -> anyhow::Result<Self>
    where
        Self: Sized;
    // Returns a stateful struct that processes new data to checkpoint,
    // finishes said data, and returns a metadata protobuf.
    // the metadata protobuf should be sufficient to know all checkpoint data for the table that
    // the subtask cares about at that epoch, including previously written data,
    // which will be determined from the previous metadata.
    fn epoch_checkpointer(
        &self,
        epoch: u32,
        previous_metadata: Option<TableSubtaskCheckpointMetadata>,
    ) -> Result<Box<dyn ErasedCheckpointer>>;
    // A controller method to merge the metadata from each subtask into a single Table metadata.
    // Will do things like dedup files and compute overall min and max watermarks.
    fn merge_checkpoint_metadata(
        config: TableConfig,
        subtask_metadata: HashMap<u32, TableSubtaskCheckpointMetadata>,
    ) -> Result<Option<TableCheckpointMetadata>>
    where
        Self: Sized;
    // compute the subtask metadata from the overall table metadata.
    // This is needed because of repartitioning, which means a subtask might need to read data "owned" by other subtasks in the previous epoch.
    fn subtask_metadata_from_table(
        &self,
        table_metadata: TableCheckpointMetadata,
    ) -> Result<Option<TableSubtaskCheckpointMetadata>>;

    fn table_type() -> TableEnum
    where
        Self: Sized;

    fn checked_proto_decode<M: Message + Default>(table_type: TableEnum, data: Vec<u8>) -> Result<M>
    where
        Self: Sized,
    {
        if Self::table_type() != table_type {
            bail!(
                "mismatched table type, expected type {:?}, got {:?}",
                Self::table_type(),
                table_type
            )
        }
        Ok(Message::decode(&mut data.as_slice())?)
    }

    fn files_to_keep(
        config: TableConfig,
        checkpoint: TableCheckpointMetadata,
    ) -> Result<HashSet<String>>
    where
        Self: Sized;

    fn as_any(&self) -> &dyn Any;
}

impl<T: Table + Sized + 'static> ErasedTable for T {
    fn from_config(
        config: TableConfig,
        task_info: TaskInfoRef,
        storage_provider: StorageProviderRef,
        checkpoint_message: Option<TableCheckpointMetadata>,
    ) -> anyhow::Result<Self>
    where
        Self: Sized,
    {
        let config = Self::checked_proto_decode(config.table_type(), config.config)?;
        let checkpoint_message = checkpoint_message
            .map(|metadata| Self::checked_proto_decode(metadata.table_type(), metadata.data))
            .transpose()?;
        info!(
            "restoring from checkpoint message:\n{:#?}",
            checkpoint_message
        );
        T::from_config(config, task_info, storage_provider, checkpoint_message)
    }

    fn epoch_checkpointer(
        &self,
        epoch: u32,
        previous_metadata: Option<TableSubtaskCheckpointMetadata>,
    ) -> Result<Box<dyn ErasedCheckpointer>> {
        let previous_metadata = previous_metadata
            .map(|metadata| Self::checked_proto_decode(metadata.table_type(), metadata.data))
            .transpose()?;
        let checkpointer = self.epoch_checkpointer(epoch, previous_metadata)?;
        Ok(Box::new(checkpointer) as Box<dyn ErasedCheckpointer>)
    }

    fn merge_checkpoint_metadata(
        config: TableConfig,
        subtask_metadata: HashMap<u32, TableSubtaskCheckpointMetadata>,
    ) -> Result<Option<TableCheckpointMetadata>>
    where
        Self: Sized,
    {
        let config = Self::checked_proto_decode(config.table_type(), config.config)?;
        let subtask_metadata = subtask_metadata
            .into_iter()
            .map(|(key, value)| {
                let value = Self::checked_proto_decode(value.table_type(), value.data)?;
                Ok((key, value))
            })
            .collect::<Result<HashMap<_, _>>>()?;
        let result = T::merge_checkpoint_metadata(config, subtask_metadata)?;
        Ok(result.map(|table| TableCheckpointMetadata {
            table_type: T::table_type().into(),
            data: table.encode_to_vec(),
        }))
    }

    fn subtask_metadata_from_table(
        &self,
        table_metadata: TableCheckpointMetadata,
    ) -> Result<Option<TableSubtaskCheckpointMetadata>> {
        let table_metadata =
            Self::checked_proto_decode(table_metadata.table_type(), table_metadata.data)?;
        let subtask_metadata = self.subtask_metadata_from_table(table_metadata)?;
        Ok(
            subtask_metadata.map(|metadata| TableSubtaskCheckpointMetadata {
                subtask_index: self.task_info().task_index as u32,
                table_type: T::table_type().into(),
                data: metadata.encode_to_vec(),
            }),
        )
    }

    fn table_type() -> TableEnum
    where
        Self: Sized,
    {
        T::table_type()
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn files_to_keep(
        config: TableConfig,
        checkpoint: TableCheckpointMetadata,
    ) -> Result<HashSet<String>>
    where
        Self: Sized,
    {
        T::files_to_keep(
            Self::checked_proto_decode(T::table_type(), config.config)?,
            Self::checked_proto_decode(T::table_type(), checkpoint.data)?,
        )
    }
}

#[async_trait::async_trait]
pub trait TableEpochCheckpointer: Send {
    type SubTableCheckpointMessage: prost::Message;
    async fn insert_data(&mut self, data: TableData) -> Result<()>;
    // returning Ok(None) means there is no state to restore.
    async fn finish(
        self,
        checkpoint: &CheckpointMessage,
    ) -> Result<Option<Self::SubTableCheckpointMessage>>;

    fn table_type() -> TableEnum;

    fn subtask_index(&self) -> u32;
}

#[async_trait::async_trait]
pub trait ErasedCheckpointer: Send {
    async fn insert_data(&mut self, data: TableData) -> Result<()>;
    async fn finish(
        mut self: Box<Self>,
        checkpoint: &CheckpointMessage,
    ) -> Result<Option<TableSubtaskCheckpointMetadata>>;
}

#[async_trait::async_trait]
impl<T: TableEpochCheckpointer + Sized> ErasedCheckpointer for T {
    async fn insert_data(&mut self, data: TableData) -> Result<()> {
        self.insert_data(data).await
    }

    async fn finish(
        mut self: Box<Self>,
        checkpoint: &CheckpointMessage,
    ) -> Result<Option<TableSubtaskCheckpointMetadata>> {
        let subtask_index = self.subtask_index();
        let subtask = (*self).finish(checkpoint).await?;
        Ok(subtask.map(|metadata| TableSubtaskCheckpointMetadata {
            subtask_index,
            table_type: T::table_type().into(),
            data: metadata.encode_to_vec(),
        }))
    }
}
