use crate::{CheckpointMessage, DataOperation, TableData};
use anyhow::{bail, Result};
use arroyo_rpc::grpc::rpc::{
    OperatorMetadata, TableCheckpointMetadata, TableConfig, TableEnum,
    TableSubtaskCheckpointMetadata,
};
use arroyo_storage::StorageProviderRef;
use arroyo_types::TaskInfoRef;
use prost::Message;
use std::any::Any;
use std::collections::{HashMap, HashSet};
use std::time::SystemTime;
use tracing::debug;

pub mod expiring_time_key_map;
pub mod global_keyed_map;
pub mod table_manager;

pub(crate) fn table_checkpoint_path(
    job_id: &str,
    operator_id: &str,
    table: &str,
    subtask_index: usize,
    epoch: u32,
    compacted: bool,
) -> String {
    format!(
        "{}/table-{}-{:0>3}{}",
        operator_path(job_id, epoch, operator_id),
        table,
        subtask_index,
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

#[async_trait::async_trait]
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

    fn apply_compacted_checkpoint(
        &self,
        epoch: u32,
        compacted_checkpoint: Self::TableSubtaskCheckpointMetadata,
        subtask_metadata: Self::TableSubtaskCheckpointMetadata,
    ) -> Result<Self::TableSubtaskCheckpointMetadata>;

    fn table_type() -> TableEnum;

    fn task_info(&self) -> TaskInfoRef;

    fn files_to_keep(
        config: Self::ConfigMessage,
        checkpoint: Self::TableCheckpointMessage,
    ) -> Result<HashSet<String>>;

    async fn compact_data(
        config: Self::ConfigMessage,
        compaction_config: &CompactionConfig,
        operator_metadata: &OperatorMetadata,
        current_metadata: Self::TableCheckpointMessage,
    ) -> Result<Option<Self::TableCheckpointMessage>>;

    fn committing_data(
        _config: Self::ConfigMessage,
        _table_metadata: Self::TableCheckpointMessage,
    ) -> Option<HashMap<u32, Vec<u8>>>
    where
        Self: Sized,
    {
        None
    }
}

pub struct CompactionConfig {
    pub storage_provider: StorageProviderRef,
    pub compact_generations: HashSet<u64>,
    pub min_compaction_epochs: usize,
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

    #[allow(async_fn_in_trait)]
    async fn compact_data(
        config: TableConfig,
        compaction_config: &CompactionConfig,
        operator_metadata: &OperatorMetadata,
        current_metadata: TableCheckpointMetadata,
    ) -> Result<Option<TableCheckpointMetadata>>
    where
        Self: Sized;

    fn committing_data(
        config: TableConfig,
        table_metadata: &TableCheckpointMetadata,
    ) -> Option<HashMap<u32, Vec<u8>>>
    where
        Self: Sized;

    fn apply_compacted_checkpoint(
        &self,
        epoch: u32,
        compacted_checkpoint: TableSubtaskCheckpointMetadata,
        subtask_metadata: TableSubtaskCheckpointMetadata,
    ) -> Result<TableSubtaskCheckpointMetadata>;
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
        debug!(
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

    fn apply_compacted_checkpoint(
        &self,
        epoch: u32,
        compacted_checkpoint: TableSubtaskCheckpointMetadata,
        subtask_metadata: TableSubtaskCheckpointMetadata,
    ) -> Result<TableSubtaskCheckpointMetadata> {
        let compacted_checkpoint = Self::checked_proto_decode(
            compacted_checkpoint.table_type(),
            compacted_checkpoint.data,
        )?;
        let subtask_metadata =
            Self::checked_proto_decode(subtask_metadata.table_type(), subtask_metadata.data)?;
        let result =
            self.apply_compacted_checkpoint(epoch, compacted_checkpoint, subtask_metadata)?;
        Ok(TableSubtaskCheckpointMetadata {
            subtask_index: self.task_info().task_index as u32,
            table_type: T::table_type().into(),
            data: result.encode_to_vec(),
        })
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
    fn committing_data(
        config: TableConfig,
        table_metadata: &TableCheckpointMetadata,
    ) -> Option<HashMap<u32, Vec<u8>>>
    where
        Self: Sized,
    {
        let config = Self::checked_proto_decode(config.table_type(), config.config).ok()?;
        let table_metadata =
            Self::checked_proto_decode(table_metadata.table_type(), table_metadata.data.clone())
                .ok()?;
        T::committing_data(config, table_metadata)
    }

    async fn compact_data(
        config: TableConfig,
        compaction_config: &CompactionConfig,
        operator_metadata: &OperatorMetadata,
        current_metadata: TableCheckpointMetadata,
    ) -> Result<Option<TableCheckpointMetadata>> {
        let config = Self::checked_proto_decode(config.table_type(), config.config)?;
        let result = T::compact_data(
            config,
            compaction_config,
            operator_metadata,
            Self::checked_proto_decode(current_metadata.table_type(), current_metadata.data)?,
        )
        .await?;
        Ok(result.map(|result| TableCheckpointMetadata {
            table_type: T::table_type().into(),
            data: result.encode_to_vec(),
        }))
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
    ) -> Result<Option<(Self::SubTableCheckpointMessage, usize)>>;

    fn table_type() -> TableEnum;

    fn subtask_index(&self) -> u32;
}

#[async_trait::async_trait]
pub trait ErasedCheckpointer: Send {
    async fn insert_data(&mut self, data: TableData) -> Result<()>;
    async fn finish(
        mut self: Box<Self>,
        checkpoint: &CheckpointMessage,
    ) -> Result<Option<(TableSubtaskCheckpointMetadata, usize)>>;
}

#[async_trait::async_trait]
impl<T: TableEpochCheckpointer + Sized> ErasedCheckpointer for T {
    async fn insert_data(&mut self, data: TableData) -> Result<()> {
        self.insert_data(data).await
    }

    async fn finish(
        mut self: Box<Self>,
        checkpoint: &CheckpointMessage,
    ) -> Result<Option<(TableSubtaskCheckpointMetadata, usize)>> {
        let subtask_index = self.subtask_index();
        let subtask = (*self).finish(checkpoint).await?;
        Ok(subtask.map(|(metadata, size)| {
            (
                TableSubtaskCheckpointMetadata {
                    subtask_index,
                    table_type: T::table_type().into(),
                    data: metadata.encode_to_vec(),
                },
                size,
            )
        }))
    }
}
