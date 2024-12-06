use std::any::Any;

use std::{collections::HashMap, sync::Arc, time::SystemTime};

use anyhow::{anyhow, bail, Result};
use arroyo_rpc::CompactionResult;
use arroyo_rpc::{
    grpc::rpc::{
        SubtaskCheckpointMetadata, TableConfig, TableEnum, TableSubtaskCheckpointMetadata,
    },
    CheckpointCompleted, ControlResp,
};
use arroyo_storage::StorageProviderRef;
use arroyo_types::{from_micros, to_micros, CheckpointBarrier, Data, Key, TaskInfo};
use tokio::sync::{
    mpsc::{self, Receiver, Sender},
    oneshot,
};

use crate::{
    get_storage_provider, tables::global_keyed_map::GlobalKeyedTable, BackingStore, StateBackend,
    StateMessage,
};
use crate::{CheckpointMessage, TableData};
use arroyo_rpc::grpc::rpc::CheckpointMetadata;
use tracing::{debug, error, info, warn};

use super::expiring_time_key_map::{
    ExpiringTimeKeyTable, ExpiringTimeKeyView, KeyTimeView, LastKeyValueView,
};
use super::global_keyed_map::GlobalKeyedView;
use super::{ErasedCheckpointer, ErasedTable};

#[allow(unused)]
pub struct TableManager {
    epoch: u32,
    min_epoch: u32,
    // ordered by table, then epoch.
    tables: HashMap<String, Arc<dyn ErasedTable>>,
    writer: BackendWriter,
    task_info: Arc<TaskInfo>,
    storage: StorageProviderRef,
    caches: HashMap<String, Box<dyn Any + Send>>,
}

pub struct BackendWriter {
    sender: Sender<StateMessage>,
    finish_rx: Option<oneshot::Receiver<()>>,
    // TODO: compaction
}

#[allow(unused)]
pub struct BackendFlusher {
    queue: Receiver<StateMessage>,
    storage: StorageProviderRef,
    control_tx: Sender<ControlResp>,
    finish_tx: Option<oneshot::Sender<()>>,
    task_info: Arc<TaskInfo>,
    tables: HashMap<String, Arc<dyn ErasedTable>>,
    table_configs: HashMap<String, TableConfig>,
    table_checkpointers: HashMap<String, Box<dyn ErasedCheckpointer>>,
    current_epoch: u32,
    last_epoch_checkpoints: HashMap<String, TableSubtaskCheckpointMetadata>,
}

impl BackendFlusher {
    fn start(mut self) {
        tokio::spawn(async move {
            loop {
                match self.flush_iteration().await {
                    Ok(continue_flushing) => {
                        if !continue_flushing {
                            return;
                        }
                    }
                    Err(err) => {
                        error!("Failed to flush state file: {:?}", err);
                        self.control_tx
                            .send(ControlResp::TaskFailed {
                                node_id: self.task_info.node_id,
                                task_index: self.task_info.task_index as usize,
                                error: err.to_string(),
                            })
                            .await
                            .unwrap();
                        return;
                    }
                }
            }
        });
    }

    async fn flush_iteration(&mut self) -> Result<bool> {
        let mut checkpoint_epoch = None;

        for (table_name, checkpointer) in &self.tables {
            let epoch_checkpointer = checkpointer.epoch_checkpointer(
                self.current_epoch,
                self.last_epoch_checkpoints.remove(table_name),
            )?;
            self.table_checkpointers
                .insert(table_name.clone(), epoch_checkpointer);
        }
        self.last_epoch_checkpoints.clear();
        let mut compacted_tables = None;

        // accumulate writes in the RecordBatchBuilders until we get a checkpoint
        while checkpoint_epoch.is_none() {
            tokio::select! {
                op = self.queue.recv() => {
                    match op {
                        Some(StateMessage::Checkpoint(checkpoint)) => {
                            checkpoint_epoch = Some(checkpoint);
                        }
                        Some(StateMessage::Compaction(compacted_tables_message)) => {
                            compacted_tables = Some(compacted_tables_message);
                        }
                        Some(StateMessage::TableData { table, data }) => {
                            self.table_checkpointers
                                .get_mut(&table).expect("checkpointer should be there")
                                .insert_data(data).await?
                        },
                        None => {
                            debug!("Parquet flusher closed");
                            return Ok(false);
                        }
                    }
                }
            }
        }
        let Some(cp) = checkpoint_epoch else {
            bail!("somehow exited loop without checkpoint_epoch being set");
        };
        let mut metadatas = HashMap::new();
        let mut bytes = 0;
        for (table_name, checkpointer) in self.table_checkpointers.drain() {
            if let Some((subtask_checkpoint_data, size)) = checkpointer.finish(&cp).await? {
                metadatas.insert(table_name.clone(), subtask_checkpoint_data);
                bytes += size;
            }
        }

        if let Some(compaction_metas) = compacted_tables {
            for (table_name, compacted_metadata) in compaction_metas {
                let table = self.tables.get(&table_name).unwrap();
                let Some(compacted_metadata) =
                    table.subtask_metadata_from_table(compacted_metadata)?
                else {
                    continue;
                };
                if let Some(current_metadata) = metadatas.get(&table_name) {
                    let new_metadata = table.apply_compacted_checkpoint(
                        self.current_epoch,
                        compacted_metadata,
                        current_metadata.clone(),
                    )?;
                    metadatas.insert(table_name, new_metadata);
                } else {
                    warn!("received compaction map for operator {} table {} but no metadata. no checkpoint emitted, as we trust the subtask. map is {:?}", self.task_info.operator_id, table_name, compacted_metadata);
                }
            }
        }
        self.last_epoch_checkpoints = metadatas.clone();
        self.current_epoch += 1;

        // send controller the subtask metadata
        let subtask_metadata = SubtaskCheckpointMetadata {
            subtask_index: self.task_info.task_index,
            start_time: to_micros(cp.time),
            finish_time: to_micros(SystemTime::now()),
            watermark: cp.watermark.map(to_micros),
            table_metadata: metadatas,
            table_configs: self.table_configs.clone(),
            bytes: bytes as u64,
        };
        self.control_tx
            .send(ControlResp::CheckpointCompleted(CheckpointCompleted {
                checkpoint_epoch: cp.epoch,
                node_id: self.task_info.node_id,
                operator_id: self.task_info.operator_id.clone(),
                subtask_metadata,
            }))
            .await?;
        if cp.then_stop {
            self.finish_tx
                .take()
                .unwrap()
                .send(())
                .map_err(|_| anyhow::anyhow!("can't send finish"))?;
            return Ok(false);
        }
        Ok(true)
    }
}

impl BackendWriter {
    fn new(
        task_info: Arc<TaskInfo>,
        control_tx: Sender<ControlResp>,
        table_configs: HashMap<String, TableConfig>,
        tables: HashMap<String, Arc<dyn ErasedTable>>,
        storage: StorageProviderRef,
        current_epoch: u32,
        last_epoch_checkpoints: HashMap<String, TableSubtaskCheckpointMetadata>,
    ) -> Self {
        let (tx, rx) = mpsc::channel(1024 * 1024);
        let (finish_tx, finish_rx) = oneshot::channel();

        (BackendFlusher {
            queue: rx,
            storage,
            control_tx,
            finish_tx: Some(finish_tx),
            task_info,
            tables,
            table_configs,
            current_epoch,
            table_checkpointers: HashMap::new(),
            last_epoch_checkpoints,
        })
        .start();

        Self {
            sender: tx,
            finish_rx: Some(finish_rx),
        }
    }
}

impl TableManager {
    pub async fn load(
        task_info: Arc<TaskInfo>,
        table_configs: HashMap<String, TableConfig>,
        tx: Sender<ControlResp>,
        restore_from: Option<&CheckpointMetadata>,
    ) -> Result<(Self, Option<SystemTime>)> {
        let (watermark, checkpoint_metadata) = if let Some(metadata) = restore_from {
            let (watermark, operator_metadata) = {
                let metadata = StateBackend::load_operator_metadata(
                    &task_info.job_id,
                    &task_info.operator_id,
                    metadata.epoch,
                )
                .await
                .expect("lookup should succeed")
                .expect("require metadata");
                (
                    metadata
                        .operator_metadata
                        .as_ref()
                        .unwrap()
                        .min_watermark
                        .map(from_micros),
                    metadata,
                )
            };

            (watermark, Some(operator_metadata))
        } else {
            (None, None)
        };

        let storage = get_storage_provider().await?;

        let tables = table_configs
            .iter()
            .map(|(table_name, table_config)| {
                let table_restore_from = checkpoint_metadata.as_ref().and_then(|metadata| {
                    metadata.table_checkpoint_metadata.get(table_name).cloned()
                });
                let erased_table = match table_config.table_type() {
                    TableEnum::MissingTableType => bail!("should have table type"),
                    TableEnum::GlobalKeyValue => {
                        Arc::new(<GlobalKeyedTable as ErasedTable>::from_config(
                            table_config.clone(),
                            task_info.clone(),
                            storage.clone(),
                            table_restore_from,
                        )?) as Arc<dyn ErasedTable>
                    }
                    TableEnum::ExpiringKeyedTimeTable => {
                        Arc::new(<ExpiringTimeKeyTable as ErasedTable>::from_config(
                            table_config.clone(),
                            task_info.clone(),
                            storage.clone(),
                            table_restore_from,
                        )?) as Arc<dyn ErasedTable>
                    }
                };
                Ok((table_name.to_string(), erased_table))
            })
            .collect::<Result<HashMap<_, _>>>()?;

        let epoch;
        let min_epoch;
        let mut last_epoch_checkpoints = HashMap::new();
        match checkpoint_metadata {
            Some(metadata) => {
                // TODO: validate this logic.
                let Some(operator_metadata) = metadata.operator_metadata else {
                    bail!("missing operator metadata");
                };
                epoch = operator_metadata.epoch + 1;
                min_epoch = operator_metadata.epoch;
                for (table, table_metadata) in metadata.table_checkpoint_metadata.clone() {
                    let table_implementation = tables
                        .get(&table)
                        .ok_or_else(|| anyhow!("missing table {}", table))?;
                    if let Some(metadata) =
                        table_implementation.subtask_metadata_from_table(table_metadata)?
                    {
                        last_epoch_checkpoints.insert(table.clone(), metadata);
                    }
                }
            }
            None => {
                epoch = 1;
                min_epoch = 1;
            }
        }

        let writer = BackendWriter::new(
            task_info.clone(),
            tx,
            table_configs,
            tables.clone(),
            storage.clone(),
            epoch,
            last_epoch_checkpoints,
        );
        Ok((
            Self {
                epoch,
                min_epoch,
                tables,
                writer,
                task_info,
                storage: Arc::clone(storage),
                caches: HashMap::new(),
            },
            watermark,
        ))
    }

    pub async fn checkpoint(&mut self, barrier: CheckpointBarrier, watermark: Option<SystemTime>) {
        self.writer
            .sender
            .send(StateMessage::Checkpoint(CheckpointMessage {
                epoch: barrier.epoch,
                time: barrier.timestamp,
                watermark,
                then_stop: barrier.then_stop,
            }))
            .await
            .expect("should be able to send checkpoint");

        if barrier.then_stop {
            match self.writer.finish_rx.take().unwrap().await {
                Ok(_) => info!("finished stopping checkpoint"),
                Err(err) => warn!("error waiting for stopping checkpoint {:?}", err),
            }
        }
    }

    pub async fn load_compacted(&mut self, compacted: &CompactionResult) -> Result<()> {
        if compacted.operator_id != self.task_info.operator_id {
            bail!("shouldn't be loading compaction for other operator");
        }
        self.writer
            .sender
            .send(StateMessage::Compaction(compacted.compacted_tables.clone()))
            .await?;
        Ok(())
    }

    pub async fn insert_committing_data(&mut self, table: &str, data: Vec<u8>) -> Result<()> {
        self.writer
            .sender
            .send(StateMessage::TableData {
                table: table.to_string(),
                data: TableData::CommitData { data },
            })
            .await?;
        Ok(())
    }

    pub async fn get_global_keyed_state<K: Key, V: Data>(
        &mut self,
        table_name: &str,
    ) -> Result<&mut GlobalKeyedView<K, V>> {
        // this is done because populating it is async, so can't use or_insert().
        if let std::collections::hash_map::Entry::Vacant(e) =
            self.caches.entry(table_name.to_string())
        {
            let table_implementation = self
                .tables
                .get(table_name)
                .ok_or_else(|| anyhow!("no registered table {}", table_name))?;
            let global_keyed_table = table_implementation
                .as_any()
                .downcast_ref::<GlobalKeyedTable>()
                .ok_or_else(|| anyhow!("wrong table type for table {}", table_name))?;
            let saved_data = global_keyed_table
                .memory_view::<K, V>(self.writer.sender.clone())
                .await?;
            let cache: Box<dyn Any + Send> = Box::new(saved_data);
            e.insert(cache);
        }

        let cache = self.caches.get_mut(table_name).unwrap();
        let cache: &mut GlobalKeyedView<K, V> = cache.downcast_mut().ok_or_else(|| {
            anyhow!(
                "Failed to downcast table {} to key type {} and value type {}",
                table_name,
                std::any::type_name::<K>(),
                std::any::type_name::<V>()
            )
        })?;
        Ok(cache)
    }

    pub async fn get_expiring_time_key_table(
        &mut self,
        table_name: &str,
        watermark: Option<SystemTime>,
    ) -> Result<&mut ExpiringTimeKeyView> {
        if let std::collections::hash_map::Entry::Vacant(e) =
            self.caches.entry(table_name.to_string())
        {
            let table_implementation = self
                .tables
                .get(table_name)
                .ok_or_else(|| anyhow!("no registered table {}", table_name))?;
            let expiring_time_key_table = table_implementation
                .as_any()
                .downcast_ref::<ExpiringTimeKeyTable>()
                .ok_or_else(|| anyhow!("wrong table type for table {}", table_name))?;
            let saved_data = expiring_time_key_table
                .get_view(self.writer.sender.clone(), watermark)
                .await?;
            let cache: Box<dyn Any + Send> = Box::new(saved_data);
            e.insert(cache);
        }
        let cache = self.caches.get_mut(table_name).unwrap();
        let cache: &mut ExpiringTimeKeyView = cache
            .downcast_mut()
            .ok_or_else(|| anyhow!("Failed to downcast table {}", table_name))?;
        Ok(cache)
    }

    pub async fn get_key_time_table(
        &mut self,
        table_name: &str,
        watermark: Option<SystemTime>,
    ) -> Result<&mut KeyTimeView> {
        if let std::collections::hash_map::Entry::Vacant(e) =
            self.caches.entry(table_name.to_string())
        {
            let table_implementation = self
                .tables
                .get(table_name)
                .ok_or_else(|| anyhow!("no registered table {}", table_name))?;
            let expiring_time_key_table = table_implementation
                .as_any()
                .downcast_ref::<ExpiringTimeKeyTable>()
                .ok_or_else(|| anyhow!("wrong table type for table {}", table_name))?;
            let saved_data = expiring_time_key_table
                .get_key_time_view(self.writer.sender.clone(), watermark)
                .await?;
            let cache: Box<dyn Any + Send> = Box::new(saved_data);
            e.insert(cache);
        }
        let cache = self.caches.get_mut(table_name).unwrap();
        let cache: &mut KeyTimeView = cache
            .downcast_mut()
            .ok_or_else(|| anyhow!("Failed to downcast table {}", table_name))?;
        Ok(cache)
    }

    pub async fn get_last_key_value_table(
        &mut self,
        table_name: &str,
        watermark: Option<SystemTime>,
    ) -> Result<&mut LastKeyValueView> {
        if let std::collections::hash_map::Entry::Vacant(e) =
            self.caches.entry(table_name.to_string())
        {
            let table_implementation = self
                .tables
                .get(table_name)
                .ok_or_else(|| anyhow!("no registered table {}", table_name))?;
            let expiring_time_key_table = table_implementation
                .as_any()
                .downcast_ref::<ExpiringTimeKeyTable>()
                .ok_or_else(|| anyhow!("wrong table type for table {}", table_name))?;
            let saved_data = expiring_time_key_table
                .get_last_key_value_view(self.writer.sender.clone(), watermark)
                .await?;
            let cache: Box<dyn Any + Send> = Box::new(saved_data);
            e.insert(cache);
        }
        let cache = self.caches.get_mut(table_name).unwrap();
        let cache: &mut LastKeyValueView = cache
            .downcast_mut()
            .ok_or_else(|| anyhow!("Failed to downcast table {}", table_name))?;
        Ok(cache)
    }
}
