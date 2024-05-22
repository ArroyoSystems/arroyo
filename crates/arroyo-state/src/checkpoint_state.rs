use std::sync::Arc;
use std::{
    collections::{HashMap, HashSet},
    time::SystemTime,
};

use anyhow::{anyhow, bail, Result};
use arroyo_rpc::grpc::{
    self,
    api::{self, OperatorCheckpointDetail},
    CheckpointMetadata, OperatorCheckpointMetadata, OperatorMetadata, SubtaskCheckpointMetadata,
    TableCheckpointMetadata, TableConfig, TableEnum, TableSubtaskCheckpointMetadata,
    TaskCheckpointCompletedReq, TaskCheckpointEventReq,
};
use arroyo_types::{from_micros, to_micros};
use tracing::{debug, warn};

use crate::{
    committing_state::CommittingState,
    tables::{
        expiring_time_key_map::ExpiringTimeKeyTable, global_keyed_map::GlobalKeyedTable,
        ErasedTable,
    },
    BackingStore, StateBackend,
};

#[derive(Debug, Clone)]
pub struct CheckpointState {
    job_id: Arc<String>,
    checkpoint_id: String,
    epoch: u32,
    min_epoch: u32,
    start_time: SystemTime,
    operators: usize,
    operators_checkpointed: usize,
    operator_state: HashMap<String, OperatorState>,
    subtasks_to_commit: HashSet<(String, u32)>,
    // map of operator_id -> table_name -> subtask_index -> Data
    commit_data: HashMap<String, HashMap<String, HashMap<u32, Vec<u8>>>>,

    // Used for the web ui -- eventually should be replaced with some other way of tracking / reporting
    // this data
    pub operator_details: HashMap<String, OperatorCheckpointDetail>,
}

#[derive(Debug, Clone)]
pub struct OperatorState {
    subtasks: usize,
    subtasks_checkpointed: usize,
    pub start_time: Option<SystemTime>,
    pub finish_time: Option<SystemTime>,
    table_state: HashMap<String, TableState>,
    watermarks: Vec<Option<SystemTime>>,
}

impl OperatorState {
    fn new(subtasks: usize) -> Self {
        OperatorState {
            subtasks,
            subtasks_checkpointed: 0,
            start_time: None,
            finish_time: None,
            table_state: HashMap::new(),
            watermarks: vec![],
        }
    }

    fn finish_subtask(
        &mut self,
        c: SubtaskCheckpointMetadata,
    ) -> Option<(
        HashMap<String, TableConfig>,
        HashMap<String, TableCheckpointMetadata>,
    )> {
        self.subtasks_checkpointed += 1;
        self.watermarks.push(c.watermark.map(from_micros));
        self.start_time = match self.start_time {
            Some(existing_start_time) => Some(existing_start_time.min(from_micros(c.start_time))),
            None => Some(from_micros(c.start_time)),
        };
        self.finish_time = match self.finish_time {
            Some(existing_finish_time) => {
                Some(existing_finish_time.max(from_micros(c.finish_time)))
            }
            None => Some(from_micros(c.finish_time)),
        };
        for (table, table_metadata) in c.table_metadata {
            self.table_state
                .entry(table)
                .or_insert_with_key(|key| TableState {
                    table_config: c
                        .table_configs
                        .get(key)
                        .expect("should have metadata")
                        .clone(),
                    subtask_tables: HashMap::new(),
                })
                .subtask_tables
                .insert(table_metadata.subtask_index, table_metadata);
        }

        if self.subtasks == self.subtasks_checkpointed {
            let (table_configs, table_metadatas) = self
                .table_state
                .drain()
                .filter_map(|(table_name, table_state)| {
                    table_state
                        .into_table_metadata()
                        .map(|(table_config, metadata)| {
                            ((table_name.clone(), table_config), (table_name, metadata))
                        })
                })
                .unzip();
            Some((table_configs, table_metadatas))
        } else {
            None
        }
    }
}

#[derive(Debug, Clone)]
pub struct TableState {
    table_config: TableConfig,
    subtask_tables: HashMap<u32, TableSubtaskCheckpointMetadata>,
}

impl TableState {
    fn into_table_metadata(self) -> Option<(TableConfig, TableCheckpointMetadata)> {
        match self.table_config.table_type() {
            TableEnum::MissingTableType => unreachable!(),
            TableEnum::GlobalKeyValue => GlobalKeyedTable::merge_checkpoint_metadata(
                self.table_config.clone(),
                self.subtask_tables,
            )
            .expect("should be able to merge checkpoints"),
            TableEnum::ExpiringKeyedTimeTable => ExpiringTimeKeyTable::merge_checkpoint_metadata(
                self.table_config.clone(),
                self.subtask_tables,
            )
            .expect("should be able to merge checkpoint metadatas"),
        }
        .map(|metadata| (self.table_config, metadata))
    }
}

impl CheckpointState {
    pub fn new(
        job_id: Arc<String>,
        checkpoint_id: String,
        epoch: u32,
        min_epoch: u32,
        tasks_per_operator: HashMap<String, usize>,
    ) -> Self {
        Self {
            job_id,
            checkpoint_id,
            epoch,
            min_epoch,
            start_time: SystemTime::now(),
            operators: tasks_per_operator.len(),
            operators_checkpointed: 0,
            operator_state: tasks_per_operator
                .into_iter()
                .map(|(operator_id, subtasks)| (operator_id, OperatorState::new(subtasks)))
                .collect(),
            subtasks_to_commit: HashSet::new(),
            commit_data: HashMap::new(),
            operator_details: HashMap::new(),
        }
    }

    pub fn checkpoint_id(&self) -> &str {
        &self.checkpoint_id
    }

    pub fn start_time(&self) -> SystemTime {
        self.start_time
    }

    pub fn checkpoint_event(&mut self, c: TaskCheckpointEventReq) -> anyhow::Result<()> {
        debug!(message = "Checkpoint event", checkpoint_id = self.checkpoint_id, event_type = ?c.event_type(), subtask_index = c.subtask_index, operator_id = ?c.operator_id);

        if grpc::TaskCheckpointEventType::FinishedCommit == c.event_type() {
            bail!(
                "shouldn't receive finished commit {:?} while checkpointing",
                c
            );
        }

        // This is all for the UI
        self.operator_details
            .entry(c.operator_id.clone())
            .or_insert_with(|| OperatorCheckpointDetail {
                operator_id: c.operator_id.clone(),
                start_time: c.time,
                finish_time: None,
                has_state: false,
                tasks: HashMap::new(),
            })
            .tasks
            .entry(c.subtask_index)
            .or_insert_with(|| api::TaskCheckpointDetail {
                subtask_index: c.subtask_index,
                start_time: c.time,
                finish_time: None,
                bytes: None,
                events: vec![],
            })
            .events
            .push(api::TaskCheckpointEvent {
                time: c.time,
                event_type: match c.event_type() {
                    grpc::TaskCheckpointEventType::StartedAlignment => {
                        api::TaskCheckpointEventType::AlignmentStarted
                    }
                    grpc::TaskCheckpointEventType::StartedCheckpointing => {
                        api::TaskCheckpointEventType::CheckpointStarted
                    }
                    grpc::TaskCheckpointEventType::FinishedOperatorSetup => {
                        api::TaskCheckpointEventType::CheckpointOperatorFinished
                    }
                    grpc::TaskCheckpointEventType::FinishedSync => {
                        api::TaskCheckpointEventType::CheckpointSyncFinished
                    }
                    grpc::TaskCheckpointEventType::FinishedCommit => {
                        api::TaskCheckpointEventType::CheckpointPreCommit
                    }
                } as i32,
            });
        Ok(())
    }

    pub async fn checkpoint_finished(&mut self, c: TaskCheckpointCompletedReq) -> Result<()> {
        debug!(
            message = "Checkpoint finished", 
            checkpoint_id = self.checkpoint_id,
            job_id = *self.job_id,
            epoch = self.epoch,
            min_epoch = self.min_epoch,
            operator_id = %c.operator_id,
            subtask_index = c.metadata.as_ref().unwrap().subtask_index,
            time = c.time);
        // TODO: UI management
        let metadata = c
            .metadata
            .as_ref()
            .ok_or_else(|| anyhow!("missing metadata for operator {}", c.operator_id))?;
        let detail = self
            .operator_details
            .entry(c.operator_id.clone())
            .or_insert_with(|| OperatorCheckpointDetail {
                operator_id: c.operator_id.clone(),
                start_time: metadata.start_time,
                finish_time: None,
                has_state: false,
                tasks: HashMap::new(),
            })
            .tasks
            .entry(metadata.subtask_index)
            .or_insert_with(|| {
                warn!(
                    "Received checkpoint completion but no start event {:?}",
                    metadata
                );
                api::TaskCheckpointDetail {
                    subtask_index: metadata.subtask_index,
                    start_time: metadata.start_time,
                    finish_time: None,
                    bytes: None,
                    events: vec![],
                }
            });
        detail.bytes = Some(metadata.bytes);

        let operator_state = self
            .operator_state
            .get_mut(&c.operator_id)
            .ok_or_else(|| anyhow!("unexpected operator checkpoint {}", c.operator_id))?;
        if let Some((table_configs, table_checkpoint_metadata)) = operator_state.finish_subtask(
            c.metadata
                .ok_or_else(|| anyhow!("missing metadata for operator {}", c.operator_id))?,
        ) {
            self.operators_checkpointed += 1;
            // watermarks are None if any subtasks are None.
            let (min_watermark, max_watermark) =
                if operator_state.watermarks.iter().any(|w| w.is_none()) {
                    (None, None)
                } else {
                    (
                        operator_state
                            .watermarks
                            .iter()
                            .map(|w| to_micros(w.unwrap()))
                            .min(),
                        operator_state
                            .watermarks
                            .iter()
                            .map(|w| to_micros(w.unwrap()))
                            .max(),
                    )
                };
            for (table, checkpoint_metadata) in table_checkpoint_metadata.iter() {
                let config = table_configs
                    .get(table)
                    .expect("should have a config for the table");
                if let Some(committing_data) = match config.table_type() {
                    TableEnum::MissingTableType => bail!("missing table type"),
                    TableEnum::GlobalKeyValue => {
                        GlobalKeyedTable::committing_data(config.clone(), checkpoint_metadata)
                    }
                    TableEnum::ExpiringKeyedTimeTable => {
                        ExpiringTimeKeyTable::committing_data(config.clone(), checkpoint_metadata)
                    }
                } {
                    for i in 0..operator_state.subtasks_checkpointed {
                        self.subtasks_to_commit
                            .insert((c.operator_id.clone(), i as u32));
                    }
                    self.commit_data
                        .entry(c.operator_id.clone())
                        .or_default()
                        .insert(table.clone(), committing_data);
                }
            }
            StateBackend::write_operator_checkpoint_metadata(OperatorCheckpointMetadata {
                start_time: to_micros(operator_state.start_time.unwrap()),
                finish_time: to_micros(operator_state.finish_time.unwrap()),
                table_checkpoint_metadata,
                table_configs,
                operator_metadata: Some(OperatorMetadata {
                    job_id: self.job_id.to_string(),
                    operator_id: c.operator_id,
                    epoch: self.epoch,
                    min_watermark,
                    max_watermark,
                    parallelism: operator_state.subtasks_checkpointed as u64,
                }),
            })
            .await
            .expect("Should be able to write operator checkpoint metadata");
        }
        Ok(())
    }

    pub fn done(&self) -> bool {
        self.operators == self.operators_checkpointed
    }

    pub fn committing_state(&self) -> CommittingState {
        CommittingState::new(
            self.checkpoint_id.clone(),
            self.subtasks_to_commit.clone(),
            self.commit_data.clone(),
        )
    }

    pub async fn save_state(&self) -> Result<()> {
        let finish_time = SystemTime::now();
        StateBackend::write_checkpoint_metadata(CheckpointMetadata {
            job_id: self.job_id.to_string(),
            epoch: self.epoch,
            min_epoch: self.min_epoch,
            start_time: to_micros(self.start_time),
            finish_time: to_micros(finish_time),
            operator_ids: self
                .operator_state
                .keys()
                .map(|key| key.to_string())
                .collect(),
        })
        .await?;
        Ok(())
    }
}
