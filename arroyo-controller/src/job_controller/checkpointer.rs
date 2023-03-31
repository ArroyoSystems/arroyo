use std::{
    collections::{BTreeMap, HashMap, HashSet},
    time::SystemTime,
};

use crate::queries::controller_queries;
use arroyo_datastream::Program;
use arroyo_rpc::grpc::{
    self,
    api::{self, OperatorCheckpointDetail},
    backend_data, BackendData, CheckpointMetadata, OperatorCheckpointMetadata,
    SubtaskCheckpointMetadata, TableDescriptor, TaskCheckpointCompletedReq, TaskCheckpointEventReq,
    TaskCheckpointEventType,
};
use arroyo_state::{BackingStore, StateBackend};
use arroyo_types::{from_micros, to_micros};
use deadpool_postgres::Pool;
use time::OffsetDateTime;
use tracing::{debug, info, warn};

struct SubtaskState {
    start_time: Option<SystemTime>,
    finish_time: Option<SystemTime>,
    metadata: Option<SubtaskCheckpointMetadata>,
}

impl SubtaskState {
    pub fn new() -> Self {
        Self {
            start_time: None,
            finish_time: None,
            metadata: None,
        }
    }

    pub fn event(&mut self, c: TaskCheckpointEventReq) {
        if c.event_type() == TaskCheckpointEventType::StartedCheckpointing {
            self.start_time = Some(from_micros(c.time));
        }
    }

    pub fn finish(&mut self, c: TaskCheckpointCompletedReq) {
        self.finish_time = Some(from_micros(c.time));
        self.metadata = Some(c.metadata.unwrap());
    }

    pub fn done(&self) -> bool {
        self.finish_time.is_some()
    }
}

pub struct CheckpointState {
    job_id: String,
    checkpoint_id: i64,
    epoch: u32,
    min_epoch: u32,
    pub start_time: SystemTime,
    tasks_per_operator: HashMap<String, usize>,
    tasks: HashMap<String, BTreeMap<u32, SubtaskState>>,
    completed_operators: HashSet<String>,

    // Used for the web ui -- eventually should be replaced with some other way of tracking / reporting
    // this data
    operator_details: HashMap<String, OperatorCheckpointDetail>,
}

impl CheckpointState {
    pub async fn start(
        job_id: String,
        organization_id: &str,
        epoch: u32,
        min_epoch: u32,
        program: &Program,
        pool: &Pool,
    ) -> anyhow::Result<Self> {
        let tasks_per_operator: HashMap<String, usize> = program
            .graph
            .node_weights()
            .map(|n| (n.operator_id.clone(), n.parallelism))
            .collect();

        info!(message = "Starting checkpointing", job_id, epoch);

        let start = OffsetDateTime::now_utc();

        let checkpoint_id = {
            let c = pool.get().await?;
            controller_queries::create_checkpoint()
                .bind(
                    &c,
                    &organization_id,
                    &job_id,
                    &StateBackend::name().to_string(),
                    &(epoch as i32),
                    &(min_epoch as i32),
                    &start,
                )
                .one()
                .await?
        };

        Ok(Self {
            job_id,
            checkpoint_id,
            epoch,
            min_epoch,
            start_time: SystemTime::now(),
            tasks_per_operator,
            tasks: HashMap::new(),
            completed_operators: HashSet::new(),
            operator_details: HashMap::new(),
        })
    }

    pub fn checkpoint_event(&mut self, c: TaskCheckpointEventReq) {
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
                } as i32,
            });

        // this is for the actual checkpoint management
        self.tasks
            .entry(c.operator_id.clone())
            .or_default()
            .entry(c.subtask_index)
            .or_insert_with(SubtaskState::new)
            .event(c);
    }

    pub async fn checkpoint_finished(&mut self, c: TaskCheckpointCompletedReq) {
        // this is just for the UI
        let metadata = c.metadata.as_ref().unwrap();

        let detail = self
            .operator_details
            .entry(c.operator_id.clone())
            .or_insert_with(|| OperatorCheckpointDetail {
                operator_id: c.operator_id.clone(),
                start_time: metadata.start_time,
                finish_time: None,
                has_state: metadata.has_state,
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
        detail.finish_time = Some(metadata.finish_time);

        // this is for the actual checkpoint management

        if self.completed_operators.contains(&c.operator_id) {
            warn!(
                "Received checkpoint completed message for already finished operator {}",
                c.operator_id
            );
            return;
        }

        let subtasks = self
            .tasks
            .get_mut(&c.operator_id)
            .unwrap_or_else(|| panic!("Received finish event without start for {}", c.operator_id));

        let total_tasks = *self.tasks_per_operator.get(&c.operator_id).unwrap();

        let operator_id = c.operator_id.clone();
        let idx = c.metadata.as_ref().unwrap().subtask_index;
        subtasks
            .get_mut(&idx)
            .unwrap_or_else(|| {
                panic!(
                    "Received finish event without start for {}-{}",
                    c.operator_id, idx
                )
            })
            .finish(c);

        if subtasks.len() == total_tasks && subtasks.values().all(|c| c.done()) {
            debug!("Finishing checkpoint for operator {}", operator_id);

            let start_time = subtasks
                .values()
                .map(|s| s.start_time.unwrap())
                .min()
                .unwrap();
            let finish_time = subtasks
                .values()
                .map(|s| s.finish_time.unwrap())
                .max()
                .unwrap();

            let min_watermark = subtasks
                .values()
                .map(|s| s.metadata.as_ref().unwrap().watermark)
                .min()
                .unwrap();

            let max_watermark = subtasks
                .values()
                .map(|s| s.metadata.as_ref().unwrap().watermark)
                .max()
                .unwrap();
            let has_state = subtasks
                .values()
                .any(|s| s.metadata.as_ref().unwrap().has_state);

            let tables: HashMap<String, TableDescriptor> = subtasks
                .values()
                .flat_map(|t| t.metadata.as_ref().unwrap().tables.clone())
                .map(|t| (t.name.clone(), t))
                .collect();

            // the sort here is load-bearing
            let backend_data: BTreeMap<(u32, String), BackendData> = subtasks
                .iter()
                .map(|(epoch, s)| (*epoch, s.metadata.as_ref().unwrap()))
                .filter(|(_epoch, metadata)| metadata.has_state)
                .flat_map(|(epoch, metadata)| {
                    metadata
                        .backend_data
                        .iter()
                        .map(move |backend_data| (epoch, backend_data.clone()))
                })
                .filter_map(|(epoch, backend_data)| {
                    Self::backend_data_to_map_pair(epoch, backend_data)
                })
                .collect();

            let size = subtasks
                .values()
                .fold(0, |size, s| size + s.metadata.as_ref().unwrap().bytes);

            StateBackend::complete_operator_checkpoint(OperatorCheckpointMetadata {
                job_id: self.job_id.to_string(),
                operator_id: operator_id.clone(),
                epoch: self.epoch,
                start_time: to_micros(start_time),
                finish_time: to_micros(finish_time),
                min_watermark,
                max_watermark,
                has_state,
                tables: tables.into_values().collect(),
                backend_data: backend_data.into_values().collect(),
                bytes: size,
            })
            .await;

            if let Some(op) = self.operator_details.get_mut(&operator_id) {
                op.finish_time = Some(to_micros(finish_time));
            }

            self.completed_operators.insert(operator_id);
        }
    }

    fn backend_data_to_map_pair(
        epoch: u32,
        backend_data: BackendData,
    ) -> Option<((u32, String), BackendData)> {
        let Some(internal_data) = &backend_data.backend_data else {
            return None
        };
        match &internal_data {
            backend_data::BackendData::ParquetStore(data) => {
                Some(((epoch, data.file.clone()), backend_data))
            }
        }
    }

    pub fn done(&self) -> bool {
        self.completed_operators.len() == self.tasks_per_operator.len()
    }

    pub async fn update_db(&self, pool: &Pool) -> anyhow::Result<()> {
        let c = pool.get().await?;

        controller_queries::update_checkpoint()
            .bind(
                &c,
                &serde_json::to_value(&self.operator_details).unwrap(),
                &None,
                &crate::types::public::CheckpointState::inprogress,
                &self.checkpoint_id,
            )
            .await?;

        Ok(())
    }

    pub async fn finish(self, pool: &Pool) -> anyhow::Result<()> {
        let finish_time = SystemTime::now();
        StateBackend::complete_checkpoint(CheckpointMetadata {
            job_id: self.job_id,
            epoch: self.epoch,
            start_time: to_micros(self.start_time),
            finish_time: to_micros(finish_time),
            min_epoch: self.min_epoch,
            operator_ids: self.completed_operators.iter().cloned().collect(),
        })
        .await;

        let operator_state = serde_json::to_value(&self.operator_details).unwrap();

        let c = pool.get().await?;
        controller_queries::update_checkpoint()
            .bind(
                &c,
                &operator_state,
                &Some(OffsetDateTime::now_utc()),
                &crate::types::public::CheckpointState::ready,
                &self.checkpoint_id,
            )
            .await?;

        Ok(())
    }
}
