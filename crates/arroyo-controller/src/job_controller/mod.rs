use std::str::FromStr;
use std::sync::Arc;
use std::{
    collections::HashMap,
    time::{Duration, Instant, SystemTime},
};

use crate::types::public::StopMode as SqlStopMode;
use anyhow::bail;
use arroyo_rpc::grpc::rpc::{
    worker_grpc_client::WorkerGrpcClient, CheckpointReq, CommitReq, JobFinishedReq, LabelPair,
    LoadCompactedDataReq, MetricsReq, StopExecutionReq, StopMode, TaskCheckpointEventType,
};
use arroyo_state::{BackingStore, StateBackend};
use arroyo_types::{to_micros, WorkerId};
use cornucopia_async::DatabaseSource;
use rand::{thread_rng, Rng};

use time::OffsetDateTime;

use crate::job_controller::job_metrics::{get_metric_name, JobMetrics};
use crate::types::public::CheckpointState as DbCheckpointState;
use crate::{queries::controller_queries, JobConfig, JobMessage, RunningMessage};
use arroyo_datastream::logical::LogicalProgram;
use arroyo_rpc::api_types::metrics::MetricName;
use arroyo_rpc::config::config;
use arroyo_rpc::notify_db;
use arroyo_rpc::public_ids::{generate_id, IdTypes};
use arroyo_state::checkpoint_state::CheckpointState;
use arroyo_state::committing_state::CommittingState;
use arroyo_state::parquet::ParquetBackend;
use tokio::{sync::mpsc::Receiver, task::JoinHandle};
use tonic::{transport::Channel, Request};
use tracing::{debug, error, info, warn};

use self::checkpointer::CheckpointingOrCommittingState;

mod checkpointer;
pub mod job_metrics;

const CHECKPOINTS_TO_KEEP: u32 = 4;
const CHECKPOINT_ROWS_TO_KEEP: u32 = 100;
const COMPACT_EVERY: u32 = 2;

#[derive(Debug, PartialEq, Eq)]
pub enum WorkerState {
    Running,
    Stopped,
}

#[allow(unused)]
pub struct WorkerStatus {
    id: WorkerId,
    connect: WorkerGrpcClient<Channel>,
    last_heartbeat: Instant,
    state: WorkerState,
}

impl WorkerStatus {
    fn heartbeat_timeout(&self) -> bool {
        self.last_heartbeat.elapsed() > *config().pipeline.worker_heartbeat_timeout
    }
}

#[derive(Debug, PartialEq, Eq)]
pub enum TaskState {
    Running,
    Finished,
    Failed(String),
}

#[derive(Debug)]
pub struct TaskStatus {
    state: TaskState,
}

// Stores a model of the current state of a running job to use in the state machine
#[derive(Debug, PartialEq, Eq)]
pub enum JobState {
    Running,
    Stopped,
}

pub struct RunningJobModel {
    job_id: Arc<String>,
    state: JobState,
    program: Arc<LogicalProgram>,
    checkpoint_state: Option<CheckpointingOrCommittingState>,
    epoch: u32,
    min_epoch: u32,
    last_checkpoint: Instant,
    workers: HashMap<WorkerId, WorkerStatus>,
    tasks: HashMap<(String, u32), TaskStatus>,
    operator_parallelism: HashMap<String, usize>,
    metrics: JobMetrics,
    metric_update_task: Option<JoinHandle<()>>,
    last_updated_metrics: Instant,
}

impl std::fmt::Debug for RunningJobModel {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RunningJobModel")
            .field("job_id", &self.job_id)
            .field("state", &self.state)
            .field("checkpointing", &self.checkpoint_state.is_some())
            .field("epoch", &self.epoch)
            .field("min_epoch", &self.min_epoch)
            .field("last_checkpoint", &self.last_checkpoint)
            .finish()
    }
}

impl RunningJobModel {
    pub async fn update_db(
        checkpoint_state: &CheckpointState,
        db: &DatabaseSource,
    ) -> anyhow::Result<()> {
        let c = db.client().await?;

        controller_queries::execute_update_checkpoint(
            &c,
            &serde_json::to_value(&checkpoint_state.operator_details).unwrap(),
            &None,
            &DbCheckpointState::inprogress,
            &checkpoint_state.checkpoint_id(),
        )
        .await?;

        Ok(())
    }

    pub async fn update_checkpoint_in_db(
        checkpoint_state: &CheckpointState,
        db: &DatabaseSource,
        db_checkpoint_state: DbCheckpointState,
    ) -> anyhow::Result<()> {
        let c = db.client().await?;
        let finish_time = if db_checkpoint_state == DbCheckpointState::ready {
            Some(SystemTime::now().into())
        } else {
            None
        };
        let operator_state = serde_json::to_value(&checkpoint_state.operator_details).unwrap();
        controller_queries::execute_update_checkpoint(
            &c,
            &operator_state,
            &finish_time,
            &db_checkpoint_state,
            &checkpoint_state.checkpoint_id(),
        )
        .await?;

        Ok(())
    }

    pub async fn finish_committing(checkpoint_id: &str, db: &DatabaseSource) -> anyhow::Result<()> {
        info!("finishing committing");
        let finish_time = SystemTime::now();

        let c = db.client().await?;
        controller_queries::execute_commit_checkpoint(&c, &finish_time.into(), &checkpoint_id)
            .await?;

        Ok(())
    }

    pub async fn handle_message(
        &mut self,
        msg: RunningMessage,
        db: &DatabaseSource,
    ) -> anyhow::Result<()> {
        match msg {
            RunningMessage::TaskCheckpointEvent(c) => {
                if let Some(checkpoint_state) = &mut self.checkpoint_state {
                    if c.epoch != self.epoch {
                        warn!(
                            message = "Received checkpoint event for wrong epoch",
                            epoch = c.epoch,
                            expected = self.epoch,
                            job_id = *self.job_id,
                        );
                    } else {
                        match checkpoint_state {
                            CheckpointingOrCommittingState::Checkpointing(checkpoint_state) => {
                                checkpoint_state.checkpoint_event(c)?;
                                Self::update_db(checkpoint_state, db).await?
                            }
                            CheckpointingOrCommittingState::Committing(committing_state) => {
                                if matches!(c.event_type(), TaskCheckpointEventType::FinishedCommit)
                                {
                                    committing_state
                                        .subtask_committed(c.operator_id.clone(), c.subtask_index);
                                    self.compact_state().await?;
                                } else {
                                    warn!("unexpected checkpoint event type {:?}", c.event_type())
                                }
                            }
                        };
                    }
                } else {
                    debug!(
                        message = "Received checkpoint event but not checkpointing",
                        job_id = *self.job_id,
                        event = format!("{:?}", c)
                    )
                }
            }
            RunningMessage::TaskCheckpointFinished(c) => {
                if let Some(checkpoint_state) = &mut self.checkpoint_state {
                    if c.epoch != self.epoch {
                        warn!(
                            message = "Received checkpoint finished for wrong epoch",
                            epoch = c.epoch,
                            expected = self.epoch,
                            job_id = *self.job_id,
                        );
                    } else {
                        let CheckpointingOrCommittingState::Checkpointing(checkpoint_state) =
                            checkpoint_state
                        else {
                            bail!("Received checkpoint finished but not checkpointing");
                        };
                        checkpoint_state.checkpoint_finished(c).await?;
                        Self::update_db(checkpoint_state, db).await?;
                    }
                } else {
                    warn!(
                        message = "Received checkpoint finished but not checkpointing",
                        job_id = *self.job_id
                    )
                }
            }
            RunningMessage::TaskFinished {
                worker_id: _,
                time: _,
                operator_id,
                subtask_index,
            } => {
                let key = (operator_id, subtask_index);
                if let Some(status) = self.tasks.get_mut(&key) {
                    status.state = TaskState::Finished;
                } else {
                    warn!(
                        message = "Received task finished for unknown task",
                        job_id = *self.job_id,
                        operator_id = key.0,
                        subtask_index
                    );
                }
            }
            RunningMessage::TaskFailed {
                operator_id,
                subtask_index,
                reason,
                ..
            } => {
                let key = (operator_id, subtask_index);
                if let Some(status) = self.tasks.get_mut(&key) {
                    status.state = TaskState::Failed(reason);
                } else {
                    warn!(
                        message = "Received task failed message for unknown task",
                        job_id = *self.job_id,
                        operator_id = key.0,
                        subtask_index,
                        reason,
                    );
                }
            }
            RunningMessage::WorkerHeartbeat { worker_id, time } => {
                if let Some(worker) = self.workers.get_mut(&worker_id) {
                    worker.last_heartbeat = time;
                } else {
                    warn!(
                        message = "Received heartbeat for unknown worker",
                        job_id = *self.job_id,
                        worker_id = worker_id.0
                    );
                }
            }
            RunningMessage::WorkerFinished { worker_id } => {
                if let Some(worker) = self.workers.get_mut(&worker_id) {
                    worker.state = WorkerState::Stopped;
                } else {
                    warn!(
                        message = "Received finish message for unknown worker",
                        job_id = *self.job_id,
                        worker_id = worker_id.0
                    );
                }
            }
        }

        if self.state == JobState::Running
            && self.all_tasks_finished()
            && self.checkpoint_state.is_none()
        {
            for w in &mut self.workers.values_mut() {
                if let Err(e) = w.connect.job_finished(JobFinishedReq {}).await {
                    warn!(
                        message = "Failed to connect to work to send job finish",
                        job_id = *self.job_id,
                        worker_id = w.id.0,
                        error = format!("{:?}", e),
                    )
                }
            }
            self.state = JobState::Stopped;
        }

        Ok(())
    }

    pub async fn start_checkpoint(
        &mut self,
        organization_id: &str,
        db: &DatabaseSource,
        then_stop: bool,
    ) -> anyhow::Result<()> {
        self.epoch += 1;

        info!(
            message = "Starting checkpointing",
            job_id = *self.job_id,
            epoch = self.epoch,
            then_stop
        );

        // TODO: maybe parallelize
        for worker in self.workers.values_mut() {
            worker
                .connect
                .checkpoint(Request::new(CheckpointReq {
                    epoch: self.epoch,
                    timestamp: to_micros(SystemTime::now()),
                    min_epoch: self.min_epoch,
                    then_stop,
                    is_commit: false,
                }))
                .await?;
        }

        let checkpoint_id = generate_id(IdTypes::Checkpoint);

        let c = db.client().await?;
        controller_queries::execute_create_checkpoint(
            &c,
            &checkpoint_id,
            &organization_id,
            &*self.job_id,
            &StateBackend::name().to_string(),
            &(self.epoch as i32),
            &(self.min_epoch as i32),
            &OffsetDateTime::now_utc(),
        )
        .await?;

        let state = CheckpointState::new(
            self.job_id.clone(),
            checkpoint_id,
            self.epoch,
            self.min_epoch,
            self.program.tasks_per_operator(),
        );

        self.checkpoint_state = Some(CheckpointingOrCommittingState::Checkpointing(state));

        Ok(())
    }

    async fn compact_state(&mut self) -> anyhow::Result<()> {
        if !config().pipeline.compaction.enabled {
            debug!("Compaction is disabled, skipping compaction");
            return Ok(());
        }

        info!(
            message = "Compacting state",
            job_id = *self.job_id,
            epoch = self.epoch,
        );

        let mut worker_clients: Vec<WorkerGrpcClient<Channel>> =
            self.workers.values().map(|w| w.connect.clone()).collect();
        for operator_id in self.operator_parallelism.keys() {
            let compacted_tables = ParquetBackend::compact_operator(
                // compact the operator's state and notify the workers to load the new files
                self.job_id.clone(),
                operator_id.clone(),
                self.epoch,
            )
            .await?;

            if compacted_tables.is_empty() {
                continue;
            }

            // TODO: these should be put on separate tokio tasks.
            for worker_client in &mut worker_clients {
                worker_client
                    .load_compacted_data(LoadCompactedDataReq {
                        operator_id: operator_id.clone(),
                        compacted_metadata: compacted_tables.clone(),
                    })
                    .await?;
            }
        }

        info!(
            message = "Finished compaction",
            job_id = *self.job_id,
            epoch = self.epoch,
        );
        Ok(())
    }

    pub async fn finish_checkpoint_if_done(&mut self, db: &DatabaseSource) -> anyhow::Result<()> {
        if self.checkpoint_state.as_ref().unwrap().done() {
            let state = self.checkpoint_state.take().unwrap();
            match state {
                CheckpointingOrCommittingState::Checkpointing(checkpointing) => {
                    checkpointing.save_state().await?;

                    let committing_state = checkpointing.committing_state();
                    let duration = checkpointing
                        .start_time()
                        .elapsed()
                        .unwrap_or(Duration::ZERO)
                        .as_secs_f32();
                    // shortcut if committing is unnecessary
                    if committing_state.done() {
                        Self::update_checkpoint_in_db(&checkpointing, db, DbCheckpointState::ready)
                            .await?;
                        self.last_checkpoint = Instant::now();
                        self.checkpoint_state = None;
                        self.compact_state().await?;

                        info!(
                            message = "Finished checkpointing",
                            job_id = *self.job_id,
                            epoch = self.epoch,
                            duration
                        );
                        // trigger a DB backup now that we're done checkpointing
                        notify_db();
                    } else {
                        Self::update_checkpoint_in_db(
                            &checkpointing,
                            db,
                            DbCheckpointState::committing,
                        )
                        .await?;
                        let committing_data = committing_state.committing_data();
                        self.checkpoint_state =
                            Some(CheckpointingOrCommittingState::Committing(committing_state));
                        info!(
                            message = "Committing checkpoint",
                            job_id = *self.job_id,
                            epoch = self.epoch,
                        );
                        for worker in self.workers.values_mut() {
                            worker
                                .connect
                                .commit(Request::new(CommitReq {
                                    epoch: self.epoch,
                                    committing_data: committing_data.clone(),
                                }))
                                .await?;
                        }
                    }
                }
                CheckpointingOrCommittingState::Committing(committing) => {
                    Self::finish_committing(committing.checkpoint_id(), db).await?;
                    self.last_checkpoint = Instant::now();
                    self.checkpoint_state = None;
                    info!(
                        message = "Finished committing checkpointing",
                        job_id = *self.job_id,
                        epoch = self.epoch,
                    );
                    // trigger a DB backup now that we're done checkpointing
                    notify_db();
                }
            }
        }
        Ok(())
    }

    pub fn cleanup_needed(&self) -> Option<u32> {
        if self.epoch - self.min_epoch > CHECKPOINTS_TO_KEEP && self.epoch % COMPACT_EVERY == 0 {
            Some(self.epoch - CHECKPOINTS_TO_KEEP)
        } else {
            None
        }
    }

    pub fn failed(&self) -> bool {
        for (worker, status) in &self.workers {
            if status.heartbeat_timeout() {
                error!(
                    message = "worker failed to heartbeat",
                    job_id = *self.job_id,
                    worker_id = worker.0
                );
                return true;
            }
        }

        for ((operator_id, subtask), status) in &self.tasks {
            if let TaskState::Failed(reason) = &status.state {
                error!(
                    message = "task failed",
                    job_id = *self.job_id,
                    operator_id,
                    subtask,
                    reason,
                );
                return true;
            }
        }

        false
    }

    pub fn any_finished_sources(&self) -> bool {
        let source_tasks = self.program.sources();

        self.tasks.iter().any(|((operator, _), t)| {
            source_tasks.contains(operator.as_str()) && t.state == TaskState::Finished
        })
    }

    pub fn all_tasks_finished(&self) -> bool {
        self.tasks
            .iter()
            .all(|(_, t)| t.state == TaskState::Finished)
    }
}

pub struct JobController {
    db: DatabaseSource,
    config: JobConfig,
    model: RunningJobModel,
    cleanup_task: Option<JoinHandle<anyhow::Result<u32>>>,
}

impl std::fmt::Debug for JobController {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("JobController")
            .field("config", &self.config)
            .field("model", &self.model)
            .field("cleaning", &self.cleanup_task.is_some())
            .finish()
    }
}

pub enum ControllerProgress {
    Continue,
    Finishing,
}

impl JobController {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        db: DatabaseSource,
        config: JobConfig,
        program: Arc<LogicalProgram>,
        epoch: u32,
        min_epoch: u32,
        worker_connects: HashMap<WorkerId, WorkerGrpcClient<Channel>>,
        commit_state: Option<CommittingState>,
        metrics: JobMetrics,
    ) -> Self {
        Self {
            db,
            model: RunningJobModel {
                job_id: config.id.clone(),
                state: JobState::Running,
                checkpoint_state: commit_state.map(CheckpointingOrCommittingState::Committing),
                epoch,
                min_epoch,
                // delay the initial checkpoint by a random amount so that on controller restart,
                // checkpoint times are staggered across jobs
                last_checkpoint: Instant::now()
                    + Duration::from_millis(
                        thread_rng().gen_range(0..config.checkpoint_interval.as_millis() as u64),
                    ),
                workers: worker_connects
                    .into_iter()
                    .map(|(id, connect)| {
                        (
                            id,
                            WorkerStatus {
                                id,
                                connect,
                                last_heartbeat: Instant::now(),
                                state: WorkerState::Running,
                            },
                        )
                    })
                    .collect(),
                tasks: program
                    .graph
                    .node_weights()
                    .flat_map(|node| {
                        (0..node.parallelism).map(|idx| {
                            (
                                (node.operator_id.clone(), idx as u32),
                                TaskStatus {
                                    state: TaskState::Running,
                                },
                            )
                        })
                    })
                    .collect(),
                operator_parallelism: program.tasks_per_operator(),
                metrics,
                metric_update_task: None,
                last_updated_metrics: Instant::now(),
                program,
            },
            config,
            cleanup_task: None,
        }
    }

    pub fn update_config(&mut self, config: JobConfig) {
        self.config = config;
    }

    pub async fn handle_message(&mut self, msg: RunningMessage) -> anyhow::Result<()> {
        self.model.handle_message(msg, &self.db).await
    }

    async fn update_metrics(&mut self) {
        if self.model.metric_update_task.is_some()
            && !self
                .model
                .metric_update_task
                .as_ref()
                .unwrap()
                .is_finished()
        {
            return;
        }

        let job_metrics = self.model.metrics.clone();
        let workers: Vec<_> = self
            .model
            .workers
            .iter()
            .filter(|(_, w)| w.state == WorkerState::Running)
            .map(|(id, w)| (*id, w.connect.clone()))
            .collect();
        let program = self.model.program.clone();

        self.model.metric_update_task = Some(tokio::spawn(async move {
            let mut metrics: HashMap<(u32, u32), HashMap<MetricName, u64>> = HashMap::new();

            for (id, mut connect) in workers {
                let Ok(e) = connect.get_metrics(MetricsReq {}).await else {
                    warn!("Failed to collect metrics from worker {:?}", id);
                    return;
                };

                fn find_label<'a>(labels: &'a [LabelPair], name: &'static str) -> Option<&'a str> {
                    Some(
                        labels
                            .iter()
                            .find(|t| t.name.as_ref().map(|t| t == name).unwrap_or(false))?
                            .value
                            .as_ref()?
                            .as_str(),
                    )
                }

                e.into_inner()
                    .metrics
                    .into_iter()
                    .filter_map(|f| Some((get_metric_name(&f.name?)?, f.metric)))
                    .flat_map(|(metric, values)| {
                        let program = program.clone();
                        values.into_iter().filter_map(move |m| {
                            let subtask_idx =
                                u32::from_str(find_label(&m.label, "subtask_idx")?).ok()?;
                            let operator_idx =
                                program.operator_index(find_label(&m.label, "operator_id")?)?;
                            let value = m
                                .counter
                                .map(|c| c.value)
                                .or_else(|| m.gauge.map(|g| g.value))??
                                as u64;
                            Some(((operator_idx, subtask_idx), (metric, value)))
                        })
                    })
                    .for_each(|(subtask_idx, (metric, value))| {
                        metrics
                            .entry(subtask_idx)
                            .or_default()
                            .insert(metric, value);
                    });
            }

            for ((operator_idx, subtask_idx), values) in metrics {
                job_metrics.update(operator_idx, subtask_idx, &values).await;
            }
        }));
    }

    pub async fn progress(&mut self) -> anyhow::Result<ControllerProgress> {
        // have any of our workers failed?
        if self.model.failed() {
            bail!("worker failed");
        }

        // have any of our tasks finished?
        if self.model.any_finished_sources() {
            return Ok(ControllerProgress::Finishing);
        }

        // check on compaction
        if self.cleanup_task.is_some() && self.cleanup_task.as_ref().unwrap().is_finished() {
            let task = self.cleanup_task.take().unwrap();

            match task.await {
                Ok(Ok(min_epoch)) => {
                    info!(
                        message = "setting new min epoch",
                        min_epoch,
                        job_id = *self.config.id
                    );
                    self.model.min_epoch = min_epoch;
                }
                Ok(Err(e)) => {
                    error!(
                        message = "cleanup failed",
                        job_id = *self.config.id,
                        error = format!("{:?}", e)
                    );

                    // wait a bit before trying again
                    tokio::time::sleep(Duration::from_millis(100)).await;
                }
                Err(e) => {
                    error!(
                        message = "cleanup panicked",
                        job_id = *self.config.id,
                        error = format!("{:?}", e)
                    );

                    // wait a bit before trying again
                    tokio::time::sleep(Duration::from_millis(100)).await;
                }
            }
        }

        if let Some(new_epoch) = self.model.cleanup_needed() {
            if self.cleanup_task.is_none() && self.model.checkpoint_state.is_none() {
                self.cleanup_task = Some(self.start_cleanup(new_epoch));
            }
        }

        // check on checkpointing
        if self.model.checkpoint_state.is_some() {
            self.model.finish_checkpoint_if_done(&self.db).await?;
        } else if self.model.last_checkpoint.elapsed() > self.config.checkpoint_interval
            && self.cleanup_task.is_none()
        {
            // or do we need to start checkpointing?
            self.checkpoint(false).await?;
        }

        // update metrics
        if self.model.last_updated_metrics.elapsed() > job_metrics::COLLECTION_RATE {
            self.update_metrics().await;
            self.model.last_updated_metrics = Instant::now();
        }

        Ok(ControllerProgress::Continue)
    }

    pub async fn stop_job(&mut self, stop_mode: StopMode) -> anyhow::Result<()> {
        for c in self.model.workers.values_mut() {
            c.connect
                .stop_execution(StopExecutionReq {
                    stop_mode: stop_mode as i32,
                })
                .await?;
        }

        Ok(())
    }

    pub async fn checkpoint(&mut self, then_stop: bool) -> anyhow::Result<bool> {
        if self.model.checkpoint_state.is_none() {
            self.model
                .start_checkpoint(&self.config.organization_id, &self.db, then_stop)
                .await?;
            Ok(true)
        } else {
            Ok(false)
        }
    }

    pub fn finished(&self) -> bool {
        self.model.all_tasks_finished()
    }

    pub async fn checkpoint_finished(&mut self) -> anyhow::Result<bool> {
        if self.model.checkpoint_state.is_some() {
            self.model.finish_checkpoint_if_done(&self.db).await?;
        }
        Ok(self.model.checkpoint_state.is_none())
    }

    pub async fn send_commit_messages(&mut self) -> anyhow::Result<()> {
        let Some(CheckpointingOrCommittingState::Committing(committing)) =
            &self.model.checkpoint_state
        else {
            bail!("should be committing")
        };
        for worker in self.model.workers.values_mut() {
            worker
                .connect
                .commit(CommitReq {
                    epoch: self.model.epoch,
                    committing_data: committing.committing_data(),
                })
                .await?;
        }
        Ok(())
    }

    pub async fn wait_for_finish(&mut self, rx: &mut Receiver<JobMessage>) -> anyhow::Result<()> {
        loop {
            if self.model.all_tasks_finished() {
                return Ok(());
            }

            match rx
                .recv()
                .await
                .ok_or_else(|| anyhow::anyhow!("channel closed while receiving"))?
            {
                JobMessage::RunningMessage(msg) => {
                    self.model.handle_message(msg, &self.db).await?;
                }
                JobMessage::ConfigUpdate(c) => {
                    if c.stop_mode == SqlStopMode::immediate {
                        info!(
                            message = "stopping job immediately",
                            job_id = *self.config.id
                        );
                        self.stop_job(StopMode::Immediate).await?;
                    }
                }
                _ => {
                    // ignore other messages
                }
            }
        }
    }

    pub fn operator_parallelism(&self, op: &str) -> Option<usize> {
        self.model.operator_parallelism.get(op).cloned()
    }

    fn start_cleanup(&mut self, new_min: u32) -> JoinHandle<anyhow::Result<u32>> {
        let min_epoch = self.model.min_epoch.max(1);
        let job_id = self.config.id.clone();
        let db = self.db.clone();

        info!(
            message = "Starting cleaning",
            job_id = *job_id,
            min_epoch,
            new_min
        );
        let start = Instant::now();
        let cur_epoch = self.model.epoch;

        tokio::spawn(async move {
            let checkpoint = StateBackend::load_checkpoint_metadata(&job_id, cur_epoch).await?;

            controller_queries::execute_mark_compacting(
                &db.client().await?,
                &*job_id,
                &(min_epoch as i32),
                &(new_min as i32),
            )
            .await?;

            StateBackend::cleanup_checkpoint(checkpoint, min_epoch, new_min).await?;

            controller_queries::execute_mark_checkpoints_compacted(
                &db.client().await?,
                &*job_id,
                &(new_min as i32),
            )
            .await?;

            if let Some(epoch_to_filter_before) = min_epoch.checked_sub(CHECKPOINT_ROWS_TO_KEEP) {
                controller_queries::execute_drop_old_checkpoint_rows(
                    &db.client().await?,
                    &*job_id,
                    &(epoch_to_filter_before as i32),
                )
                .await?;
            }

            info!(
                message = "Finished cleaning",
                job_id = *job_id,
                min_epoch,
                new_min,
                duration = start.elapsed().as_secs_f32()
            );

            Ok(new_min)
        })
    }
}
