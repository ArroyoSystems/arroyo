use std::{
    collections::HashMap,
    time::{Duration, Instant, SystemTime},
};

use crate::types::public::StopMode as SqlStopMode;
use anyhow::bail;
use arroyo_datastream::Program;
use arroyo_rpc::grpc::{
    worker_grpc_client::WorkerGrpcClient, CheckpointReq, JobFinishedReq, StopExecutionReq, StopMode,
};
use arroyo_state::{BackingStore, StateBackend};
use arroyo_types::{to_micros, WorkerId};

use deadpool_postgres::Pool;

use tokio::{sync::mpsc::Receiver, task::JoinHandle};
use tonic::{transport::Channel, Request};
use tracing::{error, info, warn};

use crate::{queries::controller_queries, JobConfig, JobMessage, RunningMessage};

use self::checkpointer::CheckpointState;

mod checkpointer;

const CHECKPOINTS_TO_KEEP: u32 = 2;
const COMPACT_EVERY: u32 = 2;
const HEARTBEAT_TIMEOUT: Duration = Duration::from_secs(30);

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
        self.last_heartbeat.elapsed() > HEARTBEAT_TIMEOUT
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
    job_id: String,
    state: JobState,
    program: Program,
    checkpoint_state: Option<CheckpointState>,
    epoch: u32,
    min_epoch: u32,
    last_checkpoint: Instant,
    workers: HashMap<WorkerId, WorkerStatus>,
    tasks: HashMap<(String, u32), TaskStatus>,
    operator_parallelism: HashMap<String, usize>,
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
    pub async fn handle_message(&mut self, msg: RunningMessage, pool: &Pool) -> anyhow::Result<()> {
        match msg {
            RunningMessage::TaskCheckpointEvent(c) => {
                if let Some(checkpoint_state) = &mut self.checkpoint_state {
                    if c.epoch != self.epoch {
                        warn!(
                            message = "Received checkpoint event for wrong epoch",
                            epoch = c.epoch,
                            expected = self.epoch,
                            job_id = self.job_id,
                        );
                    } else {
                        checkpoint_state.checkpoint_event(c);
                        checkpoint_state.update_db(pool).await?;
                    }
                } else {
                    warn!(
                        message = "Received checkpoint event but not checkpointing",
                        job_id = self.job_id,
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
                            self.job_id,
                        );
                    } else {
                        checkpoint_state.checkpoint_finished(c).await;
                        checkpoint_state.update_db(pool).await?;
                    }
                } else {
                    warn!(
                        message = "Received checkpoint finished but not checkpointing",
                        job_id = self.job_id
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
                        job_id = self.job_id,
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
                        job_id = self.job_id,
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
                        job_id = self.job_id,
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
                        job_id = self.job_id,
                        worker_id = worker_id.0
                    );
                }
            }
        }

        if self.state == JobState::Running && self.all_tasks_finished() {
            for (_, w) in &mut self.workers {
                if let Err(e) = w.connect.job_finished(JobFinishedReq {}).await {
                    warn!(
                        message = "Failed to connect to work to send job finish",
                        job_id = self.job_id,
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
        pool: &Pool,
        then_stop: bool,
    ) -> anyhow::Result<()> {
        self.epoch += 1;

        info!(
            message = "Starting checkpointing",
            job_id = self.job_id,
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
                }))
                .await?;
        }

        self.checkpoint_state = Some(
            CheckpointState::start(
                self.job_id.clone(),
                &organization_id,
                self.epoch,
                self.min_epoch,
                &self.program,
                &pool,
            )
            .await?,
        );

        Ok(())
    }

    pub async fn finish_checkpoint_if_done(&mut self, pool: &Pool) -> anyhow::Result<()> {
        if self.checkpoint_state.as_ref().unwrap().done() {
            let state = self.checkpoint_state.take().unwrap();
            let duration = state
                .start_time
                .elapsed()
                .unwrap_or(Duration::ZERO)
                .as_secs_f32();
            state.finish(pool).await?;
            self.last_checkpoint = Instant::now();
            self.checkpoint_state = None;
            info!(
                message = "Finished checkpointing",
                job_id = self.job_id,
                epoch = self.epoch,
                duration
            );
        }
        Ok(())
    }

    pub fn compaction_needed(&self) -> Option<u32> {
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
                    job_id = self.job_id,
                    worker_id = worker.0
                );
                return true;
            }
        }

        for ((operator_id, subtask), status) in &self.tasks {
            if let TaskState::Failed(reason) = &status.state {
                error!(
                    message = "task failed",
                    job_id = self.job_id,
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
    pool: Pool,
    config: JobConfig,
    model: RunningJobModel,
    compacting_task: Option<JoinHandle<anyhow::Result<u32>>>,
}

impl<'a> std::fmt::Debug for JobController {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("JobController")
            .field("config", &self.config)
            .field("model", &self.model)
            .field("compacting", &self.compacting_task.is_some())
            .finish()
    }
}

pub enum ControllerProgress {
    Continue,
    Finishing,
}

impl JobController {
    pub fn new(
        pool: Pool,
        config: JobConfig,
        program: Program,
        epoch: u32,
        min_epoch: u32,
        worker_connects: HashMap<WorkerId, WorkerGrpcClient<Channel>>,
    ) -> Self {
        Self {
            pool,
            model: RunningJobModel {
                job_id: config.id.clone(),
                state: JobState::Running,
                checkpoint_state: None,
                epoch,
                min_epoch,
                last_checkpoint: Instant::now(),
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
                operator_parallelism: program
                    .graph
                    .node_weights()
                    .map(|node| (node.operator_id.clone(), node.parallelism))
                    .collect(),
                program,
            },
            config,
            compacting_task: None,
        }
    }

    pub async fn handle_message(&mut self, msg: RunningMessage) -> anyhow::Result<()> {
        self.model.handle_message(msg, &self.pool).await
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
        if self.compacting_task.is_some() && self.compacting_task.as_ref().unwrap().is_finished() {
            let task = self.compacting_task.take().unwrap();

            match task.await {
                Ok(Ok(min_epoch)) => {
                    info!(
                        message = "setting new min epoch",
                        min_epoch,
                        job_id = self.config.id
                    );
                    self.model.min_epoch = min_epoch;
                }
                Ok(Err(e)) => {
                    error!(
                        message = "compaction failed",
                        job_id = self.config.id,
                        error = format!("{:?}", e)
                    );
                }
                Err(e) => {
                    error!(
                        message = "compaction panicked",
                        job_id = self.config.id,
                        error = format!("{:?}", e)
                    );
                }
            }
        }

        if let Some(new_epoch) = self.model.compaction_needed() {
            if self.compacting_task.is_none() && self.model.checkpoint_state.is_none() {
                self.compacting_task = Some(self.start_compaction(new_epoch));
            }
        }

        // check on checkpointing
        if self.model.checkpoint_state.is_some() {
            self.model.finish_checkpoint_if_done(&self.pool).await?;
        } else if self.model.last_checkpoint.elapsed() > self.config.checkpoint_interval
            && self.compacting_task.is_none()
        {
            // or do we need to start checkpointing?
            self.checkpoint(false).await?;
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

    pub async fn checkpoint(&mut self, then_stop: bool) -> anyhow::Result<()> {
        self.model
            .start_checkpoint(&self.config.organization_id, &self.pool, then_stop)
            .await
    }

    pub fn finished(&self) -> bool {
        self.model.all_tasks_finished()
    }

    pub async fn checkpoint_finished(&mut self) -> anyhow::Result<bool> {
        if self.model.checkpoint_state.is_some() {
            self.model.finish_checkpoint_if_done(&self.pool).await?;
        }
        Ok(self.model.checkpoint_state.is_none())
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
                    self.model.handle_message(msg, &self.pool).await?;
                }
                JobMessage::ConfigUpdate(c) => {
                    if c.stop_mode == SqlStopMode::immediate {
                        info!(
                            message = "stopping job immediately",
                            job_id = self.config.id
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

    pub fn get_workers(&self) -> Vec<WorkerId> {
        self.model.workers.keys().map(|k| *k).collect()
    }

    pub fn operator_parallelism(&self, op: &str) -> Option<usize> {
        self.model.operator_parallelism.get(op).map(|p| *p)
    }

    fn start_compaction(&self, new_min: u32) -> JoinHandle<anyhow::Result<u32>> {
        let min_epoch = self.model.min_epoch;
        let job_id = self.config.id.clone();
        let pool = self.pool.clone();

        info!(message = "Starting compaction", job_id, min_epoch, new_min);
        let start = Instant::now();
        let cur_epoch = self.model.epoch;

        tokio::spawn(async move {
            let checkpoint = StateBackend::load_checkpoint_metadata(&job_id, cur_epoch)
                .await
                .ok_or_else(|| {
                    anyhow::anyhow!("Couldn't find checkpoint for job during compaction")
                })?;

            let c = pool.get().await?;
            controller_queries::mark_compacting()
                .bind(&c, &job_id, &(min_epoch as i32), &(new_min as i32))
                .await?;

            StateBackend::compact_checkpoint(checkpoint, min_epoch, new_min).await?;

            controller_queries::mark_checkpoints_compacted()
                .bind(&c, &job_id, &(new_min as i32))
                .await?;

            info!(
                message = "Finished compaction",
                job_id,
                min_epoch,
                new_min,
                duration = start.elapsed().as_secs()
            );

            Ok(new_min)
        })
    }
}
