use crate::job_controller::model::{
    CheckpointingOrCommittingState, JobState, RunningJobModel, TaskState, TaskStatus, WorkerState,
    WorkerStatus,
};
use crate::job_controller::{
    JobControllerStatus, RetireWorkerLeader, RunningMessage, TaskFailedEvent, WorkerContext,
    connect_to_worker,
};
use anyhow::{anyhow, bail};
use arroyo_datastream::logical::LogicalProgram;
use arroyo_rpc::grpc::rpc;
use arroyo_rpc::grpc::rpc::{
    CheckpointManifest, CommitReq, GetWorkerPhaseReq, JobControllerInitReq, JobFailure, JobStatus,
    JobStopMode, OperatorCommitData, StopExecutionReq, StopMode, TableCommitData, TableEnum,
    TaskAssignment, TaskCheckpointEventType, WorkerPhase,
};
use arroyo_rpc::log_event;
use arroyo_server_common::shutdown::ShutdownGuard;
use arroyo_state::get_storage_provider;
use arroyo_state::tables::ErasedTable;
use arroyo_state::tables::expiring_time_key_map::ExpiringTimeKeyTable;
use arroyo_state::tables::global_keyed_map::GlobalKeyedTable;
use arroyo_state_protocol::ProtocolPaths;
use arroyo_state_protocol::types::{CheckpointRef, Generation};
use arroyo_state_protocol::workflow::{
    CommitPermit, CommittedMarkerOutcome, GenerationInitialization, GenerationRecovery,
    InitializeGenerationRequest, complete_commit, initialize_generation,
};
use arroyo_types::WorkerId;
use futures::future::try_join_all;
use std::collections::HashSet;
use std::sync::{Arc, Mutex};
use std::time::SystemTime;
use std::{
    collections::HashMap,
    time::{Duration, Instant},
};
use tokio::time::interval;
use tokio::{sync::mpsc::Receiver, task::JoinHandle};
use tonic::Request;
use tracing::{debug, error, info, warn};

pub struct WorkerJobController {
    worker_context: WorkerContext,
    checkpoint_interval: Duration,
    status: JobControllerStatus,
    model: RunningJobModel,
    cleanup_task: Option<JoinHandle<anyhow::Result<u32>>>,
    rx: Receiver<RunningMessage>,
    stopping: bool,
    failing: bool,
    final_checkpoint_started: bool,
    replay_commits: Option<(CommitReq, CommitPermit)>,
}

const FAILURE_CLEANUP_TIMEOUT: Duration = Duration::from_secs(60);

impl std::fmt::Debug for WorkerJobController {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("JobController")
            .field("model", &self.model)
            .field("cleaning", &self.cleanup_task.is_some())
            .finish()
    }
}

pub enum ControllerProgress {
    Continue,
    Finishing,
    Finished,
    Stopped,
    TaskFailed(TaskFailedEvent),
}

impl WorkerJobController {
    #[allow(clippy::too_many_arguments)]
    pub async fn init(
        worker_context: WorkerContext,
        program: Arc<LogicalProgram>,
        tasks: &[TaskAssignment],
        epoch: u64,
        min_epoch: u64,
        rx: Receiver<RunningMessage>,
        job_status: Arc<Mutex<JobStatus>>,
        checkpoint_interval: Duration,
        parent_ref: Option<(CheckpointRef, CheckpointManifest)>,
    ) -> anyhow::Result<Self> {
        info!(job_id =? worker_context.job_id,
            restore_from =? parent_ref.as_ref().map(|p| &p.0),
            "starting job leader");

        let mut worker_connects = HashMap::new();
        let mut workers = HashMap::new();

        for t in tasks {
            workers
                .entry(WorkerId(t.worker_id))
                .or_insert_with(|| t.worker_rpc.clone());
        }

        let futures = workers.into_iter().map(|(id, addr)| async move {
            let connect = connect_to_worker(id, addr).await?;
            anyhow::Ok((id, connect))
        });

        let mut workers = HashMap::new();

        for (id, connect) in try_join_all(futures).await? {
            worker_connects.insert(id, connect.clone());
            workers.insert(
                id,
                WorkerStatus {
                    id,
                    connect,
                    last_heartbeat: Instant::now(),
                    state: WorkerState::Running,
                },
            );
        }

        let tasks = program
            .graph
            .node_weights()
            .flat_map(|node| {
                (0..node.parallelism).map(|idx| {
                    (
                        (node.node_id, idx as u32),
                        TaskStatus {
                            state: TaskState::Running,
                        },
                    )
                })
            })
            .collect();

        let status = JobControllerStatus { job_status };
        status.transition(rpc::JobState::JobRunning)?;

        // this is also happening on the controller, so the generation manifest should already have
        // been created -- ideally we'd just do it here, but the worker initialization process makes
        // that challenging, because we need the restoration point to initialize the workers
        let (generation_manifest, recovery) = match initialize_generation(
            get_storage_provider().await?.as_ref(),
            InitializeGenerationRequest {
                pipeline_id: worker_context.pipeline_id.clone(),
                job_id: worker_context.job_id.clone(),
                generation: Generation(worker_context.generation),
                updated_at: SystemTime::now(),
            },
            false,
        )
        .await?
        {
            GenerationInitialization::Initialized {
                generation_manifest,
                recovery,
            } => (generation_manifest, recovery),
            GenerationInitialization::StaleGeneration { current_generation } => {
                return Err(RetireWorkerLeader {
                    reason: format!(
                        "leader generation {} is stale; current generation is {}",
                        worker_context.generation, current_generation.0
                    ),
                }
                .into());
            }
            GenerationInitialization::StopOrphaned { canonical_ref } => {
                return Err(RetireWorkerLeader {
                    reason: format!(
                        "leader attempted to restore an orphaned checkpoint; canonical checkpoint is {canonical_ref:?}"
                    ),
                }
                .into());
            }
            GenerationInitialization::Failed(e) => {
                bail!("failed to resolve generation manifest: {:?}", e);
            }
        };

        let (parent_ref, replay_commits) = match recovery {
            GenerationRecovery::NoCheckpoint => {
                // nothing to do -- make sure that we're not inconsistent with the controller /
                // other workers
                assert!(
                    parent_ref.is_none(),
                    "from controller, we believe that we should be restoring from {:?}, but \
                        our own query of checkpoint state lacks a parent ref",
                    parent_ref
                );
                (None, None)
            }
            GenerationRecovery::Ready { checkpoint_ref } => {
                if !parent_ref
                    .as_ref()
                    .map(|(p, _)| *p == checkpoint_ref)
                    .unwrap_or(false)
                {
                    panic!(
                        "from controller, we believe we should be restoring from {:?}, but\
                    generation manifest has {:?}",
                        parent_ref, checkpoint_ref
                    );
                }
                (Some(checkpoint_ref), None)
            }
            GenerationRecovery::ReplayCommit {
                checkpoint_ref,
                commit_permit,
            } => {
                if let Some((r, manifest)) = parent_ref {
                    assert_eq!(r, checkpoint_ref, "mismatched recovery ref");

                    (
                        Some(r),
                        Some((manifest_into_commit_req(manifest)?, commit_permit)),
                    )
                } else {
                    panic!(
                        "from controller, expected not to be restoring a checkpoint, but\
                    generation manifest wants to replay commits from {:?}",
                        checkpoint_ref
                    );
                }
            }
        };

        Ok(Self {
            checkpoint_interval,
            model: RunningJobModel {
                pipeline_id: worker_context.pipeline_id.clone(),
                job_id: worker_context.job_id.clone(),
                generation: worker_context.generation,
                state: JobState::Running,
                checkpoint_state: None,
                epoch: epoch as u32,
                min_epoch: min_epoch as u32,
                last_checkpoint: Instant::now(),
                workers,
                tasks,
                operator_parallelism: program.tasks_per_node(),
                program,
                metric_update_task: None,
                last_updated_metrics: Instant::now(),
                protocol_paths: ProtocolPaths::new(
                    worker_context.pipeline_id.clone(),
                    worker_context.job_id.clone(),
                ),
                checkpoint_parent_ref: parent_ref,
                checkpoint_spans: vec![],
                worker_leader_mode: true,
                finished_operators: vec![],
                generation_manifest: Some(generation_manifest),
            },
            status,
            worker_context,
            cleanup_task: None,
            rx,
            stopping: false,
            failing: false,
            final_checkpoint_started: false,
            replay_commits,
        })
    }

    pub fn start(self, shutdown: &ShutdownGuard) {
        shutdown.spawn_task("job_controller", async move { self.start_inner().await });
    }

    pub async fn start_inner(mut self) -> anyhow::Result<()> {
        // determine if we need to commit from our recovery checkpoint

        // initialize workers
        let futures = self.model.workers.iter_mut().map(|(id, status)| {
            let id = *id;
            let mut connect = status.connect.clone();
            async move {
                loop {
                    let phase = connect.get_worker_phase(Request::new(GetWorkerPhaseReq {})).await?
                        .into_inner();
                    match phase.phase() {
                        WorkerPhase::Idle | WorkerPhase::Initializing => {
                            // continue
                        }
                        WorkerPhase::Running => {
                            bail!("worker {:?} unexpectedly entered Running phase before being initialized by the job controller", id);
                        }
                        WorkerPhase::Failed => {
                            bail!("worker {:?} was in Failed state during startup with error: {:?}", id, phase.error_message);
                        }
                        WorkerPhase::WaitingOnLeader => {
                            break;
                        }
                    }

                    tokio::time::sleep(Duration::from_millis(50)).await;
                }

                // TODO: this really should happen after we've replayed the initial commit, but
                //  that requires a significant reworking on the worker lifecycle. However, Committing
                //  concurrently with startup is how the existing controller has always worked, so in
                //  practice it doesn't seem to be likely that workers outrun the commit, but it is
                //  possible and we should look at fixing this at some point in the future when we
                //  remove the legacy code paths.
                status.connect.job_controller_init(JobControllerInitReq {}).await?;

                Ok(())
            }
        });

        try_join_all(futures).await?;

        // before starting, we need to replay our commits from the previous checkpoint
        if let Some((commit_req, commit_permit)) = self.replay_commits.take() {
            let futures = self
                .model
                .workers
                .iter_mut()
                .map(|(_, status)| status.connect.commit(commit_req.clone()));

            try_join_all(futures).await?;

            let mut subtasks_to_commit = HashSet::new();

            for (op, data) in &commit_req.committing_data {
                for (_, t) in &data.committing_data {
                    for subtask in t.commit_data_by_subtask.keys() {
                        subtasks_to_commit.insert((op.clone(), *subtask));
                    }
                }
            }

            // now wait for ack's
            info!(job_id =? self.worker_context.job_id, "waiting for subtasks to commit...");
            // no timeout here, we rely on the controller to kill us if we take too longer to initialize
            while !subtasks_to_commit.is_empty() {
                tokio::select! {
                    msg = self.rx.recv() => {
                        match msg {
                            Some(RunningMessage::TaskCheckpointEvent(event))
                            if event.event_type() == TaskCheckpointEventType::FinishedCommit => {
                                debug!(job_id = *self.worker_context.job_id, generation =? self.worker_context.generation, "task finished committing {:?}", event);
                                let key = (event.operator_id, event.subtask_idx);
                                subtasks_to_commit.remove(&key);
                            }
                            Some(RunningMessage::TaskCheckpointEvent(t)) => {
                                warn!(job_id = *self.worker_context.job_id, generation =? self.worker_context.generation, event =? t,
                                    "received unexpected TaskCheckpointEvent while waiting for \
                                    initial commit replay to complete");
                            }
                            Some(RunningMessage::TaskCheckpointFinished(t)) => {
                                warn!(job_id = *self.worker_context.job_id, generation =? self.worker_context.generation, event =? t,
                                    "received unexpected TaskCheckpointFinished while waiting for \
                                    initial commit replay to complete");
                            }
                            Some(RunningMessage::TaskFailed(event)) => {
                                log_event!("task_error", {
                                    "service": "controller",
                                    "job_id": *self.worker_context.job_id,
                                    "operator_id": event.operator_id,
                                    "subtask_index": event.subtask_idx,
                                    "error": event.reason,
                                    "domain": event.error_domain.as_str(),
                                });
                                self.fail_job(event.into()).await?;
                                return Ok(());
                            }
                            Some(RunningMessage::WorkerHeartbeat { .. }) => {
                                // ignore heart beats
                            }
                            w @ Some(RunningMessage::WorkerFinished { ..} | RunningMessage::TaskFinished { .. }) => {
                                bail!("received unexpected finishing message while waiting for initial \
                                commit replay to complete: {:?}", w);
                            }
                            Some(RunningMessage::Stop {stop_mode }) => {
                                self.handle_stop(stop_mode).await?;
                                break;
                            }
                            None => {
                                bail!("leader job queue closed while waiting for initial commit\
                                replay to complete");
                            }
                        }
                    }
                }
            }

            info!(job_id =? self.worker_context.job_id, "completing initial commit replay");

            // now we can finalize the commit
            match complete_commit(
                get_storage_provider().await?.as_ref(),
                &commit_permit,
                Generation(self.worker_context.generation),
            )
            .await?
            {
                CommittedMarkerOutcome::Created => {
                    info!(job_id = ?self.worker_context.job_id, checkpoint_ref =? commit_permit.checkpoint_ref(), "finalized replayed commit");
                }
                CommittedMarkerOutcome::AlreadyCommitted => {
                    info!(job_id = ?self.worker_context.job_id, checkpoint_ref =? commit_permit.checkpoint_ref(), "replayed commit already finalized");
                }
            }
        }

        let mut interval = interval(Duration::from_millis(200));
        loop {
            tokio::select! {
                msg = self.rx.recv() => {
                    match msg {
                        Some(RunningMessage::Stop { stop_mode }) => {
                            self.handle_stop(stop_mode).await?;
                        }
                        Some(running) => {
                            if let Err(err) = self.model.handle_message(running, &self.status).await {
                                if let Some(retire) = err.downcast_ref::<RetireWorkerLeader>() {
                                    self.retire_job(retire.reason.clone()).await?;
                                    return Ok(());
                                }

                                return Err(err);
                            }
                        }
                        None => {
                            return Ok(())
                        }
                    }
                }
                _ = interval.tick() => {
                    match self.progress().await {
                        Ok(ControllerProgress::Continue) => {
                            // do nothing
                        },
                        Ok(ControllerProgress::Finishing) => {
                            self.status.transition(rpc::JobState::JobFinishing)?;
                        },
                        Ok(ControllerProgress::Finished) => {
                            self.status.transition(rpc::JobState::JobFinished)?;
                        }
                        Ok(ControllerProgress::Stopped) => {
                            self.status.transition(rpc::JobState::JobStopped)?;
                            return Ok(());
                        }
                        Ok(ControllerProgress::TaskFailed(event)) => {
                            log_event!("task_error", {
                                "service": "controller",
                                "job_id": *self.worker_context.job_id,
                                "operator_id": event.operator_id,
                                "subtask_index": event.subtask_idx,
                                "error": event.reason,
                                "domain": event.error_domain.as_str(),
                            });

                            self.fail_job(event.into()).await?;
                            return Ok(());
                        }
                        Err(err) => {
                            if let Some(retire) = err.downcast_ref::<RetireWorkerLeader>() {
                                self.retire_job(retire.reason.clone()).await?;
                                return Ok(());
                            }

                            error!(message = "error while running", error = format!("{:?}", err), job_id = *self.worker_context.job_id);
                            log_event!("running_error", {
                                "service": "controller",
                                "job_id": *self.worker_context.job_id,
                                "error": format!("{:?}", err),
                            });

                            self.fail_job(JobFailure {
                                operator_id: None,
                                task_id: None,
                                subtask_index: None,
                                message: err.to_string(),
                                error_domain: rpc::ErrorDomain::Internal.into(),
                                retry_hint: rpc::RetryHint::WithBackoff.into(),
                            })
                            .await?;
                            return Ok(());
                        }
                    }
                }
            }
        }
    }

    pub async fn handle_message(&mut self, msg: RunningMessage) -> anyhow::Result<()> {
        self.model.handle_message(msg, &self.status).await
    }

    async fn handle_stop(&mut self, stop_mode: JobStopMode) -> anyhow::Result<()> {
        info!(
            message = "handling stop request",
            job_id = *self.worker_context.job_id,
            stop_mode = ?stop_mode,
            already_stopping = self.stopping,
        );

        if self.stopping {
            if stop_mode == JobStopMode::JobStopImmediate {
                self.stop_job(StopMode::Immediate).await?;
            }
            return Ok(());
        }

        self.stopping = true;
        self.status.transition(rpc::JobState::JobStopping)?;

        match stop_mode {
            JobStopMode::JobStopCheckpoint => {
                if self.checkpoint(true).await? {
                    self.final_checkpoint_started = true;
                }
            }
            JobStopMode::JobStopGraceful => {
                self.stop_job(StopMode::Graceful).await?;
            }
            JobStopMode::JobStopImmediate => {
                self.stop_job(StopMode::Immediate).await?;
            }
        }

        Ok(())
    }

    pub async fn progress(&mut self) -> anyhow::Result<ControllerProgress> {
        // have any of our workers failed?
        if self.model.worker_timedout() {
            bail!("worker failed");
        }

        // have any tasks failed?
        if let Some(event) = self.model.task_failed() {
            return Ok(ControllerProgress::TaskFailed(event));
        }

        // if we're stopping and all tasks have finished, we're done
        if self.stopping && self.model.all_tasks_finished() {
            return Ok(ControllerProgress::Stopped);
        }

        // have any of our tasks finished?
        if !self.stopping && self.model.any_finished_sources() {
            return Ok(ControllerProgress::Finishing);
        }

        if !self.stopping && self.model.all_tasks_finished() {
            return Ok(ControllerProgress::Finished);
        }

        if matches!(
            self.status.job_status.lock().unwrap().job_state(),
            rpc::JobState::JobFailing | rpc::JobState::JobFailed
        ) {
            return Ok(ControllerProgress::Continue);
        }

        // check on cleanup
        if self.cleanup_task.is_some() && self.cleanup_task.as_ref().unwrap().is_finished() {
            let task = self.cleanup_task.take().unwrap();

            match task.await {
                Ok(Ok(min_epoch)) => {
                    info!(
                        message = "setting new min epoch",
                        min_epoch,
                        job_id = *self.worker_context.job_id
                    );
                    self.model.min_epoch = min_epoch;
                }
                Ok(Err(e)) => {
                    error!(
                        message = "cleanup failed",
                        job_id = *self.worker_context.job_id,
                        error = format!("{:?}", e)
                    );

                    // wait a bit before trying again
                    tokio::time::sleep(Duration::from_millis(100)).await;
                }
                Err(e) => {
                    error!(
                        message = "cleanup panicked",
                        job_id = *self.worker_context.job_id,
                        error = format!("{:?}", e)
                    );

                    // wait a bit before trying again
                    tokio::time::sleep(Duration::from_millis(100)).await;
                }
            }
        }

        // TODO: cleaning
        // if !self.stopping
        //     && let Some(new_epoch) = self.model.cleanup_needed()
        //     && self.cleanup_task.is_none()
        //     && self.model.checkpoint_state.is_none()
        // {
        //     self.cleanup_task = Some(self.start_cleanup(new_epoch));
        // }

        if self.stopping && !self.final_checkpoint_started && self.model.checkpoint_state.is_none()
        {
            info!(
                message = "retrying deferred final checkpoint",
                job_id = *self.worker_context.job_id
            );
            self.model.start_checkpoint(&self.status, true).await?;
            self.final_checkpoint_started = true;
        }

        // check on checkpointing
        if self.model.checkpoint_state.is_some() {
            self.model.finish_checkpoint_if_done(&self.status).await?;
        } else if !self.stopping
            && self.model.last_checkpoint.elapsed() > self.checkpoint_interval
            && self.cleanup_task.is_none()
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

    async fn best_effort_stop_job(&mut self, stop_mode: StopMode) {
        for c in self.model.workers.values_mut() {
            if let Err(e) = c
                .connect
                .stop_execution(StopExecutionReq {
                    stop_mode: stop_mode as i32,
                })
                .await
            {
                warn!(
                    message = "failed to stop worker during cleanup",
                    job_id = *self.worker_context.job_id,
                    worker_id = c.id.0,
                    error = format!("{:?}", e),
                );
            }
        }
    }

    async fn fail_job(&mut self, failure: JobFailure) -> anyhow::Result<()> {
        if !self.failing {
            self.failing = true;
            self.status.to_failing(failure)?;
            self.best_effort_stop_job(StopMode::Immediate).await;
        }

        let cleanup = async {
            while !self.model.all_tasks_finished() {
                let Some(msg) = self.rx.recv().await else {
                    break;
                };

                match msg {
                    RunningMessage::Stop { .. } => {
                        self.best_effort_stop_job(StopMode::Immediate).await;
                    }
                    msg => {
                        if let Err(e) = self.model.handle_message(msg, &self.status).await {
                            warn!(
                                message = "ignoring error while cleaning up failed job",
                                job_id = *self.worker_context.job_id,
                                error = format!("{:?}", e),
                            );
                        }
                    }
                }
            }
        };

        if tokio::time::timeout(FAILURE_CLEANUP_TIMEOUT, cleanup)
            .await
            .is_err()
        {
            warn!(
                message = "timed out waiting for failed job to stop",
                job_id = *self.worker_context.job_id,
            );
        }

        self.status.transition(rpc::JobState::JobFailed)?;
        Ok(())
    }

    async fn retire_job(&mut self, reason: String) -> anyhow::Result<()> {
        info!(
            message = "retiring worker leader",
            job_id = *self.worker_context.job_id,
            generation = self.worker_context.generation,
            reason,
        );

        self.best_effort_stop_job(StopMode::Immediate).await;
        self.status.retire()?;
        Ok(())
    }

    pub async fn checkpoint(&mut self, then_stop: bool) -> anyhow::Result<bool> {
        if self.model.checkpoint_state.is_none() {
            self.model.start_checkpoint(&self.status, then_stop).await?;
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
            self.model.finish_checkpoint_if_done(&self.status).await?;
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

    pub async fn wait_for_finish(
        &mut self,
        rx: &mut Receiver<RunningMessage>,
    ) -> anyhow::Result<()> {
        loop {
            if self.model.all_tasks_finished() {
                return Ok(());
            }

            match rx
                .recv()
                .await
                .ok_or_else(|| anyhow::anyhow!("channel closed while receiving"))?
            {
                RunningMessage::Stop { stop_mode } => {
                    if stop_mode == JobStopMode::JobStopImmediate {
                        info!(
                            message = "stopping job immediately",
                            job_id = *self.worker_context.job_id
                        );
                        self.stop_job(StopMode::Immediate).await?;
                    }
                }
                msg => {
                    self.model.handle_message(msg, &self.status).await?;
                }
            }
        }
    }

    pub fn operator_parallelism(&self, node_id: u32) -> Option<usize> {
        self.model.operator_parallelism.get(&node_id).cloned()
    }
}

fn manifest_into_commit_req(manifest: CheckpointManifest) -> anyhow::Result<CommitReq> {
    let mut committing_data: HashMap<String, OperatorCommitData> = HashMap::new();

    for mut operator in manifest.operators {
        let operator_metadata = operator
            .operator_metadata
            .ok_or_else(|| anyhow!("operator checkpoint metadata is missing operator metadata"))?;

        let mut operator_commit_data: HashMap<String, TableCommitData> = HashMap::new();

        for (table_name, table_metadata) in operator.table_checkpoint_metadata {
            let config = operator.table_configs.remove(&table_name).ok_or_else(|| {
                anyhow!("table config for {table_name} not found in checkpoint manifest")
            })?;

            let commit_data = match config.table_type() {
                TableEnum::MissingTableType => bail!("missing table type"),
                TableEnum::GlobalKeyValue => {
                    GlobalKeyedTable::committing_data(config, &table_metadata)
                }
                TableEnum::ExpiringKeyedTimeTable => {
                    ExpiringTimeKeyTable::committing_data(config, &table_metadata)
                }
            };

            if let Some(commit_data) = commit_data {
                operator_commit_data.insert(
                    table_name,
                    TableCommitData {
                        commit_data_by_subtask: commit_data,
                    },
                );
            }
        }

        if !operator_commit_data.is_empty() {
            committing_data.insert(
                operator_metadata.operator_id,
                OperatorCommitData {
                    committing_data: operator_commit_data,
                },
            );
        }
    }

    Ok(CommitReq {
        epoch: manifest.epoch as u32,
        committing_data,
    })
}
