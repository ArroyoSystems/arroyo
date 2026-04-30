use arroyo_rpc::grpc::rpc::{
    CheckpointManifest, StartExecutionReq, TaskAssignment, worker_grpc_client::WorkerGrpcClient,
};
use arroyo_types::{CLUSTER_ID_ENV, JobId, MachineId, WorkerId};
use std::time::SystemTime;
use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
    time::{Duration, Instant},
};
use tokio::{select, sync::Mutex, task::JoinHandle};
use tonic::{Request, transport::Channel};
use tracing::{debug, error, info, warn};

use super::{JobContext, State, Transition, leader_running::LeaderRunning, running::Running};
use crate::job_controller::checkpoint_store::DbCheckpointMetadataStore;
use crate::job_controller::job_metrics::JobMetrics;
use crate::job_controller::leader_manager::LeaderManager;
use crate::{JobMessage, schedulers::SchedulerError};
use crate::{
    job_controller::JobController, queries::controller_queries, states::stop_if_desired_non_running,
};
use crate::{
    schedulers::StartPipelineReq,
    states::{StateError, fatal},
};
use anyhow::{anyhow, bail};
use arroyo_datastream::logical::LogicalProgram;
use arroyo_rpc::config::{JobControllerMode, config};
use arroyo_rpc::grpc::api;
use arroyo_rpc::grpc_channel_builder;
use arroyo_rpc::worker_types::RunningMessage;
use arroyo_state::{
    BackingStore, StateBackend, get_storage_provider,
    tables::{ErasedTable, global_keyed_map::GlobalKeyedTable},
};
use arroyo_state_protocol::store::read_protobuf;
use arroyo_state_protocol::types::Generation;
use arroyo_state_protocol::workflow::{
    GenerationInitialization, GenerationRecovery, InitializeGenerationRequest,
    initialize_generation,
};
use arroyo_worker::job_controller::committing_state::{CheckpointIdOrRef, CommittingState};

#[derive(Debug, Clone)]
struct WorkerStatus {
    id: WorkerId,
    machine_id: MachineId,
    rpc_address: String,
    data_address: String,
    slots: usize,
    state: WorkerState,
}

#[derive(Debug, Clone)]
enum WorkerState {
    Connected,
    Initializing,
    Ready,
    Failed,
}

#[derive(Debug)]
pub struct Scheduling {}

fn slots_for_job(job: &LogicalProgram) -> usize {
    job.graph
        .node_weights()
        .map(|n| n.parallelism)
        .max()
        .unwrap_or(0)
}

fn compute_assignments(
    workers: Vec<&WorkerStatus>,
    program: &LogicalProgram,
) -> Vec<TaskAssignment> {
    let mut assignments = vec![];
    for node in program.graph.node_weights() {
        let mut worker_idx = 0;
        let mut current_count = 0;

        for i in 0..node.parallelism {
            assignments.push(TaskAssignment {
                task_id: node.node_id,
                subtask_idx: i as u32,
                worker_id: workers[worker_idx].id.0,
                worker_addr: workers[worker_idx].data_address.clone(),
                worker_rpc: workers[worker_idx].rpc_address.clone(),
            });
            current_count += 1;

            if current_count == workers[worker_idx].slots {
                worker_idx += 1;
                current_count = 0;
            }
        }
    }

    assignments
}

async fn handle_worker_connect<'a>(
    msg: JobMessage,
    workers: &mut HashMap<WorkerId, WorkerStatus>,
    worker_connects: Arc<Mutex<HashMap<WorkerId, WorkerGrpcClient<Channel>>>>,
    handles: &mut Vec<JoinHandle<()>>,
    ctx: &mut JobContext<'a>,
) -> Result<(), StateError> {
    match msg {
        JobMessage::WorkerConnect {
            worker_id,
            machine_id,
            generation: run_id,
            rpc_address,
            data_address,
            slots,
            ..
        } => {
            let job_id = ctx.config.id.clone();

            if ctx.status.generation != run_id {
                info!(
                    message = "worker connect from wrong run; ignoring",
                    job_id = *job_id,
                    worker_id = worker_id.0,
                    machine_id = *machine_id.0,
                );
                return Ok(());
            }

            workers.insert(
                worker_id,
                WorkerStatus {
                    id: worker_id,
                    machine_id: machine_id.clone(),
                    rpc_address: rpc_address.clone(),
                    data_address,
                    slots,
                    state: WorkerState::Connected,
                },
            );

            let connects = worker_connects;

            handles.push(tokio::spawn(async move {
                info!(
                    message = "connecting to worker",
                    job_id = *job_id,
                    worker_id = worker_id.0,
                    machine_id = *machine_id.0,
                    rpc_address
                );

                for i in 0..3 {
                    match grpc_channel_builder(
                        "controller",
                        rpc_address.clone(),
                        &config().controller.tls,
                        &config().worker.tls,
                    )
                    .await
                    .unwrap()
                    .timeout(Duration::from_secs(90))
                    .connect()
                    .await
                    {
                        Ok(channel) => {
                            {
                                let mut connects = connects.lock().await;
                                connects.insert(worker_id, WorkerGrpcClient::new(channel));
                            }
                            return;
                        }
                        Err(e) => {
                            error!(
                                message = "Failed to connect to worker",
                                job_id = *job_id,
                                worker_id = worker_id.0,
                                machine_id = *machine_id.0,
                                error = format!("{:?}", e),
                                rpc_address,
                                retry = i
                            );
                            tokio::time::sleep(Duration::from_millis((i + 1) * 100)).await;
                        }
                    }
                }
                panic!("Failed to connect to worker {rpc_address}");
            }));
        }
        other => {
            ctx.handle(other)?;
        }
    }

    Ok(())
}

#[derive(Clone, Debug)]
struct CheckpointInfo {
    epoch: u64,
    min_epoch: u64,
    id: String,
    needs_commits: bool,
}

async fn get_checkpoint_info_legacy<'a>(
    state: Box<Scheduling>,
    ctx: &'a JobContext<'a>,
) -> Result<
    (
        Box<Scheduling>,
        Option<CheckpointInfo>,
        Option<CommittingState>,
    ),
    StateError,
> {
    let checkpoint_info = {
        let client = match ctx.db.client().await {
            Ok(c) => c,
            Err(e) => {
                return Err(ctx.retryable(state, "failed to get db client", e.into(), 10));
            }
        };

        let checkpoint_result =
            match controller_queries::fetch_last_successful_checkpoint(&client, &*ctx.config.id)
                .await
            {
                Ok(r) => r,
                Err(e) => {
                    return Err(ctx.retryable(
                        state,
                        "failed to fetch last checkpoint",
                        e.into(),
                        10,
                    ));
                }
            };

        checkpoint_result.into_iter().next().and_then(|r| {
            // Filter checkpoint based on ignore_state_before_epoch threshold
            let should_restore = ctx
                .config
                .ignore_state_before_epoch
                .map(|threshold| r.epoch >= threshold)
                .unwrap_or(true); // If no threshold, restore any checkpoint

            if should_restore {
                info!(
                    message = "restoring checkpoint",
                    job_id = *ctx.config.id,
                    epoch = r.epoch,
                    min_epoch = r.min_epoch
                );

                Some(CheckpointInfo {
                    id: r.pub_id,
                    epoch: r.epoch as u64,
                    min_epoch: r.min_epoch as u64,
                    needs_commits: r.needs_commits,
                })
            } else {
                info!(
                    message = "skipping checkpoint due to ignore_state_before_epoch threshold",
                    job_id = *ctx.config.id,
                    checkpoint_epoch = r.epoch,
                    threshold = ctx.config.ignore_state_before_epoch.unwrap()
                );
                None
            }
        })
    };

    {
        // mark in-progress checkpoints as failed
        let last_epoch = checkpoint_info
            .as_ref()
            .map(|checkpoint_info| checkpoint_info.epoch)
            .unwrap_or(0);

        let client = match ctx.db.client().await {
            Ok(c) => c,
            Err(e) => {
                return Err(ctx.retryable(
                    state,
                    "failed to get db client for mark_failed",
                    e.into(),
                    10,
                ));
            }
        };

        if let Err(e) = controller_queries::execute_mark_failed(
            &client,
            &*ctx.config.id,
            &(last_epoch as i32 + 1),
        )
        .await
        {
            return Err(ctx.retryable(
                state,
                "failed to mark in-progress checkpoints as failed",
                e.into(),
                10,
            ));
        }
    }

    let mut committing_state = None;

    // clear all of the epochs after the one we're loading so that we don't read in-progress data
    if let Some(CheckpointInfo {
        id,
        epoch,
        min_epoch,
        needs_commits,
    }) = checkpoint_info.clone()
    {
        let mut metadata =
            match StateBackend::load_checkpoint_metadata(&ctx.config.id, epoch as u32).await {
                Ok(m) => m,
                Err(err) => {
                    return Err(ctx.retryable(
                        state,
                        format!("Failed to load checkpoint metadata for epoch {epoch}"),
                        err.into(),
                        10,
                    ));
                }
            };

        metadata.min_epoch = min_epoch as u32;
        if needs_commits {
            let mut commit_subtasks = HashSet::new();
            // (operator_id => (table_name => (subtask => data)))
            let mut committing_data: HashMap<String, HashMap<String, HashMap<u32, Vec<u8>>>> =
                HashMap::new();
            for operator_id in &metadata.operator_ids {
                let operator_metadata = match StateBackend::load_operator_metadata(
                    &ctx.config.id,
                    operator_id,
                    epoch as u32,
                )
                .await
                {
                    Ok(m) => m,
                    Err(err) => {
                        return Err(ctx.retryable(
                            state,
                            format!("Failed to load operator metadata for {operator_id} in epoch {epoch}"),
                            err.into(),
                            10,
                        ));
                    }
                };
                let Some(operator_metadata) = operator_metadata else {
                    return Err(fatal(
                        "missing operator metadata",
                        anyhow!(
                            "operator metadata for {} not found for job {}",
                            operator_id,
                            ctx.config.id
                        ),
                    ));
                };
                for (table_name, table_metadata) in &operator_metadata.table_checkpoint_metadata {
                    let config =
                        operator_metadata
                            .table_configs
                            .get(table_name)
                            .ok_or_else(|| {
                                fatal(
                                    format!(
                                        "Failed to restore job; table config for {table_name} not found."
                                    ),
                                    anyhow!("table config for {} not found", table_name),
                                )
                            })?;
                    if let Some(commit_data) = match config.table_type() {
                        arroyo_rpc::grpc::rpc::TableEnum::MissingTableType => {
                            return Err(fatal(
                                "Missing table type",
                                anyhow!("table type not found"),
                            ));
                        }
                        arroyo_rpc::grpc::rpc::TableEnum::GlobalKeyValue => {
                            GlobalKeyedTable::committing_data(config.clone(), table_metadata)
                        }
                        arroyo_rpc::grpc::rpc::TableEnum::ExpiringKeyedTimeTable => None,
                    } {
                        committing_data
                            .entry(operator_id.clone())
                            .or_default()
                            .insert(table_name.to_string(), commit_data);
                        let program_node = ctx
                            .program
                            .graph
                            .node_weights()
                            .find(|node| node.operator_chain.first().operator_id == *operator_id)
                            .unwrap();
                        for subtask_index in 0..program_node.parallelism {
                            commit_subtasks.insert((operator_id.clone(), subtask_index as u32));
                        }
                    }
                }
            }
            committing_state = Some(CommittingState::new(
                CheckpointIdOrRef::CheckpointId(id),
                commit_subtasks,
                committing_data,
            ));
        }

        if let Err(err) = StateBackend::write_checkpoint_metadata(metadata).await {
            return Err(ctx.retryable(
                state,
                format!("Failed to write checkpoint metadata for epoch {epoch}"),
                err.into(),
                10,
            ));
        }
    }

    Ok((state, checkpoint_info, committing_state))
}

async fn get_and_register_checkpoint_info_leader<'a>(
    ctx: &'a JobContext<'a>,
) -> anyhow::Result<Option<CheckpointInfo>> {
    // in the future, this should likely move to the leader, but that will require rethinking how
    // worker initialization works
    let new_gen = initialize_generation(
        get_storage_provider().await?.as_ref(),
        InitializeGenerationRequest {
            pipeline_id: ctx.pipeline_id.clone(),
            job_id: JobId(ctx.config.id.clone()),
            generation: Generation(ctx.status.generation),
            updated_at: SystemTime::now(),
        },
        true,
    )
    .await?;

    let checkpoint_ref = match new_gen {
        GenerationInitialization::Initialized {
            recovery: GenerationRecovery::NoCheckpoint,
            ..
        } => None,
        GenerationInitialization::Initialized {
            recovery:
                GenerationRecovery::Ready { checkpoint_ref }
                | GenerationRecovery::ReplayCommit { checkpoint_ref },
            ..
        } => Some(checkpoint_ref),
        GenerationInitialization::StaleGeneration { .. } => {
            unreachable!(
                "cannot end up with stale generation given that we updated the generation\
            instead of checking it"
            );
        }
        GenerationInitialization::StopOrphaned { canonical_ref } => {
            bail!(
                "somehow ended up with an orphaned checkpoint during recovery... should not happen\
            canonical_ref = {:?}",
                canonical_ref
            );
        }
        GenerationInitialization::Failed(failure) => {
            bail!(
                "failed while resolving restoration checkpoint: {:?}",
                failure
            );
        }
    };

    Ok(if let Some(r) = checkpoint_ref {
        let manifest =
            read_protobuf::<_, CheckpointManifest>(get_storage_provider().await?.as_ref(), &r)
                .await?
                .ok_or_else(|| anyhow!("recovery checkpoint manifest {r} is missing!"))?;

        Some(CheckpointInfo {
            epoch: manifest.epoch,
            min_epoch: manifest.min_epoch,
            id: r.to_string(),
            needs_commits: manifest.needs_commit,
        })
    } else {
        None
    })
}

impl Scheduling {
    async fn start_workers<'a>(
        self: Box<Self>,
        ctx: &mut JobContext<'a>,
        slots_needed: usize,
    ) -> Result<Box<Self>, StateError> {
        let start = Instant::now();
        loop {
            match ctx
                .scheduler
                .start_workers(StartPipelineReq {
                    program: ctx.program.clone(),
                    wasm_path: "".to_string(),
                    pipeline_id: ctx.pipeline_id.clone(),
                    job_id: JobId(ctx.config.id.clone()),
                    generation: ctx.status.generation,
                    name: ctx.config.pipeline_name.clone(),
                    hash: ctx.program.get_hash(),
                    slots: slots_needed,
                    env_vars: [
                        (
                            "ARROYO__CHECKPOINT_URL".to_string(),
                            config().checkpoint_url.clone(),
                        ),
                        (
                            CLUSTER_ID_ENV.to_string(),
                            arroyo_server_common::get_cluster_id(),
                        ),
                    ]
                    .into_iter()
                    .collect(),
                })
                .await
            {
                Ok(_) => break,
                Err(SchedulerError::NotEnoughSlots { slots_needed: s }) => {
                    warn!(
                        message = "not enough slots for job",
                        job_id = *ctx.config.id,
                        slots_for_job = slots_needed,
                        slots_needed = s
                    );
                    if start.elapsed() > *config().pipeline.worker_startup_time {
                        return Err(fatal(
                            "Not enough slots to schedule job",
                            anyhow!("scheduler error -- needed {} slots", slots_needed),
                        ));
                    }
                }
                Err(SchedulerError::Other(s)) => {
                    return Err(ctx.retryable(
                        self,
                        "encountered error during scheduling",
                        anyhow::anyhow!("scheduling error: {}", s),
                        20,
                    ));
                }
            }

            tokio::time::sleep(Duration::from_secs(1)).await;
        }

        Ok(self)
    }
}

#[async_trait::async_trait]
impl State for Scheduling {
    fn name(&self) -> &'static str {
        "Scheduling"
    }

    async fn next(mut self: Box<Self>, ctx: &mut JobContext) -> Result<Transition, StateError> {
        // if we've started in scheduling but the job isn't supposed to be running, don't try
        // to schedule
        stop_if_desired_non_running!(self, &ctx.config);

        // clear out any existing workers for this job
        if let Err(e) = ctx.scheduler.stop_workers(&ctx.config.id, None, true).await {
            warn!(
                message = "failed to clean cluster prior to scheduling",
                job_id = *ctx.config.id,
                error = format!("{:?}", e)
            )
        }

        ctx.program
            .update_parallelism(&ctx.config.parallelism_overrides);

        let slots_needed: usize = slots_for_job(&*ctx.program);
        self = self.start_workers(ctx, slots_needed).await?;

        let leader_mode = matches!(config().job_controller, JobControllerMode::Worker);

        let (self, checkpoint_info, committing_state) = if leader_mode {
            match get_and_register_checkpoint_info_leader(ctx).await {
                Ok(ci) => (self, ci, None),
                Err(e) => {
                    return Err(ctx.retryable(self, "failed to load checkpoint metadata", e, 20));
                }
            }
        } else {
            get_checkpoint_info_legacy(self, ctx).await?
        };

        // wait for them to connect and make outbound RPC connections
        let mut workers = HashMap::new();
        let worker_connects = Arc::new(Mutex::new(HashMap::new()));
        let mut handles = vec![];

        let pipeline_config = &config().pipeline;

        let start = Instant::now();
        loop {
            let timeout = pipeline_config
                .worker_startup_time
                .min(
                    ctx.config
                        .ttl
                        .unwrap_or(*pipeline_config.worker_startup_time),
                )
                .checked_sub(start.elapsed())
                .unwrap_or(Duration::ZERO);

            tokio::select! {
                val = ctx.rx.recv() => {
                    match val {
                        Some(JobMessage::ConfigUpdate(c)) => {
                            stop_if_desired_non_running!(self, &c);
                        }
                        Some(msg) => {
                            handle_worker_connect(msg, &mut workers, worker_connects.clone(), &mut handles, ctx).await?;
                        }
                        None => {
                            panic!("Job message channel closed: {}", ctx.config.id);
                        }
                    }
                }
                _ = tokio::time::sleep(timeout) => {
                    return Err(ctx.retryable(self,
                        "timed out while waiting for workers to start",
                        anyhow!("timed out after {:?} while waiting for worker startup", *pipeline_config.worker_startup_time), 3));
                }
            }

            if workers.values().map(|w| w.slots).sum::<usize>() >= slots_needed {
                break;
            }
        }

        for h in handles {
            if let Err(e) = h.await {
                return Err(fatal("Failed to start cluster for pipeline", e.into()));
            }
        }

        // Compute assignments and send to workers

        let assignments = compute_assignments(workers.values().collect(), &*ctx.program);
        let worker_connects = Arc::try_unwrap(worker_connects).unwrap().into_inner();
        let program = api::ArrowProgram::from(ctx.program.clone());

        // Use ignore_state_before_epoch as default so new checkpoints exceed the threshold
        let default_epoch = ctx
            .config
            .ignore_state_before_epoch
            .filter(|&t| t > 0)
            .map(|t| {
                let epoch = (t - 1) as u64;
                info!(
                    message = "starting from ignore_state_before_epoch threshold",
                    job_id = *ctx.config.id,
                    default_epoch = epoch,
                );
                epoch
            })
            .unwrap_or(0);

        let start_epoch = checkpoint_info
            .as_ref()
            .map(|info| info.epoch)
            .unwrap_or(default_epoch);
        let min_epoch = checkpoint_info
            .as_ref()
            .map(|info| info.min_epoch)
            .unwrap_or(default_epoch);

        let (leader_id, leader_addr) = leader_mode
            .then(|| {
                workers
                    .iter()
                    .min_by_key(|w| w.0.0)
                    .map(|(id, status)| (*id, status.rpc_address.clone()))
                    .unwrap()
            })
            .unzip();

        let checkpoint_interval_micros = ctx.config.checkpoint_interval.as_micros() as u64;

        let tasks: Vec<_> = worker_connects
            .into_iter()
            .map(|(id, mut c)| {
                let assignments = assignments.clone();
                let job_id = ctx.config.id.clone();
                let restore_epoch = checkpoint_info.as_ref().map(|info| info.epoch);
                let program = program.clone();
                let machine_id = workers.get(&id).as_ref().unwrap().machine_id.clone();
                let leader_addr = leader_addr.clone();
                let checkpoint_manifest_ref =
                    leader_id.and(checkpoint_info.as_ref().map(|ci| ci.id.clone()));

                tokio::spawn(async move {
                    info!(
                        message = "starting execution on worker",
                        job_id = *job_id,
                        worker_id = id.0,
                        machine_id = *machine_id.0,
                    );

                    match c
                        .start_execution(Request::new(StartExecutionReq {
                            restore_epoch,
                            start_epoch,
                            min_epoch,
                            program: Some(program.clone()),
                            tasks: assignments.clone(),
                            job_controller_addr: leader_addr,
                            is_leader: leader_id.is_some_and(|l| l == id),
                            wait_for_leader: leader_id.is_some(),
                            checkpoint_interval_micros,
                            checkpoint_manifest_ref: checkpoint_manifest_ref.clone(),
                        }))
                        .await
                    {
                        Ok(_) => {
                            debug!(
                                message = "worker entered initialization phase",
                                job_id = *job_id,
                                worker_id = id.0,
                                machine_id = *machine_id.0,
                            );
                            (id, c)
                        }
                        Err(e) => {
                            error!(
                                message = "failed to start execution on worker",
                                job_id = *job_id,
                                worker_id = id.0,
                                machine_id = *machine_id.0,
                                error = format!("{:?}", e),
                            );
                            panic!("Failed to start execution on worker {id:?}: {e}");
                        }
                    }
                })
            })
            .collect();

        let mut worker_connects = HashMap::new();
        for t in tasks {
            match t.await {
                Ok((id, c)) => {
                    if let Some(worker) = workers.get_mut(&id) {
                        worker.state = WorkerState::Initializing;
                    }
                    worker_connects.insert(id, c);
                }
                Err(e) => {
                    return Err(ctx.retryable(self, "failed to initialize workers", e.into(), 10));
                }
            }
        }

        // Now wait until all tasks are running
        let start = Instant::now();
        let mut started_tasks = HashSet::new();
        while started_tasks.len() < ctx.program.task_count() {
            let timeout = pipeline_config
                .task_startup_time
                .min(ctx.config.ttl.unwrap_or(*pipeline_config.task_startup_time))
                .checked_sub(start.elapsed())
                .unwrap_or(Duration::ZERO);

            select! {
                v = ctx.rx.recv() => {
                    match v {
                        Some(JobMessage::WorkerInitializationComplete {
                            worker_id,
                            success,
                            error_message,
                        }) => {
                            if let Some(worker) = workers.get_mut(&worker_id) {
                                if success {
                                    worker.state = WorkerState::Ready;
                                    info!(
                                        message = "worker initialization completed successfully",
                                        job_id = *ctx.config.id,
                                        worker_id = worker_id.0,
                                        machine_id = *worker.machine_id.0,
                                    );
                                } else {
                                    let error = error_message.unwrap_or_else(|| "Unknown error".to_string());
                                    worker.state = WorkerState::Failed;
                                    error!(
                                        message = "worker initialization failed",
                                        job_id = *ctx.config.id,
                                        worker_id = worker_id.0,
                                        machine_id = *worker.machine_id.0,
                                        error = error
                                    );
                                    return Err(ctx.retryable(self, "worker initialization failed",
                                        anyhow!("worker {} initialization failed: {}", worker_id.0, error), 5));
                                }
                            }
                        }
                        Some(JobMessage::TaskStarted {
                            task_id,
                            subtask_idx,
                            ..
                        }) => {
                            started_tasks.insert((task_id, subtask_idx));
                        }
                        Some(JobMessage::RunningMessage(RunningMessage::TaskFailed(event))) => {
                            return ctx.handle_task_error(self, event).await;
                        }
                        Some(JobMessage::ConfigUpdate(c)) => {
                            stop_if_desired_non_running!(self, &c);
                        }
                        Some(msg) => {
                            ctx.handle(msg)?;
                        }
                        None => {
                            panic!("Job queue shutdown");
                        }
                    }
                }
                _ = tokio::time::sleep(timeout) => {
                    return Err(ctx.retryable(self,
                        "timed out while waiting for tasks to start",
                        anyhow!("timed out after {:?} while waiting for worker startup", *pipeline_config.task_startup_time), 3));
                }
            }
        }

        ctx.status.tasks = Some(ctx.program.task_count() as i32);

        let needs_commit = committing_state.is_some();

        let program = Arc::new(ctx.program.clone());
        let metrics = JobMetrics::new(program.clone());
        ctx.metrics
            .write()
            .await
            .insert(ctx.config.id.clone(), metrics.clone());

        match leader_addr {
            None => {
                let checkpoint_store = Arc::new(DbCheckpointMetadataStore {
                    organization_id: ctx.config.organization_id.clone(),
                    job_id: ctx.config.id.clone(),
                    state_backend: StateBackend::name(),
                    db: ctx.db.clone(),
                });
                let mut controller = JobController::new(
                    checkpoint_store,
                    ctx.config.clone(),
                    ctx.pipeline_id.clone(),
                    ctx.status.generation,
                    program,
                    start_epoch as u32,
                    min_epoch as u32,
                    worker_connects,
                    committing_state,
                    metrics,
                );
                if needs_commit {
                    info!("restored checkpoint was in committing phase, sending commits");
                    controller
                        .send_commit_messages()
                        .await
                        .expect("failed to send commit messages");
                }

                ctx.job_controller = Some(controller);
                Ok(Transition::next(*self, Running {}))
            }
            Some(leader_addr) => {
                ctx.job_controller = None;

                let leader_manager = match LeaderManager::connect(
                    JobId(ctx.config.id.clone()),
                    ctx.status.generation,
                    leader_addr,
                )
                .await
                {
                    Ok(m) => m,
                    Err(e) => {
                        return Err(ctx.retryable(
                            self,
                            "failed to connect to worker leader",
                            e,
                            10,
                        ));
                    }
                };

                ctx.leader_manager = Some(leader_manager);

                Ok(Transition::next(
                    *self,
                    LeaderRunning {
                        started: Instant::now(),
                    },
                ))
            }
        }
    }
}
