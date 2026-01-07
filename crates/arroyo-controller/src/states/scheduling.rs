use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
    time::{Duration, Instant},
};

use arroyo_rpc::grpc::rpc::{
    StartExecutionReq, TaskAssignment, worker_grpc_client::WorkerGrpcClient,
};
use arroyo_types::{MachineId, WorkerId};
use tokio::{select, sync::Mutex, task::JoinHandle};
use tonic::{Request, transport::Channel};
use tracing::{debug, error, info, warn};

use anyhow::anyhow;
use arroyo_datastream::logical::LogicalProgram;
use arroyo_rpc::config::config;
use arroyo_rpc::grpc::api;
use arroyo_rpc::grpc_channel_builder;
use arroyo_state::{
    BackingStore, StateBackend,
    committing_state::CommittingState,
    tables::{ErasedTable, global_keyed_map::GlobalKeyedTable},
};

use crate::job_controller::job_metrics::JobMetrics;
use crate::{JobMessage, schedulers::SchedulerError};
use crate::{
    RunningMessage, job_controller::JobController, queries::controller_queries,
    states::stop_if_desired_non_running,
};
use crate::{
    schedulers::StartPipelineReq,
    states::{StateError, fatal},
};

use super::{JobContext, State, Transition, running::Running};

#[derive(Debug, Clone)]
struct WorkerStatus {
    id: WorkerId,
    machine_id: MachineId,
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
                node_id: node.node_id,
                subtask_idx: i as u32,
                worker_id: workers[worker_idx].id.0,
                worker_addr: workers[worker_idx].data_address.clone(),
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
            run_id,
            rpc_address,
            data_address,
            slots,
            ..
        } => {
            let job_id = ctx.config.id.clone();

            if ctx.status.run_id != run_id {
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
                    job_id: ctx.config.id.clone(),
                    run_id: ctx.status.run_id,
                    name: ctx.config.pipeline_name.clone(),
                    hash: ctx.program.get_hash(),
                    slots: slots_needed,
                    env_vars: [(
                        "ARROYO__CHECKPOINT_URL".to_string(),
                        config().checkpoint_url.clone(),
                    )]
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
                        10,
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

        // wait for them to connect and make outbound RPC connections
        let mut workers = HashMap::new();
        let worker_connects = Arc::new(Mutex::new(HashMap::new()));
        let mut handles = vec![];

        let config = &config().pipeline;

        let start = Instant::now();
        loop {
            let timeout = config
                .worker_startup_time
                .min(ctx.config.ttl.unwrap_or(*config.worker_startup_time))
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
                        anyhow!("timed out after {:?} while waiting for worker startup", *config.worker_startup_time), 3));
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

        // TODO: better error handling

        #[derive(Clone, Debug)]
        struct CheckpointInfo {
            epoch: u32,
            min_epoch: u32,
            id: String,
            needs_commits: bool,
        }

        let checkpoint_info = controller_queries::fetch_last_successful_checkpoint(
            &ctx.db.client().await.unwrap(),
            &*ctx.config.id,
        )
        .await
        .unwrap()
        .into_iter()
        .next()
        .map(|r| {
            info!(
                message = "restoring checkpoint",
                job_id = *ctx.config.id,
                epoch = r.epoch,
                min_epoch = r.min_epoch
            );

            CheckpointInfo {
                epoch: r.epoch as u32,
                min_epoch: r.min_epoch as u32,
                id: r.pub_id,
                needs_commits: r.needs_commits,
            }
        });

        {
            // mark in-progress checkpoints as failed
            let last_epoch = checkpoint_info
                .as_ref()
                .map(|checkpoint_info| checkpoint_info.epoch)
                .unwrap_or(0);
            controller_queries::execute_mark_failed(
                &ctx.db.client().await.unwrap(),
                &*ctx.config.id,
                &(last_epoch as i32 + 1),
            )
            .await
            .unwrap();
        }

        let mut committing_state = None;

        // clear all of the epochs after the one we're loading so that we don't read in-progress data
        if let Some(CheckpointInfo {
            epoch,
            min_epoch,
            id,
            needs_commits,
        }) = checkpoint_info.clone()
        {
            let mut metadata = StateBackend::load_checkpoint_metadata(&ctx.config.id, epoch)
                .await
                .map_err(|err| {
                    fatal(
                        format!("Failed to restore job; checkpoint {epoch} not found."),
                        err.into(),
                    )
                })?;

            if let Err(e) = StateBackend::prepare_checkpoint_load(&metadata).await {
                return Err(ctx.retryable(
                    self,
                    "failed to prepare checkpoint for loading",
                    e.into(),
                    10,
                ));
            }
            metadata.min_epoch = min_epoch;
            if needs_commits {
                let mut commit_subtasks = HashSet::new();
                // (operator_id => (table_name => (subtask => data)))
                let mut committing_data: HashMap<String, HashMap<String, HashMap<u32, Vec<u8>>>> =
                    HashMap::new();
                for operator_id in &metadata.operator_ids {
                    let operator_metadata =
                        StateBackend::load_operator_metadata(&ctx.config.id, operator_id, epoch)
                            .await
                            .map_err(|err| {
                                fatal(
                                    format!(
                                "Failed to restore job; operator metadata for {operator_id} not found."
                            ),
                                    err.into(),
                                )
                            })?;
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
                    for (table_name, table_metadata) in &operator_metadata.table_checkpoint_metadata
                    {
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
                                .find(|node| {
                                    node.operator_chain.first().operator_id == *operator_id
                                })
                                .unwrap();
                            for subtask_index in 0..program_node.parallelism {
                                commit_subtasks.insert((operator_id.clone(), subtask_index as u32));
                            }
                        }
                    }
                }
                committing_state = Some(CommittingState::new(id, commit_subtasks, committing_data));
            }
            StateBackend::write_checkpoint_metadata(metadata)
                .await
                .map_err(|err| {
                    fatal(
                        format!("Failed to write checkpoint metadata for epoch {epoch}."),
                        err.into(),
                    )
                })?;
        }

        let assignments = compute_assignments(workers.values().collect(), &*ctx.program);
        let worker_connects = Arc::try_unwrap(worker_connects).unwrap().into_inner();
        let program = api::ArrowProgram::from(ctx.program.clone());

        let tasks: Vec<_> = worker_connects
            .into_iter()
            .map(|(id, mut c)| {
                let assignments = assignments.clone();
                let job_id = ctx.config.id.clone();
                let restore_epoch = checkpoint_info.as_ref().map(|info| info.epoch);
                let program = program.clone();
                let machine_id = workers.get(&id).as_ref().unwrap().machine_id.clone();

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
                            program: Some(program.clone()),
                            tasks: assignments.clone(),
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
            let timeout = config
                .task_startup_time
                .min(ctx.config.ttl.unwrap_or(*config.task_startup_time))
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
                            node_id,
                            operator_subtask,
                            ..
                        }) => {
                            started_tasks.insert((node_id, operator_subtask));
                        }
                        Some(JobMessage::RunningMessage(RunningMessage::TaskFailed(event))) => {
                            return Err(ctx.handle_task_error(self, event).await);
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
                        anyhow!("timed out after {:?} while waiting for worker startup", *config.task_startup_time), 3));
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

        let mut controller = JobController::new(
            ctx.db.clone(),
            ctx.config.clone(),
            program,
            checkpoint_info.as_ref().map(|info| info.epoch).unwrap_or(0),
            checkpoint_info
                .as_ref()
                .map(|info| info.min_epoch)
                .unwrap_or(0),
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
}
