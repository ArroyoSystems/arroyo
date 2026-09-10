use arroyo_rpc::grpc::rpc::{CheckpointManifest, JobState, StartExecutionReq, TaskAssignment};
use arroyo_rpc::identity::{WorkerClient, worker_client};
use arroyo_types::{CLUSTER_ID_ENV, JobId, MachineId, WorkerId};
use std::time::SystemTime;
use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
    time::{Duration, Instant},
};
use tokio::{select, sync::Mutex, task::JoinHandle};
use tonic::Request;
use tracing::{debug, error, info, warn};

use super::{JobContext, State, Transition, leader_running::LeaderRunning};
use crate::leader_manager::LeaderManager;
use crate::states::stop_if_desired_non_running;
use crate::{JobMessage, schedulers::SchedulerError};
use crate::{
    schedulers::StartPipelineReq,
    states::{StateError, fatal},
};
use anyhow::{anyhow, bail};
use arroyo_rpc::config::config;
use arroyo_rpc::grpc::api;
use arroyo_rpc::{LeaderContext, grpc_channel_builder};
use arroyo_state::{StorageProviderFor, get_storage_provider};
use arroyo_state_protocol::store::read_protobuf;
use arroyo_state_protocol::types::Generation;
use arroyo_state_protocol::workflow::{
    GenerationInitialization, GenerationRecovery, InitializeGenerationRequest,
    initialize_generation,
};

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

fn compute_assignments(
    workers: Vec<&WorkerStatus>,
    program: &api::ArrowProgram,
) -> Vec<TaskAssignment> {
    let mut assignments = vec![];
    for node in &program.nodes {
        let mut worker_idx = 0;
        let mut current_count = 0;

        for i in 0..node.parallelism {
            assignments.push(TaskAssignment {
                task_id: node.node_id,
                subtask_idx: i,
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
    worker_connects: Arc<Mutex<HashMap<WorkerId, WorkerClient>>>,
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
            let pipeline_id = ctx.pipeline_info.pipeline_id.clone();

            if ctx.status.generation != run_id {
                info!(
                    message = "worker connect from wrong run; ignoring",
                    job_id = %job_id,
                    pipeline_id = *pipeline_id,
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
                    job_id = %job_id,
                    pipeline_id = *pipeline_id,
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
                        None,
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
                                connects.insert(worker_id, worker_client(channel, worker_id));
                            }
                            return;
                        }
                        Err(e) => {
                            error!(
                                message = "Failed to connect to worker",
                                job_id = %job_id,
                                pipeline_id = *pipeline_id,
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
}

async fn get_and_register_checkpoint_info_leader<'a>(
    ctx: &'a JobContext<'a>,
) -> anyhow::Result<Option<CheckpointInfo>> {
    // in the future, this should likely move to the leader, but that will require rethinking how
    // worker initialization works
    let storage_role = StorageProviderFor::Controller {
        storage_url: ctx.pipeline_info.state_url.clone(),
    };
    let storage_provider = get_storage_provider(&storage_role).await?;

    let new_gen = initialize_generation(
        storage_provider.as_ref(),
        InitializeGenerationRequest {
            pipeline_id: ctx.pipeline_info.pipeline_id.clone(),
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
                | GenerationRecovery::ReplayCommit { checkpoint_ref, .. },
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
        let manifest = read_protobuf::<_, CheckpointManifest>(storage_provider.as_ref(), &r)
            .await?
            .ok_or_else(|| anyhow!("recovery checkpoint manifest {r} is missing!"))?;

        Some(CheckpointInfo {
            epoch: manifest.epoch,
            min_epoch: manifest.min_epoch,
            id: r.to_string(),
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
        let mut env_vars: HashMap<String, String> =
            serde_json::from_value(ctx.config.env_vars.clone()).map_err(|e| {
                fatal(
                    format!("failed to deserialize env_vars from job_configs: {e}"),
                    anyhow::anyhow!("malformed env_vars in job_configs row"),
                )
            })?;

        let checkpoint_url = ctx
            .pipeline_info
            .state_url
            .clone()
            .unwrap_or_else(|| config().checkpoint_url.clone());
        env_vars
            .insert("ARROYO__CHECKPOINT_URL".to_string(), checkpoint_url)
            .inspect(|_| {
                warn!(
                    job_id = %ctx.config.id,
                    pipeline_id = *ctx.pipeline_info.pipeline_id,
                    key = "ARROYO__CHECKPOINT_URL",
                    "job env_vars contained a reserved infrastructure key; \
                     user-supplied value has been overridden by the controller"
                );
            });

        let cluster_id = arroyo_server_common::get_cluster_id();
        env_vars
            .insert(CLUSTER_ID_ENV.to_string(), cluster_id)
            .inspect(|_| {
                warn!(
                    job_id = %ctx.config.id,
                    pipeline_id = *ctx.pipeline_info.pipeline_id,
                    key = CLUSTER_ID_ENV,
                    "job env_vars contained a reserved infrastructure key; \
                     user-supplied value has been overridden by the controller"
                );
            });

        loop {
            match ctx
                .scheduler
                .start_workers(StartPipelineReq {
                    wasm_path: "".to_string(),
                    pipeline_id: ctx.pipeline_info.pipeline_id.clone(),
                    organization_id: ctx.config.organization_id.clone(),
                    job_id: JobId(ctx.config.id.clone()),
                    generation: ctx.status.generation,
                    name: ctx.config.pipeline_name.clone(),
                    slots: slots_needed,
                    env_vars: env_vars.clone(),
                    pipeline_tags: ctx.pipeline_info.tags.clone(),
                    scheduler_config: ctx.config.scheduler_config.clone(),
                })
                .await
            {
                Ok(_) => break,
                Err(SchedulerError::NotEnoughSlots { slots_needed: s }) => {
                    warn!(
                        message = "not enough slots for job",
                        job_id = %ctx.config.id,
                        pipeline_id = *ctx.pipeline_info.pipeline_id,
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
                Err(SchedulerError::Fatal(s)) => {
                    return Err(fatal(
                        format!("scheduling failed: {s}"),
                        anyhow::anyhow!("non-retryable scheduling error: {}", s),
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

        // update the generation for this scheduling attempt
        ctx.status.generation += 1;
        if let Err(e) = ctx.status.update_db(&ctx.db).await {
            return Err(ctx.retryable(
                self,
                "failed to advance generation for scheduling retry",
                anyhow!("{}", e),
                10,
            ));
        }

        // clear out any existing workers for this job
        if let Err(e) = ctx.scheduler.stop_workers(&ctx.config.id, None, true).await {
            warn!(
                message = "failed to clean cluster prior to scheduling",
                job_id = %ctx.config.id,
                pipeline_id = *ctx.pipeline_info.pipeline_id,
                error = format!("{:?}", e)
            )
        }

        ctx.program
            .decoded
            .update_parallelism(&ctx.config.parallelism_overrides)
            .map_err(|e| fatal(format!("invalid parallelism overrides: {e}"), e))?;
        let slots_needed = ctx.program.decoded.slots_required();
        self = self.start_workers(ctx, slots_needed).await?;

        let checkpoint_info = match get_and_register_checkpoint_info_leader(ctx).await {
            Ok(ci) => ci,
            Err(e) => {
                return Err(ctx.retryable(self, "failed to load checkpoint metadata", e, 20));
            }
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
                return Err(ctx.retryable(
                    self,
                    "Failed to start cluster for pipeline",
                    e.into(),
                    10,
                ));
            }
        }

        // Compute assignments and send to workers

        let assignments = compute_assignments(workers.values().collect(), &ctx.program.decoded);
        let worker_connects = Arc::try_unwrap(worker_connects).unwrap().into_inner();

        let start_epoch = checkpoint_info.as_ref().map(|info| info.epoch).unwrap_or(0);
        let min_epoch = checkpoint_info
            .as_ref()
            .map(|info| info.min_epoch)
            .unwrap_or(0);

        let (leader_id, leader_addr) = workers
            .iter()
            .min_by_key(|w| w.0.0)
            .map(|(id, status)| (*id, status.rpc_address.clone()))
            .unwrap();

        let checkpoint_interval_micros = ctx.config.checkpoint_interval.as_micros() as u64;

        let tasks: Vec<_> = worker_connects
            .into_iter()
            .map(|(id, mut c)| {
                let assignments = assignments.clone();
                let job_id = ctx.config.id.clone();
                let pipeline_id = ctx.pipeline_info.pipeline_id.clone();
                let restore_epoch = checkpoint_info.as_ref().map(|info| info.epoch);
                let program = ctx.program.decoded.clone();
                let program_version = ctx.program.program_version;
                let machine_id = workers.get(&id).as_ref().unwrap().machine_id.clone();
                let leader_addr = leader_addr.clone();
                let checkpoint_manifest_ref = checkpoint_info.as_ref().map(|ci| ci.id.clone());
                tokio::spawn(async move {
                    info!(
                        message = "starting execution on worker",
                        job_id = %job_id,
                        pipeline_id = *pipeline_id,
                        worker_id = id.0,
                        machine_id = *machine_id.0,
                    );

                    match c
                        .start_execution(Request::new(StartExecutionReq {
                            restore_epoch,
                            start_epoch,
                            min_epoch,
                            program: Some(program),
                            program_version: Some(program_version),
                            tasks: assignments.clone(),
                            job_controller_addr: leader_addr,
                            is_leader: leader_id == id,
                            wait_for_leader: true,
                            checkpoint_interval_micros,
                            checkpoint_manifest_ref: checkpoint_manifest_ref.clone(),
                        }))
                        .await
                    {
                        Ok(_) => {
                            debug!(
                                message = "worker entered initialization phase",
                                job_id = %job_id,
                                pipeline_id = *pipeline_id,
                                worker_id = id.0,
                                machine_id = *machine_id.0,
                            );
                            Ok(id)
                        }
                        Err(e) => {
                            error!(
                                message = "failed to start execution on worker",
                                job_id = %job_id,
                                pipeline_id = *pipeline_id,
                                worker_id = id.0,
                                machine_id = *machine_id.0,
                                error = format!("{:?}", e),
                            );
                            Err(e)
                        }
                    }
                })
            })
            .collect();

        for t in tasks {
            match t.await {
                Ok(Ok(id)) => {
                    if let Some(worker) = workers.get_mut(&id) {
                        worker.state = WorkerState::Initializing;
                    }
                }
                Ok(Err(e))
                    if matches!(
                        e.code(),
                        tonic::Code::InvalidArgument | tonic::Code::FailedPrecondition
                    ) =>
                {
                    return Err(fatal(
                        format!("worker rejected program: {}", e.message()),
                        e.into(),
                    ));
                }
                Ok(Err(e)) => {
                    return Err(ctx.retryable(self, "failed to initialize workers", e.into(), 10));
                }
                Err(e) => {
                    return Err(ctx.retryable(self, "failed to initialize workers", e.into(), 10));
                }
            }
        }

        // Now wait until all tasks are running
        let start = Instant::now();
        let mut started_tasks = HashSet::new();
        while started_tasks.len() < ctx.program.decoded.task_count() {
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
                                        job_id = %ctx.config.id,
                                        pipeline_id = *ctx.pipeline_info.pipeline_id,
                                        worker_id = worker_id.0,
                                        machine_id = *worker.machine_id.0,
                                    );
                                } else {
                                    let error = error_message.unwrap_or_else(|| "Unknown error".to_string());
                                    worker.state = WorkerState::Failed;
                                    error!(
                                        message = "worker initialization failed",
                                        job_id = %ctx.config.id,
                                        pipeline_id = *ctx.pipeline_info.pipeline_id,
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
                    // A startup timeout may mean that the leader failed before every task started.
                    if let Ok(mut leader_manager) = LeaderManager::connect(
                            JobId(ctx.config.id.clone()),
                            ctx.pipeline_info.pipeline_id.clone(),
                            ctx.status.generation,
                            leader_id,
                            leader_addr.clone(),
                            config().controller.connect_timeout.as_deref().copied(),
                        ).await
                            && let Ok(status) = leader_manager.poll_leader_status().await {
                                match JobState::try_from(status.job_state) {
                                    Ok(JobState::JobFailing | JobState::JobFailed) => {
                                        let Some(failure) = status.job_failure else {
                                            return Err(ctx.retryable(
                                                self,
                                                "leader reported failing status without failure payload",
                                                anyhow!("missing job failure"),
                                                10,
                                            ));
                                        };
                                        return ctx.handle_job_failure(*self, failure).await;
                                    }
                                    Ok(_) => {}
                                    Err(e) => {
                                        warn!(
                                            message = "leader returned invalid job state before task startup timeout",
                                            error = format!("{:?}", e),
                                            job_id = %ctx.config.id,
                                            pipeline_id = *ctx.pipeline_info.pipeline_id,
                                        );
                                    }
                                }
                            }

                    return Err(ctx.retryable(self,
                        "timed out while waiting for tasks to start",
                        anyhow!("timed out after {:?} while waiting for worker startup", *pipeline_config.task_startup_time), 3));
                }
            }
        }

        ctx.status.tasks = Some(ctx.program.decoded.task_count() as i32);

        let leader_manager = match LeaderManager::connect(
            JobId(ctx.config.id.clone()),
            ctx.pipeline_info.pipeline_id.clone(),
            ctx.status.generation,
            leader_id,
            leader_addr.clone(),
            config().controller.connect_timeout.as_deref().copied(),
        )
        .await
        {
            Ok(m) => m,
            Err(e) => {
                return Err(ctx.retryable(self, "failed to connect to worker leader", e, 10));
            }
        };

        ctx.leader_manager = Some(leader_manager);
        ctx.status.state_context.leader = Some(LeaderContext {
            worker_id: leader_id,
            rpc_address: leader_addr,
            generation: ctx.status.generation,
        });

        Ok(Transition::next(
            *self,
            LeaderRunning {
                started: Instant::now(),
            },
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn protobuf_scheduling_preserves_task_placement() {
        let program = api::ArrowProgram {
            nodes: [(0, 10, 2), (1, 20, 3), (2, 30, 1)]
                .into_iter()
                .map(|(node_index, node_id, parallelism)| api::ArrowNode {
                    node_index,
                    node_id,
                    parallelism,
                    ..Default::default()
                })
                .collect(),
            ..Default::default()
        };
        let workers: Vec<_> = (0..2)
            .map(|id| WorkerStatus {
                id: WorkerId(id),
                machine_id: MachineId(Arc::new(format!("machine-{id}"))),
                rpc_address: format!("rpc-{id}"),
                data_address: format!("data-{id}"),
                slots: 2,
                state: WorkerState::Connected,
            })
            .collect();

        for (overrides, expected) in [
            (
                HashMap::new(),
                vec![
                    (10, 0, 0),
                    (10, 1, 0),
                    (20, 0, 0),
                    (20, 1, 0),
                    (20, 2, 1),
                    (30, 0, 0),
                ],
            ),
            (
                HashMap::from([(10, 4), (30, 2)]),
                vec![
                    (10, 0, 0),
                    (10, 1, 0),
                    (10, 2, 1),
                    (10, 3, 1),
                    (20, 0, 0),
                    (20, 1, 0),
                    (20, 2, 1),
                    (30, 0, 0),
                    (30, 1, 0),
                ],
            ),
        ] {
            let mut scheduled = program.clone();
            scheduled.update_parallelism(&overrides).unwrap();
            let assignments = compute_assignments(workers.iter().collect(), &scheduled);
            assert_eq!(assignments.len(), scheduled.task_count());
            assert_eq!(
                assignments
                    .iter()
                    .map(|a| (a.task_id, a.subtask_idx, a.worker_id))
                    .collect::<Vec<_>>(),
                expected
            );
            for assignment in assignments {
                assert_eq!(
                    assignment.worker_addr,
                    format!("data-{}", assignment.worker_id)
                );
                assert_eq!(
                    assignment.worker_rpc,
                    format!("rpc-{}", assignment.worker_id)
                );
            }
        }
    }
}
