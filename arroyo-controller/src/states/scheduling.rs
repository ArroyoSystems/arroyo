use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
    time::{Duration, Instant},
};

use arroyo_datastream::Program;
use arroyo_rpc::grpc::{worker_grpc_client::WorkerGrpcClient, StartExecutionReq, TaskAssignment};
use arroyo_types::WorkerId;
use tokio::{sync::Mutex, task::JoinHandle};
use tonic::{transport::Channel, Request};
use tracing::{error, info, warn};

use anyhow::anyhow;
use arroyo_state::{BackingStore, StateBackend};

use crate::states::{fatal, StateError};
use crate::{job_controller::JobController, queries::controller_queries};
use crate::{scheduler::SchedulerError, JobMessage};

use super::{running::Running, Context, State, Transition};

const STARTUP_TIME: Duration = Duration::from_secs(10 * 60);

#[derive(Debug, Clone)]
struct WorkerStatus {
    id: WorkerId,
    data_address: String,
    slots: usize,
}

#[derive(Debug)]
pub struct Scheduling {}

fn slots_for_job(job: &Program) -> usize {
    job.graph
        .node_weights()
        .map(|n| n.parallelism)
        .max()
        .unwrap_or(0)
}

fn compute_assignments(workers: Vec<&WorkerStatus>, program: &Program) -> Vec<TaskAssignment> {
    let mut assignments = vec![];
    for node in program.graph.node_weights() {
        let mut worker_idx = 0;
        let mut current_count = 0;

        for i in 0..node.parallelism {
            assignments.push(TaskAssignment {
                operator_id: node.operator_id.clone(),
                operator_subtask: i as u64,
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
    ctx: &mut Context<'a>,
) -> Result<(), StateError> {
    match msg {
        JobMessage::WorkerConnect {
            worker_id,
            rpc_address,
            data_address,
            slots,
            ..
        } => {
            workers.insert(
                worker_id,
                WorkerStatus {
                    id: worker_id,
                    data_address,
                    slots,
                },
            );

            let connects = worker_connects;

            let job_id = ctx.config.id.clone();

            handles.push(tokio::spawn(async move {
                info!(
                    message = "connecting to worker",
                    job_id,
                    worker_id = worker_id.0,
                    rpc_address
                );

                for i in 0..10 {
                    match Channel::from_shared(rpc_address.clone())
                        .unwrap()
                        .timeout(Duration::from_secs(10))
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
                                job_id,
                                worker_id = worker_id.0,
                                error = format!("{:?}", e),
                                rpc_address,
                                retry = i
                            );
                            tokio::time::sleep(Duration::from_millis((i + 1) * 100)).await;
                        }
                    }
                }
                panic!("Failed to connect to worker {}", rpc_address);
            }));
        }
        other => {
            ctx.handle(other)?;
        }
    }

    Ok(())
}

#[async_trait::async_trait]
impl State for Scheduling {
    fn name(&self) -> &'static str {
        "Scheduling"
    }

    async fn next(self: Box<Self>, ctx: &mut Context) -> Result<Transition, StateError> {
        // clear out any existing workers for this job
        if let Err(e) = ctx.scheduler.clean_cluster(&ctx.config.id).await {
            warn!(
                message = "failed to clean cluster prior to scheduling",
                job_id = ctx.config.id,
                error = format!("{:?}", e)
            )
        }

        ctx.program
            .update_parallelism(&ctx.config.parallelism_overrides);

        let slots_needed = slots_for_job(ctx.program);

        // start workers
        loop {
            match ctx
                .scheduler
                .start_workers(
                    ctx.status.pipeline_path.as_ref().unwrap(),
                    ctx.status.wasm_path.as_ref().unwrap(),
                    ctx.config.id.clone(),
                    ctx.status.run_id,
                    ctx.config.pipeline_name.clone(),
                    ctx.program.get_hash(),
                    slots_needed,
                )
                .await
            {
                Ok(_) => break,
                Err(SchedulerError::NotEnoughSlots { slots_needed: s }) => {
                    warn!(
                        message = "not enough slots for job",
                        job_id = ctx.config.id,
                        slots_for_job = slots_needed,
                        slots_needed = s
                    );
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

        // wait for them to connect and make outbound RPC connections
        let mut workers = HashMap::new();
        let worker_connects = Arc::new(Mutex::new(HashMap::new()));
        let mut handles = vec![];

        let start = Instant::now();
        loop {
            let timeout = STARTUP_TIME
                .checked_sub(start.elapsed())
                .unwrap_or(Duration::ZERO);

            tokio::select! {
                val = ctx.rx.recv() => {
                    match val {
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
                        "timed out while waiting for job to start",
                        anyhow!("timed out after {:?} while waiting for worker startup", STARTUP_TIME), 3));
                }
            }

            if workers.values().map(|w| w.slots).sum::<usize>() == slots_needed {
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
        let c = ctx.pool.get().await.unwrap();
        let restore_epoch = controller_queries::last_successful_checkpoint()
            .bind(&c, &ctx.config.id)
            .opt()
            .await
            .unwrap()
            .map(|r| {
                info!(
                    message = "restoring checkpoint",
                    job_id = ctx.config.id,
                    epoch = r.epoch,
                    min_epoch = r.min_epoch
                );

                (r.epoch as u32, r.min_epoch as u32)
            });

        {
            // mark in-progress checkpoints as failed
            let last_epoch = restore_epoch.map(|(epoch, _)| epoch).unwrap_or(0);
            controller_queries::mark_failed()
                .bind(&c, &ctx.config.id, &(last_epoch as i32 + 1))
                .await
                .unwrap();
        }

        // clear all of the epochs after the one we're loading so that we don't read in-progress data
        if let Some((epoch, _)) = restore_epoch {
            let metadata = StateBackend::load_checkpoint_metadata(&ctx.config.id, epoch)
                .await
                .unwrap_or_else(|| panic!("epoch {} not found for job {}", epoch, ctx.config.id));
            if let Err(e) = StateBackend::prepare_checkpoint(&metadata).await {
                return Err(ctx.retryable(self, "failed to prepare checkpoint for loading", e, 10));
            }
        }

        let assignments = compute_assignments(workers.values().collect(), ctx.program);
        let worker_connects = Arc::try_unwrap(worker_connects).unwrap().into_inner();
        let tasks: Vec<_> = worker_connects
            .into_iter()
            .map(|(id, mut c)| {
                let assignments = assignments.clone();

                let job_id = ctx.config.id.clone();
                tokio::spawn(async move {
                    info!(
                        message = "starting execution on worker",
                        job_id,
                        worker_id = id.0
                    );
                    for i in 0..10 {
                        match c
                            .start_execution(Request::new(StartExecutionReq {
                                restore_epoch: restore_epoch.map(|(epoch, _)| epoch),
                                tasks: assignments.clone(),
                            }))
                            .await
                        {
                            Ok(_) => {
                                return (id, c);
                            }
                            Err(e) => {
                                error!(
                                    message = "failed to start execution on worker",
                                    job_id,
                                    worker_id = id.0,
                                    attempt = i,
                                    error = format!("{:?}", e)
                                );
                            }
                        }
                        tokio::time::sleep(Duration::from_millis(100)).await;
                    }

                    panic!("Failed to start execution on workers {:?}", id);
                })
            })
            .collect();

        let mut worker_connects = HashMap::new();
        for t in tasks {
            match t.await {
                Ok((id, c)) => {
                    worker_connects.insert(id, c);
                }
                Err(e) => {
                    return Err(fatal("Failed to start cluster for pipeline", e.into()));
                }
            }
        }

        // wait until all tasks are running
        let mut started = HashSet::new();
        while started.len() < ctx.program.task_count() {
            match ctx.rx.recv().await {
                Some(JobMessage::TaskStarted {
                    operator_id,
                    operator_subtask,
                    ..
                }) => {
                    started.insert((operator_id, operator_subtask));
                }
                Some(msg) => {
                    ctx.handle(msg)?;
                }
                None => {
                    panic!("Job queue shutdown");
                }
            }
        }

        ctx.status.tasks = Some(ctx.program.task_count() as i32);

        let controller = JobController::new(
            ctx.pool.clone(),
            ctx.config.clone(),
            ctx.program.clone(),
            restore_epoch.map(|(epoch, _)| epoch).unwrap_or(0),
            restore_epoch.map(|(_, min_epoch)| min_epoch).unwrap_or(0),
            worker_connects,
        );

        ctx.job_controller = Some(controller);

        Ok(Transition::next(*self, Running {}))
    }
}
