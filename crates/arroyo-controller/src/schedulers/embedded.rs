use crate::schedulers::{Scheduler, SchedulerError, StartPipelineReq};
use arroyo_rpc::grpc::{HeartbeatNodeReq, RegisterNodeReq, WorkerFinishedReq};
use arroyo_server_common::shutdown::Shutdown;
use arroyo_types::{default_controller_addr, WorkerId};
use arroyo_worker::WorkerServer;
use async_trait::async_trait;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tokio::sync::Mutex;
use tokio::task::JoinHandle;
use tonic::Status;
use tracing::{error, info};

pub struct EmbeddedWorker {
    job_id: Arc<String>,
    run_id: i64,
    shutdown: Shutdown,
    handle: JoinHandle<()>,
}

/// The EmbedddedScheduler runs workers within the controller process
pub struct EmbeddedScheduler {
    tasks: Arc<Mutex<HashMap<WorkerId, EmbeddedWorker>>>,
    worker_counter: AtomicU64,
}

impl EmbeddedScheduler {
    pub fn new() -> Self {
        Self {
            tasks: Arc::new(Default::default()),
            worker_counter: AtomicU64::new(100),
        }
    }

    async fn clear_finished(&self) {
        self.tasks
            .lock()
            .await
            .retain(|_, w| !w.handle.is_finished());
    }
}

#[async_trait]
impl Scheduler for EmbeddedScheduler {
    async fn start_workers(&self, req: StartPipelineReq) -> Result<(), SchedulerError> {
        self.clear_finished().await;

        let shutdown = Shutdown::new("embedded-worker");
        let guard = shutdown.guard("embedded-worker");

        let job_id = req.job_id.clone();
        let run_id = req.run_id;
        let worker_id = WorkerId(self.worker_counter.fetch_add(1, Ordering::SeqCst));
        let handle = tokio::task::spawn(async move {
            let server = WorkerServer::new(
                "job",
                worker_id,
                (*req.job_id).clone(),
                req.run_id.to_string(),
                default_controller_addr(),
                req.program,
                guard,
            );

            match tokio::task::spawn(async move {
                if let Err(e) = server.start_async().await {
                    error!("Failed to start worker {:?}: {:?}", worker_id, e);
                }
            })
            .await
            {
                Ok(_) => {
                    info!("Worker {:?} finished", worker_id);
                }
                Err(err) => {
                    error!("Worker {:?} panicked: {:?}", worker_id, err);
                }
            }
        });

        self.tasks.lock().await.insert(
            worker_id,
            EmbeddedWorker {
                job_id,
                run_id,
                shutdown,
                handle,
            },
        );

        Ok(())
    }

    async fn register_node(&self, _: RegisterNodeReq) {}

    async fn heartbeat_node(&self, _: HeartbeatNodeReq) -> Result<(), Status> {
        Ok(())
    }

    async fn worker_finished(&self, _: WorkerFinishedReq) {}

    async fn stop_workers(&self, job_id: &str, run_id: Option<i64>, _: bool) -> anyhow::Result<()> {
        for w in self.workers_for_job(job_id, run_id).await? {
            let state = self.tasks.lock().await;
            if let Some(worker) = state.get(&w) {
                worker.shutdown.token().cancel();
            }
        }

        Ok(())
    }

    async fn workers_for_job(
        &self,
        job_id: &str,
        run_id: Option<i64>,
    ) -> anyhow::Result<Vec<WorkerId>> {
        let state = self.tasks.lock().await;
        Ok(state
            .iter()
            .filter(|(_, t)| {
                *t.job_id == job_id && (run_id.is_some() && run_id.unwrap() == t.run_id)
            })
            .map(|(k, _)| *k)
            .collect())
    }
}
