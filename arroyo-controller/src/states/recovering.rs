use std::time::{Duration, Instant};

use anyhow::bail;
use arroyo_rpc::grpc::{StopMode, StopWorkerReq};
use tokio::time::timeout;
use tracing::{info, warn};

use super::{compiling::Compiling, Context, State, StateError, Transition};

#[derive(Debug)]
pub struct Recovering {}

impl Recovering {
    // tries, with increasing levels of force, to tear down the existing cluster
    async fn cleanup<'a>(&mut self, ctx: &mut Context<'a>) -> anyhow::Result<()> {
        let job_controller = ctx.job_controller.as_mut().unwrap();

        // first try to stop it gracefully
        if job_controller.finished() {
            return Ok(());
        }

        // stop the job
        info!(message = "stopping job", job_id = ctx.config.id);
        let start = Instant::now();
        match job_controller.stop_job(StopMode::Immediate).await {
            Ok(_) => {
                if let Ok(_) = timeout(
                    Duration::from_secs(5),
                    job_controller.wait_for_finish(&mut ctx.rx),
                )
                .await
                {
                    info!(
                        message = "job stopped",
                        job_id = ctx.config.id,
                        duration = start.elapsed().as_secs_f32()
                    );
                }
            }
            Err(e) => {
                warn!(
                    message = "failed to stop job",
                    error = format!("{:?}", e),
                    job_id = ctx.config.id
                );
            }
        }

        // tell the processes to stop

        for i in 0..10 {
            if ctx
                .scheduler
                .workers_for_job(&ctx.config.id)
                .await?
                .is_empty()
            {
                return Ok(());
            }

            info!(
                message = "sending SIGKILL to workers",
                job_id = ctx.config.id
            );
            for worker in job_controller.get_workers() {
                if let Err(err) = ctx
                    .scheduler
                    .stop_worker(StopWorkerReq {
                        job_id: ctx.config.id.clone(),
                        worker_id: worker.0,
                        force: true,
                    })
                    .await
                {
                    warn!(
                        message = "error while stopping worker",
                        error = format!("{:?}", err),
                        worker_id = worker.0,
                        job_id = ctx.config.id
                    );
                }
            }

            tokio::time::sleep(Duration::from_millis(i * 50)).await;
        }

        bail!("Failed to clean up cluster")
    }
}

#[async_trait::async_trait]
impl State for Recovering {
    fn name(&self) -> &'static str {
        "Recovering"
    }

    async fn next(mut self: Box<Self>, ctx: &mut Context) -> Result<Transition, StateError> {
        // tear down the existing cluster
        if let Err(e) = self.cleanup(ctx).await {
            return Err(ctx.retryable(self, "failed to tear down existing cluster", e, 10));
        }

        Ok(Transition::next(*self, Compiling))
    }
}
