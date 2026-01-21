use super::{
    JobContext, State, StateError, Transition, compiling::Compiling, fatal, state_backoff,
};
use anyhow::bail;
use arroyo_rpc::config::config;
use arroyo_rpc::errors::ErrorDomain;
use arroyo_rpc::grpc::rpc::StopMode;
use std::time::{Duration, Instant};
use tokio::time::timeout;
use tracing::{info, warn};

#[derive(Debug)]
pub struct Recovering {
    pub source: anyhow::Error,
    pub reason: String,
    pub domain: ErrorDomain,
}

impl Recovering {
    // tries, with increasing levels of force, to tear down the existing cluster
    pub async fn cleanup<'a>(ctx: &mut JobContext<'a>) -> anyhow::Result<()> {
        let job_controller = ctx.job_controller.as_mut().unwrap();

        // first try to stop it gracefully
        if job_controller.finished() {
            return Ok(());
        }

        // stop the job
        info!(message = "stopping job", job_id = *ctx.config.id);
        let start = Instant::now();
        match job_controller.stop_job(StopMode::Immediate).await {
            Ok(_) => {
                if (timeout(
                    Duration::from_secs(5),
                    job_controller.wait_for_finish(ctx.rx),
                )
                .await)
                    .is_ok()
                {
                    info!(
                        message = "job stopped",
                        job_id = *ctx.config.id,
                        duration = start.elapsed().as_secs_f32()
                    );
                }
            }
            Err(e) => {
                warn!(
                    message = "failed to stop job",
                    error = format!("{:?}", e),
                    job_id = *ctx.config.id
                );
            }
        }

        // tell the processes to stop

        for i in 0..10 {
            if ctx
                .scheduler
                .workers_for_job(&ctx.config.id, Some(ctx.status.run_id))
                .await?
                .is_empty()
            {
                return Ok(());
            }

            info!(
                message = "sending SIGKILL to workers",
                job_id = *ctx.config.id
            );

            if let Err(e) = ctx
                .scheduler
                .stop_workers(&ctx.config.id, Some(ctx.status.run_id), true)
                .await
            {
                warn!(
                    message = "error while stopping workers",
                    error = format!("{:?}", e),
                    job_id = *ctx.config.id
                );
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

    async fn next(mut self: Box<Self>, ctx: &mut JobContext) -> Result<Transition, StateError> {
        let pipeline_config = &config().pipeline;

        // only allow one restart for preview pipelines
        if ctx.config.ttl.is_some() {
            return Err(fatal(
                "Job encountered a fatal error; see worker logs for details",
                self.source,
            ));
        }

        if pipeline_config.allowed_restarts != -1
            && ctx.status.restarts >= pipeline_config.allowed_restarts
        {
            return Err(StateError::FatalError {
                message: format!("Exhausted retries: {}", self.reason),
                domain: self.domain,
                source: self.source,
            });
        }

        // backoff
        state_backoff(ctx.status.restarts as usize).await;

        info!(
            job_id = *ctx.config.id,
            retries_remaining = pipeline_config.allowed_restarts - ctx.status.restarts,
            "recovering pipeline"
        );

        // tear down the existing cluster
        if let Err(e) = Self::cleanup(ctx).await {
            return Err(ctx.retryable(self, "failed to tear down existing cluster", e, 10));
        }

        Ok(Transition::next(*self, Compiling))
    }
}
