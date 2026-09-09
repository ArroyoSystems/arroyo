use super::{
    JobContext, State, StateError, Transition, compiling::Compiling, fatal, state_backoff,
};
use crate::leader_manager::LeaderManager;
use arroyo_rpc::config::config;
use arroyo_rpc::errors::ErrorDomain;
use arroyo_rpc::grpc::rpc::{JobState, JobStopMode};
use arroyo_rpc::retry;
use std::time::Duration;
use tokio::time::timeout;
use tracing::{info, warn};

#[derive(Debug)]
pub struct Recovering {
    pub source: anyhow::Error,
    pub reason: String,
    pub domain: ErrorDomain,
}

impl Recovering {
    pub async fn cleanup_leader(
        leader_manager: &mut LeaderManager,
        job_id: &str,
        pipeline_id: &str,
    ) {
        let status =
            match timeout(Duration::from_secs(30), leader_manager.poll_leader_status()).await {
                Ok(Ok(status)) => status,
                Ok(Err(e)) => {
                    warn!(
                        %job_id,
                        pipeline_id,
                        error =? e,
                        "failed to get leader status while recovering"
                    );
                    return;
                }
                Err(e) => {
                    warn!(
                        %job_id,
                        pipeline_id,
                        error =? e,
                        "timed out polling leader status while recovering"
                    );
                    return;
                }
            };

        let expected_state = match JobState::try_from(status.job_state) {
            Ok(JobState::JobFailed) => {
                return;
            }
            Ok(JobState::JobUnknown) | Err(_) => {
                warn!(
                    %job_id,
                    pipeline_id,
                    "received unknown job state {} while cleaning job",
                    status.job_state
                );
                return;
            }
            Ok(JobState::JobInitializing) => {
                warn!(%job_id, pipeline_id, "job is in initializing while cleaning");
                return;
            }
            Ok(JobState::JobRunning) => {
                // shutdown
                info!(
                    %job_id,
                    pipeline_id, "job is still running in recovering, shutting down"
                );
                if let Err(e) = leader_manager
                    .stop_leader(JobStopMode::JobStopImmediate)
                    .await
                {
                    warn!(%job_id, pipeline_id, error =? e, "failed to stop leader");
                    return;
                }
                JobState::JobStopped
            }
            Ok(JobState::JobStopping) => {
                // wait for job to be stopped
                JobState::JobStopped
            }
            Ok(JobState::JobStopped) => {
                return;
            }
            Ok(JobState::JobFinishing) => {
                // wait for job to be finished
                JobState::JobFinished
            }
            Ok(JobState::JobFinished) => {
                return;
            }
            Ok(JobState::JobFailing) => {
                info!(
                    %job_id,
                    pipeline_id, "job is failing in recovering, shutting down"
                );
                if let Err(e) = leader_manager
                    .stop_leader(JobStopMode::JobStopImmediate)
                    .await
                {
                    warn!(%job_id, pipeline_id, error =? e, "failed to stop leader");
                    return;
                }
                JobState::JobFailed
            }
        };

        if let Err(e) = timeout(
            Duration::from_secs(60),
            leader_manager.wait_for_state(expected_state),
        )
        .await
        {
            warn!(
                %job_id,
                pipeline_id,
                error = ?e,
                ?expected_state,
                "timed out waiting for state during cleanup"
            );
        }
    }

    async fn tear_down_workers<'a>(ctx: &mut JobContext<'a>) -> anyhow::Result<()> {
        if ctx
            .scheduler
            .workers_for_job(&ctx.config.id, Some(ctx.status.generation))
            .await?
            .is_empty()
        {
            return Ok(());
        }

        info!(
            message = "tearing down workers",
            job_id = %ctx.config.id,
            pipeline_id = *ctx.pipeline_info.pipeline_id
        );

        ctx.scheduler
            .stop_workers(&ctx.config.id, Some(ctx.status.generation), true)
            .await
    }

    pub async fn cleanup<'a>(ctx: &mut JobContext<'a>) -> anyhow::Result<()> {
        // attempt to shutdown the job cleanly
        if let Some(leader_manager) = ctx.leader_manager.as_mut() {
            Self::cleanup_leader(
                leader_manager,
                &ctx.config.id,
                &ctx.pipeline_info.pipeline_id,
            )
            .await;
        }

        // clear workers
        ctx.leader_manager = None;
        ctx.status.state_context.leader = None;

        // then tear down the workers
        retry!(
            Self::tear_down_workers(ctx).await,
            10,
            Duration::from_millis(200),
            Duration::from_secs(10),
            |e| warn!(
                job_id = %ctx.config.id,
                pipeline_id = *ctx.pipeline_info.pipeline_id,
                error =? e,
                "failed to tear down cluster"
            )
        )?;

        Ok(())
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
        state_backoff(
            ctx.status.restarts as usize,
            &ctx.config.id,
            &ctx.pipeline_info.pipeline_id,
        )
        .await;

        info!(
            job_id = %ctx.config.id,
            pipeline_id = *ctx.pipeline_info.pipeline_id,
            retries_remaining = pipeline_config.allowed_restarts - ctx.status.restarts,
            "recovering pipeline"
        );

        match Self::cleanup(ctx).await {
            Ok(()) => Ok(Transition::next(*self, Compiling)),
            Err(e) => Err(ctx.retryable(self, "failed to tear down existing cluster", e, 20)),
        }
    }
}
