use super::{
    JobContext, State, StateError, Transition, compiling::Compiling, fatal, state_backoff,
};
use crate::JobMessage;
use crate::job_controller::JobController;
use crate::job_controller::leader_manager::LeaderManager;
use arroyo_rpc::config::config;
use arroyo_rpc::errors::ErrorDomain;
use arroyo_rpc::grpc::rpc::{JobState, JobStopMode, StopMode};
use arroyo_rpc::retry;
use std::time::{Duration, Instant};
use tokio::sync::mpsc::Receiver;
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
    pub async fn cleanup_job_controller(
        job_controller: &mut JobController,
        job_id: &str,
        rx: &mut Receiver<JobMessage>,
    ) {
        // first try to stop it gracefully
        if job_controller.finished() {
            return;
        }

        // stop the job
        info!(message = "stopping job", job_id);
        let start = Instant::now();
        match job_controller.stop_job(StopMode::Immediate).await {
            Ok(_) => {
                if (timeout(Duration::from_secs(5), job_controller.wait_for_finish(rx)).await)
                    .is_ok()
                {
                    info!(
                        message = "job stopped",
                        job_id,
                        duration = start.elapsed().as_secs_f32()
                    );
                }
            }
            Err(e) => {
                warn!(
                    message = "failed to stop job",
                    error = format!("{:?}", e),
                    job_id,
                );
            }
        }
    }

    pub async fn cleanup_leader(leader_manager: &mut LeaderManager, job_id: &str) {
        let status = match leader_manager.poll_leader_status().await {
            Ok(status) => status,
            Err(e) => {
                warn!(job_id, error =? e, "failed to get leader status while recovering");
                return;
            }
        };

        let expected_state = match JobState::try_from(status.job_state) {
            Ok(JobState::JobFailed) => {
                return;
            }
            Ok(JobState::JobUnknown) | Err(_) => {
                warn!(
                    job_id,
                    "received unknown job state {} while cleaning job", status.job_state
                );
                return;
            }
            Ok(JobState::JobInitializing) => {
                warn!(job_id, "job is in initializing while cleaning");
                return;
            }
            Ok(JobState::JobRunning) => {
                // shutdown
                info!(job_id, "job is still running in recovering, shutting down");
                if retry!(
                    leader_manager
                        .stop_leader(JobStopMode::JobStopImmediate)
                        .await,
                    10,
                    Duration::from_millis(200),
                    Duration::from_secs(2),
                    |e| warn!(job_id, err = ?e, "failed to stop job")
                )
                .is_err()
                {
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
                info!(job_id, "job is failing in recovering, shutting down");
                let _ = retry!(
                    leader_manager
                        .stop_leader(JobStopMode::JobStopImmediate)
                        .await,
                    5,
                    Duration::from_millis(200),
                    Duration::from_secs(2),
                    |e| warn!(job_id, err = ?e, "failed to stop failing job")
                );
                JobState::JobFailed
            }
        };

        if let Err(e) = timeout(
            Duration::from_secs(60),
            leader_manager.wait_for_state(expected_state),
        )
        .await
        {
            warn!(job_id, error = ?e, ?expected_state, "timed out waiting for state during cleanup");
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

        info!(message = "tearing down workers", job_id = *ctx.config.id);

        ctx.scheduler
            .stop_workers(&ctx.config.id, Some(ctx.status.generation), true)
            .await
    }

    pub async fn cleanup<'a>(ctx: &mut JobContext<'a>) -> anyhow::Result<()> {
        // attempt to shutdown the job cleanly
        match (ctx.job_controller.as_mut(), ctx.leader_manager.as_mut()) {
            (Some(jc), None) => Self::cleanup_job_controller(jc, &ctx.config.id, ctx.rx).await,
            (None, Some(lm)) => Self::cleanup_leader(lm, &ctx.config.id).await,
            (Some(_), Some(_)) => unreachable!("both job controller and leader manager are set!"),
            (None, None) => {
                // somehow we got here before scheduling set the job controller / leader manager
            }
        };

        // then tear down the workers
        retry!(
            Self::tear_down_workers(ctx).await,
            10,
            Duration::from_millis(200),
            Duration::from_secs(10),
            |e| warn!(job_id = *ctx.config.id, error =? e, "failed to tear down cluster")
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
        state_backoff(ctx.status.restarts as usize).await;

        info!(
            job_id = *ctx.config.id,
            retries_remaining = pipeline_config.allowed_restarts - ctx.status.restarts,
            "recovering pipeline"
        );

        match Self::cleanup(ctx).await {
            Ok(()) => Ok(Transition::next(*self, Compiling)),
            Err(e) => Err(ctx.retryable(self, "failed to tear down existing cluster", e, 20)),
        }
    }
}
