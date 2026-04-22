use super::{JobContext, State, Stopped, Transition};
use crate::states::StateError;
use arroyo_rpc::config::config;
use arroyo_rpc::grpc::rpc;
use arroyo_rpc::grpc::rpc::JobStopMode;
use std::time::Duration;
use tracing::{error, info};
const FINISH_TIMEOUT: Duration = Duration::from_secs(60);

#[derive(Copy, Clone, Debug)]
pub enum LeaderStopBehavior {
    StopJob(JobStopMode),
    StopWorkers,
}

#[derive(Debug)]
pub struct LeaderStopping {
    pub stop_behavior: LeaderStopBehavior,
}

#[async_trait::async_trait]
impl State for LeaderStopping {
    fn name(&self) -> &'static str {
        "Stopping"
    }

    async fn next(mut self: Box<Self>, ctx: &mut JobContext) -> Result<Transition, StateError> {
        match (ctx.leader_manager.as_mut(), self.stop_behavior) {
            (Some(leader_manager), LeaderStopBehavior::StopJob(mode)) => {
                if let Err(e) = leader_manager.stop_leader(mode).await {
                    return Err(ctx.retryable(
                        self,
                        "failed to send stop message to leader",
                        e,
                        10,
                    ));
                }

                let timeout = match self.stop_behavior {
                    LeaderStopBehavior::StopJob(JobStopMode::JobStopCheckpoint) => config()
                        .pipeline
                        .checkpoint
                        .timeout
                        .as_ref()
                        .map(|t| **t)
                        .unwrap_or(Duration::MAX),
                    _ => FINISH_TIMEOUT,
                };

                info!(
                    msg = "waiting for workers to terminate",
                    job_id = *ctx.config.id
                );
                // TODO: we should watch the config queue and move immediately to force stop if requested
                //  by the user
                match tokio::time::timeout(
                    timeout,
                    leader_manager.wait_for_state(rpc::JobState::JobStopped),
                )
                .await
                {
                    Ok(Ok(_)) => {}
                    Ok(Err(e)) => {
                        error!(
                            msg = "encountered error while waiting for job to stop gracefully; will try force-stopping",
                            job_id = *ctx.config.id,
                            error = e.to_string(),
                        );

                        return Ok(Transition::next(
                            *self,
                            LeaderStopping {
                                stop_behavior: LeaderStopBehavior::StopWorkers,
                            },
                        ));
                    }
                    Err(_e) => {
                        error!(
                            msg = "timed out waiting for job to stop",
                            job_id = *ctx.config.id,
                        );

                        return Ok(Transition::next(
                            *self,
                            LeaderStopping {
                                stop_behavior: LeaderStopBehavior::StopWorkers,
                            },
                        ));
                    }
                }
            }
            (_, LeaderStopBehavior::StopWorkers) | (None, _) => {
                if let Err(e) = ctx
                    .scheduler
                    .stop_workers(&ctx.config.id, Some(ctx.status.generation), true)
                    .await
                {
                    return Err(ctx.retryable(self, "failed while stopping workers", e, 20));
                }
            }
        }

        Ok(Transition::next(*self, Stopped {}))
    }
}
