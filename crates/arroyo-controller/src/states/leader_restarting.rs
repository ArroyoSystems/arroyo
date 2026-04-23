use super::{
    JobContext, State, StateError, Transition, fatal, leader_stop_if_desired_running,
    scheduling::Scheduling,
};
use crate::JobMessage;
use crate::states::recovering::Recovering;
use crate::types::public::RestartMode;
use arroyo_rpc::config::config;
use arroyo_rpc::grpc::rpc;
use arroyo_rpc::grpc::rpc::{JobFailure, JobState, JobStopMode};
use std::time::{Duration, Instant};
use tracing::{info, warn};

#[derive(Debug)]
pub struct LeaderRestarting {
    pub mode: RestartMode,
}

#[async_trait::async_trait]
impl State for LeaderRestarting {
    fn name(&self) -> &'static str {
        "Restarting"
    }

    async fn next(mut self: Box<Self>, ctx: &mut JobContext) -> Result<Transition, StateError> {
        match self.mode {
            RestartMode::safe => {
                if let Err(e) = ctx
                    .leader_manager()
                    .stop_leader(JobStopMode::JobStopCheckpoint)
                    .await
                {
                    return Err(ctx.retryable(
                        self,
                        "failed to send checkpoint-stop to leader",
                        e,
                        10,
                    ));
                }

                let started = Instant::now();

                loop {
                    let timeout = config()
                        .pipeline
                        .checkpoint
                        .timeout
                        .as_ref()
                        .map(|t| (started + **t).saturating_duration_since(Instant::now()))
                        .unwrap_or(Duration::MAX);

                    tokio::select! {
                        msg = ctx.rx.recv() => {
                            match msg {
                                Some(JobMessage::ConfigUpdate(c)) => {
                                    leader_stop_if_desired_running!(self, c, ctx);

                                }
                                Some(msg) => {
                                    warn!(job_id = *ctx.config.id, ?msg, "unexpected job message in leader mode");
                                }
                                None => {
                                    panic!("job queue shut down");
                                }
                            }
                        }
                        resp = ctx.leader_manager.as_mut().expect("leader manager not initialized").wait_for_state(JobState::JobStopped) => {
                            return if let Err(e) = resp {
                                Err(fatal("failed while waiting for checkpoint-stop during restart",e))
                            } else {
                                Ok(Transition::next(*self, Scheduling {}))
                            };
                        }
                        _ = tokio::time::sleep(timeout) => {
                            return ctx.handle_job_failure(*self, JobFailure {
                                operator_id: None,
                                task_id: None,
                                subtask_index: None,
                                message: "timed out while taking final checkpoint".to_string(),
                                error_domain: rpc::ErrorDomain::Internal as i32,
                                retry_hint: rpc::RetryHint::WithBackoff as i32,
                            }).await;
                        }
                    }
                }
            }
            RestartMode::force => {
                info!(
                    job_id = *ctx.config.id,
                    "force restarting job, tearing down cluster"
                );

                if let Err(e) = Recovering::cleanup(ctx).await {
                    return Err(ctx.retryable(self, "failed to tear down existing cluster", e, 20));
                }

                Ok(Transition::next(*self, Scheduling {}))
            }
        }
    }
}
