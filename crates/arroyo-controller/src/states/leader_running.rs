use super::{JobContext, State, Transition};
use crate::JobMessage;
use crate::states::leader_finishing::LeaderFinishing;
use crate::states::leader_rescaling::LeaderRescaling;
use crate::states::leader_restarting::LeaderRestarting;
use crate::states::leader_stop_if_desired_running;
use crate::states::{StateError, fatal};
use anyhow::anyhow;
use arroyo_rpc::config::config;
use arroyo_rpc::grpc::rpc;
use arroyo_rpc::grpc::rpc::{ErrorDomain, JobFailure, RetryHint};
use arroyo_rpc::log_event;
use std::time::{Duration, Instant};
use tokio::time::MissedTickBehavior;
use tracing::{error, warn};

#[derive(Debug)]
pub struct LeaderRunning {
    pub started: Instant,
}

#[async_trait::async_trait]
impl State for LeaderRunning {
    fn name(&self) -> &'static str {
        "Running"
    }

    async fn next(mut self: Box<Self>, ctx: &mut JobContext) -> Result<Transition, StateError> {
        let pipeline_config = &config().clone().pipeline;

        let mut log_interval = tokio::time::interval(Duration::from_secs(60));
        log_interval.set_missed_tick_behavior(MissedTickBehavior::Delay);

        let mut poll_interval = tokio::time::interval(*config().controller.leader_poll_interval);
        poll_interval.set_missed_tick_behavior(MissedTickBehavior::Delay);

        leader_stop_if_desired_running!(self, ctx.config, ctx);

        let operator_parallelism = ctx.program.tasks_per_node();

        loop {
            if ctx.leader_manager().last_heartbeat.elapsed()
                > *pipeline_config.worker_heartbeat_timeout
            {
                return ctx
                    .handle_job_failure(
                        *self,
                        JobFailure {
                            operator_id: None,
                            task_id: None,
                            subtask_index: None,
                            message: format!(
                                "no response from job controller after {} seconds",
                                pipeline_config.worker_heartbeat_timeout.as_secs()
                            ),
                            error_domain: ErrorDomain::Internal as i32,
                            retry_hint: RetryHint::WithBackoff as i32,
                        },
                    )
                    .await;
            }

            if let Some(ttl) = ctx.config.ttl
                && self.started.elapsed() > ttl
            {
                return Ok(Transition::next(
                    *self,
                    LeaderStopping {
                        stop_behavior: LeaderStopBehavior::StopWorkers,
                    },
                ));
            }

            tokio::select! {
                msg = ctx.rx.recv() => {
                    match msg {
                        Some(JobMessage::ConfigUpdate(c)) => {
                            leader_stop_if_desired_running!(self, c, ctx);

                            if c.restart_nonce != ctx.status.restart_nonce {
                                return Ok(Transition::next(
                                    *self,
                                    LeaderRestarting {
                                        mode: c.restart_mode,
                                    },
                                ));
                            }

                            for (node_id, p) in &c.parallelism_overrides {
                                if let Some(actual) = operator_parallelism.get(node_id)
                                    && *actual != *p {
                                    return Ok(Transition::next(
                                        *self,
                                        LeaderRescaling {},
                                    ));
                                }
                            }

                        }
                        Some(msg) => {
                            warn!(job_id = *ctx.config.id, msg =? msg, "unexpected job message in leader mode");
                        }
                        None => {
                            panic!("job queue shut down");
                        }
                    }
                }
                _ = poll_interval.tick() => {
                    if ctx.status.restarts > 0 && self.started.elapsed() > *pipeline_config.healthy_duration {
                        let restarts = ctx.status.restarts;
                        ctx.status.restarts = 0;
                        if let Err(e) = ctx.status.update_db(&ctx.db).await {
                            error!(message = "Failed to update status", error = format!("{:?}", e),
                                job_id = *ctx.config.id);
                            ctx.status.restarts = restarts;
                        }
                    }

                    match ctx.leader_manager().poll_leader_status().await {
                        Ok(status) => {
                            let state = match rpc::JobState::try_from(status.job_state) {
                                Ok(state) => state,
                                Err(e) => {
                                    return Err(ctx.retryable(
                                        self,
                                        "leader returned invalid job state",
                                        e.into(),
                                        10,
                                    ));
                                }
                            };
                            match state {
                                rpc::JobState::JobInitializing => {
                                    return ctx.handle_job_failure(*self, JobFailure {
                                        operator_id: None,
                                        task_id: None,
                                        subtask_index: None,
                                        message: "job unexpectedly in Initializing state, should be running".to_string(),
                                        error_domain: ErrorDomain::Internal as i32,
                                        retry_hint: RetryHint::WithBackoff as i32,
                                    }).await;
                                }
                                rpc::JobState::JobRunning => {
                                    // in progress
                                }
                                rpc::JobState::JobStopping | rpc::JobState::JobStopped => {
                                    // the job somehow is stopping without us telling it to
                                    // we may want to automatically handle this in the future, but
                                    // initially I'm being conservative about automation
                                    return Err(fatal("job unexpectedly stopped",
                                        anyhow!("job unexpectedly entered {:?} state", state)));
                                }
                                rpc::JobState::JobFinishing | rpc::JobState::JobFinished => {
                                    // finishing is initiated by the workers themselves (via the
                                    // sources consuming all of their input) so we just respond
                                    // to it
                                    return Ok(Transition::next(
                                        *self,
                                        LeaderFinishing {},
                                    ));
                                }
                                rpc::JobState::JobFailing | rpc::JobState::JobFailed => {
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
                                rpc::JobState::JobUnknown => {
                                    return Err(ctx.retryable(
                                        self,
                                        "leader returned unknown job state",
                                        anyhow!("unknown leader job state"),
                                        10,
                                    ));
                                }
                            }
                        }
                        Err(err) => {
                            warn!(message = "error while polling leader status", error = format!("{:?}", err), job_id = *ctx.config.id);
                            tokio::time::sleep(Duration::from_secs(2)).await;
                        }
                    }
                }
                _ = log_interval.tick() => {
                    log_event!(
                        "job_running",
                        {
                            "service": "controller",
                            "job_id": ctx.config.id,
                            "scheduler": &config().controller.scheduler,
                        },
                        [
                            "duration_ms" => ctx.last_transitioned_at.elapsed().as_millis() as f64,
                        ]
                    );
                }
            }
        }
    }
}
