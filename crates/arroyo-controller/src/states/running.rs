use std::time::{Duration, Instant};

use time::OffsetDateTime;
use tokio::time::MissedTickBehavior;

use tracing::error;

use crate::states::finishing::Finishing;
use crate::states::recovering::Recovering;
use crate::states::rescaling::Rescaling;
use crate::states::restarting::Restarting;
use crate::states::{fatal, stop_if_desired_running};
use crate::JobMessage;
use crate::{job_controller::ControllerProgress, states::StateError};
use arroyo_server_common::log_event;
use serde_json::json;

use super::{JobContext, State, Transition};

// after this amount of time, we consider the job to be healthy and reset the restarts counter
const HEALTHY_DURATION: Duration = Duration::from_secs(2 * 60);

// how many times we allow the job to restart before moving it to failed
const RESTARTS_ALLOWED: usize = 10;

#[derive(Debug)]
pub struct Running {}

#[async_trait::async_trait]
impl State for Running {
    fn name(&self) -> &'static str {
        "Running"
    }

    async fn next(mut self: Box<Self>, ctx: &mut JobContext) -> Result<Transition, StateError> {
        stop_if_desired_running!(self, ctx.config);

        let running_start = Instant::now();

        let mut log_interval = tokio::time::interval(Duration::from_secs(60));
        log_interval.set_missed_tick_behavior(MissedTickBehavior::Delay);

        loop {
            let ttl_end: Option<Duration> = ctx.config.ttl.map(|t| {
                let elapsed = Duration::from_micros(
                    (OffsetDateTime::now_utc() - ctx.status.start_time.unwrap())
                        .whole_microseconds() as u64,
                );

                t.checked_sub(elapsed).unwrap_or(Duration::ZERO)
            });

            tokio::select! {
                msg = ctx.rx.recv() => {
                    match msg {
                        Some(JobMessage::ConfigUpdate(c)) => {
                            stop_if_desired_running!(self, &c);

                            if c.restart_nonce != ctx.status.restart_nonce {
                                return Ok(Transition::next(*self, Restarting {
                                    mode: c.restart_mode
                                }));
                            }

                            let job_controller = ctx.job_controller.as_mut().unwrap();

                            for (op, p) in &c.parallelism_overrides {
                                if let Some(actual) = job_controller.operator_parallelism(op){
                                    if actual != *p {
                                        return Ok(Transition::next(
                                            *self,
                                            Rescaling {}
                                        ));
                                    }
                                }
                            }

                            job_controller.update_config(c);
                        }
                        Some(JobMessage::RunningMessage(msg)) => {
                            if let Err(e) = ctx.job_controller.as_mut().unwrap().handle_message(msg).await {
                                return Err(ctx.retryable(self, "job encountered an error", e, 10));
                            }
                        }
                        Some(msg) => {
                            ctx.handle(msg)?;
                        }
                        None => {
                            panic!("job queue shut down");
                        }
                    }
                }
                _ = tokio::time::sleep(Duration::from_millis(200)) => {
                    if ctx.status.restarts > 0 && running_start.elapsed() > HEALTHY_DURATION {
                        let restarts = ctx.status.restarts;
                        ctx.status.restarts = 0;
                        if let Err(e) = ctx.status.update_db(&ctx.db).await {
                            error!(message = "Failed to update status", error = format!("{:?}", e),
                                job_id = *ctx.config.id);
                            ctx.status.restarts = restarts;
                            // we'll try again on the next round
                        }
                    }

                    match ctx.job_controller.as_mut().unwrap().progress().await {
                        Ok(ControllerProgress::Continue) => {
                            // do nothing
                        },
                        Ok(ControllerProgress::Finishing) => {
                            return Ok(Transition::next(
                                *self,
                                Finishing {}
                            ))
                        },
                        Err(err) => {
                            error!(message = "error while running", error = format!("{:?}", err), job_id = *ctx.config.id);
                            log_event("running_error", json!({
                                "service": "controller",
                                "job_id": ctx.config.id,
                                "error": format!("{:?}", err),
                            }));
                            if ctx.status.restarts >= RESTARTS_ALLOWED as i32 {
                                return Err(fatal(
                                    "Job has restarted too many times",
                                    err
                                ));
                            }
                            return Ok(Transition::next(
                                *self,
                                Recovering {}
                            ))
                        }
                    }
                }
                _ = log_interval.tick() => {
                    log_event(
                        "job_running",
                        json!({
                            "service": "controller",
                            "job_id": ctx.config.id,
                            "scheduler": std::env::var("SCHEDULER").unwrap_or_else(|_| "process".to_string()),
                            "duration_ms": ctx.last_transitioned_at.elapsed().as_millis() as u64,
                        }),
                    );
                }
                _ = tokio::time::sleep(ttl_end.unwrap_or(Duration::MAX)) => {
                    // TTL has expired, stop the job
                    return Ok(Transition::next(
                        *self,
                        Stopping {
                            stop_mode: StopBehavior::StopJob(grpc::StopMode::Immediate),
                        },
                    ));
                }
            }
        }
    }
}
