use crate::JobMessage;
use crate::states::recovering::Recovering;
use crate::states::scheduling::Scheduling;
use crate::states::stop_if_desired_non_running;
use crate::types::public::RestartMode;

use super::{JobContext, State, StateError, Transition};

#[derive(Debug)]
pub struct Restarting {
    pub mode: RestartMode,
}

#[async_trait::async_trait]
impl State for Restarting {
    fn name(&self) -> &'static str {
        "Restarting"
    }

    async fn next(mut self: Box<Self>, ctx: &mut JobContext) -> Result<Transition, StateError> {
        let job_controller = ctx.job_controller.as_mut().unwrap();

        match self.mode {
            RestartMode::safe => {
                if let Err(e) = job_controller.checkpoint(true).await {
                    return Err(ctx.retryable(self, "failed to initiate final checkpoint", e, 10));
                }

                loop {
                    match job_controller.checkpoint_finished().await {
                        Ok(done) => {
                            if done && job_controller.finished() {
                                return Ok(Transition::next(*self, Scheduling {}));
                            }
                        }
                        Err(e) => {
                            return Err(ctx.retryable(
                                self,
                                "failed while monitoring final checkpoint",
                                e,
                                10,
                            ));
                        }
                    }

                    match ctx.rx.recv().await.expect("channel closed while receiving") {
                        JobMessage::RunningMessage(msg) => {
                            if let Err(e) = job_controller.handle_message(msg).await {
                                return Err(ctx.retryable(
                                    self,
                                    "failed while waiting for job finish",
                                    e,
                                    10,
                                ));
                            }
                        }
                        JobMessage::ConfigUpdate(c) => {
                            if c.restart_mode == RestartMode::force {
                                return Ok(Transition::next(
                                    *self,
                                    Restarting {
                                        mode: RestartMode::force,
                                    },
                                ));
                            }
                            stop_if_desired_non_running!(self, &c);
                        }
                        _ => {
                            // ignore other messages
                        }
                    }
                }
            }
            RestartMode::force => {
                if let Err(e) = Recovering::cleanup(ctx).await {
                    return Err(ctx.retryable(self, "failed to tear down existing cluster", e, 10));
                }

                Ok(Transition::next(*self, Scheduling {}))
            }
        }
    }
}
