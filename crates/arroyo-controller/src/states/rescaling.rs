use crate::{states::stop_if_desired_non_running, JobMessage};

use super::{scheduling::Scheduling, JobContext, State, StateError, Transition};

#[derive(Debug)]
pub struct Rescaling {}

#[async_trait::async_trait]
impl State for Rescaling {
    fn name(&self) -> &'static str {
        "Rescaling"
    }

    async fn next(mut self: Box<Self>, ctx: &mut JobContext) -> Result<Transition, StateError> {
        let job_controller = ctx.job_controller.as_mut().unwrap();

        let mut final_checkpoint_started = false;

        loop {
            match job_controller.checkpoint_finished().await {
                Ok(done) => {
                    if done && job_controller.finished() && final_checkpoint_started {
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

            if !final_checkpoint_started {
                match job_controller.checkpoint(true).await {
                    Ok(started) => final_checkpoint_started = started,
                    Err(e) => {
                        return Err(ctx.retryable(
                            self,
                            "failed to initiate final checkpoint",
                            e,
                            10,
                        ));
                    }
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
                    stop_if_desired_non_running!(self, &c);
                }
                _ => {
                    // ignore other messages
                }
            }
        }
    }
}
