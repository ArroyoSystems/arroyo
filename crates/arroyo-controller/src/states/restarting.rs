use arroyo_rpc::grpc::StopMode;

use crate::states::recovering::Recovering;
use crate::states::scheduling::Scheduling;

use crate::types::public::RestartMode;

use super::{
    stopping::{StopBehavior, Stopping},
    JobContext, State, StateError, StoppingCheckpointError, StoppingCheckpointOutcome, Transition,
};

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
        match self.mode {
            RestartMode::safe => match ctx.take_stopping_checkpoint().await {
                Ok(StoppingCheckpointOutcome::SuccessfullyStopped) => {
                    Ok(Transition::next(*self, Scheduling {}))
                }
                Ok(StoppingCheckpointOutcome::ReceivedImmediateStop) => Ok(Transition::next(
                    *self,
                    Stopping {
                        stop_mode: StopBehavior::StopJob(StopMode::Immediate),
                    },
                )),
                Ok(StoppingCheckpointOutcome::ReceivedForceStop) => Ok(Transition::next(
                    *self,
                    Stopping {
                        stop_mode: StopBehavior::StopWorkers,
                    },
                )),
                Err(StoppingCheckpointError {
                    message,
                    source,
                    retries,
                }) => Err(ctx.retryable(self, message, source, retries)),
            },
            RestartMode::force => {
                if let Err(e) = Recovering::cleanup(ctx).await {
                    return Err(ctx.retryable(self, "failed to tear down existing cluster", e, 10));
                }

                Ok(Transition::next(*self, Scheduling {}))
            }
        }
    }
}
