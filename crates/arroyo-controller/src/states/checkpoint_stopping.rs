use arroyo_rpc::grpc::StopMode;

use crate::states::StateError;

use super::{
    stopping::{StopBehavior, Stopping},
    JobContext, State, Stopped, StoppingCheckpointError, StoppingCheckpointOutcome, Transition,
};

#[derive(Debug)]
pub struct CheckpointStopping {}

#[async_trait::async_trait]
impl State for CheckpointStopping {
    fn name(&self) -> &'static str {
        "CheckpointStopping"
    }

    async fn next(mut self: Box<Self>, ctx: &mut JobContext) -> Result<Transition, StateError> {
        match ctx.take_stopping_checkpoint().await {
            Ok(StoppingCheckpointOutcome::SuccessfullyStopped) => {
                Ok(Transition::next(*self, Stopped {}))
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
        }
    }
}
