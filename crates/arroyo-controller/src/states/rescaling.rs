use arroyo_rpc::grpc::StopMode;

use super::{
    scheduling::Scheduling,
    stopping::{StopBehavior, Stopping},
    JobContext, State, StateError, StoppingCheckpointError, StoppingCheckpointOutcome, Transition,
};

#[derive(Debug)]
pub struct Rescaling {}

#[async_trait::async_trait]
impl State for Rescaling {
    fn name(&self) -> &'static str {
        "Rescaling"
    }

    async fn next(mut self: Box<Self>, ctx: &mut JobContext) -> Result<Transition, StateError> {
        match ctx.take_stopping_checkpoint().await {
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
        }
    }
}
