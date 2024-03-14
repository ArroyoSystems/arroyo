use anyhow::anyhow;
use arroyo_rpc::grpc::StopMode;

use crate::states::StateError;

use super::{
    fatal,
    stopping::{StopBehavior, Stopping},
    Finished, JobContext, State, StoppingCheckpointError, StoppingCheckpointOutcome, Transition,
};

#[derive(Debug)]
pub struct Finishing {}

#[async_trait::async_trait]
impl State for Finishing {
    fn name(&self) -> &'static str {
        "Finishing"
    }

    async fn next(mut self: Box<Self>, ctx: &mut JobContext) -> Result<Transition, StateError> {
        // check that the tasks have all finished their data processing
        if !ctx
            .job_controller
            .as_ref()
            .ok_or_else(|| {
                fatal(
                    "missing controller",
                    anyhow!("missing controller in Finishing state"),
                )
            })?
            .data_finished()
        {
            return Err(fatal(
                "tasks not finished",
                anyhow!("tasks not finished in Finishing state"),
            ));
        }
        match ctx.take_stopping_checkpoint().await {
            Ok(StoppingCheckpointOutcome::SuccessfullyStopped) => {
                // determine if Stopping is necessary here.
                Ok(Transition::next(*self, Finished {}))
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
