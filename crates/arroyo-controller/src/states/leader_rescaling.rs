use super::{JobContext, State, StateError, Transition, scheduling::Scheduling};
use crate::job_controller::leader_manager::handle_leader_stopping;
use arroyo_rpc::config::config;
use arroyo_rpc::grpc::rpc::{JobState, JobStopMode};

#[derive(Debug)]
pub struct LeaderRescaling {}

#[async_trait::async_trait]
impl State for LeaderRescaling {
    fn name(&self) -> &'static str {
        "Rescaling"
    }

    async fn next(mut self: Box<Self>, ctx: &mut JobContext) -> Result<Transition, StateError> {
        if let Err(e) = ctx
            .leader_manager()
            .stop_leader(JobStopMode::JobStopCheckpoint)
            .await
        {
            return Err(ctx.retryable(
                self,
                "failed to send checkpoint-stop to leader for rescaling",
                e,
                10,
            ));
        }

        let timeout = config().pipeline.checkpoint.timeout.as_ref().map(|t| **t);
        handle_leader_stopping(*self, ctx, JobState::JobStopped, Scheduling {}, timeout).await
    }
}
