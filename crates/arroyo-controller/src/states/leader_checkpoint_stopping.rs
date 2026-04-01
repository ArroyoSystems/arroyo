use super::{JobContext, State, Stopped, Transition};
use crate::job_controller::leader_manager::handle_leader_stopping;
use crate::states::StateError;
use arroyo_rpc::config::config;
use arroyo_rpc::grpc::rpc::{JobState, JobStopMode};

#[derive(Debug)]
pub struct LeaderCheckpointStopping {}

#[async_trait::async_trait]
impl State for LeaderCheckpointStopping {
    fn name(&self) -> &'static str {
        "CheckpointStopping"
    }

    async fn next(mut self: Box<Self>, ctx: &mut JobContext) -> Result<Transition, StateError> {
        if let Err(e) = ctx
            .leader_manager()
            .stop_leader(JobStopMode::JobStopCheckpoint)
            .await
        {
            return Err(ctx.retryable(self, "failed to send stop message to leader", e, 10));
        }

        let timeout = config().pipeline.checkpoint.timeout.as_ref().map(|t| **t);

        handle_leader_stopping(*self, ctx, JobState::JobStopped, Stopped {}, timeout).await
    }
}
