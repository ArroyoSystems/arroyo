use super::{Finished, JobContext, State, Transition};
use crate::job_controller::leader_manager::handle_leader_stopping;
use crate::states::StateError;
use arroyo_rpc::grpc::rpc::JobState;

#[derive(Debug)]
pub struct LeaderFinishing {}

#[async_trait::async_trait]
impl State for LeaderFinishing {
    fn name(&self) -> &'static str {
        "Finishing"
    }

    async fn next(mut self: Box<Self>, ctx: &mut JobContext) -> Result<Transition, StateError> {
        handle_leader_stopping(*self, ctx, JobState::JobFinished, Finished {}, None).await
    }
}
