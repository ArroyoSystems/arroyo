use crate::states::StateError;

use super::{JobContext, State, Stopped, Transition};

#[derive(Debug)]
pub struct Stopping {}

#[async_trait::async_trait]
impl State for Stopping {
    fn name(&self) -> &'static str {
        "Stopping"
    }

    async fn next(self: Box<Self>, ctx: &mut JobContext) -> Result<Transition, StateError> {
        if let Err(e) = ctx
            .scheduler
            .stop_workers(&ctx.config.id, Some(ctx.status.generation), true)
            .await
        {
            return Err(ctx.retryable(self, "failed while stopping workers", e, 20));
        }

        Ok(Transition::next(*self, Stopped {}))
    }
}
