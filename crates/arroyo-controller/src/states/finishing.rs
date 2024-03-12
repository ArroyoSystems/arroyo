use crate::states::StateError;

use super::{Finished, JobContext, State, Transition};

#[derive(Debug)]
pub struct Finishing {}

#[async_trait::async_trait]
impl State for Finishing {
    fn name(&self) -> &'static str {
        "Finishing"
    }

    async fn next(mut self: Box<Self>, ctx: &mut JobContext) -> Result<Transition, StateError> {
        if let Err(e) = ctx
            .job_controller
            .as_mut()
            .unwrap()
            .wait_for_finish(ctx.rx)
            .await
        {
            return Err(ctx.retryable(self, "failed while waiting for job to finish", e, 10));
        }

        Ok(Transition::next(*self, Finished {}))
    }
}
