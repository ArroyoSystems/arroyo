use tracing::warn;

use super::recovering::Recovering;
use super::{Failed, JobContext, State, StateError, Transition};

/// Intermediate state that attempts to cleanly shut down the pipeline before transitioning to Failed.
#[derive(Debug)]
pub struct Failing {}

#[async_trait::async_trait]
impl State for Failing {
    fn name(&self) -> &'static str {
        "Failing"
    }

    async fn next(self: Box<Self>, ctx: &mut JobContext) -> Result<Transition, StateError> {
        if ctx.job_controller.is_some() {
            if let Err(e) = Recovering::cleanup(ctx).await {
                warn!(
                    message = "failed to gracefully tear down cluster during failure",
                    error = format!("{:?}", e),
                    job_id = *ctx.config.id
                );
            }
        }

        Ok(Transition::next(*self, Failed {}))
    }
}
