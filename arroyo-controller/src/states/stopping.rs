use crate::states::StateError;
use arroyo_rpc::grpc::StopMode;

use super::{Context, State, Stopped, Transition};

#[derive(Debug)]
pub struct Stopping {
    pub stop_mode: StopMode,
}

#[async_trait::async_trait]
impl State for Stopping {
    fn name(&self) -> &'static str {
        "Stopping"
    }

    async fn next(mut self: Box<Self>, ctx: &mut Context) -> Result<Transition, StateError> {
        if let Err(e) = ctx
            .job_controller
            .as_mut()
            .unwrap()
            .stop_job(self.stop_mode)
            .await
        {
            return Err(ctx.retryable(self, "failed while stopping job", e, 10));
        }

        if let Err(e) = ctx
            .job_controller
            .as_mut()
            .unwrap()
            .wait_for_finish(&mut ctx.rx)
            .await
        {
            return Err(ctx.retryable(self, "failed while waiting for job to stop", e, 10));
        }

        Ok(Transition::next(*self, Stopped {}))
    }
}
