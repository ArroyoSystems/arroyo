use crate::states::StateError;
use arroyo_rpc::grpc::StopMode;

use super::{Context, State, Stopped, Transition};

#[derive(Copy, Clone, Debug)]
pub enum StopBehavior {
    StopJob(StopMode),
    StopWorkers,
}

#[derive(Debug)]
pub struct Stopping {
    pub stop_mode: StopBehavior,
}

#[async_trait::async_trait]
impl State for Stopping {
    fn name(&self) -> &'static str {
        "Stopping"
    }

    async fn next(mut self: Box<Self>, ctx: &mut Context) -> Result<Transition, StateError> {
        match (ctx.job_controller.as_mut(), self.stop_mode) {
            (Some(job_controller), StopBehavior::StopJob(stop_mode)) => {
                if let Err(e) = job_controller.stop_job(stop_mode).await {
                    return Err(ctx.retryable(self, "failed while stopping job", e, 10));
                }

                if let Err(e) = ctx
                    .job_controller
                    .as_mut()
                    .unwrap()
                    .wait_for_finish(ctx.rx)
                    .await
                {
                    return Err(ctx.retryable(self, "failed while waiting for job to stop", e, 10));
                }
            }
            (_, StopBehavior::StopWorkers) | (None, _) => {
                if let Err(e) = ctx
                    .scheduler
                    .stop_workers(&ctx.config.id, Some(ctx.status.run_id), true)
                    .await
                {
                    return Err(ctx.retryable(self, "failed while stopping workers", e, 20));
                }
            }
        }

        Ok(Transition::next(*self, Stopped {}))
    }
}
