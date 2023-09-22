use tokio::sync::oneshot;
use tracing::info;

use crate::states::{fatal, stop_if_desired_non_running, StateError};
use crate::{compiler::ProgramCompiler, JobMessage};

use super::{scheduling::Scheduling, JobContext, State, Transition};

#[derive(Debug)]
pub struct Compiling;

#[async_trait::async_trait]
impl State for Compiling {
    fn name(&self) -> &'static str {
        "Compiling"
    }

    async fn next(self: Box<Self>, ctx: &mut JobContext) -> Result<Transition, StateError> {
        if ctx.status.pipeline_path.is_some() {
            info!(
                message = "Pipeline already compiled",
                job_id = ctx.config.id,
            );
            return Ok(Transition::next(*self, Scheduling {}));
        }

        info!(
            message = "Compiling pipeline",
            job_id = ctx.config.id,
            hash = ctx.program.get_hash()
        );

        let pc = ProgramCompiler::new(
            ctx.config.pipeline_name.clone(),
            ctx.config.id.clone(),
            ctx.program.clone(),
        );

        let (tx, mut rx) = oneshot::channel();
        tokio::task::spawn(async move {
            tx.send(pc.compile().await).unwrap();
        });
        loop {
            tokio::select! {
                val = &mut rx => match val.map_err(|err| fatal("could not compile", err.into()))? {
                    Ok(res) => {
                        ctx.status.pipeline_path = Some(res.pipeline_path);
                        ctx.status.wasm_path = Some(res.wasm_path);
                        return Ok(Transition::next(*self, Scheduling {}));
                    }
                    Err(e) => return Err(e
                        .downcast::<StateError>()
                        .unwrap_or_else(|e| ctx.retryable(self, "Query compilation failed", e, 10))),
                },
                msg = ctx.rx.recv() => match msg {
                    Some(JobMessage::ConfigUpdate(c)) => {
                        stop_if_desired_non_running!(self, &c);
                    }
                    Some(m) => {
                        ctx.handle(m).unwrap();
                    }
                    None => {
                        panic!("Job message channel closed: {}", ctx.config.id);
                    }
                }
            }
        }
    }
}
