use tracing::info;

use crate::compiler::ProgramCompiler;
use crate::states::StateError;

use super::{scheduling::Scheduling, Context, State, Transition};

#[derive(Debug)]
pub struct Compiling;

#[async_trait::async_trait]
impl State for Compiling {
    fn name(&self) -> &'static str {
        "Compiling"
    }

    async fn next(self: Box<Self>, ctx: &mut Context) -> Result<Transition, StateError> {
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

        match pc.compile().await {
            Ok(res) => {
                ctx.status.pipeline_path = Some(res.pipeline_path);
                ctx.status.wasm_path = Some(res.wasm_path);
                Ok(Transition::next(*self, Scheduling {}))
            }
            Err(e) => Err(e
                .downcast::<StateError>()
                .unwrap_or_else(|e| ctx.retryable(self, "Query compilation failed", e, 10))),
        }
    }
}
