use crate::states::StateError;

use super::{JobContext, State, Transition, scheduling::Scheduling};

#[derive(Debug)]
pub struct Compiling;

#[async_trait::async_trait]
impl State for Compiling {
    fn name(&self) -> &'static str {
        "Compiling"
    }

    async fn next(self: Box<Self>, _ctx: &mut JobContext) -> Result<Transition, StateError> {
        return Ok(Transition::next(*self, Scheduling {}));
    }
}
