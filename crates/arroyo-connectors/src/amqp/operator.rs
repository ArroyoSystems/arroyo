use lapin::options::BasicConsumeOptions;
use arroyo_operator::{context::SourceCollector, operator::SourceOperator, SourceFinishType};


#[derive(Debug)]
pub struct AmqpSourceFunc{}

impl AmqpSourceFunc {

}

#[async_trait]
impl SourceOperator for RabbitmqStreamSourceFunc {
    fn name(&self) -> String {
        todo!()
    }

    async fn run(
        &mut self,
        ctx: &mut arroyo_operator::context::SourceContext,
        collector: &mut SourceCollector,
    ) -> SourceFinishType {
        todo!()
    }
}