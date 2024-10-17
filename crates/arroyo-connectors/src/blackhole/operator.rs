use arrow::array::RecordBatch;
use arroyo_operator::context::ArrowContext;
use arroyo_operator::operator::ArrowOperator;
use async_trait::async_trait;

#[derive(Debug)]
pub struct BlackholeSinkFunc {}

impl BlackholeSinkFunc {
    pub fn new() -> BlackholeSinkFunc {
        BlackholeSinkFunc {}
    }
}

#[async_trait]
impl ArrowOperator for BlackholeSinkFunc {
    fn name(&self) -> String {
        "BlackholeSink".to_string()
    }

    async fn process_batch(&mut self, _: RecordBatch, _: &mut ArrowContext) {
        // no-op
    }
}
