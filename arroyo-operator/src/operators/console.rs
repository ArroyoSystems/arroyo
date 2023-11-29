use crate::operator::{ArrowContext, ArrowOperator, ArrowOperatorConstructor};
use arrow_array::RecordBatch;
use arroyo_types::ArrowRecord;
use async_trait::async_trait;

pub struct ConsoleSink {}

impl ArrowOperatorConstructor for ConsoleSink {
    fn from_config(_config: Vec<u8>) -> Box<dyn ArrowOperator> {
        Box::new(Self {})
    }
}

#[async_trait]
impl ArrowOperator for ConsoleSink {
    fn name(&self) -> String {
        "ConsoleSink".to_string()
    }

    async fn process_batch(&mut self, batch: ArrowRecord, ctx: &mut ArrowContext) {
        let batch = RecordBatch::try_new(ctx.in_schemas[0].schema.clone(), batch.columns).unwrap();

        let out = std::io::stdout();
        let buf = std::io::BufWriter::new(out);
        let mut writer = arrow_json::LineDelimitedWriter::new(buf);

        writer.write_batches(&vec![&batch]).unwrap();

        writer.finish().unwrap();
    }
}
