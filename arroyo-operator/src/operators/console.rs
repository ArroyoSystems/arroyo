use crate::operator::{ArrowContext, ArrowOperator, ArrowOperatorConstructor};
use arrow_array::RecordBatch;
use arroyo_types::ArroyoRecordBatch;
use async_trait::async_trait;
use arroyo_rpc::grpc::api;

pub struct ConsoleSink {}

impl ArrowOperatorConstructor<api::ConnectorOp> for ConsoleSink {
    fn from_config(_c: api::ConnectorOp) -> Box<dyn ArrowOperator> {
        Box::new(Self {})
    }
}

#[async_trait]
impl ArrowOperator for ConsoleSink {
    fn name(&self) -> String {
        "ConsoleSink".to_string()
    }

    async fn process_batch(&mut self, batch: ArroyoRecordBatch, ctx: &mut ArrowContext) {
        let batch = RecordBatch::try_new(ctx.in_schemas[0].schema.clone(), batch.columns).unwrap();

        let out = std::io::stdout();
        let buf = std::io::BufWriter::new(out);
        let mut writer = arrow_json::LineDelimitedWriter::new(buf);

        writer.write_batches(&vec![&batch]).unwrap();

        writer.finish().unwrap();
    }
}
