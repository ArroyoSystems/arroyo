use arrow::array::RecordBatch;
use arroyo_formats::ser::ArrowSerializer;
use tokio::io::{AsyncWriteExt, BufWriter, Stdout};

use arroyo_operator::context::OperatorContext;
use arroyo_operator::operator::ArrowOperator;
use arroyo_types::SignalMessage;

pub struct StdoutSink {
    pub stdout: BufWriter<Stdout>,
    pub serializer: ArrowSerializer,
}

#[async_trait::async_trait]
impl ArrowOperator for StdoutSink {
    fn name(&self) -> String {
        "Stdout".to_string()
    }

    async fn process_batch(&mut self, batch: RecordBatch, _: &mut OperatorContext) {
        for value in self.serializer.serialize(&batch) {
            self.stdout.write_all(&value).await.unwrap();
            self.stdout.write_u8(b'\n').await.unwrap();
        }
        self.stdout.flush().await.unwrap();
    }
    async fn on_close(&mut self, _: &Option<SignalMessage>, _: &mut OperatorContext) {
        self.stdout.flush().await.unwrap();
    }
}
