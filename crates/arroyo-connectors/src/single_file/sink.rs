use std::{collections::HashMap, path::Path};

use arrow::array::RecordBatch;

use arroyo_formats::ser::ArrowSerializer;
use arroyo_rpc::grpc::TableConfig;
use arroyo_types::{CheckpointBarrier, SignalMessage};

use async_trait::async_trait;

use arroyo_operator::context::ArrowContext;
use arroyo_operator::operator::ArrowOperator;
use tokio::{
    fs::{self, File, OpenOptions},
    io::AsyncWriteExt,
};

pub struct SingleFileSink {
    pub output_path: String,
    pub serializer: ArrowSerializer,
    pub file: Option<File>,
}

#[async_trait]
impl ArrowOperator for SingleFileSink {
    fn name(&self) -> String {
        "SingleFileSink".to_string()
    }

    fn tables(&self) -> HashMap<String, TableConfig> {
        arroyo_state::global_table_config("f", "file_sink")
    }

    async fn process_batch(&mut self, batch: RecordBatch, _ctx: &mut ArrowContext) {
        let values = self.serializer.serialize(&batch);
        let file = self.file.as_mut().unwrap();
        for value in values {
            file.write_all(&value).await.unwrap();
            file.write_all(b"\n").await.unwrap();
        }
    }

    async fn on_start(&mut self, ctx: &mut ArrowContext) {
        let file_path = Path::new(&self.output_path);
        let parent = file_path.parent().unwrap();
        fs::create_dir_all(&parent).await.unwrap();
        let offset = ctx
            .table_manager
            .get_global_keyed_state("f")
            .await
            .unwrap()
            .get(&self.output_path)
            .cloned()
            .unwrap_or_default();
        let file = if offset > 0 {
            let file = OpenOptions::new()
                .append(true)
                .open(&self.output_path)
                .await
                .unwrap();
            file.set_len(offset).await.unwrap();
            file
        } else {
            OpenOptions::new()
                .write(true)
                .truncate(true)
                .create(true)
                .open(&self.output_path)
                .await
                .unwrap()
        };
        self.file = Some(file);
    }

    async fn on_close(&mut self, final_message: &Option<SignalMessage>, _ctx: &mut ArrowContext) {
        if let Some(SignalMessage::EndOfData) = final_message {
            self.file.as_mut().unwrap().flush().await.unwrap();
        }
    }

    async fn handle_checkpoint(&mut self, _b: CheckpointBarrier, ctx: &mut ArrowContext) {
        self.file.as_mut().unwrap().flush().await.unwrap();
        let state = ctx.table_manager.get_global_keyed_state("f").await.unwrap();
        state
            .insert(
                self.output_path.clone(),
                self.file.as_ref().unwrap().metadata().await.unwrap().len(),
            )
            .await;
    }
}
