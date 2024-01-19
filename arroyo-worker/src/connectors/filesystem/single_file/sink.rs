use std::{collections::HashMap, path::Path};

use anyhow::Result;
use arrow_array::RecordBatch;

use arroyo_rpc::{
    grpc::{api::ConnectorOp, TableConfig},
    OperatorConfig,
};
use arroyo_types::{CheckpointBarrier, SignalMessage};

use async_trait::async_trait;

use tokio::{
    fs::{self, File, OpenOptions},
    io::AsyncWriteExt,
};

use crate::{
    engine::ArrowContext,
    operator::{ArrowOperator, ArrowOperatorConstructor, OperatorNode},
};

use super::SingleFileTable;

pub struct FileSink {
    output_path: String,
    file: Option<File>,
}

impl FileSink {
    fn from_config(config_str: &str) -> Self {
        let config: OperatorConfig =
            serde_json::from_str(config_str).expect("Invalid config for FileSinkFunc");
        let table: SingleFileTable =
            serde_json::from_value(config.table).expect("Invalid table config for FileSinkFunc");
        Self {
            output_path: table.path,
            file: None,
        }
    }
}

impl ArrowOperatorConstructor<ConnectorOp> for FileSink {
    fn from_config(connector_op: ConnectorOp) -> Result<OperatorNode> {
        Ok(OperatorNode::from_operator(Box::new(Self::from_config(
            &connector_op.config,
        ))))
    }
}

#[async_trait]
impl ArrowOperator for FileSink {
    fn name(&self) -> String {
        "SingleFileSink".to_string()
    }

    fn tables(&self) -> HashMap<String, TableConfig> {
        arroyo_state::global_table_config("f", "file_sink")
    }

    async fn process_batch(&mut self, batch: RecordBatch, _ctx: &mut ArrowContext) {
        let json_rows: Vec<_> = arrow_json::writer::record_batches_to_json_rows(&[&batch])
            .unwrap()
            .into_iter()
            .map(|mut r| {
                r.remove("_timestamp");
                r
            })
            .collect();
        let file = self.file.as_mut().unwrap();
        for map in json_rows {
            let value = serde_json::to_string(&map).unwrap();
            //info!("sending map {:?}", value);
            file.write_all(value.as_bytes()).await.unwrap();
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
        self.file = Some(
            OpenOptions::new()
                .write(true)
                .append(offset > 0)
                .create(offset == 0)
                .open(&self.output_path)
                .await
                .unwrap(),
        );
        if offset > 0 {
            self.file.as_mut().unwrap().set_len(offset).await.unwrap();
        }
    }

    async fn on_close(&mut self, final_message: &Option<SignalMessage>, _ctx: &mut ArrowContext) {
        if let Some(SignalMessage::EndOfData) = final_message {
            self.file.as_mut().unwrap().flush().await.unwrap();
        }
    }

    async fn handle_checkpoint(&mut self, _b: CheckpointBarrier, ctx: &mut ArrowContext) {
        let state = ctx.table_manager.get_global_keyed_state("f").await.unwrap();
        state
            .insert(
                self.output_path.clone(),
                self.file.as_ref().unwrap().metadata().await.unwrap().len(),
            )
            .await;
    }
}
