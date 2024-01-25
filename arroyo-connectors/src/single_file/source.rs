use std::{collections::HashMap, io::Cursor, sync::Arc, time::SystemTime};

use arrow::array::RecordBatch;
use arroyo_rpc::{
    grpc::{StopMode, TableConfig},
    ControlMessage,
};
use arroyo_types::to_nanos;
use async_trait::async_trait;
use bincode::{Decode, Encode};
use datafusion::common::ScalarValue;
use tokio::{
    fs::File,
    io::{AsyncBufReadExt, BufReader},
};
use tracing::info;
use arroyo_operator::context::ArrowContext;
use arroyo_operator::operator::{SourceOperator};
use arroyo_operator::SourceFinishType;

#[derive(Encode, Decode, Debug, Clone, Eq, PartialEq)]
pub struct SingleFileSourceFunc {
    pub input_file: String,
    pub lines_read: usize,
}

impl SingleFileSourceFunc {
    async fn run(&mut self, ctx: &mut ArrowContext) -> SourceFinishType {
        if ctx.task_info.task_index != 0 {
            return SourceFinishType::Final;
        }

        let state: &mut arroyo_state::tables::global_keyed_map::GlobalKeyedView<String, usize> =
            ctx.table_manager.get_global_keyed_state("f").await.unwrap();

        self.lines_read = state.get(&self.input_file).map(|v| *v).unwrap_or_default();

        let file = File::open(&self.input_file).await.expect(&self.input_file);
        let mut lines = BufReader::new(file).lines();
        let schema_ref = Arc::new(ctx.out_schema.as_ref().unwrap().schema_without_timestamp());

        let mut i = 0;

        while let Some(s) = lines.next_line().await.unwrap() {
            if i < self.lines_read {
                i += 1;
                continue;
            }

            let cursor = Cursor::new(s);
            let reader = std::io::BufReader::new(cursor);
            let builder = arrow::json::reader::ReaderBuilder::new(schema_ref.clone())
                .with_batch_size(1)
                .build(reader)
                .unwrap();
            let batch = builder.into_iter().next().unwrap().unwrap();
            let mut columns = batch.columns().to_vec();
            let time_scalar =
                ScalarValue::TimestampNanosecond(Some(to_nanos(SystemTime::now()) as i64), None);
            columns.push(time_scalar.to_array().unwrap());

            ctx.collector
                .collect(
                    RecordBatch::try_new(ctx.out_schema.as_ref().unwrap().schema.clone(), columns)
                        .unwrap(),
                )
                .await;

            self.lines_read += 1;
            i += 1;

            // wait for a control message after each line
            match ctx.control_rx.recv().await {
                Some(ControlMessage::Checkpoint(c)) => {
                    let state: &mut arroyo_state::tables::global_keyed_map::GlobalKeyedView<
                        String,
                        usize,
                    > = ctx.table_manager.get_global_keyed_state("f").await.unwrap();
                    state.insert(self.input_file.clone(), self.lines_read).await;
                    // checkpoint our state
                    if self.start_checkpoint(c, ctx).await {
                        return SourceFinishType::Immediate;
                    }
                }
                Some(ControlMessage::Stop { mode }) => {
                    info!("Stopping file source {:?}", mode);

                    match mode {
                        StopMode::Graceful => {
                            return SourceFinishType::Graceful;
                        }
                        StopMode::Immediate => {
                            return SourceFinishType::Immediate;
                        }
                    }
                }
                Some(ControlMessage::NoOp) => {
                    // No-op messages allow the source to advance and process a record
                }
                _ => {}
            }
        }
        info!("file source finished");
        SourceFinishType::Final
    }
}

#[async_trait]
impl SourceOperator for SingleFileSourceFunc {
    fn name(&self) -> String {
        "SingleFileSource".to_string()
    }

    fn tables(&self) -> HashMap<String, TableConfig> {
        arroyo_state::global_table_config("f", "file_source")
    }

    async fn on_start(&mut self, ctx: &mut ArrowContext) {
        let s: &mut arroyo_state::tables::global_keyed_map::GlobalKeyedView<String, usize> = ctx
            .table_manager
            .get_global_keyed_state("f")
            .await
            .expect("should have table f in file source");

        if let Some(state) = s.get(&self.input_file) {
            self.lines_read = *state;
        }
    }
    async fn run(&mut self, ctx: &mut ArrowContext) -> SourceFinishType {
        self.run(ctx).await
    }
}
