use arrow::array::{RecordBatch, TimestampNanosecondArray};
use arrow::json::writer::JsonArray;
use arrow::json::{Writer, WriterBuilder};
use std::collections::HashMap;

use arroyo_operator::context::{Collector, OperatorContext};
use arroyo_operator::operator::ArrowOperator;
use arroyo_rpc::config::config;
use arroyo_rpc::grpc::rpc::controller_grpc_client::ControllerGrpcClient;
use arroyo_rpc::grpc::rpc::{SinkDataReq, TableConfig};
use arroyo_state::global_table_config;
use arroyo_types::{from_nanos, to_micros, CheckpointBarrier, SignalMessage};
use tonic::transport::Channel;

#[derive(Default)]
pub struct PreviewSink {
    client: Option<ControllerGrpcClient<Channel>>,
    row: usize,
}

#[async_trait::async_trait]
impl ArrowOperator for PreviewSink {
    fn name(&self) -> String {
        "Preview".to_string()
    }

    fn tables(&self) -> HashMap<String, TableConfig> {
        global_table_config(
            "s",
            "Number of rows of output produced by this preview sink",
        )
    }

    async fn on_start(&mut self, ctx: &mut OperatorContext) {
        let table = ctx.table_manager.get_global_keyed_state("s").await.unwrap();

        self.row = *table.get(&ctx.task_info.task_index).unwrap_or(&0);

        self.client = Some(
            ControllerGrpcClient::connect(config().controller_endpoint())
                .await
                .unwrap(),
        );
    }

    async fn process_batch(
        &mut self,
        mut batch: RecordBatch,
        ctx: &mut OperatorContext,
        _: &mut dyn Collector,
    ) {
        let ts = ctx.in_schemas[0].timestamp_index;
        let timestamps: Vec<_> = batch
            .column(ts)
            .as_any()
            .downcast_ref::<TimestampNanosecondArray>()
            .unwrap()
            .iter()
            .map(|t| to_micros(from_nanos(t.unwrap_or(0).max(0) as u128)))
            .collect();

        batch.remove_column(ts);

        let mut buf = Vec::with_capacity(batch.get_array_memory_size());

        let mut writer: Writer<_, JsonArray> = WriterBuilder::new()
            .with_explicit_nulls(true)
            .with_timestamp_format(arrow::json::writer::TimestampFormat::RFC3339)
            .build(&mut buf);

        writer.write(&batch).unwrap();

        writer.finish().unwrap();

        self.client
            .as_mut()
            .unwrap()
            .send_sink_data(SinkDataReq {
                job_id: ctx.task_info.job_id.clone(),
                operator_id: ctx.task_info.operator_id.clone(),
                subtask_index: ctx.task_info.task_index as u32,
                timestamps,
                batch: String::from_utf8(buf).unwrap_or_else(|_| String::new()),
                start_id: self.row as u64,
                done: false,
            })
            .await
            .unwrap();

        self.row += batch.num_rows();
    }

    async fn handle_checkpoint(
        &mut self,
        b: CheckpointBarrier,
        ctx: &mut OperatorContext,
        collector: &mut dyn Collector,
    ) {
        let table = ctx
            .table_manager
            .get_global_keyed_state::<u32, usize>("s")
            .await
            .unwrap();

        table.insert(ctx.task_info.task_index, self.row).await;
    }

    async fn on_close(
        &mut self,
        _: &Option<SignalMessage>,
        ctx: &mut OperatorContext,
        _: &mut dyn Collector,
    ) {
        self.client
            .as_mut()
            .unwrap()
            .send_sink_data(SinkDataReq {
                job_id: ctx.task_info.job_id.clone(),
                operator_id: ctx.task_info.operator_id.clone(),
                subtask_index: ctx.task_info.task_index as u32,
                timestamps: vec![],
                batch: "[]".to_string(),
                start_id: self.row as u64,
                done: true,
            })
            .await
            .unwrap();
    }
}
