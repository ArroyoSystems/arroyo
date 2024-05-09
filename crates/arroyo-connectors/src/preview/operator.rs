use arrow::array::{RecordBatch, TimestampNanosecondArray};
#[allow(deprecated)]
use arrow::json::writer::record_batches_to_json_rows;
use std::time::SystemTime;

use arroyo_operator::context::ArrowContext;
use arroyo_operator::operator::ArrowOperator;
use arroyo_rpc::grpc::controller_grpc_client::ControllerGrpcClient;
use arroyo_rpc::grpc::SinkDataReq;
use arroyo_types::{default_controller_addr, from_nanos, to_micros, SignalMessage};
use tonic::transport::Channel;

#[derive(Default)]
pub struct PreviewSink {
    client: Option<ControllerGrpcClient<Channel>>,
}

#[async_trait::async_trait]
impl ArrowOperator for PreviewSink {
    fn name(&self) -> String {
        "Preview".to_string()
    }

    async fn on_start(&mut self, _: &mut ArrowContext) {
        let controller_addr = std::env::var(arroyo_types::CONTROLLER_ADDR_ENV)
            .unwrap_or_else(|_| default_controller_addr());

        self.client = Some(
            ControllerGrpcClient::connect(controller_addr)
                .await
                .unwrap(),
        );
    }

    async fn process_batch(&mut self, record_batch: RecordBatch, ctx: &mut ArrowContext) {
        let timestamp_column = record_batch
            .column(ctx.in_schemas[0].timestamp_index)
            .as_any()
            .downcast_ref::<TimestampNanosecondArray>()
            .unwrap();

        #[allow(deprecated)]
        let json_rows = record_batches_to_json_rows(&[&record_batch]).unwrap();
        for (mut map, timestamp) in json_rows.into_iter().zip(timestamp_column.iter()) {
            map.remove("_timestamp");
            let value = serde_json::to_string(&map).unwrap();

            self.client
                .as_mut()
                .unwrap()
                .send_sink_data(SinkDataReq {
                    job_id: ctx.task_info.job_id.clone(),
                    operator_id: ctx.task_info.operator_id.clone(),
                    subtask_index: ctx.task_info.task_index as u32,
                    timestamp: to_micros(
                        timestamp
                            .map(|nanos| from_nanos(nanos as u128))
                            .unwrap_or_else(SystemTime::now),
                    ),
                    value,
                    done: false,
                })
                .await
                .unwrap();
        }
    }
    async fn on_close(&mut self, _: &Option<SignalMessage>, ctx: &mut ArrowContext) {
        self.client
            .as_mut()
            .unwrap()
            .send_sink_data(SinkDataReq {
                job_id: ctx.task_info.job_id.clone(),
                operator_id: ctx.task_info.operator_id.clone(),
                subtask_index: ctx.task_info.task_index as u32,
                timestamp: to_micros(SystemTime::now()),
                value: "".to_string(),
                done: true,
            })
            .await
            .unwrap();
    }
}
