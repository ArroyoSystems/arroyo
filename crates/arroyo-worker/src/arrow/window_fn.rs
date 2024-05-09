use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use std::{collections::BTreeMap, sync::RwLock, time::SystemTime};

use anyhow::{anyhow, Result};
use arrow::compute::{max, min};
use arrow_array::RecordBatch;
use arroyo_df::physical::{ArroyoPhysicalExtensionCodec, DecodingContext};
use arroyo_operator::context::ArrowContext;
use arroyo_operator::operator::{ArrowOperator, OperatorConstructor, OperatorNode, Registry};
use arroyo_rpc::df::ArroyoSchema;
use arroyo_rpc::grpc::TableConfig;
use arroyo_rpc::{df::ArroyoSchemaRef, grpc::api};
use arroyo_state::timestamp_table_config;
use arroyo_types::{from_nanos, CheckpointBarrier, Watermark};
use datafusion::execution::context::SessionContext;
use datafusion::execution::runtime_env::{RuntimeConfig, RuntimeEnv};
use datafusion::execution::SendableRecordBatchStream;
use datafusion::physical_plan::ExecutionPlan;
use datafusion_proto::physical_plan::AsExecutionPlan;
use datafusion_proto::protobuf::PhysicalPlanNode;
use futures::{lock::Mutex, stream::FuturesUnordered};
use prost::Message;
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};
use tracing::warn;

use super::sync::streams::KeyedCloneableStreamFuture;

type NextBatchFuture = KeyedCloneableStreamFuture<SystemTime, SendableRecordBatchStream>;

pub struct WindowFunctionOperator {
    input_schema: ArroyoSchemaRef,
    // this is for time bucketing
    input_schema_unkeyed: ArroyoSchemaRef,
    execs: BTreeMap<SystemTime, InstantComputeHolder>,
    futures: Arc<Mutex<FuturesUnordered<NextBatchFuture>>>,
    receiver: Arc<RwLock<Option<UnboundedReceiver<RecordBatch>>>>,
    window_exec: Arc<dyn ExecutionPlan>,
}

struct InstantComputeHolder {
    active_exec: NextBatchFuture,
    sender: UnboundedSender<RecordBatch>,
}

impl WindowFunctionOperator {
    fn filter_and_split_batches(
        &self,
        batch: RecordBatch,
        watermark: Option<SystemTime>,
    ) -> Result<Vec<(RecordBatch, SystemTime)>> {
        if batch.num_rows() == 0 {
            warn!("empty batch received");
            return Ok(vec![]);
        }
        let timestamp_column = self.input_schema.timestamp_column(&batch);
        let min_timestamp = from_nanos(min(timestamp_column).unwrap() as u128);
        let max_timestamp = from_nanos(max(timestamp_column).unwrap() as u128);

        // early exit if all rows should be filtered.
        if let Some(watermark) = watermark {
            if max_timestamp < watermark {
                return Ok(vec![]);
            }
        }

        if min_timestamp == max_timestamp {
            return Ok(vec![(batch, max_timestamp)]);
        }
        let sorted_batch = self.input_schema_unkeyed.sort(batch, true)?;
        let filtered_batch = self
            .input_schema_unkeyed
            .filter_by_time(sorted_batch, watermark)?;
        let filtered_timestamps = self.input_schema.timestamp_column(&filtered_batch);
        let batches = self
            .input_schema_unkeyed
            .partition(&filtered_batch, true)?
            .into_iter()
            .map(|range| {
                (
                    filtered_batch.slice(range.start, range.end - range.start),
                    from_nanos(filtered_timestamps.value(range.start) as u128),
                )
            })
            .collect();
        Ok(batches)
    }

    async fn insert_exec(&mut self, timestamp: SystemTime) {
        let (sender, receiver) = tokio::sync::mpsc::unbounded_channel();
        {
            let mut internal_receiver = self.receiver.write().unwrap();
            *internal_receiver = Some(receiver);
        }
        let new_exec = self
            .window_exec
            .execute(0, SessionContext::new().task_ctx())
            .unwrap();
        let next_batch_future = NextBatchFuture::new(timestamp, new_exec);
        self.futures.lock().await.push(next_batch_future.clone());
        self.execs.insert(
            timestamp,
            InstantComputeHolder {
                active_exec: next_batch_future,
                sender,
            },
        );
    }

    async fn get_or_insert_exec(&mut self, timestamp: SystemTime) -> &InstantComputeHolder {
        if !self.execs.contains_key(&timestamp) {
            self.insert_exec(timestamp).await;
        }
        self.execs
            .get(&timestamp)
            .expect("have just inserted if necessary")
    }
}

#[async_trait::async_trait]
impl ArrowOperator for WindowFunctionOperator {
    fn name(&self) -> String {
        "WindowFunction".to_string()
    }
    async fn on_start(&mut self, ctx: &mut ArrowContext) {
        let watermark = ctx.last_present_watermark();
        let table = ctx
            .table_manager
            .get_expiring_time_key_table("input", watermark)
            .await
            .unwrap();
        for (timestamp, batches) in table.all_batches_for_watermark(watermark) {
            let exec = self.get_or_insert_exec(*timestamp).await;
            for batch in batches {
                exec.sender.send(batch.clone()).unwrap();
            }
        }
    }
    async fn process_batch(&mut self, batch: RecordBatch, ctx: &mut ArrowContext) {
        let current_watermark = ctx.last_present_watermark();
        let table = ctx
            .table_manager
            .get_expiring_time_key_table("input", current_watermark)
            .await
            .unwrap();
        for (batch, timestamp) in self
            .filter_and_split_batches(batch, current_watermark)
            .unwrap()
        {
            table.insert(timestamp, batch.clone());
            let bin_exec = self.get_or_insert_exec(timestamp).await;
            bin_exec.sender.send(batch).unwrap();
        }
    }

    async fn handle_watermark(
        &mut self,
        watermark_message: Watermark,
        ctx: &mut ArrowContext,
    ) -> Option<Watermark> {
        let Some(watermark) = ctx.last_present_watermark() else {
            return Some(watermark_message);
        };
        loop {
            let finished = {
                match self.execs.first_key_value() {
                    Some((timestamp, _exec)) => watermark <= *timestamp,
                    None => true,
                }
            };
            if finished {
                break;
            }
            let mut active_exec = {
                // the sender should be dropped here
                let (_timestamp, exec) = self.execs.pop_first().unwrap();
                exec.active_exec
            };
            while let (_timestamp, Some((batch, new_exec))) = active_exec.await {
                active_exec = new_exec;
                let batch = batch.expect("batch should be computable");
                ctx.collect(batch).await;
            }
        }
        Some(watermark_message)
    }

    async fn handle_checkpoint(&mut self, _cb: CheckpointBarrier, ctx: &mut ArrowContext) {
        let watermark = ctx.last_present_watermark();
        ctx.table_manager
            .get_expiring_time_key_table("input", watermark)
            .await
            .expect("should have input table")
            .flush(watermark)
            .await
            .expect("should flush");
    }

    fn tables(&self) -> HashMap<String, TableConfig> {
        let mut tables = HashMap::new();
        tables.insert(
            "input".to_string(),
            timestamp_table_config(
                "input",
                "window function input",
                Duration::ZERO,
                false,
                self.input_schema.as_ref().clone(),
            ),
        );
        tables
    }
}

pub struct WindowFunctionConstructor;
impl OperatorConstructor for WindowFunctionConstructor {
    type ConfigT = api::WindowFunctionOperator;
    fn with_config(
        &self,
        config: Self::ConfigT,
        registry: Arc<Registry>,
    ) -> anyhow::Result<OperatorNode> {
        let window_exec = PhysicalPlanNode::decode(&mut config.window_function_plan.as_slice())?;
        let input_schema = Arc::new(ArroyoSchema::try_from(
            config
                .input_schema
                .ok_or_else(|| anyhow!("missing input schema"))?,
        )?);
        let receiver = Arc::new(RwLock::new(None));
        let codec = ArroyoPhysicalExtensionCodec {
            context: DecodingContext::UnboundedBatchStream(receiver.clone()),
        };
        let window_exec = window_exec.try_into_physical_plan(
            registry.as_ref(),
            &RuntimeEnv::new(RuntimeConfig::new())?,
            &codec,
        )?;
        let input_schema_unkeyed = Arc::new(ArroyoSchema::from_schema_unkeyed(
            input_schema.schema.clone(),
        )?);
        Ok(OperatorNode::from_operator(Box::new(
            WindowFunctionOperator {
                input_schema,
                input_schema_unkeyed,
                execs: BTreeMap::new(),
                futures: Arc::new(Mutex::new(FuturesUnordered::new())),
                receiver,
                window_exec,
            },
        )))
    }
}
