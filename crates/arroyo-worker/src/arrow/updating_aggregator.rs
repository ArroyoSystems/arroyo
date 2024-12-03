use anyhow::{anyhow, Result};
use arrow::compute::concat_batches;
use std::borrow::Cow;
use std::{
    any::Any,
    collections::HashMap,
    pin::Pin,
    sync::{Arc, RwLock},
};

use arrow_array::{Array, BooleanArray, RecordBatch, StructArray};

use arrow_array::cast::AsArray;
use arrow_schema::SchemaRef;
use arroyo_df::physical::{ArroyoPhysicalExtensionCodec, DecodingContext};
use arroyo_operator::context::Collector;
use arroyo_operator::{
    context::OperatorContext,
    operator::{
        ArrowOperator, AsDisplayable, ConstructedOperator, DisplayableOperator,
        OperatorConstructor, Registry,
    },
};
use arroyo_rpc::df::ArroyoSchemaRef;
use arroyo_rpc::grpc::{api::UpdatingAggregateOperator, rpc::TableConfig};
use arroyo_rpc::{updating_meta_fields, UPDATING_META_FIELD};
use arroyo_state::timestamp_table_config;
use arroyo_types::{CheckpointBarrier, SignalMessage, Watermark};
use datafusion::common::utils::coerced_fixed_size_list_to_list;
use datafusion::execution::{
    runtime_env::{RuntimeConfig, RuntimeEnv},
    SendableRecordBatchStream,
};
use datafusion::{execution::context::SessionContext, physical_plan::ExecutionPlan};
use datafusion_proto::{physical_plan::AsExecutionPlan, protobuf::PhysicalPlanNode};
use futures::{lock::Mutex, Future};
use itertools::Itertools;
use prost::Message;
use std::time::Duration;
use tokio::sync::mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender};
use tokio_stream::StreamExt;
use tracing::log::warn;

pub struct UpdatingAggregatingFunc {
    partial_aggregation_plan: Arc<dyn ExecutionPlan>,
    partial_schema: ArroyoSchemaRef,
    state_partial_schema: ArroyoSchemaRef,
    state_final_schema: ArroyoSchemaRef,
    flush_interval: Duration,
    combine_plan: Arc<dyn ExecutionPlan>,
    finish_execution_plan: Arc<dyn ExecutionPlan>,
    receiver: Arc<RwLock<Option<UnboundedReceiver<RecordBatch>>>>,
    sender: Option<UnboundedSender<RecordBatch>>,
    // this is optional because an exec with no input has unreliable behavior.
    // In particular, if it is a global aggregate it will emit a record batch with 1 row initialized with the empty aggregate state,
    // while if it does have group by keys it will emit a record batch with 0 rows.
    exec: Arc<Mutex<Option<SendableRecordBatchStream>>>,
    ttl: Duration,
}

impl UpdatingAggregatingFunc {
    async fn flush(
        &mut self,
        ctx: &mut OperatorContext,
        collector: &mut dyn Collector,
    ) -> Result<()> {
        if self.sender.is_none() {
            return Ok(());
        }
        {
            self.sender.take();
        }

        let mut partial_batches = vec![];
        let mut flushing_exec = self.exec.lock().await.take().unwrap();

        while let Some(batch) = flushing_exec.next().await {
            partial_batches.push(batch?);
        }

        let new_partial_batch =
            concat_batches(&self.state_partial_schema.schema, &partial_batches)?;
        let prior_partials = ctx
            .table_manager
            .get_last_key_value_table("p", ctx.last_present_watermark())
            .await?;

        let mut final_input_batches = vec![];

        if let Some((prior_partial_batch, _filter)) =
            prior_partials.get_current_matching_values(&new_partial_batch)?
        {
            let combining_batches = vec![new_partial_batch, prior_partial_batch];
            let combine_batch = concat_batches(&self.partial_schema.schema, &combining_batches)?;
            let mut combine_exec = {
                let (sender, receiver) = unbounded_channel();
                sender.send(combine_batch)?;
                self.receiver.write().unwrap().replace(receiver);
                self.combine_plan
                    .execute(0, SessionContext::new().task_ctx())?
            };

            while let Some(batch) = combine_exec.next().await {
                let batch = batch?;
                let renamed_batch = RecordBatch::try_new(
                    self.state_partial_schema.schema.clone(),
                    batch.columns().to_vec(),
                )?;
                prior_partials.insert_batch(renamed_batch).await?;
                final_input_batches.push(batch);
            }
        } else {
            // all the new data is disjoint from what's in state, no need to combine.
            prior_partials
                .insert_batch(new_partial_batch.clone())
                .await?;
            final_input_batches.push(new_partial_batch);
        }

        let final_input_batch = concat_batches(&self.partial_schema.schema, &final_input_batches)?;

        let mut final_exec = {
            let (sender, receiver) = unbounded_channel();
            sender.send(final_input_batch)?;
            self.receiver.write().unwrap().replace(receiver);

            self.finish_execution_plan
                .execute(0, SessionContext::new().task_ctx())?
        };

        let final_output_table = ctx
            .table_manager
            .get_last_key_value_table("f", ctx.last_present_watermark())
            .await?;

        let mut batches_to_write = vec![];

        let out_schema = ctx.out_schema.as_ref().unwrap().schema.clone();

        while let Some(results) = final_exec.next().await {
            let results = results?;
            let renamed_results = RecordBatch::try_new(
                self.state_final_schema.schema.clone(),
                results.columns().to_vec(),
            )?;

            if let Some((prior_batch, _filter)) =
                final_output_table.get_current_matching_values(&renamed_results)?
            {
                batches_to_write.push(Self::set_retract_metadata(
                    out_schema.clone(),
                    prior_batch,
                    true,
                )?);
            }

            final_output_table
                .insert_batch(renamed_results.clone())
                .await?;

            // Update the is_retract field within the _updating_meta struct for additions
            let result_batch = Self::set_retract_metadata(out_schema.clone(), results, false)?;
            batches_to_write.push(result_batch);
        }

        if !batches_to_write.is_empty() {
            collector
                .collect(concat_batches(
                    &batches_to_write[0].schema(),
                    batches_to_write.iter(),
                )?)
                .await;
        }

        Ok(())
    }

    fn set_retract_metadata(
        out_schema: SchemaRef,
        mut batch: RecordBatch,
        is_retract: bool,
    ) -> Result<RecordBatch> {
        let updating_idx = batch.schema().index_of(UPDATING_META_FIELD)?;
        let c = batch.remove_column(updating_idx);
        let metadata = c.as_struct();
        let len = metadata.len();

        let arrays: Vec<Arc<dyn Array>> = vec![
            Arc::new(BooleanArray::from(vec![is_retract; len])),
            metadata.column(1).clone(),
        ];
        let metadata = Arc::new(StructArray::new(updating_meta_fields(), arrays, None));

        let mut columns = batch.columns().iter().cloned().collect_vec();
        columns.push(metadata);

        Ok(RecordBatch::try_new(out_schema, columns)?)
    }

    fn init_exec(&mut self) {
        let (sender, receiver) = unbounded_channel();
        {
            let mut internal_receiver = self.receiver.write().unwrap();
            *internal_receiver = Some(receiver);
        }
        let new_exec = self
            .partial_aggregation_plan
            .execute(0, SessionContext::new().task_ctx())
            .unwrap();
        self.exec = Arc::new(Mutex::new(Some(new_exec)));
        self.sender = Some(sender);
    }
}

#[async_trait::async_trait]
impl ArrowOperator for UpdatingAggregatingFunc {
    fn name(&self) -> String {
        "UpdatingAggregatingFunc".to_string()
    }

    fn display(&self) -> DisplayableOperator {
        DisplayableOperator {
            name: Cow::Borrowed("UpdatingAggregatingFunc"),
            fields: vec![
                ("flush_interval", AsDisplayable::Debug(&self.flush_interval)),
                ("ttl", AsDisplayable::Debug(&self.ttl)),
                (
                    "partial_aggregation_schema",
                    (&*self.partial_schema.schema).into(),
                ),
                (
                    "partial_aggregation_plan",
                    self.partial_aggregation_plan.as_ref().into(),
                ),
                (
                    "state_partial_schema",
                    (&*self.state_partial_schema.schema).into(),
                ),
                ("combine_plan", self.combine_plan.as_ref().into()),
                (
                    "state_final_schema",
                    (&*self.state_final_schema.schema).into(),
                ),
                (
                    "finish_execution_plan",
                    self.finish_execution_plan.as_ref().into(),
                ),
            ],
        }
    }

    async fn process_batch(
        &mut self,
        batch: RecordBatch,
        _ctx: &mut OperatorContext,
        _: &mut dyn Collector,
    ) {
        if self.sender.is_none() {
            self.init_exec();
        }
        self.sender.as_ref().unwrap().send(batch).unwrap();
    }

    async fn handle_checkpoint(
        &mut self,
        b: CheckpointBarrier,
        ctx: &mut OperatorContext,
        collector: &mut dyn Collector,
    ) {
        self.flush(ctx, collector).await.unwrap();
    }

    fn tables(&self) -> HashMap<String, TableConfig> {
        vec![
            (
                "f".to_string(),
                timestamp_table_config(
                    "f",
                    "final_table",
                    self.ttl,
                    true,
                    self.state_final_schema.as_ref().clone(),
                ),
            ),
            (
                "p".to_string(),
                timestamp_table_config(
                    "p",
                    "partial_table",
                    self.ttl,
                    true,
                    self.state_partial_schema.as_ref().clone(),
                ),
            ),
        ]
        .into_iter()
        .collect()
    }
    fn tick_interval(&self) -> Option<Duration> {
        Some(self.flush_interval)
    }

    async fn handle_tick(
        &mut self,
        _tick: u64,
        ctx: &mut OperatorContext,
        collector: &mut dyn Collector,
    ) {
        self.flush(ctx, collector).await.unwrap();
    }

    async fn handle_watermark(
        &mut self,
        watermark: Watermark,
        ctx: &mut OperatorContext,
        collector: &mut dyn Collector,
    ) -> Option<Watermark> {
        let last_watermark = ctx.last_present_watermark();
        let partial_table = ctx
            .table_manager
            .get_last_key_value_table("p", last_watermark)
            .await
            .expect("should have partial table");
        if partial_table.would_expire(last_watermark) {
            self.flush(ctx, collector).await.unwrap();
        }
        let partial_table = ctx
            .table_manager
            .get_last_key_value_table("p", last_watermark)
            .await
            .expect("should have partial table");
        partial_table
            .expire(last_watermark)
            .expect("should expire partial table");
        let final_table = ctx
            .table_manager
            .get_last_key_value_table("f", last_watermark)
            .await
            .expect("should have final table");
        final_table
            .expire(last_watermark)
            .expect("should expire final table");
        Some(watermark)
    }

    fn future_to_poll(
        &mut self,
    ) -> Option<Pin<Box<dyn Future<Output = Box<dyn Any + Send>> + Send>>> {
        self.sender.as_ref()?;
        let exec = self.exec.clone();
        Some(Box::pin(async move {
            let batch = exec.lock().await.as_mut().unwrap().next().await;
            Box::new(batch) as Box<dyn Any + Send>
        }))
    }

    async fn on_close(
        &mut self,
        final_message: &Option<SignalMessage>,
        ctx: &mut OperatorContext,
        collector: &mut dyn Collector,
    ) {
        if let Some(SignalMessage::EndOfData) = final_message {
            self.flush(ctx, collector).await.unwrap();
        }
    }

    async fn on_start(&mut self, ctx: &mut OperatorContext) {
        // fetch the tables so they are ready to be queried.
        ctx.table_manager
            .get_last_key_value_table("f", ctx.last_present_watermark())
            .await
            .unwrap();
        ctx.table_manager
            .get_last_key_value_table("p", ctx.last_present_watermark())
            .await
            .unwrap();
    }
}

pub struct UpdatingAggregatingConstructor;

impl OperatorConstructor for UpdatingAggregatingConstructor {
    type ConfigT = UpdatingAggregateOperator;

    fn with_config(
        &self,
        config: Self::ConfigT,
        registry: Arc<Registry>,
    ) -> anyhow::Result<ConstructedOperator> {
        let receiver = Arc::new(RwLock::new(None));

        let codec = ArroyoPhysicalExtensionCodec {
            context: DecodingContext::UnboundedBatchStream(receiver.clone()),
        };

        let partial_aggregation_plan =
            PhysicalPlanNode::decode(&mut config.partial_aggregation_plan.as_slice())?;

        // deserialize partial aggregation into execution plan with an UnboundedBatchStream source.
        let partial_aggregation_plan = partial_aggregation_plan.try_into_physical_plan(
            registry.as_ref(),
            &RuntimeEnv::try_new(RuntimeConfig::new()).unwrap(),
            &codec,
        )?;

        let partial_schema = config
            .partial_schema
            .ok_or_else(|| anyhow!("requires partial schema"))?
            .try_into()?;

        let combine_plan = PhysicalPlanNode::decode(&mut config.combine_plan.as_slice())?;
        let combine_execution_plan = combine_plan.try_into_physical_plan(
            registry.as_ref(),
            &RuntimeEnv::try_new(RuntimeConfig::new()).unwrap(),
            &codec,
        )?;

        let finish_plan = PhysicalPlanNode::decode(&mut config.final_aggregation_plan.as_slice())?;

        let finish_execution_plan = finish_plan.try_into_physical_plan(
            registry.as_ref(),
            &RuntimeEnv::try_new(RuntimeConfig::new()).unwrap(),
            &codec,
        )?;

        let ttl = if config.ttl_micros == 0 {
            warn!("ttl was not set for updating aggregate");
            24 * 60 * 60 * 1000 * 1000
        } else {
            config.ttl_micros
        };

        Ok(ConstructedOperator::from_operator(Box::new(
            UpdatingAggregatingFunc {
                partial_aggregation_plan,
                partial_schema: Arc::new(partial_schema),
                combine_plan: combine_execution_plan,
                state_partial_schema: Arc::new(
                    config
                        .state_partial_schema
                        .ok_or_else(|| anyhow!("requires partial schema"))?
                        .try_into()?,
                ),
                state_final_schema: Arc::new(
                    config
                        .state_final_schema
                        .ok_or_else(|| anyhow!("requires final schema"))?
                        .try_into()?,
                ),
                flush_interval: Duration::from_micros(config.flush_interval_micros),
                finish_execution_plan,
                receiver,
                sender: None,
                exec: Arc::new(Mutex::new(None)),
                ttl: Duration::from_micros(ttl),
            },
        )))
    }
}
