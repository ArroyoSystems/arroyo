use anyhow::{anyhow, Result};
use arrow::compute::{concat, concat_batches, filter_record_batch, not};
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
use arroyo_operator::context::Collector;
use arroyo_operator::{
    context::OperatorContext,
    operator::{
        ArrowOperator, AsDisplayable, ConstructedOperator, DisplayableOperator,
        OperatorConstructor, Registry,
    },
};
use arroyo_planner::physical::{ArroyoPhysicalExtensionCodec, DecodingContext};
use arroyo_rpc::df::{ArroyoSchema, ArroyoSchemaRef};
use arroyo_rpc::grpc::{api::UpdatingAggregateOperator, rpc::TableConfig};
use arroyo_rpc::{updating_meta_fields, UPDATING_META_FIELD};
use arroyo_state::timestamp_table_config;
use arroyo_types::{CheckpointBarrier, SignalMessage, Watermark};
use datafusion::execution::{
    runtime_env::{RuntimeConfig, RuntimeEnv},
    SendableRecordBatchStream,
};
use datafusion::{execution::context::SessionContext, physical_plan::ExecutionPlan};
use datafusion_proto::{physical_plan::AsExecutionPlan, protobuf::PhysicalPlanNode};
use futures::{lock::Mutex, Future};
use itertools::Itertools;
use datafusion::common::{Result as DFResult};
use prost::Message;
use std::time::Duration;
use arrow::row::Row;
use datafusion::physical_plan::Accumulator;
use datafusion::physical_plan::aggregates::{PhysicalGroupBy};
use datafusion::physical_plan::udaf::AggregateFunctionExpr;
use tokio::sync::mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender};
use tracing::log::warn;
use arroyo_state::tables::expiring_time_key_map::{LastKeyValueView};

pub struct UpdatingAggregatingFunc {
    flush_interval: Duration,
    group_by: PhysicalGroupBy,
    aggregates: Vec<Arc<AggregateFunctionExpr>>,
    partial_schema: Arc<ArroyoSchema>,
    ttl: Duration,
}

impl UpdatingAggregatingFunc {
    async fn flush(
        &mut self,
        ctx: &mut OperatorContext,
        collector: &mut dyn Collector,
    ) -> Result<()> {


        Ok(())
    }
    
    fn get_aggregate_state_cols(&self, batch: &RecordBatch) -> Vec<Vec<Arc<dyn Array>>> {
        self.aggregates.iter().map(|agg| {
            agg.state_fields().unwrap()
                .into_iter()
                .map(|f| {
                    let idx = self.partial_schema.schema.index_of(f.name()).unwrap();
                    batch.column(idx).clone()
                })
                .collect()
        }).collect()
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
            ],
        }
    }

    async fn process_batch(
        &mut self,
        batch: RecordBatch,
        ctx: &mut OperatorContext,
        collector: &mut dyn Collector,
    ) {
        let table = ctx
            .table_manager
            .get_last_key_value_table("p", ctx.last_present_watermark())
            .await
            .expect("Failed to get table");


        let retracts = if let Some(meta_col) = batch.column_by_name(UPDATING_META_FIELD) {
            let meta_struct = meta_col
                .as_any()
                .downcast_ref::<StructArray>()
                .expect("_updating_meta must be StructArray");

            let is_retract_array = meta_struct
                .column_by_name("is_retract")
                .expect("meta struct must have is_retract");
            let is_retract = is_retract_array
                .as_any()
                .downcast_ref::<BooleanArray>()
                .expect("is_retract must be BooleanArray");


            Some(is_retract)
        } else {
            None
        };
        

        // First we compute the accumulators for each group-by key, which are either pulled from state
        // (if we've seen this key before) or created fresh.
        let mut accumulators: HashMap<Row, Vec<Box<dyn Accumulator>>> = HashMap::new();
        let (rows, data, mask) = table.get_current_matching_values(&batch).unwrap();
        {
            let aggregate_state_cols = self.get_aggregate_state_cols(&data);
            
            // the data array is dense (containing only present elements) so we need to separately
            // track the index that we're at, accounting for missing values
            let mut idx = 0;
            
            for (row, present) in rows.iter().zip(mask.iter()) {
                if accumulators.contains_key(&row) {
                    continue;
                }

                accumulators.insert(row, self.aggregates.iter().zip(aggregate_state_cols.iter()).map(|(agg, cols)| {
                    let mut accumulator = agg.create_sliding_accumulator().unwrap();
                    if present == Some(true) {
                        let state: Vec<_> = cols.iter().map(|col| col.slice(idx, 1)).collect();
                        accumulator.merge_batch(&state).unwrap();
                        idx += 1
                    }
                    accumulator
                }).collect());
            }
            drop(data);
            drop(mask);
        };
        
        // Next we go through our data, either retracting or appending it
        let aggregate_input_cols = self.get_aggregate_state_cols(&batch);
        
        for (i, row) in rows.iter().enumerate() {
            let retract = retracts.map(|r| r.value(i)).unwrap_or_default();
            // all necessary accumulators should have been constructed already
            for (acc, cols) in accumulators.get_mut(&row).unwrap().iter_mut().zip(aggregate_input_cols.iter()) {
                let values: Vec<_> = cols.iter().map(|c| c.slice(i, 1)).collect();
                if retract {
                    acc.retract_batch(&values).unwrap();
                } else {
                    acc.update_batch(&values).unwrap()
                }
            }
        }
        
        // now we'll emit all of the results 
        let (rows, accumulators): (Vec<_>, Vec<_>) = accumulators.into_iter()
            .collect();
        
        let mut cols = table.convert_keys(rows).unwrap();
        for acc in accumulators {

            acc.into_iter().map(|mut acc| acc.evaluate().unwrap().to_array_of_size(1).unwrap())
                .collect::<Vec<_>>())
        }
    }


    async fn handle_checkpoint(
        &mut self,
        _: CheckpointBarrier,
        ctx: &mut OperatorContext,
        collector: &mut dyn Collector,
    ) {
        self.flush(ctx, collector).await.unwrap();
    }

    fn tables(&self) -> HashMap<String, TableConfig> {
        vec![
            (
                "p".to_string(),
                timestamp_table_config(
                    "p",
                    "partial_table",
                    self.ttl,
                    true,
                    self.partial_schema.as_ref().clone(),
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

        Some(watermark)
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
        ctx.table_manager
            .get_key_time_table("p", ctx.last_present_watermark())
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

        todo!()
        // Ok(ConstructedOperator::from_operator(Box::new(
        //     UpdatingAggregatingFunc {
        //         partial_aggregation_plan,
        //         partial_schema: Arc::new(partial_schema),
        //         combine_plan: combine_execution_plan,
        //         state_partial_schema: Arc::new(
        //             config
        //                 .state_partial_schema
        //                 .ok_or_else(|| anyhow!("requires partial schema"))?
        //                 .try_into()?,
        //         ),
        //         state_final_schema: Arc::new(
        //             config
        //                 .state_final_schema
        //                 .ok_or_else(|| anyhow!("requires final schema"))?
        //                 .try_into()?,
        //         ),
        //         flush_interval: Duration::from_micros(config.flush_interval_micros),
        //         finish_execution_plan,
        //         receiver,
        //         sender: None,
        //         exec: Arc::new(Mutex::new(None)),
        //         ttl: Duration::from_micros(ttl),
        //     },
        // )))
    }
}
