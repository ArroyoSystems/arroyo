use std::{
    collections::HashMap,
    sync::{Arc, RwLock},
    time::Duration,
};

use anyhow::Result;
use arrow::compute::concat_batches;
use arrow_array::RecordBatch;
use arroyo_df::physical::{ArroyoPhysicalExtensionCodec, DecodingContext};
use arroyo_operator::context::ArrowContext;
use arroyo_operator::operator::{ArrowOperator, OperatorConstructor, OperatorNode, Registry};
use arroyo_rpc::{
    df::ArroyoSchema,
    grpc::{api, TableConfig},
};
use arroyo_state::timestamp_table_config;
use datafusion::execution::context::SessionContext;
use datafusion::execution::runtime_env::{RuntimeConfig, RuntimeEnv};
use datafusion::physical_plan::ExecutionPlan;
use datafusion_proto::{physical_plan::AsExecutionPlan, protobuf::PhysicalPlanNode};
use futures::StreamExt;
use prost::Message;

pub struct JoinWithExpiration {
    left_expiration: Duration,
    right_expiration: Duration,
    left_input_schema: ArroyoSchema,
    right_input_schema: ArroyoSchema,
    left_schema: ArroyoSchema,
    right_schema: ArroyoSchema,
    left_passer: Arc<RwLock<Option<RecordBatch>>>,
    right_passer: Arc<RwLock<Option<RecordBatch>>>,
    join_execution_plan: Arc<dyn ExecutionPlan>,
}

impl JoinWithExpiration {
    async fn process_left(
        &mut self,
        record_batch: RecordBatch,
        ctx: &mut ArrowContext,
    ) -> Result<()> {
        let left_table = ctx
            .table_manager
            .get_key_time_table("left", ctx.last_present_watermark())
            .await
            .expect("should have left table");
        let left_rows = left_table
            .insert(record_batch.clone())
            .await
            .expect("should insert");
        let right_table = ctx
            .table_manager
            .get_key_time_table("right", ctx.last_present_watermark())
            .await
            .expect("should have right table");
        let mut right_batches = vec![];
        for row in left_rows {
            if let Some(batch) = right_table
                .get_batch(row.as_ref())
                .expect("shouldn't error getting batch")
            {
                right_batches.push(batch.clone());
            }
        }
        let right_batch = concat_batches(&self.right_schema.schema, right_batches.iter()).unwrap();
        self.compute_pair(
            self.left_input_schema.unkeyed_batch(&record_batch)?,
            right_batch,
            ctx,
        )
        .await;
        Ok(())
    }

    async fn process_right(
        &mut self,
        right_batch: RecordBatch,
        ctx: &mut ArrowContext,
    ) -> Result<()> {
        let right_table = ctx
            .table_manager
            .get_key_time_table("right", ctx.last_present_watermark())
            .await
            .expect("should have right table");
        let right_rows = right_table
            .insert(right_batch.clone())
            .await
            .expect("should insert");
        let left_table = ctx
            .table_manager
            .get_key_time_table("left", ctx.last_present_watermark())
            .await
            .expect("should have left table");
        let mut left_batches = vec![];
        for row in right_rows {
            if let Some(batch) = left_table
                .get_batch(row.as_ref())
                .expect("shouldn't error getting batch")
            {
                left_batches.push(batch.clone());
            }
        }
        let left_batch = concat_batches(&self.left_schema.schema, left_batches.iter()).unwrap();
        self.compute_pair(
            left_batch,
            self.right_input_schema.unkeyed_batch(&right_batch)?,
            ctx,
        )
        .await;
        Ok(())
    }

    async fn compute_pair(
        &mut self,
        left: RecordBatch,
        right: RecordBatch,
        ctx: &mut ArrowContext,
    ) {
        {
            self.right_passer.write().unwrap().replace(right);
            self.left_passer.write().unwrap().replace(left);
        }
        self.join_execution_plan.reset().unwrap();
        let mut records = self
            .join_execution_plan
            .execute(0, SessionContext::new().task_ctx())
            .expect("successfully computed?");
        while let Some(batch) = records.next().await {
            let batch = batch.expect("should be able to compute batch");
            ctx.collect(batch).await;
        }
    }
}

#[async_trait::async_trait]
impl ArrowOperator for JoinWithExpiration {
    fn name(&self) -> String {
        "JoinWithExpiration".to_string()
    }

    async fn process_batch(&mut self, _record_batch: RecordBatch, _ctx: &mut ArrowContext) {
        unreachable!();
    }
    async fn process_batch_index(
        &mut self,
        index: usize,
        total_inputs: usize,
        record_batch: RecordBatch,
        ctx: &mut ArrowContext,
    ) {
        match index / (total_inputs / 2) {
            0 => self
                .process_left(record_batch, ctx)
                .await
                .expect("should process left"),
            1 => self
                .process_right(record_batch, ctx)
                .await
                .expect("should process right"),
            _ => unreachable!(),
        }
    }

    fn tables(&self) -> HashMap<String, TableConfig> {
        let mut tables = HashMap::new();
        tables.insert(
            "left".to_string(),
            timestamp_table_config(
                "left",
                "left join data",
                self.left_expiration,
                false,
                self.left_input_schema.clone(),
            ),
        );
        tables.insert(
            "right".to_string(),
            timestamp_table_config(
                "right",
                "right join data",
                self.right_expiration,
                false,
                self.right_input_schema.clone(),
            ),
        );
        tables
    }
}

pub struct JoinWithExpirationConstructor;
impl OperatorConstructor for JoinWithExpirationConstructor {
    type ConfigT = api::JoinOperator;
    fn with_config(
        &self,
        config: Self::ConfigT,
        registry: Arc<Registry>,
    ) -> anyhow::Result<OperatorNode> {
        let left_passer = Arc::new(RwLock::new(None));
        let right_passer = Arc::new(RwLock::new(None));

        let codec = ArroyoPhysicalExtensionCodec {
            context: DecodingContext::LockedJoinPair {
                left: left_passer.clone(),
                right: right_passer.clone(),
            },
        };
        let join_physical_plan_node = PhysicalPlanNode::decode(&mut config.join_plan.as_slice())?;
        let join_execution_plan = join_physical_plan_node.try_into_physical_plan(
            registry.as_ref(),
            &RuntimeEnv::new(RuntimeConfig::new())?,
            &codec,
        )?;

        let left_input_schema: ArroyoSchema = config.left_schema.unwrap().try_into()?;
        let right_input_schema: ArroyoSchema = config.right_schema.unwrap().try_into()?;
        let left_schema = left_input_schema.schema_without_keys()?;
        let right_schema = right_input_schema.schema_without_keys()?;

        Ok(OperatorNode::from_operator(Box::new(JoinWithExpiration {
            left_expiration: Duration::from_secs(3600),
            right_expiration: Duration::from_secs(3600),
            left_input_schema,
            right_input_schema,
            left_schema,
            right_schema,
            left_passer,
            right_passer,
            join_execution_plan,
        })))
    }
}
