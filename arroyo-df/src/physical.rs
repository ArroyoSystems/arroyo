use std::{
    any::Any,
    collections::HashMap,
    sync::{Arc, RwLock},
};

use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;
use datafusion::{
    datasource::TableProvider,
    execution::context::SessionState,
    physical_plan::{
        memory::MemoryExec, streaming::PartitionStream, DisplayAs, EmptyRecordBatchStream,
        ExecutionPlan, Partitioning,
    },
};
use datafusion_common::{DataFusionError, Result as DFResult};
use datafusion_expr::Expr;
use datafusion_proto::physical_plan::PhysicalExtensionCodec;
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc::UnboundedReceiver;

#[derive(Debug, Default)]
pub struct ArroyoPhysicalExtensionCodec {
    single_locked_batch: Arc<RwLock<HashMap<String, Option<RecordBatch>>>>,
    binned_unbounded_record_batches:
        Arc<RwLock<HashMap<String, HashMap<usize, UnboundedReceiver<RecordBatch>>>>>,
}

enum DecodingContext {
    SingleLockedBatch(Arc<RwLock<HashMap<String, Option<RecordBatch>>>>),
}

// TODO: use protobufs?
#[derive(Debug, Serialize, Deserialize)]
pub enum ExecutionPlanExtensions {
    SingleLockedBatch(SingleLockedBatch),
    UnboundedRecordBatchReader(),
}

impl PhysicalExtensionCodec for ArroyoPhysicalExtensionCodec {
    fn try_decode(
        &self,
        buf: &[u8],
        inputs: &[Arc<dyn datafusion::physical_plan::ExecutionPlan>],
        registry: &dyn datafusion::execution::FunctionRegistry,
    ) -> datafusion_common::Result<Arc<dyn datafusion::physical_plan::ExecutionPlan>> {
        todo!()
    }

    fn try_encode(
        &self,
        node: Arc<dyn datafusion::physical_plan::ExecutionPlan>,
        buf: &mut Vec<u8>,
    ) -> datafusion_common::Result<()> {
        let mem_table: Option<&SingleLockedBatch> = node.as_any().downcast_ref();
        if let Some(table) = mem_table {
            serde_json::to_writer(buf, table).map_err(|err| {
                DataFusionError::Internal(format!(
                    "couldn't serialize empty partition stream {}",
                    err
                ))
            })?;
            return Ok(());
        }
        Err(DataFusionError::Internal(format!(
            "cannot serialize {:?}",
            node
        )))
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SingleLockedBatch {
    pub table_name: String,
    pub schema: SchemaRef,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ArroyoMemExec {
    pub table_name: String,
    pub schema: SchemaRef,
}

impl DisplayAs for ArroyoMemExec {
    fn fmt_as(
        &self,
        _t: datafusion::physical_plan::DisplayFormatType,
        f: &mut std::fmt::Formatter,
    ) -> std::fmt::Result {
        write!(f, "EmptyPartitionStream: schema={}", self.schema)
    }
}

impl ExecutionPlan for ArroyoMemExec {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn output_partitioning(&self) -> datafusion::physical_plan::Partitioning {
        Partitioning::UnknownPartitioning(1)
    }

    fn output_ordering(&self) -> Option<&[datafusion::physical_expr::PhysicalSortExpr]> {
        None
    }

    fn children(&self) -> Vec<Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        _children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        Err(DataFusionError::Internal("unimplemented".into()))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<datafusion::execution::TaskContext>,
    ) -> DFResult<datafusion::physical_plan::SendableRecordBatchStream> {
        MemoryExec::try_new(&vec![], self.schema.clone(), None)?.execute(partition, context)
    }

    fn statistics(&self) -> DFResult<datafusion_common::Statistics> {
        Ok(datafusion_common::Statistics::new_unknown(&self.schema))
    }
}

impl DisplayAs for SingleLockedBatch {
    fn fmt_as(
        &self,
        _t: datafusion::physical_plan::DisplayFormatType,
        f: &mut std::fmt::Formatter,
    ) -> std::fmt::Result {
        write!(f, "EmptyPartitionStream: schema={}", self.schema)
    }
}

#[async_trait::async_trait]

impl TableProvider for SingleLockedBatch {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    #[doc = " Get the type of this table for metadata/catalog purposes."]
    fn table_type(&self) -> datafusion_expr::TableType {
        datafusion_expr::TableType::Temporary
    }

    /// Create an ExecutionPlan that will scan the table.
    /// The table provider will be usually responsible of grouping
    /// the source data into partitions that can be efficiently
    /// parallelized or distributed.
    async fn scan(
        &self,
        _state: &SessionState,
        _projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        // limit can be used to reduce the amount scanned
        // from the datasource as a performance optimization.
        // If set, it contains the amount of rows needed by the `LogicalPlan`,
        // The datasource should return *at least* this number of rows if available.
        _limit: Option<usize>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(self.clone()))
    }
}

impl ExecutionPlan for SingleLockedBatch {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn output_partitioning(&self) -> datafusion::physical_plan::Partitioning {
        Partitioning::UnknownPartitioning(1)
    }

    fn output_ordering(&self) -> Option<&[datafusion::physical_expr::PhysicalSortExpr]> {
        None
    }

    fn children(&self) -> Vec<Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        _children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        Err(DataFusionError::Internal("unimplemented".into()))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<datafusion::execution::TaskContext>,
    ) -> DFResult<datafusion::physical_plan::SendableRecordBatchStream> {
        MemoryExec::try_new(&vec![], self.schema.clone(), None)?.execute(partition, context)
    }

    fn statistics(&self) -> DFResult<datafusion_common::Statistics> {
        Ok(datafusion_common::Statistics::new_unknown(&self.schema))
    }
}

impl PartitionStream for SingleLockedBatch {
    fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    fn execute(
        &self,
        _ctx: Arc<datafusion::execution::TaskContext>,
    ) -> datafusion::physical_plan::SendableRecordBatchStream {
        Box::pin(EmptyRecordBatchStream::new(self.schema.clone()))
    }
}
