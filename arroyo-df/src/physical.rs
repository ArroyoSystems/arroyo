use std::{
    any::Any,
    collections::HashSet,
    mem,
    sync::{Arc, RwLock},
};

use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;
use datafusion::{
    execution::TaskContext,
    physical_plan::{
        memory::{MemoryExec, MemoryStream},
        stream::RecordBatchStreamAdapter,
        DisplayAs, ExecutionPlan, Partitioning,
    },
};
use datafusion_common::{DataFusionError, Result as DFResult, Statistics};

use datafusion_execution::FunctionRegistry;
use datafusion_expr::{AggregateUDF, ScalarUDF, WindowUDF};
use datafusion_proto::physical_plan::PhysicalExtensionCodec;
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc::UnboundedReceiver;
use tokio_stream::{wrappers::UnboundedReceiverStream, StreamExt};

pub struct EmptyRegistry {}

impl FunctionRegistry for EmptyRegistry {
    fn udfs(&self) -> HashSet<String> {
        HashSet::new()
    }

    fn udf(&self, name: &str) -> datafusion_common::Result<Arc<ScalarUDF>> {
        DFResult::Err(DataFusionError::NotImplemented(format!(
            "udf {} not implemented",
            name
        )))
    }

    fn udaf(&self, name: &str) -> datafusion_common::Result<Arc<AggregateUDF>> {
        DFResult::Err(DataFusionError::NotImplemented(format!(
            "udaf {} not implemented",
            name
        )))
    }

    fn udwf(&self, name: &str) -> datafusion_common::Result<Arc<WindowUDF>> {
        DFResult::Err(DataFusionError::NotImplemented(format!(
            "udwf {} not implemented",
            name
        )))
    }
}

#[derive(Debug)]
pub struct ArroyoPhysicalExtensionCodec {
    pub context: DecodingContext,
}

impl Default for ArroyoPhysicalExtensionCodec {
    fn default() -> Self {
        Self {
            context: DecodingContext::None,
        }
    }
}
#[derive(Debug)]
pub enum DecodingContext {
    None,
    Planning,
    SingleLockedBatch(Arc<RwLock<Option<RecordBatch>>>),
    UnboundedBatchStream(Arc<RwLock<Option<UnboundedReceiver<RecordBatch>>>>),
    LockedBatchVec(Arc<RwLock<Vec<RecordBatch>>>),
    LockedJoinPair {
        left: Arc<RwLock<Option<RecordBatch>>>,
        right: Arc<RwLock<Option<RecordBatch>>>,
    },
}

impl PhysicalExtensionCodec for ArroyoPhysicalExtensionCodec {
    fn try_decode(
        &self,
        buf: &[u8],
        _inputs: &[Arc<dyn datafusion::physical_plan::ExecutionPlan>],
        _registry: &dyn datafusion::execution::FunctionRegistry,
    ) -> datafusion_common::Result<Arc<dyn datafusion::physical_plan::ExecutionPlan>> {
        let mem_exec: ArroyoMemExec = serde_json::from_slice(buf)
            .map_err(|err| DataFusionError::Internal(format!("couldn't deserialize: {}", err)))?;
        match &self.context {
            DecodingContext::SingleLockedBatch(single_batch) => {
                Ok(Arc::new(RwLockRecordBatchReader {
                    schema: mem_exec.schema.clone(),
                    locked_batch: single_batch.clone(),
                }))
            }
            DecodingContext::UnboundedBatchStream(unbounded_stream) => {
                Ok(Arc::new(UnboundedRecordBatchReader {
                    schema: mem_exec.schema.clone(),
                    receiver: unbounded_stream.clone(),
                }))
            }
            DecodingContext::LockedBatchVec(locked_batches) => Ok(Arc::new(RecordBatchVecReader {
                schema: mem_exec.schema(),
                receiver: locked_batches.clone(),
            })),
            DecodingContext::Planning => Ok(Arc::new(ArroyoMemExec {
                table_name: mem_exec.table_name,
                schema: mem_exec.schema.clone(),
            })),
            DecodingContext::None => Err(DataFusionError::Internal(
                "Need an internal context to decode".into(),
            )),
            DecodingContext::LockedJoinPair { left, right } => match mem_exec.table_name.as_str() {
                "left" => Ok(Arc::new(RwLockRecordBatchReader {
                    schema: mem_exec.schema.clone(),
                    locked_batch: left.clone(),
                })),
                "right" => Ok(Arc::new(RwLockRecordBatchReader {
                    schema: mem_exec.schema.clone(),
                    locked_batch: right.clone(),
                })),
                _ => Err(DataFusionError::Internal(format!(
                    "unknown table name {}",
                    mem_exec.table_name
                ))),
            },
        }
    }

    fn try_encode(
        &self,
        node: Arc<dyn datafusion::physical_plan::ExecutionPlan>,
        buf: &mut Vec<u8>,
    ) -> datafusion_common::Result<()> {
        let mem_table: Option<&ArroyoMemExec> = node.as_any().downcast_ref();
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

#[derive(Debug)]
struct RwLockRecordBatchReader {
    schema: SchemaRef,
    locked_batch: Arc<RwLock<Option<RecordBatch>>>,
}

impl DisplayAs for RwLockRecordBatchReader {
    fn fmt_as(
        &self,
        _t: datafusion::physical_plan::DisplayFormatType,
        f: &mut std::fmt::Formatter,
    ) -> std::fmt::Result {
        write!(f, "RW Lock RecordBatchReader")
    }
}

impl ExecutionPlan for RwLockRecordBatchReader {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn output_partitioning(&self) -> datafusion_physical_expr::Partitioning {
        datafusion_physical_expr::Partitioning::UnknownPartitioning(1)
    }

    fn output_ordering(&self) -> Option<&[datafusion_physical_expr::PhysicalSortExpr]> {
        None
    }

    fn children(&self) -> Vec<Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        _children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> datafusion_common::Result<Arc<dyn ExecutionPlan>> {
        Err(DataFusionError::Internal("not supported".into()))
    }

    fn execute(
        &self,
        _partition: usize,
        _context: Arc<TaskContext>,
    ) -> datafusion_common::Result<datafusion_execution::SendableRecordBatchStream> {
        let result = self
            .locked_batch
            .write()
            .unwrap()
            .take()
            .expect("should have set a record batch before calling execute()");
        Ok(Box::pin(MemoryStream::try_new(
            vec![result],
            self.schema.clone(),
            None,
        )?))
    }

    fn statistics(&self) -> DFResult<datafusion_common::Statistics> {
        Ok(Statistics::new_unknown(&self.schema))
    }
}

#[derive(Debug)]
struct UnboundedRecordBatchReader {
    schema: SchemaRef,
    receiver: Arc<RwLock<Option<UnboundedReceiver<RecordBatch>>>>,
}

impl DisplayAs for UnboundedRecordBatchReader {
    fn fmt_as(
        &self,
        _t: datafusion::physical_plan::DisplayFormatType,
        f: &mut std::fmt::Formatter,
    ) -> std::fmt::Result {
        write!(f, "unbounded record batch reader")
    }
}

impl ExecutionPlan for UnboundedRecordBatchReader {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn output_partitioning(&self) -> datafusion_physical_expr::Partitioning {
        datafusion_physical_expr::Partitioning::UnknownPartitioning(1)
    }

    fn output_ordering(&self) -> Option<&[datafusion_physical_expr::PhysicalSortExpr]> {
        None
    }

    fn children(&self) -> Vec<Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        _children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> datafusion_common::Result<Arc<dyn ExecutionPlan>> {
        Err(DataFusionError::Internal("not supported".into()))
    }

    fn execute(
        &self,
        _partition: usize,
        _context: Arc<TaskContext>,
    ) -> datafusion_common::Result<datafusion_execution::SendableRecordBatchStream> {
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            self.schema.clone(),
            UnboundedReceiverStream::new(
                self.receiver
                    .write()
                    .unwrap()
                    .take()
                    .expect("unbounded receiver should be present before calling exec. In general, set it and then immediately call execute()"),
            )
            .map(Ok),
        )))
    }

    fn statistics(&self) -> datafusion_common::Result<datafusion_common::Statistics> {
        Ok(datafusion_common::Statistics::new_unknown(&self.schema))
    }
}

#[derive(Debug)]
struct RecordBatchVecReader {
    schema: SchemaRef,
    receiver: Arc<RwLock<Vec<RecordBatch>>>,
}

impl DisplayAs for RecordBatchVecReader {
    fn fmt_as(
        &self,
        _t: datafusion::physical_plan::DisplayFormatType,
        f: &mut std::fmt::Formatter,
    ) -> std::fmt::Result {
        write!(f, " record batch vec reader")
    }
}

impl ExecutionPlan for RecordBatchVecReader {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn output_partitioning(&self) -> datafusion_physical_expr::Partitioning {
        datafusion_physical_expr::Partitioning::UnknownPartitioning(1)
    }

    fn output_ordering(&self) -> Option<&[datafusion_physical_expr::PhysicalSortExpr]> {
        None
    }

    fn children(&self) -> Vec<Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        _children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> datafusion_common::Result<Arc<dyn ExecutionPlan>> {
        Err(DataFusionError::Internal("not supported".into()))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> datafusion_common::Result<datafusion_execution::SendableRecordBatchStream> {
        MemoryExec::try_new(
            &[mem::take(self.receiver.write().unwrap().as_mut())],
            self.schema.clone(),
            None,
        )?
        .execute(partition, context)
    }

    fn statistics(&self) -> datafusion_common::Result<datafusion_common::Statistics> {
        Ok(datafusion_common::Statistics::new_unknown(&self.schema))
    }
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
        MemoryExec::try_new(&[], self.schema.clone(), None)?.execute(partition, context)
    }

    fn statistics(&self) -> DFResult<datafusion_common::Statistics> {
        Ok(datafusion_common::Statistics::new_unknown(&self.schema))
    }
}
