use std::{
    any::Any,
    mem,
    sync::{Arc, RwLock},
};

use arrow_array::{Array, RecordBatch, StructArray};
use arrow_schema::{DataType, Schema, SchemaRef, TimeUnit};
use datafusion::{
    execution::TaskContext,
    physical_plan::{
        memory::{MemoryExec, MemoryStream},
        stream::RecordBatchStreamAdapter,
        DisplayAs, ExecutionPlan, Partitioning,
    },
};
use datafusion_common::{
    plan_err, DataFusionError, Result as DFResult, ScalarValue, Statistics, UnnestOptions,
};

use crate::json::get_json_functions;
use crate::rewriters::UNNESTED_COL;
use arroyo_operator::operator::Registry;
use arroyo_rpc::grpc::api::arroyo_exec_node::Node;
use arroyo_rpc::grpc::api::{arroyo_exec_node, ArroyoExecNode, MemExecNode, UnnestExecNode};
use datafusion::physical_plan::unnest::UnnestExec;
use datafusion_expr::{ColumnarValue, ScalarUDF, Signature, TypeSignature};
use datafusion_physical_expr::expressions::Column;
use datafusion_proto::physical_plan::PhysicalExtensionCodec;
use prost::Message;
use serde::{Deserialize, Serialize};
use std::fmt::Debug;
use tokio::sync::mpsc::UnboundedReceiver;
use tokio_stream::{wrappers::UnboundedReceiverStream, StreamExt};

pub fn window_function(columns: &[ColumnarValue]) -> DFResult<ColumnarValue> {
    if columns.len() != 2 {
        return DFResult::Err(DataFusionError::Internal(format!(
            "window function expected 2 argument, got {}",
            columns.len()
        )));
    }
    // check both columns are of the correct type
    if columns[0].data_type() != DataType::Timestamp(TimeUnit::Nanosecond, None) {
        return DFResult::Err(DataFusionError::Internal(format!(
            "window function expected first argument to be a timestamp, got {:?}",
            columns[0].data_type()
        )));
    }
    if columns[1].data_type() != DataType::Timestamp(TimeUnit::Nanosecond, None) {
        return DFResult::Err(DataFusionError::Internal(format!(
            "window function expected second argument to be a timestamp, got {:?}",
            columns[1].data_type()
        )));
    }
    let fields = vec![
        Arc::new(arrow::datatypes::Field::new(
            "start",
            DataType::Timestamp(TimeUnit::Nanosecond, None),
            false,
        )),
        Arc::new(arrow::datatypes::Field::new(
            "end",
            DataType::Timestamp(TimeUnit::Nanosecond, None),
            false,
        )),
    ]
    .into();

    match (&columns[0], &columns[1]) {
        (ColumnarValue::Array(start), ColumnarValue::Array(end)) => {
            Ok(ColumnarValue::Array(Arc::new(StructArray::new(
                fields,
                vec![start.clone(), end.clone()],
                None,
            ))))
        }
        (ColumnarValue::Array(start), ColumnarValue::Scalar(end)) => {
            let end = end.to_array_of_size(start.len())?;
            Ok(ColumnarValue::Array(Arc::new(StructArray::new(
                fields,
                vec![start.clone(), end],
                None,
            ))))
        }
        (ColumnarValue::Scalar(start), ColumnarValue::Array(end)) => {
            let start = start.to_array_of_size(end.len())?;
            Ok(ColumnarValue::Array(Arc::new(StructArray::new(
                fields,
                vec![start, end.clone()],
                None,
            ))))
        }
        (ColumnarValue::Scalar(start), ColumnarValue::Scalar(end)) => {
            Ok(ColumnarValue::Scalar(ScalarValue::Struct(
                StructArray::new(fields, vec![start.to_array()?, end.to_array()?], None).into(),
            )))
        }
    }
}

fn tumble_function_implementation(
) -> Arc<dyn Fn(&[ColumnarValue]) -> DFResult<ColumnarValue> + Send + Sync> {
    Arc::new(window_function)
}

fn tumble_signature() -> Signature {
    Signature::new(
        TypeSignature::Exact(vec![
            DataType::Timestamp(TimeUnit::Nanosecond, None),
            DataType::Timestamp(TimeUnit::Nanosecond, None),
        ]),
        datafusion_expr::Volatility::Immutable,
    )
}

fn window_return_type() -> Arc<dyn Fn(&[DataType]) -> DFResult<Arc<DataType>> + Send + Sync> {
    Arc::new(|_| {
        Ok(Arc::new(DataType::Struct(
            vec![
                Arc::new(arrow::datatypes::Field::new(
                    "start",
                    DataType::Timestamp(TimeUnit::Nanosecond, None),
                    false,
                )),
                Arc::new(arrow::datatypes::Field::new(
                    "end",
                    DataType::Timestamp(TimeUnit::Nanosecond, None),
                    false,
                )),
            ]
            .into(),
        )))
    })
}

pub fn window_scalar_function() -> ScalarUDF {
    #[allow(deprecated)]
    ScalarUDF::new(
        "window",
        &tumble_signature(),
        &window_return_type(),
        &tumble_function_implementation(),
    )
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
    LockedJoinStream {
        left: Arc<RwLock<Option<UnboundedReceiver<RecordBatch>>>>,
        right: Arc<RwLock<Option<UnboundedReceiver<RecordBatch>>>>,
    },
}

pub fn new_registry() -> Registry {
    let mut registry = Registry::default();
    registry.add_udf(Arc::new(window_scalar_function()));
    for json_function in get_json_functions().values() {
        registry.add_udf(json_function.clone());
    }
    registry
}

impl PhysicalExtensionCodec for ArroyoPhysicalExtensionCodec {
    fn try_decode(
        &self,
        buf: &[u8],
        inputs: &[Arc<dyn datafusion::physical_plan::ExecutionPlan>],
        _registry: &dyn datafusion::execution::FunctionRegistry,
    ) -> datafusion_common::Result<Arc<dyn datafusion::physical_plan::ExecutionPlan>> {
        let exec: ArroyoExecNode = Message::decode(buf)
            .map_err(|err| DataFusionError::Internal(format!("couldn't deserialize: {}", err)))?;

        match exec
            .node
            .ok_or_else(|| DataFusionError::Internal("exec node is empty".to_string()))?
        {
            Node::MemExec(mem_exec) => {
                let schema: Schema = serde_json::from_str(&mem_exec.schema).map_err(|e| {
                    DataFusionError::Internal(format!("invalid schema in exec codec: {:?}", e))
                })?;
                let schema = Arc::new(schema);
                match &self.context {
                    DecodingContext::SingleLockedBatch(single_batch) => {
                        Ok(Arc::new(RwLockRecordBatchReader {
                            schema,
                            locked_batch: single_batch.clone(),
                        }))
                    }
                    DecodingContext::UnboundedBatchStream(unbounded_stream) => {
                        Ok(Arc::new(UnboundedRecordBatchReader {
                            schema,
                            receiver: unbounded_stream.clone(),
                        }))
                    }
                    DecodingContext::LockedBatchVec(locked_batches) => {
                        Ok(Arc::new(RecordBatchVecReader {
                            schema,
                            receiver: locked_batches.clone(),
                        }))
                    }
                    DecodingContext::Planning => Ok(Arc::new(ArroyoMemExec {
                        table_name: mem_exec.table_name,
                        schema,
                    })),
                    DecodingContext::None => Err(DataFusionError::Internal(
                        "Need an internal context to decode".into(),
                    )),
                    DecodingContext::LockedJoinPair { left, right } => {
                        match mem_exec.table_name.as_str() {
                            "left" => Ok(Arc::new(RwLockRecordBatchReader {
                                schema,
                                locked_batch: left.clone(),
                            })),
                            "right" => Ok(Arc::new(RwLockRecordBatchReader {
                                schema,
                                locked_batch: right.clone(),
                            })),
                            _ => Err(DataFusionError::Internal(format!(
                                "unknown table name {}",
                                mem_exec.table_name
                            ))),
                        }
                    }
                    DecodingContext::LockedJoinStream { left, right } => {
                        match mem_exec.table_name.as_str() {
                            "left" => Ok(Arc::new(UnboundedRecordBatchReader {
                                schema,
                                receiver: left.clone(),
                            })),
                            "right" => Ok(Arc::new(UnboundedRecordBatchReader {
                                schema,
                                receiver: right.clone(),
                            })),
                            _ => Err(DataFusionError::Internal(format!(
                                "unknown table name {}",
                                mem_exec.table_name
                            ))),
                        }
                    }
                }
            }
            Node::UnnestExec(unnest) => {
                let schema: Schema = serde_json::from_str(&unnest.schema).map_err(|e| {
                    DataFusionError::Internal(format!("invalid schema in exec codec: {:?}", e))
                })?;
                let column = Column::new(
                    UNNESTED_COL,
                    schema.index_of(UNNESTED_COL).map_err(|_| {
                        DataFusionError::Internal(format!(
                            "unnest node schema does not contain {} col",
                            UNNESTED_COL
                        ))
                    })?,
                );

                Ok(Arc::new(UnnestExec::new(
                    inputs
                        .first()
                        .ok_or_else(|| {
                            DataFusionError::Internal("no input for unnest node".to_string())
                        })?
                        .clone(),
                    column,
                    Arc::new(schema),
                    UnnestOptions::default(),
                )))
            }
        }
    }

    fn try_encode(
        &self,
        node: Arc<dyn datafusion::physical_plan::ExecutionPlan>,
        buf: &mut Vec<u8>,
    ) -> datafusion_common::Result<()> {
        let mut proto = None;

        let mem_table: Option<&ArroyoMemExec> = node.as_any().downcast_ref();
        if let Some(table) = mem_table {
            proto = Some(ArroyoExecNode {
                node: Some(arroyo_exec_node::Node::MemExec(MemExecNode {
                    table_name: table.table_name.clone(),
                    schema: serde_json::to_string(&table.schema).unwrap(),
                })),
            });
        }

        let unnest: Option<&UnnestExec> = node.as_any().downcast_ref();
        if let Some(unnest) = unnest {
            proto = Some(ArroyoExecNode {
                node: Some(arroyo_exec_node::Node::UnnestExec(UnnestExecNode {
                    schema: serde_json::to_string(&unnest.schema()).unwrap(),
                })),
            });
        }

        if let Some(node) = proto {
            node.encode(buf).map_err(|err| {
                DataFusionError::Internal(format!("couldn't serialize exec node {}", err))
            })?;
            Ok(())
        } else {
            Err(DataFusionError::Internal(format!(
                "cannot serialize {:?}",
                node
            )))
        }
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

    fn reset(&self) -> DFResult<()> {
        Ok(())
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

    fn reset(&self) -> DFResult<()> {
        Ok(())
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

    fn reset(&self) -> DFResult<()> {
        Ok(())
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
        _partition: usize,
        _context: Arc<datafusion::execution::TaskContext>,
    ) -> DFResult<datafusion::physical_plan::SendableRecordBatchStream> {
        plan_err!("EmptyPartitionStream cannot be executed, this is only used for physical planning before serialization")
    }

    fn statistics(&self) -> DFResult<datafusion_common::Statistics> {
        Ok(datafusion_common::Statistics::new_unknown(&self.schema))
    }

    fn reset(&self) -> DFResult<()> {
        Ok(())
    }
}
