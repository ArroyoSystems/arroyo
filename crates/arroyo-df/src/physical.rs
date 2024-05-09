use arrow::{
    array::{AsArray, BooleanBuilder, TimestampNanosecondBuilder, UInt32Builder},
    buffer::{BooleanBuffer, NullBuffer},
    compute::{concat, kernels::zip, not, take},
};
use datafusion::execution::{RecordBatchStream, SendableRecordBatchStream};
use std::{
    any::Any,
    mem,
    pin::Pin,
    sync::{Arc, RwLock},
    task::{Context, Poll},
};

use arrow_array::{array, Array, BooleanArray, RecordBatch, StringArray, StructArray};
use arrow_schema::{DataType, Schema, SchemaRef, TimeUnit};
use datafusion::common::{
    plan_err, DataFusionError, Result as DFResult, ScalarValue, Statistics, UnnestOptions,
};
use datafusion::{
    execution::TaskContext,
    physical_plan::{
        memory::{MemoryExec, MemoryStream},
        stream::RecordBatchStreamAdapter,
        DisplayAs, ExecutionPlan, Partitioning,
    },
};

use crate::json::get_json_functions;
use crate::rewriters::UNNESTED_COL;
use arroyo_operator::operator::Registry;
use arroyo_rpc::grpc::api::{
    arroyo_exec_node, ArroyoExecNode, DebeziumEncodeNode, MemExecNode, UnnestExecNode,
};
use arroyo_rpc::{
    grpc::api::{arroyo_exec_node::Node, DebeziumDecodeNode},
    IS_RETRACT_FIELD, TIMESTAMP_FIELD,
};
use datafusion::logical_expr::{
    ColumnarValue, ReturnTypeFunction, ScalarFunctionImplementation, ScalarUDF, Signature,
    TypeSignature, Volatility,
};
use datafusion::physical_expr::expressions::Column;
use datafusion::physical_expr::EquivalenceProperties;
use datafusion::physical_plan::unnest::UnnestExec;
use datafusion::physical_plan::{ExecutionMode, PlanProperties};
use datafusion_proto::physical_plan::PhysicalExtensionCodec;
use futures::{
    ready,
    stream::{Stream, StreamExt},
};
use prost::Message;
use std::fmt::Debug;
use tokio::sync::mpsc::UnboundedReceiver;
use tokio_stream::wrappers::UnboundedReceiverStream;

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

fn tumble_function_implementation() -> ScalarFunctionImplementation {
    Arc::new(window_function)
}

fn tumble_signature() -> Signature {
    Signature::new(
        TypeSignature::Exact(vec![
            DataType::Timestamp(TimeUnit::Nanosecond, None),
            DataType::Timestamp(TimeUnit::Nanosecond, None),
        ]),
        Volatility::Immutable,
    )
}

fn window_return_type() -> ReturnTypeFunction {
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

    datafusion::functions::register_all(&mut registry).unwrap();
    datafusion::functions_array::register_all(&mut registry).unwrap();
    registry
}

fn make_properties(schema: SchemaRef) -> PlanProperties {
    PlanProperties::new(
        EquivalenceProperties::new(schema),
        Partitioning::UnknownPartitioning(1),
        ExecutionMode::Unbounded,
    )
}

impl PhysicalExtensionCodec for ArroyoPhysicalExtensionCodec {
    fn try_decode(
        &self,
        buf: &[u8],
        inputs: &[Arc<dyn datafusion::physical_plan::ExecutionPlan>],
        _registry: &dyn datafusion::execution::FunctionRegistry,
    ) -> datafusion::common::Result<Arc<dyn datafusion::physical_plan::ExecutionPlan>> {
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
                    DecodingContext::SingleLockedBatch(single_batch) => Ok(Arc::new(
                        RwLockRecordBatchReader::new(schema, single_batch.clone()),
                    )),
                    DecodingContext::UnboundedBatchStream(unbounded_stream) => Ok(Arc::new(
                        UnboundedRecordBatchReader::new(schema, unbounded_stream.clone()),
                    )),
                    DecodingContext::LockedBatchVec(locked_batches) => Ok(Arc::new(
                        RecordBatchVecReader::new(schema, locked_batches.clone()),
                    )),
                    DecodingContext::Planning => {
                        Ok(Arc::new(ArroyoMemExec::new(mem_exec.table_name, schema)))
                    }
                    DecodingContext::None => Err(DataFusionError::Internal(
                        "Need an internal context to decode".into(),
                    )),
                    DecodingContext::LockedJoinPair { left, right } => {
                        match mem_exec.table_name.as_str() {
                            "left" => {
                                Ok(Arc::new(RwLockRecordBatchReader::new(schema, left.clone())))
                            }
                            "right" => Ok(Arc::new(RwLockRecordBatchReader::new(
                                schema,
                                right.clone(),
                            ))),
                            _ => Err(DataFusionError::Internal(format!(
                                "unknown table name {}",
                                mem_exec.table_name
                            ))),
                        }
                    }
                    DecodingContext::LockedJoinStream { left, right } => {
                        match mem_exec.table_name.as_str() {
                            "left" => Ok(Arc::new(UnboundedRecordBatchReader::new(
                                schema,
                                left.clone(),
                            ))),
                            "right" => Ok(Arc::new(UnboundedRecordBatchReader::new(
                                schema,
                                right.clone(),
                            ))),
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
            Node::DebeziumDecode(debezium) => {
                let schema = Arc::new(serde_json::from_str::<Schema>(&debezium.schema).map_err(
                    |e| DataFusionError::Internal(format!("invalid schema in exec codec: {:?}", e)),
                )?);
                Ok(Arc::new(DebeziumUnrollingExec {
                    input: inputs
                        .first()
                        .ok_or_else(|| {
                            DataFusionError::Internal("no input for debezium node".to_string())
                        })?
                        .clone(),
                    schema: schema.clone(),
                    properties: make_properties(schema),
                }))
            }
            Node::DebeziumEncode(debezium) => {
                let schema = Arc::new(serde_json::from_str::<Schema>(&debezium.schema).map_err(
                    |e| DataFusionError::Internal(format!("invalid schema in exec codec: {:?}", e)),
                )?);
                Ok(Arc::new(ToDebeziumExec {
                    input: inputs
                        .first()
                        .ok_or_else(|| {
                            DataFusionError::Internal("no input for debezium node".to_string())
                        })?
                        .clone(),
                    schema: schema.clone(),
                    properties: make_properties(schema),
                }))
            }
        }
    }

    fn try_encode(
        &self,
        node: Arc<dyn datafusion::physical_plan::ExecutionPlan>,
        buf: &mut Vec<u8>,
    ) -> datafusion::common::Result<()> {
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
        let debezium_decode: Option<&DebeziumUnrollingExec> = node.as_any().downcast_ref();
        if let Some(decode) = debezium_decode {
            proto = Some(ArroyoExecNode {
                node: Some(arroyo_exec_node::Node::DebeziumDecode(DebeziumDecodeNode {
                    schema: serde_json::to_string(&decode.schema).unwrap(),
                })),
            });
        }

        let debezium_encode: Option<&ToDebeziumExec> = node.as_any().downcast_ref();
        if let Some(encode) = debezium_encode {
            proto = Some(ArroyoExecNode {
                node: Some(arroyo_exec_node::Node::DebeziumEncode(DebeziumEncodeNode {
                    schema: serde_json::to_string(&encode.schema).unwrap(),
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
    properties: PlanProperties,
}

impl RwLockRecordBatchReader {
    fn new(schema: SchemaRef, locked_batch: Arc<RwLock<Option<RecordBatch>>>) -> Self {
        Self {
            schema: schema.clone(),
            locked_batch,
            properties: make_properties(schema),
        }
    }
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

    fn children(&self) -> Vec<Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        _children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> datafusion::common::Result<Arc<dyn ExecutionPlan>> {
        Err(DataFusionError::Internal("not supported".into()))
    }

    fn execute(
        &self,
        _partition: usize,
        _context: Arc<TaskContext>,
    ) -> datafusion::common::Result<datafusion::execution::SendableRecordBatchStream> {
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

    fn statistics(&self) -> DFResult<datafusion::common::Statistics> {
        Ok(Statistics::new_unknown(&self.schema))
    }

    fn reset(&self) -> DFResult<()> {
        Ok(())
    }

    fn properties(&self) -> &PlanProperties {
        &self.properties
    }
}

#[derive(Debug)]
struct UnboundedRecordBatchReader {
    schema: SchemaRef,
    receiver: Arc<RwLock<Option<UnboundedReceiver<RecordBatch>>>>,
    properties: PlanProperties,
}

impl UnboundedRecordBatchReader {
    fn new(
        schema: SchemaRef,
        receiver: Arc<RwLock<Option<UnboundedReceiver<RecordBatch>>>>,
    ) -> Self {
        Self {
            schema: schema.clone(),
            receiver,
            properties: make_properties(schema),
        }
    }
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

    fn properties(&self) -> &PlanProperties {
        &self.properties
    }

    fn children(&self) -> Vec<Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        _children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> datafusion::common::Result<Arc<dyn ExecutionPlan>> {
        Err(DataFusionError::Internal("not supported".into()))
    }

    fn execute(
        &self,
        _partition: usize,
        _context: Arc<TaskContext>,
    ) -> datafusion::common::Result<datafusion::execution::SendableRecordBatchStream> {
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

    fn statistics(&self) -> datafusion::common::Result<datafusion::common::Statistics> {
        Ok(datafusion::common::Statistics::new_unknown(&self.schema))
    }

    fn reset(&self) -> DFResult<()> {
        Ok(())
    }
}

#[derive(Debug)]
struct RecordBatchVecReader {
    schema: SchemaRef,
    receiver: Arc<RwLock<Vec<RecordBatch>>>,
    properties: PlanProperties,
}

impl RecordBatchVecReader {
    fn new(schema: SchemaRef, receiver: Arc<RwLock<Vec<RecordBatch>>>) -> Self {
        Self {
            schema: schema.clone(),
            receiver,
            properties: make_properties(schema),
        }
    }
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

    fn properties(&self) -> &PlanProperties {
        &self.properties
    }

    fn children(&self) -> Vec<Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        _children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> datafusion::common::Result<Arc<dyn ExecutionPlan>> {
        Err(DataFusionError::Internal("not supported".into()))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> datafusion::common::Result<datafusion::execution::SendableRecordBatchStream> {
        MemoryExec::try_new(
            &[mem::take(self.receiver.write().unwrap().as_mut())],
            self.schema.clone(),
            None,
        )?
        .execute(partition, context)
    }

    fn statistics(&self) -> datafusion::common::Result<datafusion::common::Statistics> {
        Ok(datafusion::common::Statistics::new_unknown(&self.schema))
    }

    fn reset(&self) -> DFResult<()> {
        Ok(())
    }
}

#[derive(Debug, Clone)]
pub struct ArroyoMemExec {
    pub table_name: String,
    pub schema: SchemaRef,
    properties: PlanProperties,
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

impl ArroyoMemExec {
    pub fn new(table_name: String, schema: SchemaRef) -> Self {
        Self {
            schema: schema.clone(),
            table_name,
            properties: make_properties(schema),
        }
    }
}

impl ExecutionPlan for ArroyoMemExec {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn properties(&self) -> &PlanProperties {
        &self.properties
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

    fn statistics(&self) -> DFResult<datafusion::common::Statistics> {
        Ok(datafusion::common::Statistics::new_unknown(&self.schema))
    }

    fn reset(&self) -> DFResult<()> {
        Ok(())
    }
}

#[derive(Debug)]
pub struct DebeziumUnrollingExec {
    input: Arc<dyn ExecutionPlan>,
    schema: SchemaRef,
    properties: PlanProperties,
}

impl DebeziumUnrollingExec {
    pub fn try_new(input: Arc<dyn ExecutionPlan>) -> DFResult<Self> {
        let input_schema = input.schema();
        // confirm that the input schema has before, after and op columns, and before and after match
        let before_index = input_schema.index_of("before")?;
        let after_index = input_schema.index_of("after")?;
        let op_index = input_schema.index_of("op")?;
        let _timestamp_index = input_schema.index_of(TIMESTAMP_FIELD)?;
        let before_type = input_schema.field(before_index).data_type();
        let after_type = input_schema.field(after_index).data_type();
        if before_type != after_type {
            return Err(DataFusionError::Internal(
                "before and after columns must have the same type".to_string(),
            ));
        }
        // check that op is a string
        let op_type = input_schema.field(op_index).data_type();
        if *op_type != DataType::Utf8 {
            return Err(DataFusionError::Internal(
                "op column must be a string".to_string(),
            ));
        }
        // create the output schema
        let DataType::Struct(fields) = before_type else {
            return Err(DataFusionError::Internal(
                "before and after columns must be structs".to_string(),
            ));
        };
        let mut fields = fields.to_vec();
        fields.push(Arc::new(arrow::datatypes::Field::new(
            IS_RETRACT_FIELD,
            DataType::Boolean,
            false,
        )));
        fields.push(Arc::new(arrow::datatypes::Field::new(
            TIMESTAMP_FIELD,
            DataType::Timestamp(TimeUnit::Nanosecond, None),
            false,
        )));

        let schema = Arc::new(Schema::new(fields));
        Ok(Self {
            input,
            schema: schema.clone(),
            properties: make_properties(schema),
        })
    }
}

impl DisplayAs for DebeziumUnrollingExec {
    fn fmt_as(
        &self,
        _t: datafusion::physical_plan::DisplayFormatType,
        f: &mut std::fmt::Formatter,
    ) -> std::fmt::Result {
        write!(f, "DebeziumUnrollingExec")
    }
}

impl ExecutionPlan for DebeziumUnrollingExec {
    fn as_any(&self) -> &dyn Any {
        self as &dyn Any
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn properties(&self) -> &PlanProperties {
        &self.properties
    }

    fn children(&self) -> Vec<Arc<dyn ExecutionPlan>> {
        vec![self.input.clone()]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        if children.len() != 1 {
            return Err(DataFusionError::Internal(
                "DebeziumUnrollingExec wrong number of children".to_string(),
            ));
        }
        Ok(Arc::new(DebeziumUnrollingExec {
            input: children[0].clone(),
            schema: self.schema.clone(),
            properties: self.properties.clone(),
        }))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> DFResult<SendableRecordBatchStream> {
        Ok(Box::pin(DebeziumUnrollingStream::try_new(
            self.input.execute(partition, context)?,
            self.schema.clone(),
        )?))
    }

    fn reset(&self) -> DFResult<()> {
        self.input.reset()
    }
}

struct DebeziumUnrollingStream {
    input: SendableRecordBatchStream,
    schema: SchemaRef,
    before_index: usize,
    after_index: usize,
    op_index: usize,
    timestamp_index: usize,
}

impl DebeziumUnrollingStream {
    fn try_new(input: SendableRecordBatchStream, schema: SchemaRef) -> DFResult<Self> {
        let input_schema = input.schema();
        let before_index = input_schema.index_of("before")?;
        let after_index = input_schema.index_of("after")?;
        let op_index = input_schema.index_of("op")?;
        let timestamp_index = input_schema.index_of(TIMESTAMP_FIELD)?;

        Ok(Self {
            input,
            schema,
            before_index,
            after_index,
            op_index,
            timestamp_index,
        })
    }
    fn unroll_batch(&self, batch: &RecordBatch) -> DFResult<RecordBatch> {
        let before = batch.column(self.before_index).as_ref();
        let after = batch.column(self.after_index).as_ref();
        let op = batch
            .column(self.op_index)
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| DataFusionError::Internal("op column is not a string".to_string()))?;

        let timestamp = batch
            .column(self.timestamp_index)
            .as_any()
            .downcast_ref::<array::TimestampNanosecondArray>()
            .ok_or_else(|| {
                DataFusionError::Internal("timestamp column is not a timestamp".to_string())
            })?;

        let num_rows = batch.num_rows();
        let combined_array = concat(&[before, after])?;
        let mut take_indices = UInt32Builder::with_capacity(2 * num_rows);
        let mut is_retract_builder = BooleanBuilder::with_capacity(2 * num_rows);
        let mut timestamp_builder = TimestampNanosecondBuilder::with_capacity(2 * num_rows);
        for i in 0..num_rows {
            let op = op.value(i);
            match op {
                "c" | "r" => {
                    take_indices.append_value((i + num_rows) as u32);
                    is_retract_builder.append_value(false);
                    timestamp_builder.append_value(timestamp.value(i));
                }
                "u" => {
                    take_indices.append_value(i as u32);
                    is_retract_builder.append_value(true);
                    timestamp_builder.append_value(timestamp.value(i));
                    take_indices.append_value((i + num_rows) as u32);
                    is_retract_builder.append_value(false);
                    timestamp_builder.append_value(timestamp.value(i));
                }
                "d" => {
                    take_indices.append_value(i as u32);
                    is_retract_builder.append_value(true);
                    timestamp_builder.append_value(timestamp.value(i));
                }
                _ => {
                    return Err(DataFusionError::Internal(format!(
                        "unexpected op value: {}",
                        op
                    )));
                }
            }
        }
        let take_indices = take_indices.finish();
        let unrolled_array = take(&combined_array, &take_indices, None)?;
        let mut columns = unrolled_array.as_struct().columns().to_vec();
        columns.push(Arc::new(is_retract_builder.finish()));
        columns.push(Arc::new(timestamp_builder.finish()));
        Ok(RecordBatch::try_new(self.schema.clone(), columns)?)
    }
}

impl Stream for DebeziumUnrollingStream {
    type Item = DFResult<RecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
        let result =
            ready!(self.input.poll_next_unpin(cx)).map(|result| self.unroll_batch(&result?));
        Poll::Ready(result)
    }
}

impl RecordBatchStream for DebeziumUnrollingStream {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

#[derive(Debug)]
pub struct ToDebeziumExec {
    input: Arc<dyn ExecutionPlan>,
    schema: SchemaRef,
    properties: PlanProperties,
}

impl ToDebeziumExec {
    pub fn try_new(input: Arc<dyn ExecutionPlan>) -> DFResult<Self> {
        let input_schema = input.schema();
        let timestamp_index = input_schema.index_of(TIMESTAMP_FIELD)?;
        let struct_fields: Vec<_> = input_schema
            .fields()
            .into_iter()
            .enumerate()
            .filter_map(|(index, field)| {
                if field.name() == IS_RETRACT_FIELD || index == timestamp_index {
                    None
                } else {
                    Some(field.clone())
                }
            })
            .collect();
        let struct_data_type = DataType::Struct(struct_fields.into());
        let before_field = Arc::new(arrow::datatypes::Field::new(
            "before",
            struct_data_type.clone(),
            true,
        ));
        let after_field = Arc::new(arrow::datatypes::Field::new(
            "after",
            struct_data_type,
            true,
        ));
        let op_field = Arc::new(arrow::datatypes::Field::new("op", DataType::Utf8, false));
        let timestamp_field = Arc::new(input_schema.field(timestamp_index).clone());

        let output_schema = Arc::new(Schema::new(vec![
            before_field,
            after_field,
            op_field,
            timestamp_field,
        ]));

        Ok(Self {
            input,
            schema: output_schema.clone(),
            properties: make_properties(output_schema),
        })
    }
}

impl DisplayAs for ToDebeziumExec {
    fn fmt_as(
        &self,
        _t: datafusion::physical_plan::DisplayFormatType,
        f: &mut std::fmt::Formatter,
    ) -> std::fmt::Result {
        write!(f, "ToDebeziumExec")
    }
}

impl ExecutionPlan for ToDebeziumExec {
    fn as_any(&self) -> &dyn Any {
        self as &dyn Any
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn properties(&self) -> &PlanProperties {
        &self.properties
    }

    fn children(&self) -> Vec<Arc<dyn ExecutionPlan>> {
        vec![self.input.clone()]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        if children.len() != 1 {
            return Err(DataFusionError::Internal(
                "DebeziumUnrollingExec wrong number of children".to_string(),
            ));
        }
        Ok(Arc::new(DebeziumUnrollingExec::try_new(
            children[0].clone(),
        )?))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> DFResult<SendableRecordBatchStream> {
        let is_retract_index = self.input.schema().index_of(IS_RETRACT_FIELD).ok();
        let timestamp_index = self.input.schema().index_of(TIMESTAMP_FIELD)?;
        let struct_projection = (0..self.input.schema().fields().len())
            .filter(|index| {
                is_retract_index
                    .map(|is_retract_index| *index != is_retract_index)
                    .unwrap_or(true)
                    && *index != timestamp_index
            })
            .collect();
        Ok(Box::pin(ToDebeziumStream {
            input: self.input.execute(partition, context)?,
            schema: self.schema.clone(),
            is_retract_index,
            timestamp_index,
            struct_projection,
        }))
    }

    fn reset(&self) -> DFResult<()> {
        self.input.reset()
    }
}

struct ToDebeziumStream {
    input: SendableRecordBatchStream,
    schema: SchemaRef,
    is_retract_index: Option<usize>,
    timestamp_index: usize,
    struct_projection: Vec<usize>,
}

impl ToDebeziumStream {
    fn as_debezium_batch(&mut self, batch: &RecordBatch) -> DFResult<RecordBatch> {
        let value_struct = batch.project(&self.struct_projection)?;
        let timestamp_column = batch.column(self.timestamp_index).clone();
        match self.is_retract_index {
            Some(retract_index) => self.create_debezium(
                value_struct,
                timestamp_column,
                batch
                    .column(retract_index)
                    .as_any()
                    .downcast_ref::<BooleanArray>()
                    .unwrap(),
            ),
            None => {
                let is_retract_array =
                    BooleanArray::new(BooleanBuffer::new_unset(batch.num_rows()), None);
                self.create_debezium(value_struct, timestamp_column, &is_retract_array)
            }
        }
    }
    fn create_debezium(
        &mut self,
        value_struct: RecordBatch,
        timestamp_column: Arc<dyn Array>,
        is_retract: &BooleanArray,
    ) -> DFResult<RecordBatch> {
        let after_nullability = Some(NullBuffer::new(not(is_retract)?.values().clone()));
        let after_array = StructArray::try_new(
            value_struct.schema().fields().clone(),
            value_struct.columns().to_vec(),
            after_nullability,
        )?;
        let before_nullability = Some(NullBuffer::new(is_retract.values().clone()));
        let before_array = StructArray::try_new(
            value_struct.schema().fields().clone(),
            value_struct.columns().to_vec(),
            before_nullability,
        )?;
        let append_datum = StringArray::new_scalar("c");
        let retract_datum = StringArray::new_scalar("d");
        let op_array = zip::zip(is_retract, &retract_datum, &append_datum)?;

        let columns = vec![
            Arc::new(before_array),
            Arc::new(after_array),
            op_array,
            timestamp_column,
        ];

        Ok(RecordBatch::try_new(self.schema.clone(), columns)?)
    }
}

impl Stream for ToDebeziumStream {
    type Item = DFResult<RecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
        let result =
            ready!(self.input.poll_next_unpin(cx)).map(|result| self.as_debezium_batch(&result?));
        Poll::Ready(result)
    }
}

impl RecordBatchStream for ToDebeziumStream {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}
