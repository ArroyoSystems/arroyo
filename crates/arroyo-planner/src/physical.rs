use arrow::{
    array::{AsArray, BooleanBuilder, TimestampNanosecondBuilder, UInt32Builder},
    buffer::NullBuffer,
    compute::{concat, take},
};
use arrow_array::{Array, PrimitiveArray, RecordBatch, StringArray, StructArray, array};
use arrow_schema::{DataType, Schema, SchemaRef, TimeUnit};
use datafusion::common::{
    DataFusionError, Result, ScalarValue, Statistics, UnnestOptions, not_impl_err, plan_err,
};
use datafusion::execution::{RecordBatchStream, SendableRecordBatchStream};
use datafusion::{
    execution::TaskContext,
    physical_plan::{
        DisplayAs, ExecutionPlan, Partitioning, memory::MemoryStream,
        stream::RecordBatchStreamAdapter,
    },
};
use std::collections::HashMap;
use std::{
    any::Any,
    mem,
    pin::Pin,
    sync::{Arc, RwLock},
    task::{Context, Poll},
};

use crate::functions::MultiHashFunction;
use crate::rewriters::UNNESTED_COL;
use crate::schemas::window_arrow_struct;
use crate::{make_udf_function, register_functions};
use arrow_array::types::{TimestampNanosecondType, UInt64Type};
use arroyo_operator::operator::Registry;
use arroyo_rpc::grpc::api::{
    ArroyoExecNode, DebeziumEncodeNode, MemExecNode, UnnestExecNode, arroyo_exec_node,
};
use arroyo_rpc::{
    TIMESTAMP_FIELD, UPDATING_META_FIELD,
    grpc::api::{DebeziumDecodeNode, arroyo_exec_node::Node},
    updating_meta_field, updating_meta_fields,
};
use datafusion::catalog::memory::MemorySourceConfig;
use datafusion::datasource::memory::DataSourceExec;
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, TypeSignature, Volatility,
};
use datafusion::physical_expr::EquivalenceProperties;
use datafusion::physical_plan::PlanProperties;
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::unnest::{ListUnnest, UnnestExec};
use datafusion_proto::physical_plan::PhysicalExtensionCodec;
use futures::{
    ready,
    stream::{Stream, StreamExt},
};
use prost::Message;
use std::fmt::Debug;
use tokio::sync::mpsc::UnboundedReceiver;
use tokio_stream::wrappers::UnboundedReceiverStream;

#[derive(Debug)]
pub struct WindowFunctionUdf {
    signature: Signature,
}

impl Default for WindowFunctionUdf {
    fn default() -> Self {
        Self {
            signature: Signature::new(
                TypeSignature::Exact(vec![
                    DataType::Timestamp(TimeUnit::Nanosecond, None),
                    DataType::Timestamp(TimeUnit::Nanosecond, None),
                ]),
                Volatility::Immutable,
            ),
        }
    }
}

impl ScalarUDFImpl for WindowFunctionUdf {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "window"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _: &[DataType]) -> Result<DataType> {
        Ok(window_arrow_struct())
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let columns = args.args;
        if columns.len() != 2 {
            return plan_err!("window function expected 2 argument, got {}", columns.len());
        }
        // check both columns are of the correct type
        if columns[0].data_type() != DataType::Timestamp(TimeUnit::Nanosecond, None) {
            return plan_err!(
                "window function expected first argument to be a timestamp, got {:?}",
                columns[0].data_type()
            );
        }
        if columns[1].data_type() != DataType::Timestamp(TimeUnit::Nanosecond, None) {
            return plan_err!(
                "window function expected second argument to be a timestamp, got {:?}",
                columns[1].data_type()
            );
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
}

make_udf_function!(WindowFunctionUdf, WINDOW_FUNCTION, window);

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
    registry.add_udf(window());
    register_functions(&mut registry);
    registry
}

fn make_properties(schema: SchemaRef) -> PlanProperties {
    PlanProperties::new(
        EquivalenceProperties::new(schema),
        Partitioning::UnknownPartitioning(1),
        EmissionType::Incremental,
        Boundedness::Unbounded {
            requires_infinite_memory: false,
        },
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
            .map_err(|err| DataFusionError::Internal(format!("couldn't deserialize: {err}")))?;

        match exec
            .node
            .ok_or_else(|| DataFusionError::Internal("exec node is empty".to_string()))?
        {
            Node::MemExec(mem_exec) => {
                let schema: Schema = serde_json::from_str(&mem_exec.schema).map_err(|e| {
                    DataFusionError::Internal(format!("invalid schema in exec codec: {e:?}"))
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
                    DataFusionError::Internal(format!("invalid schema in exec codec: {e:?}"))
                })?;

                let column = schema.index_of(UNNESTED_COL).map_err(|_| {
                    DataFusionError::Internal(format!(
                        "unnest node schema does not contain {UNNESTED_COL} col"
                    ))
                })?;

                Ok(Arc::new(UnnestExec::new(
                    inputs
                        .first()
                        .ok_or_else(|| {
                            DataFusionError::Internal("no input for unnest node".to_string())
                        })?
                        .clone(),
                    vec![ListUnnest {
                        index_in_input_schema: column,
                        depth: 1,
                    }],
                    vec![],
                    Arc::new(schema),
                    UnnestOptions::default(),
                )))
            }
            Node::DebeziumDecode(debezium) => {
                let schema = Arc::new(serde_json::from_str::<Schema>(&debezium.schema).map_err(
                    |e| DataFusionError::Internal(format!("invalid schema in exec codec: {e:?}")),
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
                    primary_keys: debezium
                        .primary_keys
                        .into_iter()
                        .map(|c| c as usize)
                        .collect(),
                }))
            }
            Node::DebeziumEncode(debezium) => {
                let schema = Arc::new(serde_json::from_str::<Schema>(&debezium.schema).map_err(
                    |e| DataFusionError::Internal(format!("invalid schema in exec codec: {e:?}")),
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
                    primary_keys: (*decode.primary_keys).iter().map(|c| *c as u64).collect(),
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
                DataFusionError::Internal(format!("couldn't serialize exec node {err}"))
            })?;
            Ok(())
        } else {
            Err(DataFusionError::Internal(format!(
                "cannot serialize {node:?}"
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

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
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

    fn statistics(&self) -> Result<datafusion::common::Statistics> {
        Ok(Statistics::new_unknown(&self.schema))
    }

    fn reset(&self) -> Result<()> {
        Ok(())
    }

    fn properties(&self) -> &PlanProperties {
        &self.properties
    }

    fn name(&self) -> &str {
        "rw_lock_reader"
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
    fn name(&self) -> &str {
        "unbounded_reader"
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn properties(&self) -> &PlanProperties {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
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

    fn statistics(&self) -> Result<datafusion::common::Statistics> {
        Ok(datafusion::common::Statistics::new_unknown(&self.schema))
    }

    fn reset(&self) -> Result<()> {
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
    fn name(&self) -> &str {
        "vec_reader"
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn properties(&self) -> &PlanProperties {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
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
        let memory = MemorySourceConfig::try_new(
            &[mem::take(self.receiver.write().unwrap().as_mut())],
            self.schema.clone(),
            None,
        )?;

        DataSourceExec::new(Arc::new(memory)).execute(partition, context)
    }

    fn statistics(&self) -> datafusion::common::Result<datafusion::common::Statistics> {
        Ok(datafusion::common::Statistics::new_unknown(&self.schema))
    }

    fn reset(&self) -> Result<()> {
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
    fn name(&self) -> &str {
        "mem_exec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn properties(&self) -> &PlanProperties {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        _children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        not_impl_err!("with_new_children is not implemented for mem_exec; should not be called")
    }

    fn execute(
        &self,
        _partition: usize,
        _context: Arc<datafusion::execution::TaskContext>,
    ) -> Result<datafusion::physical_plan::SendableRecordBatchStream> {
        plan_err!(
            "EmptyPartitionStream cannot be executed, this is only used for physical planning before serialization"
        )
    }

    fn statistics(&self) -> Result<datafusion::common::Statistics> {
        Ok(datafusion::common::Statistics::new_unknown(&self.schema))
    }

    fn reset(&self) -> Result<()> {
        Ok(())
    }
}

#[derive(Debug)]
pub struct DebeziumUnrollingExec {
    input: Arc<dyn ExecutionPlan>,
    schema: SchemaRef,
    properties: PlanProperties,
    primary_keys: Vec<usize>,
}

impl DebeziumUnrollingExec {
    pub fn try_new(input: Arc<dyn ExecutionPlan>, primary_keys: Vec<usize>) -> Result<Self> {
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
        fields.push(updating_meta_field());
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
            primary_keys,
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
    fn name(&self) -> &str {
        "debezium_unrolling_exec"
    }

    fn as_any(&self) -> &dyn Any {
        self as &dyn Any
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn properties(&self) -> &PlanProperties {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if children.len() != 1 {
            return Err(DataFusionError::Internal(
                "DebeziumUnrollingExec wrong number of children".to_string(),
            ));
        }
        Ok(Arc::new(DebeziumUnrollingExec {
            input: children[0].clone(),
            schema: self.schema.clone(),
            properties: self.properties.clone(),
            primary_keys: self.primary_keys.clone(),
        }))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        Ok(Box::pin(DebeziumUnrollingStream::try_new(
            self.input.execute(partition, context)?,
            self.schema.clone(),
            self.primary_keys.clone(),
        )?))
    }

    fn reset(&self) -> Result<()> {
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
    primary_keys: Vec<usize>,
}

impl DebeziumUnrollingStream {
    fn try_new(
        input: SendableRecordBatchStream,
        schema: SchemaRef,
        primary_keys: Vec<usize>,
    ) -> Result<Self> {
        if primary_keys.is_empty() {
            return plan_err!("there must be at least one primary key for a Debezium source");
        }
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
            primary_keys,
        })
    }
    fn unroll_batch(&self, batch: &RecordBatch) -> Result<RecordBatch> {
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
        let mut take_indices = UInt32Builder::with_capacity(num_rows);
        let mut is_retract_builder = BooleanBuilder::with_capacity(num_rows);

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
                        "unexpected op value: {op}"
                    )));
                }
            }
        }
        let take_indices = take_indices.finish();
        let unrolled_array = take(&combined_array, &take_indices, None)?;

        let mut columns = unrolled_array.as_struct().columns().to_vec();

        let hash = MultiHashFunction::default().invoke(
            &self
                .primary_keys
                .iter()
                .map(|i| ColumnarValue::Array(columns[*i].clone()))
                .collect::<Vec<_>>(),
        )?;

        let ids = hash.into_array(num_rows)?;

        let meta = StructArray::try_new(
            updating_meta_fields(),
            vec![Arc::new(is_retract_builder.finish()), ids],
            None,
        )?;
        columns.push(Arc::new(meta));
        columns.push(Arc::new(timestamp_builder.finish()));
        Ok(RecordBatch::try_new(self.schema.clone(), columns)?)
    }
}

impl Stream for DebeziumUnrollingStream {
    type Item = Result<RecordBatch>;

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
    pub fn try_new(input: Arc<dyn ExecutionPlan>) -> Result<Self> {
        let input_schema = input.schema();
        let timestamp_index = input_schema.index_of(TIMESTAMP_FIELD)?;
        let struct_fields: Vec<_> = input_schema
            .fields()
            .into_iter()
            .enumerate()
            .filter_map(|(index, field)| {
                if field.name() == UPDATING_META_FIELD || index == timestamp_index {
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
    fn name(&self) -> &str {
        "to_debezium_exec"
    }

    fn as_any(&self) -> &dyn Any {
        self as &dyn Any
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn properties(&self) -> &PlanProperties {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if children.len() != 1 {
            return Err(DataFusionError::Internal(
                "ToDebeziumExec wrong number of children".to_string(),
            ));
        }
        Ok(Arc::new(ToDebeziumExec::try_new(children[0].clone())?))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let updating_meta_index = self.input.schema().index_of(UPDATING_META_FIELD).ok();
        let timestamp_index = self.input.schema().index_of(TIMESTAMP_FIELD)?;
        let struct_projection = (0..self.input.schema().fields().len())
            .filter(|index| {
                updating_meta_index
                    .map(|is_retract_index| *index != is_retract_index)
                    .unwrap_or(true)
                    && *index != timestamp_index
            })
            .collect();

        Ok(Box::pin(ToDebeziumStream {
            input: self.input.execute(partition, context)?,
            schema: self.schema.clone(),
            updating_meta_index,
            timestamp_index,
            struct_projection,
        }))
    }

    fn reset(&self) -> Result<()> {
        self.input.reset()
    }
}

struct ToDebeziumStream {
    input: SendableRecordBatchStream,
    schema: SchemaRef,
    updating_meta_index: Option<usize>,
    timestamp_index: usize,
    struct_projection: Vec<usize>,
}

impl ToDebeziumStream {
    fn as_debezium_batch(&mut self, batch: &RecordBatch) -> Result<RecordBatch> {
        let value_struct = batch.project(&self.struct_projection)?;
        let timestamps = batch
            .column(self.timestamp_index)
            .as_primitive::<TimestampNanosecondType>();

        let columns: Vec<Arc<dyn Array>> = if let Some(metadata_index) = self.updating_meta_index {
            let metadata = batch
                .column(metadata_index)
                .as_any()
                .downcast_ref::<StructArray>()
                .ok_or_else(|| {
                    DataFusionError::Internal("Invalid type for updating_meta column".to_string())
                })?;

            let is_retract = metadata.column(0).as_boolean();

            let id = metadata.column(1).as_fixed_size_binary();

            // (first idx, last idx, first is create, last is create, latest time)
            let mut id_map: HashMap<&[u8], (usize, usize, bool, bool, i64)> = HashMap::new();
            let mut order = vec![];
            for i in 0..batch.num_rows() {
                let row_id = id.value(i);
                let is_create = !is_retract.value(i);
                let timestamp = timestamps.value(i);

                id_map
                    .entry(row_id)
                    .and_modify(|e| {
                        e.1 = i;
                        e.3 = is_create;
                        e.4 = e.4.max(timestamp);
                    })
                    .or_insert_with(|| {
                        order.push(row_id);
                        (i, i, is_create, is_create, timestamp)
                    });
            }

            // scenarios:
            // 1. Create -- value index is last
            //    -- create, (retract, create)*
            // 2. Delete -- value index is first
            //    -- (retract,create)*,retract
            // 3. Update -- value index is last
            //    -- (retract,create)*

            let mut before = Vec::with_capacity(id_map.len());
            let mut after = Vec::with_capacity(id_map.len());
            let mut op = Vec::with_capacity(id_map.len());
            let mut ts = TimestampNanosecondBuilder::with_capacity(id_map.len());

            for row_id in order {
                let (first_idx, last_idx, first_is_create, last_is_create, timestamp) =
                    id_map.get(row_id).unwrap();

                if *first_is_create && *last_is_create {
                    // create case
                    // sequence: create, (retract, create)*
                    before.push(None);
                    after.push(Some(*last_idx));
                    op.push("c");
                } else if !(*first_is_create) && !(*last_is_create) {
                    // delete case
                    // sequence: (retract,create)*, retract
                    before.push(Some(*first_idx));
                    after.push(None);
                    op.push("d");
                } else if !(*first_is_create) && *last_is_create {
                    // update case
                    // sequence: (retract,create)*
                    before.push(Some(*first_idx));
                    after.push(Some(*last_idx));
                    op.push("u");
                } else {
                    // no-op case
                    // sequence: (create,retract)*
                    continue;
                }

                ts.append_value(*timestamp);
            }

            let before_array = Self::create_output_array(&value_struct, &before)?;
            let after_array = Self::create_output_array(&value_struct, &after)?;
            let op_array = StringArray::from(op);

            vec![
                Arc::new(before_array),
                Arc::new(after_array),
                Arc::new(op_array),
                Arc::new(ts.finish()),
            ]
        } else {
            // Append-only
            let after_array = StructArray::try_new(
                value_struct.schema().fields().clone(),
                value_struct.columns().to_vec(),
                None,
            )?;

            let before_array = StructArray::new_null(
                value_struct.schema().fields().clone(),
                value_struct.num_rows(),
            );

            vec![
                Arc::new(before_array),
                Arc::new(after_array),
                Arc::new(StringArray::from(vec!["c"; value_struct.num_rows()])),
                batch.column(self.timestamp_index).clone(),
            ]
        };

        Ok(RecordBatch::try_new(self.schema.clone(), columns)?)
    }

    fn create_output_array(
        value_struct: &RecordBatch,
        indices: &[Option<usize>],
    ) -> Result<StructArray> {
        let mut arrays: Vec<Arc<dyn Array>> = Vec::with_capacity(value_struct.num_columns());
        for col in value_struct.columns() {
            let new_array = arrow::compute::take(
                col.as_ref(),
                &indices
                    .iter()
                    .map(|&idx| idx.map(|i| i as u64))
                    .collect::<PrimitiveArray<UInt64Type>>(),
                None,
            )?;
            arrays.push(new_array);
        }

        Ok(StructArray::try_new(
            value_struct.schema().fields().clone(),
            arrays,
            Some(NullBuffer::from(
                indices.iter().map(|&idx| idx.is_some()).collect::<Vec<_>>(),
            )),
        )?)
    }
}

impl Stream for ToDebeziumStream {
    type Item = Result<RecordBatch>;

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
