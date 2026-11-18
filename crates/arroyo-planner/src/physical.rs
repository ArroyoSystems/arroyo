use arrow::{
    array::{AsArray, BooleanBuilder, TimestampNanosecondBuilder, UInt32Builder},
    buffer::NullBuffer,
    compute::{concat, take},
};
use arrow_array::{Array, PrimitiveArray, RecordBatch, StringArray, StructArray, array};
use arrow_schema::{DataType, Schema, SchemaRef, TimeUnit};
use datafusion::common::{DataFusionError, Result, ScalarValue, UnnestOptions, plan_err};
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
    timestamp_field_index, updating_meta_field, updating_meta_fields,
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

#[derive(Debug, PartialEq, Eq, Hash)]
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

fn make_properties(schema: SchemaRef) -> Arc<PlanProperties> {
    Arc::new(PlanProperties::new(
        EquivalenceProperties::new(schema),
        Partitioning::UnknownPartitioning(1),
        EmissionType::Incremental,
        Boundedness::Unbounded {
            requires_infinite_memory: false,
        },
    ))
}

impl PhysicalExtensionCodec for ArroyoPhysicalExtensionCodec {
    fn try_decode(
        &self,
        buf: &[u8],
        inputs: &[Arc<dyn datafusion::physical_plan::ExecutionPlan>],
        _ctx: &TaskContext,
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
                )?))
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

        let mem_table: Option<&ArroyoMemExec> = node.downcast_ref();
        if let Some(table) = mem_table {
            proto = Some(ArroyoExecNode {
                node: Some(arroyo_exec_node::Node::MemExec(MemExecNode {
                    table_name: table.table_name.clone(),
                    schema: serde_json::to_string(&table.schema).unwrap(),
                })),
            });
        }

        let unnest: Option<&UnnestExec> = node.downcast_ref();
        if let Some(unnest) = unnest {
            proto = Some(ArroyoExecNode {
                node: Some(arroyo_exec_node::Node::UnnestExec(UnnestExecNode {
                    schema: serde_json::to_string(&unnest.schema()).unwrap(),
                })),
            });
        }
        let debezium_decode: Option<&DebeziumUnrollingExec> = node.downcast_ref();
        if let Some(decode) = debezium_decode {
            proto = Some(ArroyoExecNode {
                node: Some(arroyo_exec_node::Node::DebeziumDecode(DebeziumDecodeNode {
                    schema: serde_json::to_string(&decode.schema).unwrap(),
                    primary_keys: (*decode.primary_keys).iter().map(|c| *c as u64).collect(),
                })),
            });
        }

        let debezium_encode: Option<&ToDebeziumExec> = node.downcast_ref();
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
    properties: Arc<PlanProperties>,
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
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if !children.is_empty() {
            return plan_err!("batch reader cannot have children");
        }
        // The input buffer is supplied by Arroyo before each execution, not cached plan state.
        Ok(self)
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

    fn properties(&self) -> &Arc<PlanProperties> {
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
    properties: Arc<PlanProperties>,
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

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if !children.is_empty() {
            return plan_err!("batch reader cannot have children");
        }
        // The input buffer is supplied by Arroyo before each execution, not cached plan state.
        Ok(self)
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
}

#[derive(Debug)]
struct RecordBatchVecReader {
    schema: SchemaRef,
    receiver: Arc<RwLock<Vec<RecordBatch>>>,
    properties: Arc<PlanProperties>,
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

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if !children.is_empty() {
            return plan_err!("batch reader cannot have children");
        }
        // The input buffer is supplied by Arroyo before each execution, not cached plan state.
        Ok(self)
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
}

#[derive(Debug, Clone)]
pub struct ArroyoMemExec {
    pub table_name: String,
    pub schema: SchemaRef,
    properties: Arc<PlanProperties>,
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

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if !children.is_empty() {
            return plan_err!("mem_exec cannot have children");
        }
        Ok(self)
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
}

#[derive(Debug)]
pub struct DebeziumUnrollingExec {
    input: Arc<dyn ExecutionPlan>,
    schema: SchemaRef,
    properties: Arc<PlanProperties>,
    primary_keys: Vec<usize>,
}

/// Index of the timestamp field, accepting the legacy name used by programs compiled before
/// the rename
fn timestamp_index(schema: &Schema) -> Result<usize> {
    timestamp_field_index(schema).ok_or_else(|| {
        DataFusionError::Plan(format!("no {TIMESTAMP_FIELD} field in schema {schema:?}"))
    })
}

impl DebeziumUnrollingExec {
    pub fn try_new(input: Arc<dyn ExecutionPlan>, primary_keys: Vec<usize>) -> Result<Self> {
        let input_schema = input.schema();
        // confirm that the input schema has before, after and op columns, and before and after match
        let before_index = input_schema.index_of("before")?;
        let after_index = input_schema.index_of("after")?;
        let op_index = input_schema.index_of("op")?;
        timestamp_index(&input_schema)?;
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

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn properties(&self) -> &Arc<PlanProperties> {
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
        let input_schema = input.schema();
        let before_index = input_schema.index_of("before")?;
        let after_index = input_schema.index_of("after")?;
        let op_index = input_schema.index_of("op")?;
        let timestamp_index = timestamp_index(&input_schema)?;

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

        // without a primary key, a row is identified by all of its fields
        let id_columns: Vec<_> = if self.primary_keys.is_empty() {
            columns.clone()
        } else {
            self.primary_keys
                .iter()
                .map(|i| columns[*i].clone())
                .collect()
        };
        let hash = MultiHashFunction::default().invoke(
            &id_columns
                .into_iter()
                .map(ColumnarValue::Array)
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
    properties: Arc<PlanProperties>,
}

impl ToDebeziumExec {
    pub fn try_new(input: Arc<dyn ExecutionPlan>) -> Result<Self> {
        let input_schema = input.schema();
        let timestamp_index = timestamp_index(&input_schema)?;
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

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn properties(&self) -> &Arc<PlanProperties> {
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
        let timestamp_index = timestamp_index(&self.input.schema())?;
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
}

/// The changes to one row id within a batch. Equal rows without a primary key share an id, so it keeps their net count:
/// creates less retracts.
#[derive(Clone, Copy)]
struct IdChanges {
    first_idx: usize,
    last_idx: usize,
    first_is_create: bool,
    last_is_create: bool,
    net: i64,
    /// the latest
    timestamp: i64,
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

            let mut id_map: HashMap<&[u8], IdChanges> = HashMap::new();
            let mut order = vec![];
            for i in 0..batch.num_rows() {
                let row_id = id.value(i);
                let is_create = !is_retract.value(i);
                let change = if is_create { 1 } else { -1 };
                let timestamp = timestamps.value(i);

                id_map
                    .entry(row_id)
                    .and_modify(|e| {
                        e.last_idx = i;
                        e.last_is_create = is_create;
                        e.net += change;
                        e.timestamp = e.timestamp.max(timestamp);
                    })
                    .or_insert_with(|| {
                        order.push(row_id);
                        IdChanges {
                            first_idx: i,
                            last_idx: i,
                            first_is_create: is_create,
                            last_is_create: is_create,
                            net: change,
                            timestamp,
                        }
                    });
            }

            // scenarios:
            // 1. Create -- value index is last, once per net create
            //    -- create, (retract, create)*, or more creates than retracts
            // 2. Delete -- value index is first, once per net retract
            //    -- (retract,create)*,retract, or more retracts than creates
            // 3. Update -- value index is last
            //    -- (retract,create)*
            // 4. No-op -- otherwise

            let mut before = Vec::with_capacity(id_map.len());
            let mut after = Vec::with_capacity(id_map.len());
            let mut op = Vec::with_capacity(id_map.len());
            let mut ts = TimestampNanosecondBuilder::with_capacity(id_map.len());

            for row_id in order {
                let IdChanges {
                    first_idx,
                    last_idx,
                    first_is_create,
                    last_is_create,
                    net,
                    timestamp,
                } = id_map[row_id];

                if net > 0 {
                    for _ in 0..net {
                        before.push(None);
                        after.push(Some(last_idx));
                        op.push("c");
                        ts.append_value(timestamp);
                    }
                } else if net < 0 {
                    for _ in 0..-net {
                        before.push(Some(first_idx));
                        after.push(None);
                        op.push("d");
                        ts.append_value(timestamp);
                    }
                } else if !first_is_create && last_is_create {
                    before.push(Some(first_idx));
                    after.push(Some(last_idx));
                    op.push("u");
                    ts.append_value(timestamp);
                }
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

#[cfg(test)]
mod test {
    use super::*;
    use arrow_array::{Int64Array, TimestampNanosecondArray};
    use arrow_schema::{Field, Fields};
    use datafusion::physical_plan::common::collect;

    fn fields() -> Fields {
        Fields::from(vec![
            Field::new("product", DataType::Utf8, true),
            Field::new("quantity", DataType::Int64, true),
        ])
    }

    /// A Debezium batch of rows without a primary key: (op, product, quantity).
    fn debezium(rows: &[(&str, &str, i64)]) -> RecordBatch {
        let values = StructArray::new(
            fields(),
            vec![
                Arc::new(StringArray::from_iter_values(rows.iter().map(|r| r.1))),
                Arc::new(Int64Array::from_iter_values(rows.iter().map(|r| r.2))),
            ],
            None,
        );
        let side = |present: &dyn Fn(&str) -> bool| {
            StructArray::new(
                fields(),
                values.columns().to_vec(),
                Some(NullBuffer::from(
                    rows.iter().map(|r| present(r.0)).collect::<Vec<_>>(),
                )),
            )
        };
        let struct_type = DataType::Struct(fields());
        let schema = Schema::new(vec![
            Field::new("before", struct_type.clone(), true),
            Field::new("after", struct_type, true),
            Field::new("op", DataType::Utf8, false),
            Field::new(
                TIMESTAMP_FIELD,
                DataType::Timestamp(TimeUnit::Nanosecond, None),
                false,
            ),
        ]);
        RecordBatch::try_new(
            Arc::new(schema),
            vec![
                Arc::new(side(&|op| op == "d")),
                Arc::new(side(&|op| op == "c")),
                Arc::new(StringArray::from_iter_values(rows.iter().map(|r| r.0))),
                Arc::new(TimestampNanosecondArray::from(vec![0; rows.len()])),
            ],
        )
        .unwrap()
    }

    /// Unrolls each batch without a primary key, then turns it back into Debezium, as a pass-through to a Debezium
    /// sink does; the ops of each output batch.
    async fn pass_through(batches: Vec<RecordBatch>) -> Vec<Vec<String>> {
        let schema = batches[0].schema();
        let source = MemorySourceConfig::try_new_exec(&[batches], schema, None).unwrap();
        let unrolled = Arc::new(DebeziumUnrollingExec::try_new(source, vec![]).unwrap());
        let plan = ToDebeziumExec::try_new(unrolled).unwrap();
        let output = collect(plan.execute(0, Arc::new(TaskContext::default())).unwrap())
            .await
            .unwrap();
        output
            .iter()
            .map(|b| {
                b.column_by_name("op")
                    .unwrap()
                    .as_string::<i32>()
                    .iter()
                    .map(|op| op.unwrap().to_string())
                    .collect()
            })
            .collect()
    }

    #[tokio::test]
    async fn test_debezium_keeps_equal_rows_without_primary_key() {
        let ops = pass_through(vec![
            // a snapshot of two equal rows and another
            debezium(&[("c", "Mouse", 1), ("c", "Mouse", 1), ("c", "Keyboard", 2)]),
            // one of the equal rows is deleted
            debezium(&[("d", "Mouse", 1)]),
            // two more are created and one deleted
            debezium(&[("c", "Mouse", 1), ("c", "Mouse", 1), ("d", "Mouse", 1)]),
        ])
        .await;
        assert_eq!(ops, [vec!["c", "c", "c"], vec!["d"], vec!["c"]]);
    }
}
