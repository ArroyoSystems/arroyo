use arrow_schema::FieldRef;
use arroyo_datastream::logical::LogicalNode;
use std::time::Duration;

use arroyo_datastream::ConnectorOp;

use crate::types::StructDef;

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ProcessingMode {
    Append,
    Update,
}

#[derive(Clone, Debug)]
pub struct SqlSource {
    pub id: Option<i64>,
    pub struct_def: Vec<FieldRef>,
    pub config: ConnectorOp,
    pub processing_mode: ProcessingMode,
    pub idle_time: Option<Duration>,
}

#[derive(Clone, Debug)]
pub struct SqlSink {
    pub id: Option<i64>,
    pub struct_def: StructDef,
    pub operator: LogicalNode,
    pub updating_type: SinkUpdateType,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SinkUpdateType {
    Allow,
    Disallow,
    Force,
}

#[derive(Clone, Debug)]
pub struct SqlTable {
    pub struct_def: StructDef,
    pub name: String,
}
