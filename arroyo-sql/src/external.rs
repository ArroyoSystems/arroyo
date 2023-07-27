use std::time::Duration;

use arroyo_datastream::Operator;

use crate::types::StructDef;

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ProcessingMode {
    Append,
    Update,
}

#[derive(Clone, Debug)]
pub struct SqlSource {
    pub id: Option<i64>,
    pub struct_def: StructDef,
    pub operator: Operator,
    pub processing_mode: ProcessingMode,
    pub idle_time: Option<Duration>,
}

#[derive(Clone, Debug)]
pub struct SqlSink {
    pub id: Option<i64>,
    pub struct_def: StructDef,
    pub operator: Operator,
    pub updating_type: SinkUpdateType,
}

#[derive(Clone, Debug)]
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
