use std::collections::HashMap;

use anyhow::Result;
use arroyo_datastream::Operator;
use arroyo_datastream::SerializationMode;
use arroyo_rpc::grpc::api::Connection;

use crate::types::StructDef;

#[derive(Clone, Debug)]
pub struct SqlSource {
    pub id: Option<i64>,
    pub struct_def: StructDef,
    pub operator: Operator,
    pub serialization_mode: SerializationMode,
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
