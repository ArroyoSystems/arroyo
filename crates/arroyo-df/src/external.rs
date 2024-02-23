use arrow_schema::FieldRef;
use arroyo_rpc::grpc::api::ConnectorOp;
use std::time::Duration;

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

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SinkUpdateType {
    Allow,
    Disallow,
    Force,
}
