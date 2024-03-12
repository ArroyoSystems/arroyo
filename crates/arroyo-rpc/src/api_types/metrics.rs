use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

#[derive(Serialize, Deserialize, Clone, Debug, ToSchema, Hash, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum MetricNames {
    BytesRecv,
    BytesSent,
    MessagesRecv,
    MessagesSent,
    Backpressure,
}

#[derive(Serialize, Deserialize, Clone, Debug, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct Metric {
    pub time: u64,
    pub value: f64,
}

#[derive(Serialize, Deserialize, Clone, Debug, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct SubtaskMetrics {
    pub index: u32,
    pub metrics: Vec<Metric>,
}

#[derive(Serialize, Deserialize, Clone, Debug, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct MetricGroup {
    pub name: MetricNames,
    pub subtasks: Vec<SubtaskMetrics>,
}

#[derive(Serialize, Deserialize, Clone, Debug, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct OperatorMetricGroup {
    pub operator_id: String,
    pub metric_groups: Vec<MetricGroup>,
}
