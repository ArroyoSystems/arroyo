use crate::api_types::checkpoints::Checkpoint;
use crate::api_types::checkpoints::OperatorCheckpointGroup;
use crate::api_types::connections::ConnectionProfile;
use crate::api_types::connections::ConnectionTable;
use crate::api_types::connections::Connector;
use crate::api_types::metrics::OperatorMetricGroup;
use crate::api_types::pipelines::{Job, JobLogMessage, Pipeline};
use serde::{Deserialize, Serialize};
use utoipa::{IntoParams, ToSchema};

pub mod checkpoints;
pub mod connections;
pub mod metrics;
pub mod pipelines;
pub mod udfs;

#[derive(Serialize, Deserialize, Clone, Debug, ToSchema)]
#[serde(rename_all = "camelCase")]
#[aliases(
    PipelineCollection = PaginatedCollection<Pipeline>,
    JobLogMessageCollection = PaginatedCollection<JobLogMessage>,
    ConnectionTableCollection = PaginatedCollection<ConnectionTable>,
)]
pub struct PaginatedCollection<T> {
    pub data: Vec<T>,
    pub has_more: bool,
}

#[derive(Serialize, Deserialize, Clone, Debug, ToSchema)]
#[serde(rename_all = "camelCase")]
#[aliases(
    JobCollection = NonPaginatedCollection<Job>,
    OperatorCheckpointGroupCollection = NonPaginatedCollection<OperatorCheckpointGroup>,
    CheckpointCollection = NonPaginatedCollection<Checkpoint>,
    OperatorMetricGroupCollection = NonPaginatedCollection<OperatorMetricGroup>,
    ConnectorCollection = NonPaginatedCollection<Connector>,
    ConnectionProfileCollection = NonPaginatedCollection<ConnectionProfile>,
)]
pub struct NonPaginatedCollection<T> {
    pub data: Vec<T>,
}

#[derive(Serialize, Deserialize, Clone, Debug, IntoParams, ToSchema)]
#[into_params(parameter_in = Query)]
#[serde(rename_all = "snake_case")]
pub struct PaginationQueryParams {
    pub starting_after: Option<String>,
    pub limit: Option<u32>,
}
