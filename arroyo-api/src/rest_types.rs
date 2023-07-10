use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

#[derive(Serialize, Deserialize, Clone, Debug, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct PipelinePost {
    pub name: String,
    pub query: String,
    pub udfs: Vec<Udf>,
    pub preview: Option<bool>,
    pub parallelism: u64,
}

#[derive(Serialize, Deserialize, Clone, Debug, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct Pipeline {
    pub id: String,
    pub name: String,
    pub query: String,
    pub udfs: Vec<Udf>,
}

#[derive(Serialize, Deserialize, Clone, Debug, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct Job {
    pub id: String,
    pub running_desired: bool,
    pub state: String,
    pub run_id: u64,
    pub start_time: Option<u64>,
    pub finish_time: Option<u64>,
    pub tasks: Option<u64>,
    pub failure_message: Option<String>,
}

#[derive(Serialize, Deserialize, Clone, Debug, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct Udf {
    pub definition: String,
}

// Collections need to be created with this macro rather than a generic type
// because utoipa::ToSchema (and the OpenAPI spec) don't support generics natively
macro_rules! collection_type {
    ($struct_name:ident, $item_type:ty) => {
        #[derive(Serialize, Deserialize, Clone, Debug, ToSchema)]
        #[serde(rename_all = "camelCase")]
        pub struct $struct_name {
            pub data: Vec<$item_type>,
            pub has_more: bool,
        }
    };
}

collection_type!(JobCollection, Job);
collection_type!(PipelineCollection, Pipeline);
