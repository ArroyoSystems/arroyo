use crate::types::public::StopMode;
use arroyo_rpc::grpc::api;
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
pub struct PipelinePatch {
    pub parallelism: Option<u64>,
    pub checkpoint_interval_micros: Option<u64>,
    pub stop: Option<StopType>,
}

#[derive(Serialize, Deserialize, Clone, Debug, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct Pipeline {
    pub id: String,
    pub name: String,
    pub query: String,
    pub udfs: Vec<Udf>,
    pub checkpoint_interval_micros: u64,
    pub stop: StopType,
    pub created_at: u64,
}

#[derive(Serialize, Deserialize, Clone, Debug, ToSchema)]
#[serde(rename_all = "camelCase")]
pub enum StopType {
    None,
    Checkpoint,
    Graceful,
    Immediate,
    Force,
}

impl From<StopMode> for StopType {
    fn from(value: StopMode) -> Self {
        match value {
            StopMode::none => StopType::None,
            StopMode::checkpoint => StopType::Checkpoint,
            StopMode::graceful => StopType::Graceful,
            StopMode::immediate => StopType::Immediate,
            StopMode::force => StopType::Force,
        }
    }
}

impl Into<arroyo_rpc::grpc::api::StopType> for StopType {
    fn into(self) -> arroyo_rpc::grpc::api::StopType {
        match self {
            StopType::None => arroyo_rpc::grpc::api::StopType::None,
            StopType::Checkpoint => arroyo_rpc::grpc::api::StopType::Checkpoint,
            StopType::Graceful => arroyo_rpc::grpc::api::StopType::Graceful,
            StopType::Immediate => arroyo_rpc::grpc::api::StopType::Immediate,
            StopType::Force => arroyo_rpc::grpc::api::StopType::Force,
        }
    }
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
    pub created_at: u64,
}

#[derive(Serialize, Deserialize, Clone, Debug, ToSchema)]
#[serde(rename_all = "camelCase")]
pub enum UdfLanguage {
    Rust,
}

impl From<api::UdfLanguage> for UdfLanguage {
    fn from(value: api::UdfLanguage) -> Self {
        match value {
            api::UdfLanguage::Rust => UdfLanguage::Rust,
        }
    }
}

impl From<api::Udf> for Udf {
    fn from(value: api::Udf) -> Self {
        Udf {
            language: value.language().into(),
            definition: value.definition,
        }
    }
}

#[derive(Serialize, Deserialize, Clone, Debug, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct Udf {
    pub language: UdfLanguage,
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
