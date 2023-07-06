use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

#[derive(Serialize, Deserialize, Clone, ToSchema, Debug)]
#[serde(rename_all = "camelCase")]
pub struct PipelinePost {
    pub name: String,
    pub definition: Option<String>,
    pub udfs: Option<Vec<Udf>>,
}

#[derive(Serialize, Deserialize, Clone, ToSchema, Debug)]
#[serde(rename_all = "camelCase")]
pub struct Pipeline {
    pub id: String,
    pub name: String,
    pub definition: Option<String>,
    pub udfs: Vec<Udf>,
}

#[derive(Serialize, Deserialize, Clone, ToSchema, Debug)]
#[serde(rename_all = "camelCase")]
pub struct Job {
    pub id: String,
    pub pipeline_id: String,
    pub running_desired: bool,
    pub state: String,
    pub run_id: u64,
    pub start_time: Option<u64>,
    pub finish_time: Option<u64>,
    pub tasks: Option<u64>,
    pub failure_message: Option<String>,
}

#[derive(Serialize, Deserialize, Clone, ToSchema, Debug, FromPrimitive)]
#[serde(rename_all = "camelCase")]
pub enum UdfLanguage {
    Rust = 0,
}

#[derive(Serialize, Deserialize, Clone, ToSchema, Debug)]
#[serde(rename_all = "camelCase")]
pub struct Udf {
    pub language: UdfLanguage,
    pub definition: String,
}

#[derive(Serialize, Deserialize, Clone, ToSchema, Debug)]
#[serde(rename_all = "camelCase")]
pub struct PipelineCollection {
    pub total: i32,
    pub page: i32,
    pub per_page: i32,
    pub pages: i32,
    pub items: Vec<Pipeline>,
}

#[derive(Serialize, Deserialize, Clone, ToSchema, Debug)]
#[serde(rename_all = "camelCase")]
pub struct JobCollection {
    pub total: i32,
    pub page: i32,
    pub per_page: i32,
    pub pages: i32,
    pub items: Vec<Job>,
}
