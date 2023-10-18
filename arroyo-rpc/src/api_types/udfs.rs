use crate::grpc::api as api_proto;
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

#[derive(Serialize, Deserialize, Clone, Debug, ToSchema, Eq, PartialEq)]
#[serde(rename_all = "camelCase")]
pub enum UdfLanguage {
    Rust,
}

impl ToString for UdfLanguage {
    fn to_string(&self) -> String {
        match self {
            UdfLanguage::Rust => "rust".to_string(),
        }
    }
}

impl From<api_proto::UdfLanguage> for UdfLanguage {
    fn from(value: api_proto::UdfLanguage) -> Self {
        match value {
            api_proto::UdfLanguage::Rust => UdfLanguage::Rust,
        }
    }
}

impl From<String> for UdfLanguage {
    fn from(value: String) -> Self {
        match value.to_lowercase().as_str() {
            "rust" => UdfLanguage::Rust,
            _ => panic!("Invalid UDF language: {}", value),
        }
    }
}

impl From<api_proto::Udf> for Udf {
    fn from(value: api_proto::Udf) -> Self {
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

#[derive(Serialize, Deserialize, Clone, Debug, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct ValidateUdfsPost {
    pub udfs_rs: String,
}

#[derive(Serialize, Deserialize, Clone, Debug, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct UdfValidationResult {
    pub udfs_rs: Option<String>,
    pub errors: Option<Vec<String>>,
}
