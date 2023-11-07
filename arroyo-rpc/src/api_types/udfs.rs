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
pub struct ValidateUdfPost {
    pub definition: String,
}

#[derive(Serialize, Deserialize, Clone, Debug, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct UdfValidationResult {
    pub udf_name: Option<String>,
    pub errors: Vec<String>,
}

#[derive(Serialize, Deserialize, Clone, Debug, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct UdfPost {
    pub prefix: String,
    pub definition: String,
    pub description: Option<String>,
}

#[derive(Serialize, Deserialize, Clone, Debug, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct GlobalUdf {
    pub id: String,
    pub prefix: String,
    pub name: String,
    pub created_at: u64,
    pub updated_at: u64,
    pub definition: String,
    pub language: UdfLanguage,
    pub description: Option<String>,
}
