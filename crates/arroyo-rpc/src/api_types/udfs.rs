use serde::{Deserialize, Serialize};
use strum_macros::{Display, EnumString};
use utoipa::ToSchema;

#[derive(Serialize, Deserialize, Clone, Debug, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct Udf {
    pub definition: String,
    #[serde(default)]
    pub language: UdfLanguage,
}

#[derive(Serialize, Deserialize, Clone, Debug, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct ValidateUdfPost {
    pub definition: String,
    #[serde(default)]
    pub language: UdfLanguage,
}

#[derive(Serialize, Deserialize, Clone, Debug, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct UdfValidationResult {
    pub udf_name: Option<String>,
    pub errors: Vec<String>,
}

#[derive(
    Serialize,
    Deserialize,
    Copy,
    Clone,
    Debug,
    ToSchema,
    Default,
    Display,
    EnumString,
    Eq,
    PartialEq,
)]
#[serde(rename_all = "camelCase")]
pub enum UdfLanguage {
    Python,
    #[default]
    Rust,
}

#[derive(Serialize, Deserialize, Clone, Debug, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct UdfPost {
    pub prefix: String,
    #[serde(default)]
    pub language: UdfLanguage,
    pub definition: String,
    pub description: Option<String>,
}

#[derive(Serialize, Deserialize, Clone, Debug, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct GlobalUdf {
    pub id: String,
    pub prefix: String,
    pub name: String,
    pub language: UdfLanguage,
    pub created_at: u64,
    pub updated_at: u64,
    pub definition: String,
    pub description: Option<String>,
    pub dylib_url: Option<String>,
}
