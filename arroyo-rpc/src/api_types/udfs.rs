use crate::grpc::api as api_proto;
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

#[derive(Serialize, Deserialize, Clone, Debug, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct Udf {
    pub definition: String,
}

impl Into<api_proto::Udf> for Udf {
    fn into(self) -> api_proto::Udf {
        api_proto::Udf {
            definition: self.definition,
        }
    }
}

impl From<api_proto::Udf> for Udf {
    fn from(value: api_proto::Udf) -> Self {
        Udf {
            definition: value.definition,
        }
    }
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
    pub description: Option<String>,
}
