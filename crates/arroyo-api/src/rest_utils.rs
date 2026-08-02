use crate::{AuthData, cloud};
use arroyo_rpc::log_event;
use axum::Json;
use axum::extract::rejection::JsonRejection;
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use cornucopia_async::{DatabaseSource, DbError};
use serde::{Deserialize, Serialize};
use tracing::{error, warn};
use utoipa::ToSchema;

pub type BearerAuth = cloud::BearerAuth;

pub(crate) trait AuthRequest {
    fn impersonated_organization_id(&self) -> Option<&str>;
}

#[derive(Debug, Deserialize)]
pub(crate) struct PipelinePath {
    pub id: String,
}

#[derive(Debug, Deserialize)]
pub(crate) struct PipelineJobPath {
    pub id: String,
    pub job_id: String,
}

#[derive(Debug, Deserialize)]
pub(crate) struct PipelineJobCheckpointPath {
    pub id: String,
    pub job_id: String,
    pub epoch: u32,
}

const DEFAULT_ITEMS_PER_PAGE: u32 = 10;

#[derive(Debug, ToSchema, Serialize, Deserialize)]
pub struct ErrorResp {
    #[serde(skip)]
    pub(crate) status_code: StatusCode,
    #[serde(rename = "error")]
    pub(crate) message: String,
}

#[derive(Debug, thiserror::Error)]
pub enum ApiError {
    #[error(transparent)]
    JsonExtractorRejection(#[from] JsonRejection),
}

pub fn map_insert_err(name: &str, error: DbError) -> ErrorResp {
    if error == DbError::DuplicateViolation {
        bad_request(format!("{name} with that name already exists"))
    } else {
        error.into()
    }
}

pub fn map_delete_err(name: &str, user: &str, error: DbError) -> ErrorResp {
    if error == DbError::ForeignKeyViolation {
        bad_request(format!(
            "Cannot delete {name}; it is still being used by {user}"
        ))
    } else {
        error.into()
    }
}

impl From<DbError> for ErrorResp {
    fn from(value: DbError) -> Self {
        match value {
            DbError::DuplicateViolation => {
                bad_request("A record already exists with that name or id")
            }
            DbError::ForeignKeyViolation => {
                bad_request("Cannot delete; other records depend on this one")
            }
            DbError::Other(e) => {
                warn!("Unhandled database error {}", e);
                ErrorResp {
                    status_code: StatusCode::INTERNAL_SERVER_ERROR,
                    message: e,
                }
            }
        }
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let (status, message) = match self {
            ApiError::JsonExtractorRejection(json_rejection) => {
                (json_rejection.status(), json_rejection.body_text())
            }
        };

        ErrorResp {
            status_code: status,
            message,
        }
        .into_response()
    }
}

pub fn log_and_map<E>(err: E) -> ErrorResp
where
    E: core::fmt::Debug,
{
    error!("Error while handling: {:?}", err);
    log_event!("api_error", { "error": format!("{:?}", err) });
    ErrorResp {
        status_code: StatusCode::INTERNAL_SERVER_ERROR,
        message: "Something went wrong".to_string(),
    }
}

impl IntoResponse for ErrorResp {
    fn into_response(self) -> Response {
        let status_code = self.status_code;
        let body = Json(serde_json::to_value(self).unwrap());
        (status_code, body).into_response()
    }
}

pub(crate) async fn authenticate(
    db: &DatabaseSource,
    bearer_auth: BearerAuth,
) -> Result<AuthData, ErrorResp> {
    let impersonated_organization_id = bearer_auth
        .impersonated_organization_id()
        .map(str::to_string);
    let mut auth_data = cloud::authenticate(&db.client().await?, bearer_auth).await?;
    apply_impersonated_organization(&mut auth_data, impersonated_organization_id)?;
    Ok(auth_data)
}

fn apply_impersonated_organization(
    auth_data: &mut AuthData,
    impersonated_organization_id: Option<String>,
) -> Result<(), ErrorResp> {
    let Some(organization_id) = impersonated_organization_id else {
        return Ok(());
    };

    if organization_id.is_empty() {
        return Err(bad_request(
            "organization_id path parameter must not be empty",
        ));
    }

    if auth_data.role != "admin" {
        warn!(role = %auth_data.role, "rejecting non-admin caller on org-scoped admin route");
        return Err(ErrorResp {
            status_code: StatusCode::FORBIDDEN,
            message: "Admin role required".to_string(),
        });
    }

    auth_data.organization_id = organization_id;
    Ok(())
}

pub(crate) fn bad_request(message: impl Into<String>) -> ErrorResp {
    ErrorResp {
        status_code: StatusCode::BAD_REQUEST,
        message: message.into(),
    }
}

pub(crate) fn service_unavailable(object: &str) -> ErrorResp {
    ErrorResp {
        status_code: StatusCode::SERVICE_UNAVAILABLE,
        message: format!("{object} not available"),
    }
}

pub(crate) fn internal_server_error(message: impl Into<String>) -> ErrorResp {
    ErrorResp {
        status_code: StatusCode::INTERNAL_SERVER_ERROR,
        message: message.into(),
    }
}

pub(crate) fn not_found(object: &str) -> ErrorResp {
    ErrorResp {
        status_code: StatusCode::NOT_FOUND,
        message: format!("{object} not found"),
    }
}

pub(crate) fn required_field(field: &str) -> ErrorResp {
    bad_request(format!("Field {field} must be set"))
}

pub fn validate_pagination_params(
    starting_after: Option<String>,
    limit: Option<u32>,
) -> Result<(Option<String>, u32), ErrorResp> {
    // return ErrorResp if limit is less than 1
    if let Some(limit) = limit
        && limit < 1
    {
        return Err(ErrorResp {
            status_code: StatusCode::BAD_REQUEST,
            message: "Limit must be greater than 0".to_string(),
        });
    }

    // increase limit by 1 to determine if there are more results
    let limit = limit.unwrap_or(DEFAULT_ITEMS_PER_PAGE) + 1;

    Ok((starting_after.clone(), limit))
}

pub fn paginate_results<T>(results: Vec<T>, limit: u32) -> (Vec<T>, bool) {
    // this limit is one more than the requested limit to determine if there are more results
    let mut results = results;
    let has_more = results.len() as u32 == limit;
    if has_more {
        results.pop();
    }

    (results, has_more)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::OrgMetadata;

    fn auth(role: &str, organization_id: &str) -> AuthData {
        AuthData {
            user_id: "user".to_string(),
            organization_id: organization_id.to_string(),
            role: role.to_string(),
            org_metadata: OrgMetadata::default(),
        }
    }

    #[test]
    fn admin_can_impersonate_path_organization() {
        let mut auth_data = auth("admin", "");

        apply_impersonated_organization(&mut auth_data, Some("acct_42".to_string())).unwrap();

        assert_eq!(auth_data.organization_id, "acct_42");
    }

    #[test]
    fn tenant_cannot_impersonate_path_organization() {
        let mut auth_data = auth("tenant", "acct_1");

        let err = apply_impersonated_organization(&mut auth_data, Some("acct_2".to_string()))
            .unwrap_err();

        assert_eq!(err.status_code, StatusCode::FORBIDDEN);
        assert_eq!(auth_data.organization_id, "acct_1");
    }

    #[test]
    fn impersonated_organization_must_not_be_empty() {
        let mut auth_data = auth("admin", "");

        let err = apply_impersonated_organization(&mut auth_data, Some(String::new())).unwrap_err();

        assert_eq!(err.status_code, StatusCode::BAD_REQUEST);
    }

    #[test]
    fn missing_impersonation_keeps_authenticated_organization() {
        let mut auth_data = auth("tenant", "acct_1");

        apply_impersonated_organization(&mut auth_data, None).unwrap();

        assert_eq!(auth_data.organization_id, "acct_1");
    }
}
