use crate::{cloud, AuthData};
use arroyo_server_common::log_event;
use axum::extract::rejection::JsonRejection;
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::{Json, TypedHeader};
use deadpool_postgres::{Object, Pool};
use serde_json::json;
use thiserror::Error;
use tracing::error;

use axum::headers::authorization::{Authorization, Bearer};
use tonic::Code;

pub type BearerAuth = Option<TypedHeader<Authorization<Bearer>>>;

const DEFAULT_ITEMS_PER_PAGE: u32 = 10;

#[derive(Debug)]
pub struct ErrorResp {
    pub(crate) status_code: StatusCode,
    pub(crate) message: String,
}

#[derive(Debug, Error)]
pub enum ApiError {
    #[error(transparent)]
    JsonExtractorRejection(#[from] JsonRejection),
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

impl From<tonic::Status> for ErrorResp {
    fn from(value: tonic::Status) -> Self {
        let status_code = match value.code() {
            Code::Cancelled => StatusCode::REQUEST_TIMEOUT,
            Code::Unknown => StatusCode::INTERNAL_SERVER_ERROR,
            Code::InvalidArgument => StatusCode::BAD_REQUEST,
            Code::DeadlineExceeded => StatusCode::GATEWAY_TIMEOUT,
            Code::NotFound => StatusCode::NOT_FOUND,
            Code::AlreadyExists => StatusCode::CONFLICT,
            Code::PermissionDenied => StatusCode::FORBIDDEN,
            Code::ResourceExhausted => StatusCode::TOO_MANY_REQUESTS,
            Code::FailedPrecondition => StatusCode::PRECONDITION_FAILED,
            Code::Aborted => StatusCode::CONFLICT,
            Code::OutOfRange => StatusCode::BAD_REQUEST,
            Code::Unimplemented => StatusCode::NOT_IMPLEMENTED,
            Code::Internal => StatusCode::INTERNAL_SERVER_ERROR,
            Code::Unavailable => StatusCode::SERVICE_UNAVAILABLE,
            Code::DataLoss => StatusCode::INTERNAL_SERVER_ERROR,
            Code::Unauthenticated => StatusCode::UNAUTHORIZED,
            _ => StatusCode::INTERNAL_SERVER_ERROR,
        };

        ErrorResp {
            status_code,
            message: value.message().to_string(),
        }
    }
}

pub fn log_and_map_rest<E>(err: E) -> ErrorResp
where
    E: core::fmt::Debug,
{
    error!("Error while handling: {:?}", err);
    log_event("api_error", json!({ "error": format!("{:?}", err) }));
    ErrorResp {
        status_code: StatusCode::INTERNAL_SERVER_ERROR,
        message: "Something went wrong".to_string(),
    }
}

impl IntoResponse for ErrorResp {
    fn into_response(self) -> Response {
        let body = Json(json!({
            "error": self.message,
        }));
        (self.status_code, body).into_response()
    }
}

pub async fn client(pool: &Pool) -> Result<Object, ErrorResp> {
    pool.get().await.map_err(log_and_map_rest)
}

pub(crate) async fn authenticate(
    pool: &Pool,
    bearer_auth: BearerAuth,
) -> Result<AuthData, ErrorResp> {
    let client = client(pool).await?;
    cloud::authenticate_rest(client, bearer_auth).await
}

pub(crate) fn bad_request(message: String) -> ErrorResp {
    ErrorResp {
        status_code: StatusCode::BAD_REQUEST,
        message: message.to_string(),
    }
}

pub(crate) fn unauthorized(message: String) -> ErrorResp {
    ErrorResp {
        status_code: StatusCode::UNAUTHORIZED,
        message: message.to_string(),
    }
}

pub(crate) fn not_found(object: String) -> ErrorResp {
    ErrorResp {
        status_code: StatusCode::NOT_FOUND,
        message: format!("{} not found", object),
    }
}

pub(crate) fn required_field(field: &str) -> ErrorResp {
    bad_request(format!("Field {} must be set", field))
}

pub fn validate_pagination_params(
    starting_after: Option<String>,
    limit: Option<u32>,
) -> Result<(Option<String>, u32), ErrorResp> {
    // return ErrorResp if limit is less than 1
    if let Some(limit) = limit {
        if limit < 1 {
            return Err(ErrorResp {
                status_code: StatusCode::BAD_REQUEST,
                message: "Limit must be greater than 0".to_string(),
            });
        }
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
