use crate::{cloud, AuthData};
use arroyo_server_common::log_event;
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::{Json, TypedHeader};
use deadpool_postgres::{Object, Pool};
use serde_json::json;
use tracing::error;

use axum::headers::authorization::{Authorization, Bearer};
pub type BearerAuth = Option<TypedHeader<Authorization<Bearer>>>;

impl From<tonic::Status> for ErrorResp {
    fn from(value: tonic::Status) -> Self {
        ErrorResp {
            status_code: StatusCode::INTERNAL_SERVER_ERROR,
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

pub struct ErrorResp {
    pub(crate) status_code: StatusCode,
    pub(crate) message: String,
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
