use arroyo_server_common::log_event;
use axum::extract::State;
use axum::headers::authorization::{Authorization, Bearer};
use axum::response::{IntoResponse, Response};
use axum::{http::StatusCode, routing::post, routing::get, Json, Router, TypedHeader};
use deadpool_postgres::{Object, Pool};
use serde_json::json;
use tonic::Status;
use tracing::error;
use utoipa::OpenApi;
use utoipa_swagger_ui::SwaggerUi;
use serde::{Deserialize, Serialize};
use tower_http::cors;
use tower_http::cors::CorsLayer;
use utoipa::ToSchema;
use arroyo_rpc::grpc::api::TestSourceMessage;
use arroyo_types::api::{ConnectionTypes, HttpConnection, KafkaAuthConfig, KafkaConnection, PostConnections, SaslAuth, ConnectionCollection};
use crate::{cloud, connections, AuthData};

type BearerAuth = Option<TypedHeader<Authorization<Bearer>>>;

#[derive(OpenApi)]
#[openapi(
    info(title = "Arroyo REST API", version = "1.0.0"),
    paths(create_connection, get_connections, test_connection),
    components(schemas(
        PostConnections,
        ConnectionTypes,
        KafkaConnection,
        KafkaAuthConfig,
        SaslAuth,
        HttpConnection,
        ConnectionTestResult,
        ConnectionCollection
    )),
    tags(
        (name = "connection", description = "Connections management API")
    )
)]
pub struct ApiDoc;

#[derive(Clone)]
struct AppState {
    pool: Pool,
}

pub struct ErrorResp {
    pub(crate) status_code: StatusCode,
    pub(crate) message: String,
}

// Since some of the grpc methods depend on `get_connections`,
// so implement this trait to maintain backwards compatibility.
impl From<ErrorResp> for Status {
    fn from(error: ErrorResp) -> Self {
        Status::new(tonic::Code::Unknown, error.message)
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

async fn client(pool: &Pool) -> Result<Object, ErrorResp> {
    pool.get().await.map_err(log_and_map_rest)
}

async fn authenticate(pool: &Pool, bearer_auth: BearerAuth) -> Result<AuthData, ErrorResp> {
    let client = client(pool).await?;
    cloud::authenticate_rest(client, bearer_auth).await
}

#[utoipa::path(
    post,
    path = "/v1/connections",
    tag = "connection",
    request_body = PostConnections,
    responses(
        (status = 200, description = "Connection created successfully"),
    ),
)]
async fn create_connection(
    State(state): State<AppState>,
    bearer_auth: BearerAuth,
    Json(payload): Json<PostConnections>,
) -> Result<(), ErrorResp> {
    let auth_data = authenticate(&state.pool, bearer_auth).await?;
    let client = client(&state.pool).await?;
    let connection = payload.clone().into();
    connections::create_connection(connection, auth_data, client).await
}

#[utoipa::path(
    get,
    path = "/v1/connections",
    tag = "connection",
    responses(
        (status = 200, description = "Connection retrieved successfully", body = ConnectionCollection),
    ),
)]
async fn get_connections(
    State(state): State<AppState>,
    bearer_auth: BearerAuth,
) -> Result<Json<ConnectionCollection>, ErrorResp> {
    let auth_data = authenticate(&state.pool, bearer_auth).await?;
    let client = client(&state.pool).await?;
    Ok(Json(
        ConnectionCollection {
            items: connections::get_connections(&auth_data, &client).await?
        }
    ))
}


#[derive(Serialize, Deserialize, Clone, ToSchema, Debug)]
#[serde(rename_all = "camelCase")]
pub struct ConnectionTestResult {
    error: bool,
    message: String
}


impl From<TestSourceMessage> for ConnectionTestResult {
    fn from(value: TestSourceMessage) -> Self {
        ConnectionTestResult {
            error: value.error,
            message: value.message
        }
    }
}


#[utoipa::path(
    post,
    path = "/v1/connections/test",
    tag = "connection",
    request_body = PostConnections,
    responses(
        (status = 200, description = "Connection tested successfully", body = ConnectionTestResult),
    ),
)]
async fn test_connection(
    Json(payload): Json<PostConnections>,
) -> Result<Json<ConnectionTestResult>, ErrorResp> {
    let connection = payload.clone().into();
    Ok(Json(connections::test_connection(connection).await?.into()))
}

pub(crate) fn create_rest_app(pool: Pool) -> Router {
    let cors = CorsLayer::new()
        .allow_headers(cors::Any)
        .allow_origin(cors::Any);

    Router::new()
        .merge(SwaggerUi::new("/swagger-ui").url("/api-docs/openapi.json", ApiDoc::openapi()))
        .route("/v1/connections", post(create_connection))
        .route("/v1/connections", get(get_connections))
        .route("/v1/connections/test", post(test_connection))
        .with_state(AppState { pool })
        .layer(cors)
}
