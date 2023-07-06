use axum::extract::{Path, State};
use axum::headers::authorization::{Authorization, Bearer};
use axum::response::{IntoResponse, Response};
use axum::{
    routing::{get, post},
    Json, Router, TypedHeader,
};
use deadpool_postgres::Pool;
use http::StatusCode;
use serde::Serialize;
use tower_http::validate_request::ValidateRequestHeaderLayer;
use utoipa::OpenApi;
use utoipa_swagger_ui::SwaggerUi;

use arroyo_types::api::{
    Job, JobCollection, Pipeline, PipelineCollection, PipelinePost, Udf, UdfLanguage,
};

use crate::auth::RestAuth;
use crate::pipelines::__path_get_jobs;
use crate::pipelines::__path_get_pipeline;
use crate::pipelines::__path_get_pipelines;
use crate::pipelines::__path_post_pipeline;
use crate::pipelines::{get_jobs, get_pipeline, get_pipelines, post_pipeline};
use crate::rest_utils::{authenticate, client, BearerAuth, ErrorResp};
use crate::{cloud, jobs, pipelines, AuthData, OrgMetadata};

#[derive(OpenApi)]
#[openapi(
    info(title = "Arroyo REST API", version = "1.0.0"),
    paths(ping, post_pipeline, get_pipeline, get_pipelines, get_jobs),
    components(schemas(PipelinePost, Pipeline, Job, Udf, UdfLanguage, PipelineCollection, JobCollection)),
    tags(
        (name = "pipelines", description = "Pipelines"),
        (name = "ping", description = "Ping"),
    )
)]
pub struct ApiDoc;

#[derive(Clone)]
pub struct AppState {
    pub(crate) pool: Pool,
}

#[utoipa::path(
    get,
    path = "/v1/ping",
    tag = "ping",
    responses(
        (status = 200, description = "Ping endpoint"),
    ),
)]
pub async fn ping() -> impl IntoResponse {
    Json("Pong")
}

pub fn create_rest_app(pool: Pool) -> Router {
    Router::new()
        .merge(SwaggerUi::new("/swagger-ui").url("/api-docs/openapi.json", ApiDoc::openapi()))
        .route("/v1/ping", get(ping))
        .route("/v1/pipelines", post(post_pipeline))
        .route("/v1/pipelines", get(get_pipelines))
        .route("/v1/pipelines/:id", get(get_pipeline))
        .route("/v1/pipelines/{id}/jobs", get(get_jobs))
        .with_state(AppState { pool })
        .layer(ValidateRequestHeaderLayer::custom(RestAuth {}))
}
