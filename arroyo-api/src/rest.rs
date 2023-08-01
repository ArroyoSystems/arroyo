use axum::body::Body;
use axum::response::IntoResponse;
use axum::{
    routing::{delete, get, patch, post},
    Json, Router,
};
use deadpool_postgres::Pool;
use http::StatusCode;
use once_cell::sync::Lazy;
use std::env;
use std::path::PathBuf;
use std::str::FromStr;
use tower::service_fn;
use tower_http::cors;
use tower_http::cors::CorsLayer;
use tower_http::services::ServeDir;
use utoipa::OpenApi;
use utoipa_swagger_ui::SwaggerUi;

use crate::jobs::{get_job_checkpoints, get_job_errors, get_job_output, get_jobs};
use crate::pipelines::{
    delete_pipeline, get_pipeline, get_pipeline_jobs, get_pipelines, patch_pipeline, post_pipeline,
    validate_pipeline,
};
use crate::rest_utils::ErrorResp;
use crate::ApiDoc;
use crate::ApiServer;
use arroyo_types::{telemetry_enabled, API_ENDPOINT_ENV, ASSET_DIR_ENV};

#[derive(Clone)]
pub struct AppState {
    pub(crate) grpc_api_server: ApiServer,
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

pub async fn api_fallback() -> impl IntoResponse {
    ErrorResp {
        status_code: StatusCode::NOT_FOUND,
        message: "Route not found.".to_string(),
    }
}

pub fn create_rest_app(server: ApiServer, pool: Pool) -> Router {
    let asset_dir = env::var(ASSET_DIR_ENV).unwrap_or_else(|_| "arroyo-console/dist".to_string());

    static INDEX_HTML: Lazy<String> = Lazy::new(|| {
        let asset_dir =
            env::var(ASSET_DIR_ENV).unwrap_or_else(|_| "arroyo-console/dist".to_string());

        let endpoint = env::var(API_ENDPOINT_ENV).unwrap_or_else(|_| String::new());

        std::fs::read_to_string(PathBuf::from_str(&asset_dir).unwrap()
            .join("index.html"))
            .expect("Could not find index.html in asset dir (you may need to build the console sources)")
            .replace("{{API_ENDPOINT}}", &endpoint)
            .replace("{{CLUSTER_ID}}", &arroyo_server_common::get_cluster_id())
            .replace("{{DISABLE_TELEMETRY}}", if telemetry_enabled() { "false" } else { "true" })
    });

    let fallback = service_fn(|_: http::Request<_>| async move {
        let body = Body::from(INDEX_HTML.as_str());
        let res = http::Response::new(body);
        Ok::<_, _>(res)
    });

    let serve_dir = ServeDir::new(&asset_dir).not_found_service(fallback);

    // TODO: enable in development only!!!
    let cors = CorsLayer::new()
        .allow_methods(cors::Any)
        .allow_headers(cors::Any)
        .allow_origin(cors::Any);

    let jobs_routes = Router::new()
        .route("/", get(get_pipeline_jobs))
        .route("/:job_id/errors", get(get_job_errors))
        .route("/:job_id/checkpoints", get(get_job_checkpoints))
        .route("/:job_id/output", get(get_job_output));

    let api_routes = Router::new()
        .route("/ping", get(ping))
        .route("/pipelines", post(post_pipeline))
        .route("/pipelines", get(get_pipelines))
        .route("/jobs", get(get_jobs))
        .route("/pipelines/validate", post(validate_pipeline))
        .route("/pipelines/:id", patch(patch_pipeline))
        .route("/pipelines/:id", get(get_pipeline))
        .route("/pipelines/:id", delete(delete_pipeline))
        .nest("/pipelines/:id/jobs", jobs_routes)
        .fallback(api_fallback);

    Router::new()
        .merge(
            SwaggerUi::new("/api/v1/swagger-ui")
                .url("/api/v1/api-docs/openapi.json", ApiDoc::openapi()),
        )
        .nest("/api/v1", api_routes)
        .route_service("/", fallback)
        .fallback_service(serve_dir)
        .with_state(AppState {
            grpc_api_server: server,
            pool,
        })
        .layer(cors)
}
