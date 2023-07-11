use axum::body::Body;
use axum::response::IntoResponse;
use axum::{
    routing::{delete, get, post},
    Json, Router,
};
use deadpool_postgres::Pool;
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

use crate::ApiServer;
use crate::rest_types::{Job, JobCollection, Pipeline, PipelineCollection, PipelinePost, Udf};
use arroyo_types::{telemetry_enabled, API_ENDPOINT_ENV, ASSET_DIR_ENV};

use crate::pipelines::__path_post_pipeline;
use crate::pipelines::{__path_delete_pipeline, __path_get_jobs, __path_get_pipeline};
use crate::pipelines::{__path_get_pipelines, delete_pipeline};
use crate::pipelines::{get_jobs, get_pipeline, get_pipelines, post_pipeline};

#[derive(OpenApi)]
#[openapi(
    info(title = "Arroyo REST API", version = "1.0.0"),
    servers((url = "/api/")),
    paths(ping, post_pipeline, get_pipeline, delete_pipeline, get_pipelines, get_jobs),
    components(schemas(PipelinePost, Pipeline, Job, Udf, PipelineCollection, JobCollection)),
    tags(
        (name = "pipelines", description = "Pipeline management endpoints"),
        (name = "ping", description = "Ping endpoint"),
    )
)]
pub struct ApiDoc;

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
        .allow_headers(cors::Any)
        .allow_origin(cors::Any);

    let api_routes = Router::new()
        .route("/ping", get(ping))
        .route("/pipelines", post(post_pipeline))
        .route("/pipelines", get(get_pipelines))
        .route("/pipelines/:id", get(get_pipeline))
        .route("/pipelines/:id", delete(delete_pipeline))
        .route("/pipelines/:id/jobs", get(get_jobs));

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
