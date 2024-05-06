use axum::response::{Html, IntoResponse, Response};
use axum::{
    routing::{delete, get, patch, post},
    Json, Router,
};
use deadpool_postgres::Pool;

use http::{header, StatusCode, Uri};
use rust_embed::RustEmbed;
use std::env;
use tower_http::cors;
use tower_http::cors::CorsLayer;
use utoipa::OpenApi;
use utoipa_swagger_ui::SwaggerUi;

use crate::connection_profiles::{
    create_connection_profile, delete_connection_profile, get_connection_profile_autocomplete,
    get_connection_profiles, test_connection_profile,
};
use crate::connection_tables::{
    create_connection_table, delete_connection_table, get_connection_tables, test_connection_table,
    test_schema,
};
use crate::connectors::get_connectors;
use crate::jobs::{
    get_checkpoint_details, get_job_checkpoints, get_job_errors, get_job_output, get_jobs,
};
use crate::metrics::get_operator_metric_groups;
use crate::pipelines::{
    create_pipeline, delete_pipeline, get_pipeline, get_pipeline_jobs, get_pipelines,
    patch_pipeline, restart_pipeline, validate_query,
};
use crate::rest_utils::not_found;
use crate::udfs::{create_udf, delete_udf, get_udfs, validate_udf};
use crate::ApiDoc;
use arroyo_types::{telemetry_enabled, API_ENDPOINT_ENV};

#[derive(RustEmbed)]
#[folder = "../../webui/dist"]
struct Assets;

#[derive(Clone)]
pub struct AppState {
    pub(crate) controller_addr: String,
    pub(crate) pool: Pool,
}

/// Ping endpoint
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
    not_found("Route")
}

async fn not_found_static() -> Response {
    (StatusCode::NOT_FOUND, "404").into_response()
}

async fn static_handler(uri: Uri) -> impl IntoResponse {
    let path = uri.path().trim_start_matches('/');

    if path.is_empty() || path == "index.html" {
        return index_html().await;
    }

    match Assets::get(path) {
        Some(content) => {
            let mime = mime_guess::from_path(path).first_or_octet_stream();

            ([(header::CONTENT_TYPE, mime.as_ref())], content.data).into_response()
        }
        None => {
            if path.contains('.') {
                return not_found_static().await;
            }

            index_html().await
        }
    }
}

async fn index_html() -> Response {
    match Assets::get("index.html") {
        Some(content) => {
            let endpoint = env::var(API_ENDPOINT_ENV).unwrap_or_else(|_| String::new());

            let replaced = String::from_utf8(content.data.to_vec())
                .expect("index.html is invalid UTF-8")
                .replace("{{API_ENDPOINT}}", &endpoint)
                .replace("{{CLUSTER_ID}}", &arroyo_server_common::get_cluster_id())
                .replace(
                    "{{DISABLE_TELEMETRY}}",
                    if telemetry_enabled() { "false" } else { "true" },
                );

            Html(replaced).into_response()
        }
        None => not_found_static().await,
    }
}

pub fn create_rest_app(pool: Pool, controller_addr: &str) -> Router {
    // TODO: enable in development only!!!
    let cors = CorsLayer::new()
        .allow_methods(cors::Any)
        .allow_headers(cors::Any)
        .allow_origin(cors::Any);

    let jobs_routes = Router::new()
        .route("/", get(get_pipeline_jobs))
        .route("/:job_id/errors", get(get_job_errors))
        .route("/:job_id/checkpoints", get(get_job_checkpoints))
        .route(
            "/:job_id/checkpoints/:checkpoint_id/operator_checkpoint_groups",
            get(get_checkpoint_details),
        )
        .route("/:job_id/output", get(get_job_output))
        .route(
            "/:job_id/operator_metric_groups",
            get(get_operator_metric_groups),
        );

    let api_routes = Router::new()
        .route("/ping", get(ping))
        .route("/connectors", get(get_connectors))
        .route("/connection_profiles/test", post(test_connection_profile))
        .route("/connection_profiles", post(create_connection_profile))
        .route("/connection_profiles", get(get_connection_profiles))
        .route(
            "/connection_profiles/:id",
            delete(delete_connection_profile),
        )
        .route(
            "/connection_profiles/:id/autocomplete",
            get(get_connection_profile_autocomplete),
        )
        .route("/connection_tables", get(get_connection_tables))
        .route("/connection_tables", post(create_connection_table))
        .route("/connection_tables/test", post(test_connection_table))
        .route("/connection_tables/schemas/test", post(test_schema))
        .route("/connection_tables/:id", delete(delete_connection_table))
        .route("/udfs", post(create_udf))
        .route("/udfs", get(get_udfs))
        .route("/udfs/validate", post(validate_udf))
        .route("/udfs/:id", delete(delete_udf))
        .route("/pipelines", post(create_pipeline))
        .route("/pipelines", get(get_pipelines))
        .route("/jobs", get(get_jobs))
        .route("/pipelines/validate_query", post(validate_query))
        .route("/pipelines/:id", patch(patch_pipeline))
        .route("/pipelines/:id", get(get_pipeline))
        .route("/pipelines/:id/restart", post(restart_pipeline))
        .route("/pipelines/:id", delete(delete_pipeline))
        .nest("/pipelines/:id/jobs", jobs_routes)
        .fallback(api_fallback);

    Router::new()
        .merge(
            SwaggerUi::new("/api/v1/swagger-ui")
                .url("/api/v1/api-docs/openapi.json", ApiDoc::openapi()),
        )
        .nest("/api/v1", api_routes)
        .fallback(static_handler)
        .with_state(AppState {
            controller_addr: controller_addr.to_string(),
            pool,
        })
        .layer(cors)
}
