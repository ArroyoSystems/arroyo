use axum::response::{Html, IntoResponse, Response};
use axum::{
    Json, Router,
    routing::{delete, get, patch, post, put},
};

use anyhow::{Context, bail};
use http::{HeaderMap, HeaderName, HeaderValue, StatusCode, Uri, header};
use rust_embed::RustEmbed;
use tower_http::cors::{AllowHeaders, AllowMethods, AllowOrigin, Any, CorsLayer};
use utoipa::OpenApi;
use utoipa_swagger_ui::SwaggerUi;

use crate::ApiDoc;
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
    create_pipeline, create_preview_pipeline, delete_pipeline, get_pipeline, get_pipeline_jobs,
    get_pipelines, patch_pipeline, put_pipeline, restart_pipeline, validate_query,
};
use crate::rest_utils::not_found;
use crate::udfs::{create_udf, delete_udf, get_udfs, validate_udf};
use arroyo_rpc::config::{CorsConfig, CorsOriginPolicy, config};
use cornucopia_async::DatabaseSource;

static BASENAME_HEADER: HeaderName = HeaderName::from_static("x-arroyo-basename");

#[derive(RustEmbed)]
#[folder = "../../webui/dist"]
struct Assets;

#[derive(Clone)]
pub struct AppState {
    pub(crate) database: DatabaseSource,
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

fn pipeline_jobs_routes() -> Router<AppState> {
    Router::new()
        .route("/", get(get_pipeline_jobs))
        .route("/:job_id/errors", get(get_job_errors))
        .route("/:job_id/checkpoints", get(get_job_checkpoints))
        .route(
            "/:job_id/checkpoints/:epoch/operator_checkpoint_groups",
            get(get_checkpoint_details),
        )
        .route("/:job_id/output", get(get_job_output))
        .route(
            "/:job_id/operator_metric_groups",
            get(get_operator_metric_groups),
        )
}

fn pipeline_and_job_routes() -> Router<AppState> {
    Router::new()
        .route("/pipelines", post(create_pipeline))
        .route("/pipelines/preview", post(create_preview_pipeline))
        .route("/pipelines", get(get_pipelines))
        .route("/jobs", get(get_jobs))
        .route("/pipelines/validate_query", post(validate_query))
        .route("/pipelines/:id", put(put_pipeline))
        .route("/pipelines/:id", patch(patch_pipeline))
        .route("/pipelines/:id", get(get_pipeline))
        .route("/pipelines/:id/restart", post(restart_pipeline))
        .route("/pipelines/:id", delete(delete_pipeline))
        .nest("/pipelines/:id/jobs", pipeline_jobs_routes())
}

async fn not_found_static() -> Response {
    (StatusCode::NOT_FOUND, "404").into_response()
}

async fn static_handler(uri: Uri, headers: HeaderMap) -> impl IntoResponse {
    let path = uri.path().trim_start_matches('/');

    if path.is_empty() || path == "index.html" {
        return index_html(headers).await;
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

            index_html(headers).await
        }
    }
}

async fn index_html(headers: HeaderMap) -> Response {
    let basename = headers
        .get(&BASENAME_HEADER)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("");

    match Assets::get("index.html") {
        Some(content) => {
            let replaced = String::from_utf8(content.data.to_vec())
                .expect("index.html is invalid UTF-8")
                .replace(
                    "{{API_ENDPOINT}}",
                    &config()
                        .api_endpoint
                        .as_ref()
                        .map(|t| t.to_string())
                        .unwrap_or_default(),
                )
                .replace("{{CLUSTER_ID}}", &arroyo_server_common::get_cluster_id())
                .replace(
                    "{{DISABLE_TELEMETRY}}",
                    if config().disable_telemetry {
                        "true"
                    } else {
                        "false"
                    },
                )
                .replace("{{BASENAME}}", basename)
                .replace("/assets", &format!("{basename}/assets"));

            Html(replaced).into_response()
        }
        None => not_found_static().await,
    }
}

fn cors_layer(config: &CorsConfig) -> anyhow::Result<CorsLayer> {
    let cors = CorsLayer::new()
        .allow_methods(AllowMethods::mirror_request())
        .allow_headers(AllowHeaders::mirror_request());

    match config.origin_policy {
        CorsOriginPolicy::Any => {
            if config.allow_credentials {
                bail!("CORS credentials cannot be enabled when allowing any origin");
            }
            if !config.allowed_origins.is_empty() {
                bail!("CORS allowed origins must be empty when allowing any origin");
            }

            Ok(cors.allow_credentials(false).allow_origin(Any))
        }
        CorsOriginPolicy::AllowList => {
            if config.allowed_origins.iter().any(|origin| origin == "*") {
                bail!("CORS allow-list cannot contain the wildcard origin '*'");
            }

            let origins = config
                .allowed_origins
                .iter()
                .map(|origin| {
                    let url = url::Url::parse(origin)
                        .with_context(|| format!("invalid CORS allowed origin {origin:?}"))?;
                    if url.origin().ascii_serialization() != *origin {
                        bail!(
                            "CORS allowed origin {origin:?} must contain only a canonical scheme, host, and optional port"
                        );
                    }

                    origin
                        .parse::<HeaderValue>()
                        .with_context(|| format!("invalid CORS allowed origin {origin:?}"))
                })
                .collect::<anyhow::Result<Vec<_>>>()?;

            Ok(cors
                .allow_credentials(config.allow_credentials)
                .allow_origin(AllowOrigin::list(origins)))
        }
    }
}

pub fn create_rest_app(database: DatabaseSource) -> anyhow::Result<Router> {
    let cors = cors_layer(&config().api.cors)?;

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
        .merge(pipeline_and_job_routes())
        .fallback(api_fallback);

    Ok(Router::new()
        .merge(
            SwaggerUi::new("/api/v1/swagger-ui")
                .url("/api/v1/api-docs/openapi.json", ApiDoc::openapi()),
        )
        .nest("/api/v1", api_routes)
        .nest(
            "/api/v1/admin/organizations/:organization_id",
            pipeline_and_job_routes(),
        )
        .fallback(static_handler)
        .with_state(AppState { database })
        .layer(cors))
}
