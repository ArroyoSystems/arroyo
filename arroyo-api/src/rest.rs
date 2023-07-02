use axum::response::IntoResponse;
use axum::{routing::get, Json, Router};
use utoipa::OpenApi;
use utoipa_swagger_ui::SwaggerUi;

#[derive(OpenApi)]
#[openapi(
    info(title = "Arroyo REST API", version = "1.0.0"),
    paths(ping),
    tags(
        (name = "ping", description = "Ping")
    )
)]
pub struct ApiDoc;

#[utoipa::path(
    get,
    path = "/v1/ping",
    tag = "ping",
    responses(
        (status = 200, description = "Ping endpoint",),
    ),
)]
pub async fn ping() -> impl IntoResponse {
    Json("Pong")
}

pub fn create_rest_app() -> Router {
    Router::new()
        .merge(SwaggerUi::new("/swagger-ui").url("/api-docs/openapi.json", ApiDoc::openapi()))
        .route("/v1/ping", get(ping))
}
