use std::env;
use std::path::PathBuf;
use std::str::FromStr;

use ::time::OffsetDateTime;
use axum::body::Body;
use axum::{response::IntoResponse, Json, Router};
use cornucopia_async::GenericClient;
use deadpool_postgres::{ManagerConfig, Pool, RecyclingMethod};
use http::StatusCode;
use once_cell::sync::Lazy;
use serde::{Deserialize, Serialize};
use serde_json::json;
use tokio::{select, sync::broadcast};
use tokio_postgres::error::SqlState;
use tokio_postgres::NoTls;
use tonic::Status;
use tower::service_fn;
use tower_http::{
    cors::{self, CorsLayer},
    services::ServeDir,
};
use tracing::{debug, error, info, warn};
use uuid::Uuid;

use crate::rest::{log_and_map_rest, ErrorResp};
use arroyo_rpc::grpc::api::api_grpc_server::ApiGrpcServer;
use arroyo_server_common::{log_event, start_admin_server};
use arroyo_types::{
    grpc_port, ports, service_port, telemetry_enabled, DatabaseConfig, API_ENDPOINT_ENV,
    ASSET_DIR_ENV, CONTROLLER_ADDR_ENV, HTTP_PORT_ENV,
};

mod api_server;
mod cloud;
mod connections;
mod job_log;
mod jobs;
mod json_schema;
mod metrics;
mod optimizations;
mod pipelines;
mod rest;
mod sinks;
mod sources;
mod testers;

include!(concat!(env!("OUT_DIR"), "/api-sql.rs"));

#[tokio::main]
pub async fn main() {
    let _guard = arroyo_server_common::init_logging("api");

    let config = DatabaseConfig::load();
    let mut cfg = deadpool_postgres::Config::new();
    cfg.dbname = Some(config.name);
    cfg.host = Some(config.host);
    cfg.port = Some(config.port);
    cfg.user = Some(config.user);
    cfg.password = Some(config.password);
    cfg.manager = Some(ManagerConfig {
        recycling_method: RecyclingMethod::Fast,
    });
    let pool = cfg
        .create_pool(Some(deadpool_postgres::Runtime::Tokio1), NoTls)
        .unwrap_or_else(|e| {
            panic!(
                "Unable to connect to database {:?}@{:?}:{:?}/{:?} {}",
                cfg.user, cfg.host, cfg.port, cfg.dbname, e
            )
        });

    match pool
        .get()
        .await
        .unwrap_or_else(|e| {
            panic!(
                "Unable to create database connection for {:?}@{:?}:{:?}/{:?} {}",
                cfg.user, cfg.host, cfg.port, cfg.dbname, e
            )
        })
        .query_one("select id from cluster_info", &[])
        .await
    {
        Ok(row) => {
            let uuid: Uuid = row.get(0);
            arroyo_server_common::set_cluster_id(&uuid.to_string());
        }
        Err(e) => {
            debug!("Failed to get cluster info {:?}", e);
        }
    };

    server(pool).await;
}

fn to_micros(dt: OffsetDateTime) -> u64 {
    (dt.unix_timestamp_nanos() / 1_000) as u64
}

async fn server(pool: Pool) {
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

    let app = Router::new()
        .route_service("/", fallback)
        .fallback_service(serve_dir)
        .layer(cors);

    let (shutdown_tx, shutdown_rx) = broadcast::channel(1);

    start_admin_server("api", ports::API_ADMIN, shutdown_rx.resubscribe());

    let http_port = service_port("api", ports::API_HTTP, HTTP_PORT_ENV);
    let addr = format!("0.0.0.0:{}", http_port).parse().unwrap();
    info!("Starting http server on {:?}", addr);

    log_event("service_startup", json!({"service": "api"}));

    let mut receiver1 = shutdown_rx.resubscribe();

    tokio::spawn(async move {
        select! {
            result = axum::Server::bind(&addr)
            .serve(app.into_make_service()) => {
                result.unwrap();
            }
            _ = receiver1.recv() => {

            }
        }
    });

    let reflection = tonic_reflection::server::Builder::configure()
        .register_encoded_file_descriptor_set(arroyo_rpc::grpc::API_FILE_DESCRIPTOR_SET)
        .build()
        .unwrap();

    let controller_addr = std::env::var(CONTROLLER_ADDR_ENV)
        .unwrap_or_else(|_| format!("http://localhost:{}", ports::CONTROLLER_GRPC));

    let pool1 = pool.clone();
    let server = api_server::ApiServer {
        pool: pool1,
        controller_addr,
    };

    let rest_addr = format!("0.0.0.0:{}", 8003).parse().unwrap();
    let app = rest::create_rest_app(pool);
    let mut receiver2 = shutdown_rx.resubscribe();

    info!("Starting rest api server on {:?}", rest_addr);
    tokio::spawn(async move {
        select! {
            result = axum::Server::bind(&rest_addr)
            .serve(app.into_make_service()) => {
                result.unwrap();
            }
            _ = receiver2.recv() => {
            }
        }
    });

    let addr = format!("0.0.0.0:{}", grpc_port("api", ports::API_GRPC))
        .parse()
        .unwrap();
    info!("Starting gRPC server on {:?}", addr);

    arroyo_server_common::grpc_server()
        .accept_http1(true)
        .add_service(
            tonic_web::config()
                .allow_all_origins()
                .enable(ApiGrpcServer::new(server)),
        )
        .add_service(reflection)
        .serve(addr)
        .await
        .unwrap();

    shutdown_tx.send(0).unwrap();
}

#[derive(Debug, Serialize, Deserialize)]
pub struct SessionInfo {
    email: String,
    organization_id: String,
    organization_name: String,
}

#[derive(Debug, Copy, Clone, Serialize, Deserialize)]
pub enum HttpErrorCode {
    Unauthorized,
    InvalidCredentials,
    ServerError,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct HttpError {
    code: HttpErrorCode,
    message: String,
}

impl HttpError {
    pub fn new(code: HttpErrorCode, message: impl Into<String>) -> Self {
        Self {
            code,
            message: message.into(),
        }
    }

    pub fn login_error() -> Self {
        Self::new(
            HttpErrorCode::InvalidCredentials,
            "The username or password was incorrect",
        )
    }

    pub fn unauthorized_error() -> Self {
        Self::new(
            HttpErrorCode::Unauthorized,
            "You are not authorized to access this endpoint",
        )
    }

    pub fn server_error() -> Self {
        Self::new(HttpErrorCode::ServerError, "Something went wrong")
    }
}

impl IntoResponse for HttpError {
    fn into_response(self) -> axum::response::Response {
        let status = match self.code {
            HttpErrorCode::InvalidCredentials | HttpErrorCode::Unauthorized => {
                StatusCode::UNAUTHORIZED
            }
            HttpErrorCode::ServerError => StatusCode::INTERNAL_SERVER_ERROR,
        };

        let mut resp = Json(self).into_response();
        *resp.status_mut() = status;
        resp
    }
}

// TODO: look in to using gRPC rich errors
pub fn required_field(field: &str) -> Status {
    Status::invalid_argument(format!("Field {} must be set", field))
}

fn log_and_map<E>(err: E) -> Status
where
    E: core::fmt::Debug,
{
    error!("Error while handling: {:?}", err);
    log_event("api_error", json!({ "error": format!("{:?}", err) }));
    Status::internal("Something went wrong")
}

fn handle_db_error(name: &str, err: tokio_postgres::Error) -> Status {
    if let Some(db) = &err.as_db_error() {
        if *db.code() == SqlState::UNIQUE_VIOLATION {
            // TODO improve error message
            warn!("SQL error: {}", db.message());
            return Status::invalid_argument(format!("A {} with that name already exists", name));
        }
    }

    log_and_map(err)
}

fn handle_db_error_rest(name: &str, err: tokio_postgres::Error) -> ErrorResp {
    if let Some(db) = &err.as_db_error() {
        if *db.code() == SqlState::UNIQUE_VIOLATION {
            // TODO improve error message
            warn!("SQL error: {}", db.message());
            return ErrorResp {
                status_code: StatusCode::BAD_REQUEST,
                message: format!("A {} with that name already exists", name),
            };
        }
    }

    log_and_map_rest(err)
}

fn handle_delete(name: &str, users: &str, err: tokio_postgres::Error) -> Status {
    if let Some(db) = &err.as_db_error() {
        if *db.code() == SqlState::FOREIGN_KEY_VIOLATION {
            return Status::invalid_argument(format!(
                "Cannot delete {}; it is still being used by {}",
                name, users
            ));
        }
    }

    log_and_map(err)
}
