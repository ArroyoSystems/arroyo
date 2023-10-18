use axum::{response::IntoResponse, Json};
use deadpool_postgres::{ManagerConfig, Pool, RecyclingMethod};
use http::StatusCode;
use serde::{Deserialize, Serialize};
use serde_json::json;
use tokio::{select, sync::broadcast};
use tokio_postgres::NoTls;
use tracing::{debug, error, info};
use uuid::Uuid;

use arroyo_api::rest::{self, AppState};
use arroyo_server_common::{log_event, start_admin_server};
use arroyo_types::{ports, service_port, DatabaseConfig, CONTROLLER_ADDR_ENV, HTTP_PORT_ENV};

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
    let controller_addr = std::env::var(CONTROLLER_ADDR_ENV)
        .unwrap_or_else(|_| format!("http://localhost:{}", ports::CONTROLLER_GRPC));
    let args = std::env::args().collect::<Vec<_>>();
    match args.get(1) {
        Some(arg) if arg == "--pipelines" => {
            if let Some(path) = args.get(2) {
                if let Err(err) = arroyo_api::pipelines::register_pipelines_in_folder(
                    path,
                    AppState::new(controller_addr, pool.clone()),
                )
                .await
                {
                    error!("Failed to register pipelines: {:?}", err);
                }
            }
        }
        _ => {}
    }

    server(pool).await;
}

async fn server(pool: Pool) {
    let (shutdown_tx, shutdown_rx) = broadcast::channel(1);

    start_admin_server("api", ports::API_ADMIN, shutdown_rx.resubscribe());

    log_event("service_startup", json!({"service": "api"}));

    let controller_addr = std::env::var(CONTROLLER_ADDR_ENV)
        .unwrap_or_else(|_| format!("http://localhost:{}", ports::CONTROLLER_GRPC));

    let http_port = service_port("api", ports::API_HTTP, HTTP_PORT_ENV);
    let addr = format!("0.0.0.0:{}", http_port).parse().unwrap();

    let app = rest::create_rest_app(pool, &controller_addr);
    let mut rest_shutdown_rx = shutdown_rx.resubscribe();

    info!("Starting rest api server on {:?}", addr);
    select! {
        result = axum::Server::bind(&addr)
        .serve(app.into_make_service()) => {
            result.unwrap();
        }
        _ = rest_shutdown_rx.recv() => {
        }
    }

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
