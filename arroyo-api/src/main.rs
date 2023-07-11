use arroyo_api::{ApiServer, rest};
use arroyo_rpc::grpc::api::api_grpc_server::ApiGrpcServer;
use arroyo_server_common::{log_event, start_admin_server};
use arroyo_types::{
    grpc_port, ports, service_port, DatabaseConfig, CONTROLLER_ADDR_ENV, HTTP_PORT_ENV,
};
use cornucopia_async::GenericClient;
use deadpool_postgres::{ManagerConfig, Pool, RecyclingMethod};
use serde_json::json;
use tokio::{select, sync::broadcast};
use tokio_postgres::NoTls;
use tracing::{debug, info};
use uuid::Uuid;

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

async fn server(pool: Pool) {
    let (shutdown_tx, shutdown_rx) = broadcast::channel(1);

    start_admin_server("api", ports::API_ADMIN, shutdown_rx.resubscribe());

    log_event("service_startup", json!({"service": "api"}));

    let reflection = tonic_reflection::server::Builder::configure()
        .register_encoded_file_descriptor_set(arroyo_rpc::grpc::API_FILE_DESCRIPTOR_SET)
        .build()
        .unwrap();

    let controller_addr = std::env::var(CONTROLLER_ADDR_ENV)
        .unwrap_or_else(|_| format!("http://localhost:{}", ports::CONTROLLER_GRPC));

    let http_port = service_port("api", ports::API_HTTP, HTTP_PORT_ENV);
    let addr = format!("0.0.0.0:{}", http_port).parse().unwrap();
    let api_server_pool = pool.clone();
    let server = ApiServer {
        pool: api_server_pool,
        controller_addr,
    };

    let app = rest::create_rest_app(server.clone(), pool);
    let mut rest_shutdown_rx = shutdown_rx.resubscribe();

    info!("Starting rest api server on {:?}", addr);
    tokio::spawn(async move {
        select! {
            result = axum::Server::bind(&addr)
            .serve(app.into_make_service()) => {
                result.unwrap();
            }
            _ = rest_shutdown_rx.recv() => {
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


