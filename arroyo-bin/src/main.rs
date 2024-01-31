use arroyo_server_common::shutdown::Shutdown;
use arroyo_server_common::{log_event, start_admin_server};
use arroyo_types::{ports, DatabaseConfig};
use arroyo_worker::WorkerServer;
use clap::{Parser, Subcommand};
use deadpool_postgres::{ManagerConfig, Pool, RecyclingMethod};
use serde_json::json;
use std::time::Duration;
use tokio_postgres::NoTls;
use tracing::error;
use uuid::Uuid;

#[derive(Parser)]
#[command(version, about)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Starts an Arroyo API server
    Api {},

    /// Starts an Arroyo Controller
    Controller {},

    /// Starts a complete Arroyo cluster
    Cluster {},

    /// Starts an Arroyo worker
    Worker {},
}

#[derive(Debug, Eq, PartialEq, Copy, Clone)]
enum CPService {
    Api,
    Controller,
    All,
}

impl CPService {
    pub fn name(&self) -> &'static str {
        match self {
            CPService::Api => "api",
            CPService::Controller => "controller",
            CPService::All => "cluster",
        }
    }
}

#[tokio::main]
async fn main() {
    let cli = Cli::parse();

    match &cli.command {
        Commands::Api { .. } => {
            start_control_plane(CPService::Api).await;
        }
        Commands::Controller { .. } => {
            start_control_plane(CPService::Controller).await;
        }
        Commands::Cluster { .. } => {
            start_control_plane(CPService::All).await;
        }
        Commands::Worker { .. } => {
            start_worker().await;
        }
    };
}

async fn db_pool() -> Pool {
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
            panic!("Failed to get cluster info {:?}", e);
        }
    };

    pool
}

async fn start_control_plane(service: CPService) {
    let _guard = arroyo_server_common::init_logging(service.name());

    let pool = db_pool().await;

    log_event(
        "service_startup",
        json!({
            "service": service.name(),
            "scheduler": std::env::var("SCHEDULER").unwrap_or_else(|_| "process".to_string())
        }),
    );

    let shutdown = Shutdown::new(service.name());

    shutdown.spawn_task(
        "admin",
        start_admin_server(service.name(), ports::API_ADMIN),
    );

    if service == CPService::Api || service == CPService::All {
        shutdown.spawn_task("api", arroyo_api::start_server(pool.clone()));
    }

    if service == CPService::Controller || service == CPService::All {
        arroyo_controller::ControllerServer::new(pool)
            .await
            .start(shutdown.guard("controller"));
    }

    let _ = shutdown.wait_for_shutdown(Duration::from_secs(30)).await;
}

async fn start_worker() {
    let shutdown = Shutdown::new("worker");
    let server = WorkerServer::from_env(shutdown.guard("worker"));

    let _guard = arroyo_server_common::init_logging(&format!(
        "worker-{}-{}",
        server.id().0,
        server.job_id()
    ));

    shutdown.spawn_task("admin", start_admin_server("worker", 0));
    let token = shutdown.token();
    tokio::spawn(async move {
        if let Err(e) = server.start_async().await {
            error!("Failed to start worker server: {:?}", e);
            token.cancel();
        }
    });

    let _ = shutdown.wait_for_shutdown(Duration::from_secs(30)).await;
}
