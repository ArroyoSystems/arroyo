mod run;

use anyhow::{anyhow, bail};
use arroyo_df::{ArroyoSchemaProvider, SqlConfig};
use arroyo_rpc::config;
use arroyo_rpc::config::{config, DatabaseType};
use arroyo_server_common::shutdown::{Shutdown, SignalBehavior};
use arroyo_server_common::{log_event, start_admin_server};
use arroyo_worker::{utils, WorkerServer};
use clap::{Args, Parser, Subcommand};
use clio::Input;
use cornucopia_async::DatabaseSource;
use deadpool_postgres::{ManagerConfig, Pool, RecyclingMethod};
use serde_json::json;
use std::env::temp_dir;
use std::path::PathBuf;
use std::process::exit;
use std::sync::Arc;
use std::time::Duration;
use std::{env, fs};
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::time::timeout;
use tokio_postgres::{Client, Connection, NoTls};
use tracing::{debug, error, info};
use uuid::Uuid;

#[cfg(all(
    not(target_env = "msvc"),
    not(target_os = "macos"),
    any(target_arch = "x86_64", target_arch = "aarch64")
))]
#[global_allocator]
static ALLOC: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

#[cfg(all(
    feature = "profiling",
    not(target_env = "msvc"),
    not(target_os = "macos"),
    any(target_arch = "x86_64", target_arch = "aarch64")
))]
#[allow(non_upper_case_globals)]
#[export_name = "malloc_conf"]
pub static malloc_conf: &[u8] = b"prof:true,prof_active:true,lg_prof_sample:19\0";

#[derive(Parser)]
#[command(version, about)]
struct Cli {
    #[command(subcommand)]
    command: Commands,

    /// Path to an Arroyo config file, in TOML or YAML format
    #[arg(short, long)]
    config: Option<PathBuf>,

    /// Directory in which to look for configuration files
    #[arg(long)]
    config_dir: Option<PathBuf>,
}

#[derive(Args)]
struct RunArgs {
    /// Name for this pipeline
    #[arg(short, long)]
    name: Option<String>,

    /// Directory or URL where checkpoints and metadata will be written and restored from
    #[arg(short = 's', long)]
    state_dir: Option<String>,

    /// Number of parallel subtasks to run
    #[arg(short, long, default_value = "1")]
    parallelism: u32,

    /// Force the pipeline to start even if the state file does not match the query
    #[clap(short, long)]
    force: bool,

    /// The query to run
    #[clap(value_parser, default_value = "-")]
    query: Input,
}

#[derive(Subcommand)]
enum Commands {
    /// Run a query as a local pipeline cluster
    Run(RunArgs),

    /// Starts an Arroyo API server
    Api {},

    /// Starts an Arroyo Controller
    Controller {},

    /// Starts a complete Arroyo cluster
    Cluster {},

    /// Starts an Arroyo worker
    Worker {},

    /// Starts an Arroyo compiler server
    Compiler {},

    /// Starts an Arroyo node server
    Node {},

    /// Runs database migrations on the configured Postgres database
    Migrate {
        /// If set, waits for the specified number of seconds until Postgres is ready before running migrations
        #[arg(long)]
        wait: Option<u32>,
    },

    /// Visualizes a query plan
    Visualize {
        /// Open the visualization in the browser
        #[clap(short, long, action)]
        open: bool,

        /// The query to visualize
        #[clap(value_parser, default_value = "-")]
        query: Input,
    },
}

#[derive(Debug, Eq, PartialEq, Copy, Clone)]
enum CPService {
    Api,
    Compiler,
    Controller,
    All,
}

impl CPService {
    pub fn name(&self) -> &'static str {
        match self {
            CPService::Api => "api",
            CPService::Compiler => "compiler",
            CPService::Controller => "controller",
            CPService::All => "cluster",
        }
    }
}

#[tokio::main]
async fn main() {
    let cli = Cli::parse();

    config::initialize_config(
        cli.config.as_ref().map(|t| t.as_ref()),
        cli.config_dir.as_ref().map(|t| t.as_ref()),
    );

    match cli.command {
        Commands::Api { .. } => {
            start_control_plane(CPService::Api).await;
        }
        Commands::Compiler { .. } => {
            start_control_plane(CPService::Compiler).await;
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
        Commands::Migrate { wait } => {
            if let Err(e) = migrate(wait).await {
                error!("{}", e);
                exit(1);
            }
        }
        Commands::Node { .. } => {
            start_node().await;
        }
        Commands::Run(args) => {
            run::run(args).await;
        }
        Commands::Visualize { query, open } => {
            visualize(query, open).await;
        }
    };
}

async fn pg_pool() -> Pool {
    let config = &config().database.postgres;
    let mut cfg = deadpool_postgres::Config::new();
    cfg.dbname = Some(config.database_name.clone());
    cfg.host = Some(config.host.clone());
    cfg.port = Some(config.port);
    cfg.user = Some(config.user.clone());
    cfg.password = Some((*config.password).clone());
    cfg.manager = Some(ManagerConfig {
        recycling_method: RecyclingMethod::Fast,
    });
    let pool = cfg
        .create_pool(Some(deadpool_postgres::Runtime::Tokio1), NoTls)
        .unwrap_or_else(|e| {
            error!("Unable to connect to database {:?}: {:?}", config, e);
            exit(1);
        });

    let object_manager = pool.get().await.unwrap_or_else(|e| {
        error!(
            "Unable to create database connection for {:?} {}",
            config, e
        );
        exit(1);
    });

    match object_manager
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

fn sqlite_connection() -> rusqlite::Connection {
    let path = config().database.sqlite.path.clone();

    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).unwrap_or_else(|e| {
            panic!(
                "Could not create database directory {}: {:?}",
                path.to_string_lossy(),
                e
            )
        });
    }

    let exists = path.exists();

    let mut conn = rusqlite::Connection::open(&path)
        .unwrap_or_else(|e| panic!("Could not open sqlite database at path {:?}: {:?}", path, e));

    if !exists {
        info!("Creating config database at {}", path.to_string_lossy());
        if let Err(e) = sqlite_migrations::migrations::runner().run(&mut conn) {
            let _ = fs::remove_file(&path);
            panic!("Failed to set up database: {}", e);
        }

        let uuid = Uuid::new_v4().to_string();
        conn.execute(
            "INSERT INTO cluster_info (id, name) VALUES (?1, 'default');",
            [&uuid],
        )
        .expect("Unable to write to sqlite database");
    } else {
        // migrate database
        if let Err(e) = sqlite_migrations::migrations::runner().run(&mut conn) {
            error!("Unable to migrate database to latest schema: {e}");
            error!(
                "To continue, delete or move the existing database at '{}'",
                path.to_string_lossy()
            );
            exit(1);
        }
    }

    let mut statement = conn.prepare("select id from cluster_info").unwrap();

    let results = statement
        .query_map([], |r| r.get(0))
        .expect("Unable to read from sqlite database");

    let uuid: String = results
        .into_iter()
        .next()
        .unwrap_or_else(|| panic!("Invalid sqlite database at {:?}; delete to recreate", path))
        .unwrap();

    arroyo_server_common::set_cluster_id(&uuid);

    drop(statement);

    // enable WAL mode
    conn.pragma_update(None, "journal_mode", "WAL")
        .expect("Unable to enable WAL mode in sqlite");

    // enable foreign keys
    conn.pragma_update(None, "foreign_keys", "ON")
        .expect("Unable to enable foreign key support in sqlite");

    conn
}

async fn db_source() -> DatabaseSource {
    match config().database.r#type {
        DatabaseType::Postgres => DatabaseSource::Postgres(pg_pool().await),
        DatabaseType::Sqlite => {
            DatabaseSource::Sqlite(Arc::new(std::sync::Mutex::new(sqlite_connection())))
        }
    }
}

mod migrations {
    use refinery::embed_migrations;
    embed_migrations!("../arroyo-api/migrations");
}

mod sqlite_migrations {
    use refinery::embed_migrations;
    embed_migrations!("../arroyo-api/sqlite_migrations");
}

async fn connect(
    retry: bool,
) -> anyhow::Result<(
    Client,
    Connection<impl AsyncRead + AsyncWrite + Unpin, impl AsyncRead + AsyncWrite + Unpin>,
)> {
    let config = &config().database.postgres;

    loop {
        match tokio_postgres::config::Config::new()
            .host(&config.host)
            .port(config.port)
            .user(&config.user)
            .password(&*config.password)
            .dbname(&config.database_name)
            .connect(NoTls)
            .await
        {
            Ok(r) => {
                return Ok(r);
            }
            Err(e) => {
                if !e.to_string().contains("authentication") && retry {
                    debug!("Received error from database while waiting: {}", e);
                    tokio::time::sleep(Duration::from_millis(500)).await;
                    continue;
                }

                bail!("Failed to connect to database {:?}: {}", config, e);
            }
        }
    }
}

async fn migrate(wait: Option<u32>) -> anyhow::Result<()> {
    let _guard = arroyo_server_common::init_logging("migrate");

    let (mut client, connection) = if let Some(wait) = wait {
        info!("Waiting for database to be ready to run migrations");
        timeout(Duration::from_secs(wait as u64), connect(true))
            .await
            .map_err(|e| anyhow!("Timed out waiting for database to connect after {}", e))??
    } else {
        connect(false).await?
    };

    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("connection error: {}", e);
        }
    });

    info!(
        "Running migrations on database {:?}",
        config().database.postgres
    );

    let report = migrations::migrations::runner()
        .run_async(&mut client)
        .await
        .map_err(|e| {
            anyhow!(
                "Failed to run migrations on {:?}: {:?}",
                config().database.postgres,
                e
            )
        })?;

    for migration in report.applied_migrations() {
        info!("Applying V{} {}", migration.version(), migration.name());
    }

    info!(
        "Successfully applied {} migration(s)",
        report.applied_migrations().len()
    );

    Ok(())
}

async fn start_control_plane(service: CPService) {
    let _guard = arroyo_server_common::init_logging(service.name());

    let config = config::config();

    let db = db_source().await;

    log_event(
        "service_startup",
        json!({
            "service": service.name(),
            "scheduler": config.controller.scheduler,
        }),
    );

    let shutdown = Shutdown::new(service.name(), SignalBehavior::Handle);

    shutdown.spawn_task("admin", start_admin_server(service.name()));

    if service == CPService::Api || service == CPService::All {
        arroyo_api::start_server(db.clone(), shutdown.guard("api")).unwrap();
    }

    if service == CPService::Compiler || service == CPService::All {
        shutdown.spawn_task("compiler", arroyo_compiler_service::start_service());
    }

    if service == CPService::Controller || service == CPService::All {
        arroyo_controller::ControllerServer::new(db)
            .await
            .start(shutdown.guard("controller"))
            .await
            .expect("Could not start controller");
    }

    Shutdown::handle_shutdown(shutdown.wait_for_shutdown(Duration::from_secs(30)).await);
}

async fn start_worker() {
    let shutdown = Shutdown::new(
        "worker",
        if env::var("UNDER_PROCESS_SCHEDULER").is_ok() {
            SignalBehavior::Ignore
        } else {
            SignalBehavior::Handle
        },
    );
    let server =
        WorkerServer::from_config(shutdown.guard("worker")).expect("Could not start worker");

    let _guard = arroyo_server_common::init_logging(&format!(
        "worker-{}-{}",
        server.id().0,
        server.job_id()
    ));

    shutdown.spawn_task("admin", start_admin_server("worker"));
    let token = shutdown.token();
    tokio::spawn(async move {
        if let Err(e) = server.start_async().await {
            error!("Failed to start worker server: {:?}", e);
            token.cancel();
        }
    });

    Shutdown::handle_shutdown(shutdown.wait_for_shutdown(Duration::from_secs(30)).await);
}

async fn start_node() {
    let shutdown = Shutdown::new("node", SignalBehavior::Handle);
    let id = arroyo_node::start_server(shutdown.guard("node")).await;

    let _guard = arroyo_server_common::init_logging(&format!("node-{}", id.0,));

    shutdown.spawn_task("admin", start_admin_server("worker"));

    Shutdown::handle_shutdown(shutdown.wait_for_shutdown(Duration::from_secs(30)).await);
}

async fn visualize(query: Input, open: bool) {
    let query = std::io::read_to_string(query).expect("Failed to read query");

    let schema_provider = ArroyoSchemaProvider::new();
    let compiled = arroyo_df::parse_and_get_program(&query, schema_provider, SqlConfig::default())
        .await
        .expect("Failed while planning query");

    if open {
        let tmp = temp_dir().join("plan.d2");
        tokio::fs::write(&tmp, utils::to_d2(&compiled.program))
            .await
            .expect("Failed to write plan");
        let output = tmp.with_extension("svg");
        let result = tokio::process::Command::new("d2")
            .arg(&tmp)
            .arg(&output)
            .output()
            .await
            .expect("d2 must be installed to visualize the plan");

        if !result.status.success() {
            panic!(
                "Failed to run d2: {}",
                String::from_utf8_lossy(&result.stderr)
            );
        }

        eprintln!("Wrote svg to {:?}", output);

        let _ = open::that(format!("file://{}", output.to_string_lossy()));
    } else {
        println!("{}", utils::to_d2(&compiled.program));
    }
}
