use crate::{db_source, RunArgs};
use anyhow::{anyhow, bail};
use arroyo_openapi::types::{Pipeline, PipelinePatch, PipelinePost, StopType, ValidateQueryPost};
use arroyo_openapi::Client;
use arroyo_rpc::config::{config, DatabaseType, DefaultSink, Scheduler};
use arroyo_rpc::{config, init_db_notifier, notify_db, retry};
use arroyo_server_common::log_event;
use arroyo_server_common::shutdown::{Shutdown, ShutdownHandler, SignalBehavior};
use arroyo_storage::StorageProvider;
use arroyo_types::to_millis;
use async_trait::async_trait;
use rand::random;
use rusqlite::{Connection, DatabaseName, OpenFlags};
use serde_json::json;
use std::env;
use std::env::{set_var, temp_dir};
use std::path::PathBuf;
use std::process::exit;
use std::str::FromStr;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime};
use tokio::select;
use tokio::task::spawn_blocking;
use tokio::time::{interval, timeout, MissedTickBehavior};
use tracing::level_filters::LevelFilter;
use tracing::{error, info, warn};
use tracing_subscriber::EnvFilter;

async fn get_state(client: &Client, pipeline_id: &str) -> String {
    let jobs = retry!(
        client.get_pipeline_jobs().id(pipeline_id).send().await,
        10,
        Duration::from_millis(100),
        Duration::from_secs(2),
        |e| { warn!("Failed to get job state from API: {}", e) }
    )
    .unwrap()
    .into_inner();

    jobs.data.into_iter().next().unwrap().state
}

async fn wait_for_state(
    client: &Client,
    pipeline_id: &str,
    expected_states: &[&str],
) -> anyhow::Result<()> {
    let mut last_state: String = get_state(client, pipeline_id).await;
    while !expected_states.contains(&last_state.as_str()) {
        let state = get_state(client, pipeline_id).await;
        if last_state != state {
            info!("Job transitioned to {}", state);
            last_state = state;
        }

        if last_state == "Failed" {
            bail!("Job transitioned to failed");
        }

        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    Ok(())
}

async fn wait_for_connect(client: &Client) -> anyhow::Result<()> {
    for _ in 0..50 {
        if client.ping().send().await.is_ok() {
            return Ok(());
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }

    bail!("API server did not start up successfully; see logs for more details");
}

#[derive(Clone)]
struct PipelineShutdownHandler {
    client: Arc<Client>,
    pipeline_id: Arc<Mutex<Option<String>>>,
}

#[async_trait]
impl ShutdownHandler for PipelineShutdownHandler {
    async fn shutdown(&self) {
        let Some(pipeline_id) = (*self.pipeline_id.lock().unwrap()).clone() else {
            return;
        };

        info!("Stopping pipeline with a final checkpoint...");
        if let Err(e) = self
            .client
            .patch_pipeline()
            .id(&pipeline_id)
            .body(PipelinePatch::builder().stop(StopType::Checkpoint))
            .send()
            .await
        {
            warn!("Unable to stop pipeline with a final checkpoint: {}", e);
        }

        if (timeout(
            Duration::from_secs(120),
            wait_for_state(&self.client, &pipeline_id, &["Stopped", "Failed"]),
        )
        .await)
            .is_err()
        {
            error!(
                "Pipeline did not complete checkpoint within timeout; shutting down immediately"
            );
        }

        // start a final backup and wait for it to finish
        if let Some(c) = notify_db() {
            let _ = c.await;
        }
    }
}

async fn get_pipelines(client: &Client) -> anyhow::Result<Vec<Pipeline>> {
    let mut starting_after = "".to_string();
    let mut result = vec![];
    loop {
        let pipelines = client
            .get_pipelines()
            .starting_after(&starting_after)
            .send()
            .await?
            .into_inner();

        if let Some(next) = pipelines.data.last().map(|p| p.id.to_string()) {
            starting_after = next;
        }

        result.extend(pipelines.data.into_iter());

        if !pipelines.has_more {
            break;
        }
    }

    Ok(result)
}

async fn run_pipeline(
    client: Arc<Client>,
    name: Option<String>,
    query: String,
    parallelism: u32,
    http_port: u16,
    shutdown_handler: PipelineShutdownHandler,
    force: bool,
) -> anyhow::Result<()> {
    // wait until server is available
    wait_for_connect(&client).await.unwrap();

    // validate the pipeline
    let errors = client
        .validate_query()
        .body(ValidateQueryPost::builder().query(&query))
        .send()
        .await?
        .into_inner();

    if !errors.errors.is_empty() {
        eprintln!("There were some issues with the provided query");
        for error in errors.errors {
            eprintln!("  * {error}");
        }
        exit(1);
    }

    // see if our current pipeline is in the existing pipelines
    let id = match get_pipelines(&client)
        .await?
        .into_iter()
        .find(|p| p.query == query)
    {
        Some(p) => {
            info!("Pipeline already exists in database as {}", p.id);
            client
                .patch_pipeline()
                .id(&p.id)
                .body(PipelinePatch::builder().stop(StopType::None))
                .send()
                .await?;
            p.id
        }
        None => {
            // or create it, unless there are other pipelines, which would indicate that this is
            // a DB for another query
            if !client.get_pipelines().send().await?.data.is_empty() {
                let msg =
                    "The specified state is for a different pipeline; this likely means either \
                    the state directory is incorrect or the query is incorrect.";
                if force {
                    warn!("{}", msg);
                    warn!("--force was supplied, so continuing anyways");
                } else {
                    error!("{}", msg);
                    bail!("Exiting... if you would like to continue pass --force");
                }
            }

            client
                .create_pipeline()
                .body(
                    PipelinePost::builder()
                        .name(name.unwrap_or_else(|| "query".to_string()))
                        .parallelism(parallelism)
                        .query(&query),
                )
                .send()
                .await?
                .into_inner()
                .id
        }
    };

    {
        *shutdown_handler.pipeline_id.lock().unwrap() = Some(id.clone());
    }

    wait_for_state(&client, &id, &["Running"]).await?;

    info!("Pipeline running... dashboard at http://localhost:{http_port}/pipelines/{id}");

    Ok(())
}

struct MaybeLocalDb {
    provider: StorageProvider,
    local_path: PathBuf,
    connection: Option<Arc<Mutex<Connection>>>,
}

const REMOTE_STATE_KEY: &str = "state.sqlite";

impl MaybeLocalDb {
    pub async fn from_dir(s: &str) -> Self {
        let provider = StorageProvider::for_url(s)
            .await
            .unwrap_or_else(|e| panic!("Provided state dir '{}' is not valid: {}", s, e));

        let local_path = if !provider.config().is_local() {
            let local_path = temp_dir().join(format!("arroyo-local-{}.sqlite", random::<u32>()));
            let db = provider
                .get_if_present(REMOTE_STATE_KEY)
                .await
                .unwrap_or_else(|e| {
                    panic!(
                        "Failed to fetch database from configured state directory '{}': {}",
                        s, e
                    );
                });

            if let Some(db) = db {
                tokio::fs::write(&local_path, db).await.unwrap_or_else(|e| {
                    panic!(
                        "Unable to write local database copy at '{}': {}",
                        local_path.to_string_lossy(),
                        e
                    )
                });
            }
            local_path
        } else {
            PathBuf::from_str(s).unwrap().join(REMOTE_STATE_KEY)
        };

        Self {
            provider,
            local_path,
            connection: None,
        }
    }

    pub fn init_connection(&mut self) {
        let conn = Connection::open_with_flags(&self.local_path, OpenFlags::SQLITE_OPEN_READ_ONLY)
            .unwrap_or_else(|e| {
                panic!(
                    "Could not open sqlite database at path {:?}: {:?}",
                    self.local_path, e
                )
            });

        self.connection = Some(Arc::new(Mutex::new(conn)));
    }

    pub async fn backup(&self) -> anyhow::Result<()> {
        if self.provider.config().is_local() {
            return Ok(());
        }

        info!("Backing up database to remote storage");
        let start = Instant::now();
        let backup_file = temp_dir().join(format!("arroyo-db-backup-{}.sqlite", random::<u32>()));

        spawn_blocking({
            let connection = self.connection.as_ref().unwrap().clone();
            let backup_file = backup_file.clone();
            move || {
                connection
                    .lock()
                    .unwrap()
                    .backup(DatabaseName::Main, &backup_file, None)
                    .map_err(|e| {
                        anyhow!(
                            "Unable to create backup file in tmp dir ({:?}): {e}",
                            backup_file
                        )
                    })?;
                Ok::<_, anyhow::Error>(())
            }
        })
        .await??;

        // TODO: make this streaming so we don't need to read in the whole database into memory
        self.provider
            .put(
                REMOTE_STATE_KEY,
                tokio::fs::read(&backup_file).await.map_err(|e| {
                    anyhow!(
                        "Unable to read local database backup file '{:?}': {e}",
                        backup_file
                    )
                })?,
            )
            .await
            .map_err(|e| anyhow!("Unable to backup database to state: {e}"))?;

        info!(
            "Finished backing up database; took {:.3} seconds",
            start.elapsed().as_secs_f32()
        );

        // delete the temp backup
        if let Err(e) = tokio::fs::remove_file(&backup_file).await {
            warn!("Unable to remove temporary db backup file: {:?}", e);
        }

        Ok(())
    }
}

fn schedule_db_backups(shutdown: &Shutdown, maybe_local_db: MaybeLocalDb) {
    let backup_guard = shutdown.guard("backup_db");
    tokio::spawn(async move {
        let guard = backup_guard;
        let token = guard.token();
        let mut timer = interval(Duration::from_secs(60));
        timer.set_missed_tick_behavior(MissedTickBehavior::Delay);

        let mut rx = init_db_notifier();

        while !token.is_cancelled() {
            let ch = select! {
                _ = timer.tick() => None,
                Some(ch) = rx.recv() => {
                    Some(ch)
                }
                _ = token.cancelled() => {
                    return;
                }
            };

            if let Err(e) = maybe_local_db.backup().await {
                error!("Failed to back up database: {:?}", e);
            }

            if let Some(ch) = ch {
                // this just means the client dropped the channel and doesn't care to be notified
                let _ = ch.send(());
            }
        }
    });
}

pub async fn run(args: RunArgs) {
    let _guard = arroyo_server_common::init_logging_with_filter(
        "pipeline",
        if env::var("RUST_LOG").is_err() {
            set_var("RUST_LOG", "WARN");
            EnvFilter::builder()
                .with_default_directive(LevelFilter::WARN.into())
                .from_env_lossy()
                .add_directive("arroyo::run=info".parse().unwrap())
        } else {
            EnvFilter::builder()
                .with_default_directive(LevelFilter::INFO.into())
                .from_env_lossy()
        },
    );

    let query = match config().run.query.clone() {
        Some(query) => query,
        None => std::io::read_to_string(args.query).unwrap(),
    };

    let mut shutdown = Shutdown::new("pipeline", SignalBehavior::Handle);

    let state_dir = args
        .state_dir
        .or_else(|| config().run.state_dir.clone())
        .unwrap_or_else(|| {
            format!(
                "{}/pipeline-{}",
                config().checkpoint_url,
                to_millis(SystemTime::now())
            )
        });

    let mut maybe_local_db = MaybeLocalDb::from_dir(&state_dir).await;

    config::update(|c| {
        c.database.r#type = DatabaseType::Sqlite;
        c.database.sqlite.path = maybe_local_db.local_path.clone();

        c.checkpoint_url = state_dir.clone();

        if let Some(port) = c.api.run_http_port {
            c.api.http_port = port;
        } else {
            c.api.http_port = 0;
        }
        c.controller.rpc_port = 0;

        if c.controller.scheduler != Scheduler::Embedded {
            c.controller.scheduler = Scheduler::Process;
        }

        c.pipeline.default_sink = DefaultSink::Stdout;
    });

    let db = db_source().await;
    maybe_local_db.init_connection();

    // check if this is the correct database for this pipeline

    log_event("pipeline_cluster_start", json!({}));

    let controller_port = arroyo_controller::ControllerServer::new(db.clone())
        .await
        .start(shutdown.guard("controller"))
        .await
        .expect("could not start system");

    config::update(|c| c.controller.rpc_port = controller_port);

    let http_port = arroyo_api::start_server(db.clone(), shutdown.guard("api"))
        .await
        .unwrap();

    let client = Arc::new(Client::new_with_client(
        &format!("http://localhost:{http_port}/api",),
        reqwest::ClientBuilder::new()
            .timeout(Duration::from_secs(60))
            .build()
            .unwrap(),
    ));

    // start DB backup task
    schedule_db_backups(&shutdown, maybe_local_db);

    let shutdown_handler = PipelineShutdownHandler {
        client: client.clone(),
        pipeline_id: Arc::new(Mutex::new(None)),
    };

    shutdown.set_handler(Box::new(shutdown_handler.clone()));

    shutdown.spawn_temporary(async move {
        run_pipeline(
            client,
            args.name,
            query,
            args.parallelism,
            http_port,
            shutdown_handler,
            args.force,
        )
        .await
    });

    Shutdown::handle_shutdown(shutdown.wait_for_shutdown(Duration::from_secs(60)).await);
}
