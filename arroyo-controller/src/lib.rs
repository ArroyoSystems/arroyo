#![allow(clippy::new_without_default)]
// TODO: factor out complex types
#![allow(clippy::type_complexity)]

use anyhow::bail;
use arroyo_rpc::grpc::compiler_grpc_client::CompilerGrpcClient;
use arroyo_rpc::grpc::controller_grpc_server::{ControllerGrpc, ControllerGrpcServer};
use arroyo_rpc::grpc::{
    CheckUdfsCompilerReq, CheckUdfsReq, CheckUdfsResp, GrpcOutputSubscription, HeartbeatNodeReq,
    HeartbeatNodeResp, HeartbeatReq, HeartbeatResp, OutputData, RegisterNodeReq, RegisterNodeResp,
    RegisterWorkerReq, RegisterWorkerResp, TaskCheckpointCompletedReq, TaskCheckpointCompletedResp,
    TaskFailedReq, TaskFailedResp, TaskFinishedReq, TaskFinishedResp, TaskStartedReq,
    TaskStartedResp, WorkerFinishedReq, WorkerFinishedResp,
};
use arroyo_rpc::grpc::{
    SinkDataReq, SinkDataResp, TaskCheckpointEventReq, TaskCheckpointEventResp, UdfCrate,
    WorkerErrorReq, WorkerErrorRes,
};
use arroyo_rpc::public_ids::{generate_id, IdTypes};
use arroyo_server_common::log_event;
use arroyo_types::{
    from_micros, ports, DatabaseConfig, NodeId, WorkerId, REMOTE_COMPILER_ENDPOINT_ENV,
};
use deadpool_postgres::{ManagerConfig, Pool, RecyclingMethod};
use lazy_static::lazy_static;
use prometheus::{register_gauge, Gauge};
use regex::Regex;
use serde_json::json;
use states::{Created, State, StateMachine};
use std::collections::{HashMap, HashSet};
use std::env;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime};
use syn::{parse_file, Item};
use time::OffsetDateTime;
use tokio::sync::broadcast;
use tokio::sync::mpsc::error::TrySendError;
use tokio::sync::mpsc::Sender;
use tokio_postgres::NoTls;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status};
use tracing::{debug, info, warn};
use uuid::Uuid;

pub mod compiler;
pub mod job_controller;
pub mod schedulers;
mod states;

include!(concat!(env!("OUT_DIR"), "/controller-sql.rs"));

use crate::schedulers::{nomad::NomadScheduler, NodeScheduler, ProcessScheduler, Scheduler};
use types::public::LogLevel;
use types::public::{RestartMode, StopMode};

pub const CHECKPOINTS_TO_KEEP: u32 = 5;

lazy_static! {
    static ref ACTIVE_PIPELINES: Gauge = register_gauge!(
        "arroyo_controller_active_pipelines",
        "number of active pipelines in arroyo-controller"
    )
    .unwrap();
    pub static ref COMPACTION_TUPLES_IN: Gauge = register_gauge!(
        "arroyo_controller_compaction_tuples_in",
        "Number of tuples being considered for compaction"
    )
    .unwrap();
    static ref COMPACTION_TUPLES_OUT: Gauge = register_gauge!(
        "arroyo_controller_compaction_tuples_in",
        "Number of tuples being considered for compaction"
    )
    .unwrap();
}

#[derive(Eq, PartialEq, Clone, Debug)]
pub struct JobConfig {
    id: String,
    organization_id: String,
    pipeline_name: String,
    pipeline_id: i64,
    stop_mode: StopMode,
    checkpoint_interval: Duration,
    ttl: Option<Duration>,
    parallelism_overrides: HashMap<String, usize>,
    restart_nonce: i32,
    restart_mode: RestartMode,
}

#[derive(Clone, Debug)]
pub struct JobStatus {
    id: String,
    run_id: i64,
    state: String,
    start_time: Option<OffsetDateTime>,
    finish_time: Option<OffsetDateTime>,
    tasks: Option<i32>,
    failure_message: Option<String>,
    restarts: i32,
    pipeline_path: Option<String>,
    wasm_path: Option<String>,
    restart_nonce: i32,
}

impl JobStatus {
    pub async fn update_db(&self, pool: &Pool) -> Result<(), String> {
        let c = pool.get().await.map_err(|e| format!("{:?}", e))?;
        let res = queries::controller_queries::update_job_status()
            .bind(
                &c,
                &self.state,
                &self.start_time,
                &self.finish_time,
                &self.tasks,
                &self.failure_message,
                &self.restarts,
                &self.pipeline_path,
                &self.wasm_path,
                &self.run_id,
                &self.restart_nonce,
                &self.id,
            )
            .await
            .map_err(|e| format!("{:?}", e))?;

        if res == 0 {
            Err("Job status does not exist".to_string())
        } else {
            Ok(())
        }
    }
}

#[derive(Debug)]
pub enum RunningMessage {
    TaskCheckpointEvent(TaskCheckpointEventReq),
    TaskCheckpointFinished(TaskCheckpointCompletedReq),
    TaskFinished {
        worker_id: WorkerId,
        time: SystemTime,
        operator_id: String,
        subtask_index: u32,
    },
    TaskFailed {
        worker_id: WorkerId,
        operator_id: String,
        subtask_index: u32,
        reason: String,
    },
    WorkerHeartbeat {
        worker_id: WorkerId,
        time: Instant,
    },
    WorkerFinished {
        worker_id: WorkerId,
    },
}

#[derive(Debug)]
pub enum JobMessage {
    ConfigUpdate(JobConfig),
    WorkerConnect {
        worker_id: WorkerId,
        node_id: NodeId,
        rpc_address: String,
        data_address: String,
        slots: usize,
        job_hash: String,
    },
    TaskStarted {
        worker_id: WorkerId,
        operator_id: String,
        operator_subtask: u64,
    },
    RunningMessage(RunningMessage),
}

#[derive(Clone)]
pub struct ControllerServer {
    job_state: Arc<tokio::sync::Mutex<HashMap<String, StateMachine>>>,
    data_txs: Arc<tokio::sync::Mutex<HashMap<String, Vec<Sender<Result<OutputData, Status>>>>>>,
    scheduler: Arc<dyn Scheduler>,
    db: Pool,
}

#[tonic::async_trait]
impl ControllerGrpc for ControllerServer {
    async fn register_worker(
        &self,
        request: Request<RegisterWorkerReq>,
    ) -> Result<Response<RegisterWorkerResp>, Status> {
        info!(
            "Worker registered: {:?} -- {:?}",
            request.get_ref(),
            request.remote_addr()
        );

        let req = request.into_inner();

        self.send_to_job_queue(
            &req.job_id,
            JobMessage::WorkerConnect {
                worker_id: WorkerId(req.worker_id),
                node_id: NodeId(req.node_id),
                rpc_address: req.rpc_address,
                data_address: req.data_address,
                slots: req.slots as usize,
                job_hash: req.job_hash,
            },
        )
        .await?;

        Ok(Response::new(RegisterWorkerResp {}))
    }

    async fn heartbeat(
        &self,
        request: Request<HeartbeatReq>,
    ) -> Result<Response<HeartbeatResp>, Status> {
        let req = request.into_inner();

        self.send_to_job_queue(
            &req.job_id,
            JobMessage::RunningMessage(RunningMessage::WorkerHeartbeat {
                worker_id: WorkerId(req.worker_id),
                time: Instant::now(),
            }),
        )
        .await?;

        return Ok(Response::new(HeartbeatResp {}));
    }

    async fn task_started(
        &self,
        request: Request<TaskStartedReq>,
    ) -> Result<Response<TaskStartedResp>, Status> {
        let req = request.into_inner();
        info!("task started: {:?}", req);

        self.send_to_job_queue(
            &req.job_id,
            JobMessage::TaskStarted {
                worker_id: WorkerId(req.worker_id),
                operator_id: req.operator_id,
                operator_subtask: req.operator_subtask,
            },
        )
        .await?;

        Ok(Response::new(TaskStartedResp {}))
    }

    async fn task_checkpoint_event(
        &self,
        request: Request<TaskCheckpointEventReq>,
    ) -> Result<Response<TaskCheckpointEventResp>, Status> {
        let req = request.into_inner();

        debug!("received task checkpoint event {:?}", req);
        let job_id = req.job_id.clone();
        self.send_to_job_queue(
            &job_id,
            JobMessage::RunningMessage(RunningMessage::TaskCheckpointEvent(req)),
        )
        .await?;

        Ok(Response::new(TaskCheckpointEventResp {}))
    }

    async fn task_checkpoint_completed(
        &self,
        request: Request<TaskCheckpointCompletedReq>,
    ) -> Result<Response<TaskCheckpointCompletedResp>, Status> {
        let req = request.into_inner();

        debug!("received task checkpoint completed {:?}", req);
        let job_id = req.job_id.clone();

        self.send_to_job_queue(
            &job_id,
            JobMessage::RunningMessage(RunningMessage::TaskCheckpointFinished(req)),
        )
        .await?;

        Ok(Response::new(TaskCheckpointCompletedResp {}))
    }

    async fn task_finished(
        &self,
        request: Request<TaskFinishedReq>,
    ) -> Result<Response<TaskFinishedResp>, Status> {
        let req = request.into_inner();

        self.send_to_job_queue(
            &req.job_id,
            JobMessage::RunningMessage(RunningMessage::TaskFinished {
                worker_id: WorkerId(req.worker_id),
                time: from_micros(req.time),
                operator_id: req.operator_id,
                subtask_index: req.operator_subtask as u32,
            }),
        )
        .await?;

        Ok(Response::new(TaskFinishedResp {}))
    }

    async fn task_failed(
        &self,
        request: Request<TaskFailedReq>,
    ) -> Result<Response<TaskFailedResp>, Status> {
        let req = request.into_inner();

        self.send_to_job_queue(
            &req.job_id,
            JobMessage::RunningMessage(RunningMessage::TaskFailed {
                worker_id: WorkerId(req.worker_id),
                operator_id: req.operator_id,
                subtask_index: req.operator_subtask as u32,
                reason: req.error,
            }),
        )
        .await?;

        Ok(Response::new(TaskFailedResp {}))
    }

    async fn register_node(
        &self,
        request: Request<RegisterNodeReq>,
    ) -> Result<Response<RegisterNodeResp>, Status> {
        let req = request.into_inner();
        info!(
            "Received node registration from {} at {} with {} slots",
            req.node_id, req.addr, req.task_slots
        );

        self.scheduler.register_node(req).await;

        Ok(Response::new(RegisterNodeResp {}))
    }

    async fn heartbeat_node(
        &self,
        request: Request<HeartbeatNodeReq>,
    ) -> Result<Response<HeartbeatNodeResp>, Status> {
        self.scheduler.heartbeat_node(request.into_inner()).await?;
        Ok(Response::new(HeartbeatNodeResp {}))
    }

    async fn worker_finished(
        &self,
        request: Request<WorkerFinishedReq>,
    ) -> Result<Response<WorkerFinishedResp>, Status> {
        self.scheduler.worker_finished(request.into_inner()).await;
        Ok(Response::new(WorkerFinishedResp {}))
    }

    async fn send_sink_data(
        &self,
        request: Request<SinkDataReq>,
    ) -> Result<Response<SinkDataResp>, Status> {
        let req = request.into_inner();
        let mut data_txs = self.data_txs.lock().await;
        if let Some(v) = data_txs.get_mut(&req.job_id) {
            let output = OutputData {
                operator_id: req.operator_id,
                timestamp: req.timestamp,
                key: req.key,
                value: req.value,
                done: req.done,
            };

            let mut remove = HashSet::new();
            for (i, tx) in v.iter().enumerate() {
                match tx.try_send(Ok(output.clone())) {
                    Ok(_) => {}
                    Err(TrySendError::Closed(_)) => {
                        remove.insert(i);
                    }
                    Err(TrySendError::Full(_)) => {
                        warn!("queue full");
                    }
                }
            }

            let mut i = 0;
            v.retain(|_tx| {
                i += 1;
                !remove.contains(&(i - 1))
            });
        }
        Ok(Response::new(SinkDataResp::default()))
    }

    type SubscribeToOutputStream = ReceiverStream<Result<OutputData, Status>>;

    async fn subscribe_to_output(
        &self,
        request: Request<GrpcOutputSubscription>,
    ) -> Result<Response<Self::SubscribeToOutputStream>, Status> {
        let (tx, rx) = tokio::sync::mpsc::channel(32);

        let mut data_txs = self.data_txs.lock().await;
        data_txs
            .entry(request.into_inner().job_id)
            .or_default()
            .push(tx);

        Ok(Response::new(ReceiverStream::new(rx)))
    }

    async fn worker_error(
        &self,
        request: Request<WorkerErrorReq>,
    ) -> Result<Response<WorkerErrorRes>, Status> {
        info!("Got worker error.");
        let req = request.into_inner();
        let client = self.db.get().await.unwrap();
        match queries::controller_queries::create_job_log_message()
            .bind(
                &client,
                &generate_id(IdTypes::JobLogMessage),
                &req.job_id,
                &req.operator_id,
                &(req.task_index as i64),
                &LogLevel::error,
                &req.message,
                &req.details,
            )
            .one()
            .await
        {
            Ok(_) => Ok(Response::new(WorkerErrorRes {})),
            Err(err) => Err(Status::from_error(Box::new(err))),
        }
    }

    async fn check_udfs(
        &self,
        request: Request<CheckUdfsReq>,
    ) -> Result<Response<CheckUdfsResp>, Status> {
        let endpoint = env::var(REMOTE_COMPILER_ENDPOINT_ENV)
            .map_err(|_| Status::unavailable("Remote compiler is required for checking UDFs"))?;

        let mut client = CompilerGrpcClient::connect(endpoint).await.map_err(|e| {
            Status::unavailable(format!("Failed to connect to compiler service: {}", e))
        })?;

        let req = request.into_inner();
        let definition = req.definition.clone();

        let dependencies = match parse_dependencies(&definition) {
            Ok(dependencies) => dependencies,
            Err(e) => {
                return Ok(udf_error_resp(e));
            }
        };

        let mut function_name = None;
        {
            let result = match parse_file(definition.as_str()) {
                Ok(result) => result,
                Err(e) => {
                    return Ok(udf_error_resp(e));
                }
            };

            for item in result.items {
                match item {
                    Item::Fn(f) => {
                        if function_name.is_some() {
                            return Ok(Response::new(CheckUdfsResp {
                                errors: vec!["Only one function is allowed in UDFs".to_string()],
                                udf_name: None,
                            }));
                        }
                        function_name = Some(f.sig.ident.to_string());
                    }
                    Item::Use(_) => {}
                    _ => {
                        return Ok(Response::new(CheckUdfsResp {
                            errors: vec![
                                "Only functions and use statements are allowed in UDFs".to_string()
                            ],
                            udf_name: None,
                        }))
                    }
                }
            }
        }

        // unwrap function or return error
        let Some(function_name) = function_name else {
            return Ok(Response::new(CheckUdfsResp {
                errors: vec!["No function found in UDF".to_string()],
                udf_name: None,
            }));
        };

        // build cargo.toml
        let cargo_toml = cargo_toml(&function_name, &dependencies);

        // send to compiler
        let compiler_res = client
            .check_udfs(CheckUdfsCompilerReq {
                udf_crate: Some(UdfCrate {
                    name: function_name.clone(),
                    definition: req.definition,
                    cargo_toml,
                }),
            })
            .await?
            .into_inner();

        Ok(Response::new(CheckUdfsResp {
            errors: compiler_res.errors,
            udf_name: Some(function_name),
        }))
    }
}

impl ControllerServer {
    pub async fn new() -> Self {
        let scheduler: Arc<dyn Scheduler> = match std::env::var("SCHEDULER").ok().as_deref() {
            Some("node") => {
                info!("Using node scheduler");
                Arc::new(NodeScheduler::new())
            }
            Some("nomad") => {
                info!("Using nomad scheduler");
                Arc::new(NomadScheduler::new())
            }
            Some("kubernetes") | Some("k8s") => {
                #[cfg(feature = "k8s")]
                {
                    info!("Using kubernetes scheduler");
                    Arc::new(crate::schedulers::kubernetes::KubernetesScheduler::from_env().await)
                }
                #[cfg(not(feature = "k8s"))]
                panic!("Kubernetes not enabled -- compile with `--features k8s` to enable")
            }
            _ => {
                info!("Using process scheduler");
                Arc::new(ProcessScheduler::new())
            }
        };

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
            .unwrap();

        // test that the DB connection is valid
        let _ = pool.get().await.unwrap_or_else(|e| {
            panic!(
                "Failed to connect to database {} at {}@{}:{} {:?}",
                cfg.dbname.unwrap(),
                cfg.user.unwrap(),
                cfg.host.unwrap(),
                cfg.port.unwrap(),
                e
            );
        });

        match pool
            .get()
            .await
            .expect("Failed to connect to database")
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

        log_event(
            "service_startup",
            json!({
                "service": "controller",
                "scheduler": std::env::var("SCHEDULER").unwrap_or_else(|_| "process".to_string())
            }),
        );

        Self {
            scheduler,
            data_txs: Arc::new(tokio::sync::Mutex::new(HashMap::new())),
            job_state: Arc::new(tokio::sync::Mutex::new(HashMap::new())),
            db: pool,
        }
    }

    async fn send_to_job_queue(&self, job_id: &str, msg: JobMessage) -> Result<(), Status> {
        let mut jobs = self.job_state.lock().await;

        if let Some(sm) = jobs.get_mut(job_id) {
            if let Err(e) = sm.send(msg).await {
                Err(Status::failed_precondition(format!(
                    "Cannot handle message for {}: {}",
                    job_id, e
                )))
            } else {
                Ok(())
            }
        } else {
            warn!(message = "Received message for unknown job id", job_id);
            Err(Status::failed_precondition(format!(
                "No job with id {}",
                job_id
            )))
        }
    }

    fn start_updater(&self) {
        let db = self.db.clone();
        let jobs = Arc::clone(&self.job_state);
        let scheduler = Arc::clone(&self.scheduler);

        tokio::spawn(async move {
            loop {
                let client = db.get().await.unwrap();
                let res = queries::controller_queries::all_jobs()
                    .bind(&client)
                    .all()
                    .await
                    .unwrap();
                for p in res {
                    let config = JobConfig {
                        id: p.id.clone(),
                        organization_id: p.org_id,
                        pipeline_name: p.pipeline_name,
                        pipeline_id: p.pipeline_id,
                        stop_mode: p.stop,
                        checkpoint_interval: Duration::from_micros(
                            p.checkpoint_interval_micros as u64,
                        ),
                        ttl: p.ttl_micros.map(|t| Duration::from_micros(t as u64)),
                        parallelism_overrides: p
                            .parallelism_overrides
                            .as_object()
                            .unwrap()
                            .into_iter()
                            .map(|(k, v)| (k.clone(), v.as_u64().unwrap() as usize))
                            .collect(),
                        restart_nonce: p.config_restart_nonce,
                        restart_mode: p.restart_mode,
                    };

                    let mut jobs = jobs.lock().await;

                    let status = JobStatus {
                        id: p.id,
                        run_id: p.run_id.unwrap_or(0),
                        state: p.state.unwrap_or_else(|| Created {}.name().to_string()),
                        start_time: p.start_time,
                        finish_time: p.finish_time,
                        tasks: p.tasks,
                        failure_message: p.failure_message,
                        restarts: p.restarts,
                        pipeline_path: p.pipeline_path,
                        wasm_path: p.wasm_path,
                        restart_nonce: p.status_restart_nonce,
                    };

                    if let Some(sm) = jobs.get_mut(&config.id) {
                        sm.update(config, status).await;
                    } else {
                        jobs.insert(
                            config.id.clone(),
                            StateMachine::new(config, status, db.clone(), scheduler.clone()).await,
                        );
                    }
                }

                tokio::time::sleep(Duration::from_millis(500)).await;
            }
        });
    }

    pub async fn start(self, addr: SocketAddr) -> Result<(), Box<dyn std::error::Error>> {
        let reflection = tonic_reflection::server::Builder::configure()
            .register_encoded_file_descriptor_set(arroyo_rpc::grpc::API_FILE_DESCRIPTOR_SET)
            .build()?;

        info!("Starting arroyo-controller on {}", addr);

        let (shutdown_tx, shutdown_rx) = broadcast::channel(16);

        arroyo_server_common::start_admin_server(
            "controller",
            ports::CONTROLLER_ADMIN,
            shutdown_rx,
        );

        self.start_updater();

        arroyo_server_common::grpc_server()
            .accept_http1(true)
            .add_service(ControllerGrpcServer::new(self.clone()))
            .add_service(reflection)
            .serve(addr)
            .await?;

        shutdown_tx.send(0).unwrap();
        Ok(())
    }
}

fn cargo_toml(name: &str, dependencies: &str) -> String {
    format!(
        r#"
[package]
name = "{}"
version = "1.0.0"
edition = "2021"

{}
        "#,
        name, dependencies
    )
}

fn udf_error_resp<E>(e: E) -> Response<CheckUdfsResp>
where
    E: core::fmt::Display,
{
    Response::new(CheckUdfsResp {
        errors: vec![e.to_string()],
        udf_name: None,
    })
}

fn parse_dependencies(definition: &str) -> anyhow::Result<String> {
    // get content of dependencies comment using regex
    let re = Regex::new(r"\/\*\n(\[dependencies\]\n[\s\S]*?)\*\/").unwrap();
    if re.find_iter(&definition).count() > 1 {
        bail!("Only one dependencies definition is allowed in a UDF");
    }

    return if let Some(captures) = re.captures(&definition) {
        if captures.len() != 2 {
            bail!("Error parsing dependencies");
        }
        Ok(captures.get(1).unwrap().as_str().to_string())
    } else {
        Ok("[dependencies]\n# none defined\n".to_string())
    };
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_dependencies_valid() {
        let definition = r#"
/*
[dependencies]
serde = "1.0"
*/

pub fn my_udf() -> i64 {
    1
}
        "#;

        assert_eq!(
            parse_dependencies(definition).unwrap(),
            r#"[dependencies]
serde = "1.0"
"#
        );
    }

    #[test]
    fn test_parse_dependencies_none() {
        let definition = r#"
pub fn my_udf() -> i64 {
    1
}
        "#;

        assert_eq!(
            parse_dependencies(definition).unwrap(),
            r#"[dependencies]
# none defined
"#
        );
    }

    #[test]
    fn test_parse_dependencies_multiple() {
        let definition = r#"
/*
[dependencies]
serde = "1.0"
*/

/*
[dependencies]
serde = "1.0"
*/

pub fn my_udf() -> i64 {
    1

        "#;
        assert!(parse_dependencies(definition).is_err());
    }
}
