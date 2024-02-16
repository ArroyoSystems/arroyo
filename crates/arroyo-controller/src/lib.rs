#![allow(clippy::new_without_default)]
// TODO: factor out complex types
#![allow(clippy::type_complexity)]

use anyhow::{bail, Result};
use arrow_schema::DataType;
use arroyo_df::{parse_dependencies, ArroyoSchemaProvider};
use arroyo_rpc::grpc::compiler_grpc_client::CompilerGrpcClient;
use arroyo_rpc::grpc::controller_grpc_server::{ControllerGrpc, ControllerGrpcServer};
use arroyo_rpc::grpc::{
    BuildUdfCompilerReq, BuildUdfReq, BuildUdfResp, GrpcOutputSubscription, HeartbeatNodeReq,
    HeartbeatNodeResp, HeartbeatReq, HeartbeatResp, OutputData, RegisterNodeReq, RegisterNodeResp,
    RegisterWorkerReq, RegisterWorkerResp, TaskCheckpointCompletedReq, TaskCheckpointCompletedResp,
    TaskFailedReq, TaskFailedResp, TaskFinishedReq, TaskFinishedResp, TaskStartedReq,
    TaskStartedResp, UdfCrate, WorkerFinishedReq, WorkerFinishedResp,
};
use arroyo_rpc::grpc::{
    SinkDataReq, SinkDataResp, TaskCheckpointEventReq, TaskCheckpointEventResp, WorkerErrorReq,
    WorkerErrorRes,
};
use arroyo_rpc::public_ids::{generate_id, IdTypes};
use arroyo_server_common::shutdown::ShutdownGuard;
use arroyo_types::{
    dylib_name, from_micros, grpc_port, ports, service_port, NodeId, WorkerId, COMPILER_ADDR_ENV,
    COMPILER_PORT_ENV,
};
use deadpool_postgres::Pool;
use lazy_static::lazy_static;
use proc_macro2::TokenStream;
use prometheus::{register_gauge, Gauge};
use quote::{format_ident, quote};
use states::{Created, State, StateMachine};
use std::collections::{HashMap, HashSet};
use std::env;
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime};
use syn::parse_quote;
use time::OffsetDateTime;
use tokio::sync::mpsc::error::TrySendError;
use tokio::sync::mpsc::Sender;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status};
use tracing::{debug, info, warn};

//pub mod compiler;
pub mod job_controller;
pub mod schedulers;
mod states;

include!(concat!(env!("OUT_DIR"), "/controller-sql.rs"));

use crate::schedulers::{NodeScheduler, ProcessScheduler, Scheduler};
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
    compiler_addr: String,
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
        //info!("received {:#?}", req);
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

    async fn build_udf(
        &self,
        request: Request<BuildUdfReq>,
    ) -> Result<Response<BuildUdfResp>, Status> {
        let mut client = CompilerGrpcClient::connect(self.compiler_addr.clone())
            .await
            .map_err(|e| {
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

        // use the ArroyoSchemaProvider to do some validation and to get the function name
        let function_name = match ArroyoSchemaProvider::new().add_rust_udf(&definition) {
            Ok(function_name) => function_name,
            Err(e) => return Ok(udf_error_resp(e)),
        };

        // build cargo.toml
        let cargo_toml = cargo_toml(&dependencies);

        // send to compiler
        let compiler_res = client
            .build_udf(BuildUdfCompilerReq {
                udf_crate: Some(UdfCrate {
                    name: function_name.clone(),
                    definition: req.definition,
                    cargo_toml,
                    lib_rs: match udf_lib_rs(&function_name, &definition) {
                        Ok(lib_rs) => lib_rs,
                        Err(e) => return Ok(udf_error_resp(e)),
                    },
                }),
                dylib_path: format!("udfs/{}", dylib_name(&function_name)),
            })
            .await?
            .into_inner();

        Ok(Response::new(BuildUdfResp {
            errors: compiler_res.errors,
            udf_name: Some(function_name),
        }))
    }
}

impl ControllerServer {
    pub async fn new(pool: Pool) -> Self {
        let scheduler: Arc<dyn Scheduler> = match std::env::var("SCHEDULER").ok().as_deref() {
            Some("node") => {
                info!("Using node scheduler");
                Arc::new(NodeScheduler::new())
            }
            Some("kubernetes") | Some("k8s") => {
                info!("Using kubernetes scheduler");
                Arc::new(schedulers::kubernetes::KubernetesScheduler::from_env().await)
            }
            Some("embedded") => {
                info!("Using embedded scheduler");
                Arc::new(schedulers::embedded::EmbeddedScheduler::new())
            }
            _ => {
                info!("Using process scheduler");
                Arc::new(ProcessScheduler::new())
            }
        };

        let compiler_addr = env::var(COMPILER_ADDR_ENV).unwrap_or_else(|_| {
            format!(
                "http://localhost:{}",
                service_port("compiler", ports::COMPILER_GRPC, COMPILER_PORT_ENV)
            )
        });

        Self {
            scheduler,
            data_txs: Arc::new(tokio::sync::Mutex::new(HashMap::new())),
            job_state: Arc::new(tokio::sync::Mutex::new(HashMap::new())),
            db: pool,
            compiler_addr: compiler_addr.to_string(),
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

    fn start_updater(&self, guard: ShutdownGuard) {
        let db = self.db.clone();
        let jobs = Arc::clone(&self.job_state);
        let scheduler = Arc::clone(&self.scheduler);

        let token = guard.token();

        let our_guard = guard.child("update-thread");
        our_guard.into_spawn_task(async move {
            while !token.is_cancelled() {
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
                        sm.update(config, status, &guard).await;
                    } else {
                        jobs.insert(
                            config.id.clone(),
                            StateMachine::new(
                                config,
                                status,
                                db.clone(),
                                scheduler.clone(),
                                guard.clone_temporary(),
                            )
                            .await,
                        );
                    }
                }

                tokio::time::sleep(Duration::from_millis(500)).await;
            }
        });
    }

    pub fn start(self, guard: ShutdownGuard) {
        let reflection = tonic_reflection::server::Builder::configure()
            .register_encoded_file_descriptor_set(arroyo_rpc::grpc::API_FILE_DESCRIPTOR_SET)
            .build()
            .unwrap();

        let addr = format!(
            "0.0.0.0:{}",
            grpc_port("controller", ports::CONTROLLER_GRPC)
        )
        .parse()
        .expect("Invalid port");

        info!("Starting arroyo-controller on {}", addr);

        self.start_updater(guard.child("updater"));
        guard.into_spawn_task(
            arroyo_server_common::grpc_server()
                .accept_http1(true)
                .add_service(ControllerGrpcServer::new(self.clone()))
                .add_service(reflection)
                .serve(addr),
        );
    }
}

fn cargo_toml(dependencies: &str) -> String {
    format!(
        r#"
[package]
name = "udf"
version = "1.0.0"
edition = "2021"

{}
        "#,
        dependencies
    )
}

fn udf_lib_rs(udf_name: &str, definition: &str) -> Result<String> {
    let mut schema_provider = ArroyoSchemaProvider::new();
    schema_provider.add_rust_udf(definition).unwrap();
    let udf_def = schema_provider.udf_defs.get(udf_name).unwrap().clone();
    let udf_name = format_ident!("{}", udf_name);

    let results_builder = if matches!(udf_def.ret.data_type, DataType::Utf8) {
        quote!(let mut results_builder = array::StringBuilder::with_capacity(args[0].len(), args[0].len() * 64);)
    } else {
        let return_type = data_type_to_arrow_type_token(udf_def.ret.data_type.clone())?;
        quote!(let mut results_builder = array::PrimitiveBuilder::<datatypes::#return_type>::with_capacity(args[0].len());)
    };

    let mut arrow_types = vec![];
    for arg in &udf_def.args {
        let arrow_type = data_type_to_arrow_type_token(arg.data_type.clone())?;
        arrow_types.push(arrow_type);
    }

    let (defs, args): (Vec<_>, Vec<_>) = udf_def
        .args
        .iter().zip(arrow_types.iter())
        .enumerate()
        .map(|(i, (arg_type, arrow_type))| {
            let id = format_ident!("arg_{}", i);
            let def = if matches!(arg_type.data_type, DataType::Utf8) {
                quote!(let #id = array::StringArray::from(args[#i].clone());)
            } else {
                quote!(let #id = array::PrimitiveArray::<datatypes::#arrow_type>::from(args[#i].clone());)
            };

            (def, quote!(#id))
        })
        .unzip();

    let unwrapping: Vec<_> = udf_def
        .args
        .iter()
        .enumerate()
        .map(|(i, arg_type)| {
            let id = format_ident!("arg_{}", i);

            let append_none = if matches!(udf_def.ret.data_type, DataType::Utf8) {
                quote!(results_builder.append_option(None::<String>);)
            } else {
                quote!(results_builder.append_option(None);)
            };

            if arg_type.nullable {
                quote!()
            } else {
                parse_quote! {
                    let Some(#id) = #id else {
                        #append_none
                        continue;
                    };
                }
            }
        })
        .collect();

    let to_string: Vec<_> = udf_def
        .args
        .iter()
        .enumerate()
        .map(|(i, arg_type)| {
            let id = format_ident!("arg_{}", i);
            if matches!(arg_type.data_type, DataType::Utf8) {
                quote!(let #id = #id.to_string();)
            } else {
                quote!()
            }
        })
        .collect();

    let mut arg_zip = quote!(arg_0.iter());
    for i in 1..args.len() {
        let next_arg = format_ident!("arg_{}", i);
        arg_zip = quote!(#arg_zip.zip(#next_arg.iter()));
    }

    let call = if udf_def.ret.nullable {
        quote!(results_builder.append_option(udf::#udf_name(#(#args),*));)
    } else {
        quote!(results_builder.append_option(Some(udf::#udf_name(#(#args),*)));)
    };

    Ok(prettyplease::unparse(&parse_quote! {
        use arrow::array;
        use arrow::array::Array;
        use arrow::datatypes;
        use arrow::ffi::{FFI_ArrowArray, FFI_ArrowSchema, from_ffi, to_ffi};
        use udf;

        #[repr(C)]
        pub struct FfiArraySchemaPair(FFI_ArrowArray, FFI_ArrowSchema);

        #[no_mangle]
        pub extern "C" fn run(args_ptr: *mut FfiArraySchemaPair, args_len: usize, args_capacity: usize) -> FfiArraySchemaPair {

            let args = unsafe {
                Vec::from_raw_parts(args_ptr, args_len, args_capacity)
            };

            let args = args
                .into_iter()
                .map(|pair| {
                    let FfiArraySchemaPair(array, schema) = pair;
                    unsafe { from_ffi(array, &schema).unwrap() }
                })
                .collect::<Vec<_>>();

            #results_builder

            #(#defs;)*

            for (#(#args),*) in #arg_zip {
                #(#unwrapping;)*
                #(#to_string;)*

                #call
            }

            let (array, schema) = to_ffi(&results_builder.finish().to_data()).unwrap();
            FfiArraySchemaPair(array, schema)
        }
    }))
}

fn udf_error_resp<E>(e: E) -> Response<BuildUdfResp>
where
    E: core::fmt::Display,
{
    Response::new(BuildUdfResp {
        errors: vec![e.to_string()],
        udf_name: None,
    })
}

fn data_type_to_arrow_type_token(data_type: DataType) -> Result<TokenStream> {
    let t = match data_type {
        DataType::Utf8 => quote!(GenericStringType<i32>),
        DataType::Boolean => quote!(BooleanType),
        DataType::Int16 => quote!(Int16Type),
        DataType::Int32 => quote!(Int32Type),
        DataType::Int64 => quote!(Int64Type),
        DataType::Int8 => quote!(Int8Type),
        DataType::UInt8 => quote!(UInt8Type),
        DataType::UInt16 => quote!(UInt16Type),
        DataType::UInt32 => quote!(UInt32Type),
        DataType::UInt64 => quote!(UInt64Type),
        DataType::Float32 => quote!(Float32Type),
        DataType::Float64 => quote!(Float64Type),
        _ => bail!("Unsupported data type: {:?}", data_type),
    };
    Ok(t)
}
