use std::io::ErrorKind;
use std::process::exit;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use std::{
    io,
    path::{Path, PathBuf},
    str::FromStr,
    sync::Arc,
};

use arroyo_rpc::grpc::{
    compiler_grpc_server::{CompilerGrpc, CompilerGrpcServer},
    CompileQueryReq, CompileQueryResp,
};

use object_store::aws::AmazonS3Builder;
use object_store::local::LocalFileSystem;
use object_store::ObjectStore;
use tokio::{process::Command, sync::Mutex};
use tonic::{transport::Server, Request, Response, Status};
use tracing::error;
use tracing::log::info;

pub fn to_millis(time: SystemTime) -> u64 {
    time.duration_since(UNIX_EPOCH).unwrap().as_millis() as u64
}

pub fn from_millis(ts: u64) -> SystemTime {
    UNIX_EPOCH + Duration::from_millis(ts)
}

#[tokio::main]
pub async fn main() {
    let _guard = arroyo_server_common::init_logging("compiler-service");

    let build_dir = std::env::var("BUILD_DIR").expect("BUILD_DIR is not set");

    let last_used = Arc::new(AtomicU64::new(to_millis(SystemTime::now())));

    let s3_bucket = std::env::var("S3_BUCKET").ok();
    let output_dir = std::env::var("OUTPUT_DIR").ok();

    let (object_store, base_path): (Arc<Box<dyn ObjectStore>>, _) = match (s3_bucket, output_dir) {
        (Some(s3_bucket), _) => (
            Arc::new(Box::new(
                AmazonS3Builder::new()
                    .with_bucket_name(&s3_bucket)
                    .with_region("us-east-1")
                    .build()
                    .unwrap(),
            )),
            format!("s3://{}.s3-us-east-1.amazonaws.com", s3_bucket),
        ),
        (None, Some(output_dir)) => (
            Arc::new(Box::new(
                LocalFileSystem::new_with_prefix(PathBuf::from_str(&output_dir).unwrap()).unwrap(),
            )),
            format!("file:///{}", output_dir),
        ),
        _ => {
            panic!("One of S3_BUCKET or OUTPUT_DIR must be set")
        }
    };

    let service = CompileService {
        build_dir: PathBuf::from_str(&build_dir).unwrap(),
        lock: Arc::new(Mutex::new(())),
        last_used: last_used.clone(),
        object_store,
        base_path,
    };

    let addr = "0.0.0.0:9000".parse().unwrap();

    println!("Starting compiler service at 0.0.0.0:9000");

    if let Some(idle_time) = std::env::var("IDLE_SHUTDOWN_MS")
        .map(|t| Duration::from_millis(u64::from_str(&t).unwrap()))
        .ok()
    {
        tokio::spawn(async move {
            loop {
                if from_millis(last_used.load(Ordering::Relaxed))
                    .elapsed()
                    .unwrap()
                    > idle_time
                {
                    println!("Idle time exceeded, shutting down");
                    exit(0);
                }
                tokio::time::sleep(Duration::from_secs(1)).await;
            }
        });
    }

    Server::builder()
        .max_frame_size(Some((1 << 24) - 1)) // 16MB
        .add_service(CompilerGrpcServer::new(service))
        .serve(addr)
        .await
        .unwrap();
}

async fn rustfmt(file: &Path) -> io::Result<()> {
    Command::new("rustfmt").arg(file).output().await?;
    Ok(())
}

pub struct CompileService {
    build_dir: PathBuf,
    lock: Arc<Mutex<()>>,
    last_used: Arc<AtomicU64>,
    object_store: Arc<Box<dyn ObjectStore>>,
    base_path: String,
}

impl CompileService {
    async fn compile(
        &self,
        build_dir: &Path,
        req: CompileQueryReq,
    ) -> io::Result<CompileQueryResp> {
        info!("Starting compilation");
        let start = Instant::now();
        tokio::fs::write(build_dir.join("pipeline/src/main.rs"), &req.pipeline).await?;
        rustfmt(&build_dir.join("pipeline/src/main.rs")).await?;

        tokio::fs::write(build_dir.join("types/src/lib.rs"), &req.types).await?;

        tokio::fs::write(build_dir.join("wasm-fns/src/lib.rs"), &req.wasm_fns).await?;

        let result = Command::new("cargo")
            .current_dir(&build_dir)
            .arg("build")
            .arg("--release")
            .output()
            .await?;

        if !result.status.success() {
            return Err(io::Error::new(
                ErrorKind::InvalidData,
                format!(
                    "Failed to compile job: {}",
                    String::from_utf8_lossy(&result.stderr)
                ),
            ));
        }

        if !req.wasm_fns.is_empty() {
            let result = Command::new("wasm-pack")
                .arg("build")
                .current_dir(&build_dir.join("wasm-fns"))
                .output()
                .await
                .unwrap();

            if !result.status.success() {
                return Err(io::Error::new(
                    ErrorKind::InvalidData,
                    format!(
                        "Failed to compile wasm: {}",
                        String::from_utf8_lossy(&result.stderr)
                    ),
                ));
            }
        }

        info!(
            "Finished compilation after {:.2}s",
            start.elapsed().as_secs_f32()
        );

        // TODO: replace this with the SHA of the worker code once that's available
        let id = (to_millis(SystemTime::now()) / 1000).to_string();

        let base: object_store::path::Path = format!("{}/{}", &req.job_id, id).try_into().unwrap();

        {
            let pipeline = tokio::fs::read(&build_dir.join("target/release/pipeline")).await?;
            self.object_store
                .put(&base.child("pipeline"), pipeline.into())
                .await?;
        }

        {
            let wasm_fns =
                tokio::fs::read(&build_dir.join("wasm-fns/pkg/wasm_fns_bg.wasm")).await?;
            self.object_store
                .put(&base.child("wasm_fns_bg.wasm"), wasm_fns.into())
                .await?;
        }

        let full_path = format!("{}/{}", self.base_path, base);

        Ok(CompileQueryResp {
            pipeline_path: format!("{}/pipeline", full_path),
            wasm_fns_path: format!("{}/wasm_fns_bg.wasm", full_path),
        })
    }
}

#[tonic::async_trait]
impl CompilerGrpc for CompileService {
    async fn compile_query(
        &self,
        request: Request<CompileQueryReq>,
    ) -> Result<Response<CompileQueryResp>, Status> {
        self.last_used
            .store(to_millis(SystemTime::now()), Ordering::Relaxed);

        // only allow one request to be active at a given time
        let _guard = self.lock.lock().await;

        let req = request.into_inner();

        self.compile(&self.build_dir, req)
            .await
            .map(|r| Response::new(r))
            .map_err(|e| {
                error!("Failed to compile: {:?}", e);
                match e.kind() {
                    ErrorKind::InvalidData => Status::unimplemented(e.to_string()),
                    _ => Status::internal(e.to_string()),
                }
            })
    }
}
