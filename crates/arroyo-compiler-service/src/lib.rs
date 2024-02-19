use std::str::from_utf8;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use std::{path::PathBuf, str::FromStr, sync::Arc};

use arroyo_rpc::grpc::{
    compiler_grpc_server::{CompilerGrpc, CompilerGrpcServer},
    BuildUdfCompilerReq, BuildUdfCompilerResp, UdfCrate,
};

use arroyo_storage::StorageProvider;
use arroyo_types::{dylib_name, grpc_port, ports};
use serde_json::Value;
use tokio::{process::Command, sync::Mutex};
use tonic::{Request, Response, Status};
use tracing::info;

pub fn to_millis(time: SystemTime) -> u64 {
    time.duration_since(UNIX_EPOCH).unwrap().as_millis() as u64
}

pub fn from_millis(ts: u64) -> SystemTime {
    UNIX_EPOCH + Duration::from_millis(ts)
}

pub async fn start_service() {
    let service = CompileService::new().await;
    let grpc = grpc_port("compiler", ports::COMPILER_GRPC);

    let addr = format!("0.0.0.0:{}", grpc).parse().unwrap();

    info!("Starting compiler service at {}", addr);

    arroyo_server_common::grpc_server()
        .add_service(CompilerGrpcServer::new(service))
        .serve(addr)
        .await
        .expect("Compiler service failed");
}

pub struct CompileService {
    build_dir: PathBuf,
    lock: Arc<Mutex<()>>,
    storage: StorageProvider,
}

impl CompileService {
    pub async fn new() -> Self {
        let build_dir = std::env::var("BUILD_DIR").unwrap_or("build_dir".to_string());

        let storage = StorageProvider::for_url("/")
            .await
            .expect("unable to construct storage provider");

        CompileService {
            build_dir: PathBuf::from_str(&build_dir).unwrap(),
            lock: Arc::new(Mutex::new(())),
            storage,
        }
    }

    async fn write_udf_crate(&self, udf_crate: UdfCrate) -> anyhow::Result<()> {
        let udf_build_dir = self.build_dir.join("udfs_dir/udf");

        tokio::fs::create_dir_all(&udf_build_dir.join("src")).await?;
        tokio::fs::write(udf_build_dir.join("src/lib.rs"), &udf_crate.definition).await?;
        tokio::fs::write(udf_build_dir.join("Cargo.toml"), &udf_crate.cargo_toml).await?;

        let udf_wrapper_dir = self.build_dir.join("udfs_dir").join("udf_wrapper");
        tokio::fs::create_dir_all(&udf_wrapper_dir.join("src")).await?;
        tokio::fs::write(udf_wrapper_dir.join("src/lib.rs"), &udf_crate.lib_rs).await?;

        let udf_wrapper_cargo_toml = format!(
            r#"
[package]
name = "udf_wrapper"
version = "0.1.0"
edition = "2021"


[dependencies]
udf = {{ path = "../udf" }}
arrow = {{ version = "50.0.0", features = ["ffi"] }}


[lib]
crate-type = ["cdylib", "rlib"]
"#,
        );
        tokio::fs::write(udf_wrapper_dir.join("Cargo.toml"), udf_wrapper_cargo_toml).await?;

        Ok(())
    }
}

#[tonic::async_trait]
impl CompilerGrpc for CompileService {
    async fn build_udf(
        &self,
        request: Request<BuildUdfCompilerReq>,
    ) -> Result<Response<BuildUdfCompilerResp>, Status> {
        // only allow one request to be active at a given time
        let _guard = self.lock.lock().await;

        let req = request.into_inner();

        // exit early if udf is already compiled
        if self
            .storage
            .exists(req.dylib_path.clone())
            .await
            .is_ok_and(|x| x)
        {
            info!("UDF already compiled, skipping");
            return Ok(Response::new(BuildUdfCompilerResp { errors: vec![] }));
        }

        info!("Checking UDFs");
        let start = Instant::now();

        let Some(udf_crate) = req.udf_crate else {
            return Err(Status::internal("No UDF crate provided"));
        };

        let name = udf_crate.name.clone();
        self.write_udf_crate(udf_crate)
            .await
            .map_err(|e| Status::internal(format!("Writing UDFs failed: {}", e)))?;

        let udf_build_dir = self.build_dir.join("udfs_dir").join("udf_wrapper");
        let cargo_command = if req.save { "build" } else { "check" };

        let output = Command::new("cargo")
            .current_dir(&udf_build_dir)
            .arg(cargo_command)
            .arg("--release")
            .arg("--message-format=json")
            .output()
            .await?;

        info!(
            "Finished running cargo check on udfs crate {} after {:.2}s, exit code: {:?}",
            &name,
            start.elapsed().as_secs_f32(),
            output.status.code()
        );

        if output.status.success() {
            if req.save {
                // save dylib to storage
                let dylib = tokio::fs::read(
                    &udf_build_dir
                        .join("target/release/")
                        .join(dylib_name("libudf_wrapper")),
                )
                .await?;

                self.storage
                    .put(req.dylib_path.clone(), dylib)
                    .await
                    .map_err(|e| Status::internal(format!("Failed to save UDF library: {}", e)))?;

                info!("Saved UDF dylib to {}", req.dylib_path);
            }
            return Ok(Response::new(BuildUdfCompilerResp { errors: vec![] }));
        }

        let stdout = from_utf8(&output.stdout)
            .map_err(|_| Status::internal("Failed to parse cargo output"))?;
        let stderr = from_utf8(&output.stderr)
            .map_err(|_| Status::internal("Failed to parse cargo output"))?;

        let mut lines: Vec<&str> = stdout.lines().collect();
        lines.extend(stderr.lines());

        // parse output.stdout as json
        let mut errors = vec![];
        for line in lines {
            let line_json: serde_json::Result<Value> = serde_json::from_str(&line.to_string());
            if let Ok(line_json) = line_json {
                if line_json["reason"] == "compiler-message"
                    && line_json["message"]["level"] == "error"
                {
                    errors.push(
                        line_json["message"]["rendered"]
                            .to_string()
                            .trim_matches(|c| c == '"')
                            .to_string(),
                    );
                }
            } else {
                errors.push(line.to_string());
            }
        }

        info!("Cargo check on udfs crate found {} errors", errors.len());

        return Ok(Response::new(BuildUdfCompilerResp { errors }));
    }
}
