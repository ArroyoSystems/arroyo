use anyhow::{anyhow, bail};
use base64::prelude::BASE64_STANDARD_NO_PAD;
use base64::Engine;
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::net::SocketAddr;
use std::process::Stdio;
use std::str::from_utf8;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use std::{env, path::PathBuf, str::FromStr, sync::Arc};

use arroyo_rpc::grpc::{
    compiler_grpc_server::{CompilerGrpc, CompilerGrpcServer},
    BuildUdfReq, BuildUdfResp, GetUdfPathReq, GetUdfPathResp, UdfCrate,
};
use arroyo_rpc::var_str::VarStr;

use arroyo_server_common::wrap_start;
use arroyo_storage::StorageProvider;
use arroyo_types::{
    bool_config, grpc_port, ports, ARTIFACT_URL_DEFAULT, ARTIFACT_URL_ENV, INSTALL_CLANG_ENV,
    INSTALL_RUSTC_ENV,
};
use dlopen2::utils::PLATFORM_FILE_EXTENSION;
use serde_json::Value;
use tokio::time::timeout;
use tokio::{process::Command, sync::Mutex};
use toml::{toml, Table};
use tonic::{Request, Response, Status};
use tracing::{error, info};

const RUSTUP: &str = "curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y";

pub fn to_millis(time: SystemTime) -> u64 {
    time.duration_since(UNIX_EPOCH).unwrap().as_millis() as u64
}

pub fn from_millis(ts: u64) -> SystemTime {
    UNIX_EPOCH + Duration::from_millis(ts)
}

pub async fn start_service() -> anyhow::Result<()> {
    let service = CompileService::new().await?;
    let grpc = grpc_port("compiler", ports::COMPILER_GRPC);

    let addr: SocketAddr = format!("0.0.0.0:{}", grpc).parse().unwrap();

    info!("Starting compiler service at {}", addr);

    wrap_start(
        "compiler service",
        addr.clone(),
        arroyo_server_common::grpc_server()
            .add_service(CompilerGrpcServer::new(service))
            .serve(addr),
    )
    .await
}

pub struct CompileService {
    build_dir: PathBuf,
    lock: Arc<Mutex<()>>,
    storage: StorageProvider,
    cargo_path: Arc<Mutex<String>>,
}

async fn binary_present(bin: &str) -> bool {
    Command::new("which")
        .arg(bin)
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .await
        .map(|s| s.success())
        .unwrap_or(false)
}

impl CompileService {
    pub async fn new() -> anyhow::Result<Self> {
        let build_dir = env::var("BUILD_DIR").unwrap_or("/tmp/arroyo/udf_build".to_string());

        let artifact_url = env::var(ARTIFACT_URL_ENV).unwrap_or(ARTIFACT_URL_DEFAULT.to_string());

        let storage = StorageProvider::for_url(&artifact_url)
            .await
            .map_err(|e| anyhow!("unable to construct storage provider: {}", e))?;

        Ok(CompileService {
            build_dir: PathBuf::from_str(&build_dir).unwrap(),
            lock: Arc::new(Mutex::new(())),
            storage,
            cargo_path: Arc::new(Mutex::new("cargo".to_string())),
        })
    }

    async fn write_udf_crate(&self, udf_crate: UdfCrate) -> anyhow::Result<()> {
        tokio::fs::create_dir_all(&self.build_dir.join("src")).await?;
        tokio::fs::write(self.build_dir.join("src/lib.rs"), &udf_crate.definition).await?;

        let mut cargo_toml = toml! {
            [package]
            name = "udf"
            version = "1.0.0"
            edition = "2021"

            [lib]
            crate-type = ["cdylib"]
        };

        let dependencies: Table = VarStr::new(udf_crate.dependencies)
            .sub_env_vars()
            .and_then(|deps| {
                toml::from_str(&deps).map_err(|e| anyhow!("Invalid dependency in RPC: {:?}", e))
            })?;

        cargo_toml.insert("dependencies".to_string(), toml::Value::Table(dependencies));

        tokio::fs::write(self.build_dir.join("Cargo.toml"), &cargo_toml.to_string()).await?;

        tokio::fs::create_dir_all(&self.build_dir.join("src")).await?;
        tokio::fs::write(self.build_dir.join("src/lib.rs"), &udf_crate.definition).await?;
        Ok(())
    }

    async fn check_cargo(&self) -> anyhow::Result<()> {
        if binary_present(&self.cargo_path.lock().await).await {
            return Ok(());
        }

        if !bool_config(INSTALL_RUSTC_ENV, false) {
            error!(
                "Rustc is not installed, and compiler server was not configured to automatically \
            install it. To compile UDFs, either manually install rustc or re-run the cluster with\
            {}=true",
                INSTALL_RUSTC_ENV
            );
            bail!("Rustc is not installed and compiler service is not configured to automatically install it; \
            cannot compile UDFs");
        }

        info!("Rustc is not installed, but is required to compile UDFs. Attempting to install.");

        let output = timeout(
            Duration::from_secs(2 * 60),
            Command::new("/bin/sh")
                .arg("-c")
                .arg(RUSTUP)
                .stdout(Stdio::inherit())
                .stderr(Stdio::inherit())
                .output(),
        )
        .await
        .map_err(|e| anyhow!("Timed out while installing rust for UDF compilation after {e}"))?
        .map_err(|e| anyhow!("Failed to install Rust: {e}"))?;

        if output.status.success() {
            info!("Rustc successfully installed...");
            *self.cargo_path.lock().await = format!(
                "{}/.cargo/bin/cargo",
                std::env::var("HOME")
                    .map_err(|_| anyhow!("Unable to determine cargo installation directory"))?
            );
            Ok(())
        } else {
            error!(
                "Failed to install rustc, will not be able to compile UDFs\
            \n------------------------------\
            \nInstall script stderr: {}",
                String::from_utf8_lossy(&output.stderr)
            );
            bail!("UDFs are unavailable because Rust compiler is not installed and was not able to be installed. See controller logs for details.")
        }
    }

    async fn run_command(action: &str, command: &mut Command) -> anyhow::Result<()> {
        let output = timeout(
            Duration::from_secs(2 * 60),
            command
                .stdout(Stdio::inherit())
                .stderr(Stdio::inherit())
                .output(),
        )
        .await
        .map_err(|e| anyhow!("Timed out while {action} for UDF compilation after {e}"))?
        .map_err(|e| anyhow!("Failed while {action} via apt-get: {e}"))?;

        if !output.status.success() {
            error!(
                "Failed to install clang, will not be able to compile UDFs\
            \n------------------------------\
            \napt-get stderr: {}",
                String::from_utf8_lossy(&output.stderr)
            );

            bail!(
                "UDFs are unavailable because a C compiler is not installed and was not able to \
            be installed. See controller logs for details."
            );
        }

        Ok(())
    }

    async fn check_cc(&self) -> anyhow::Result<()> {
        if binary_present("cc").await {
            return Ok(());
        }

        if !bool_config(INSTALL_CLANG_ENV, false) {
            let error = "UDF compilation requires clang or gcc to be available. Ensure you have a \
            working C compilation environment.";
            error!("{}", error);
            bail!("{}", error);
        }

        info!(
            "cc is not available, but required for UDF compilation. Attempting to install clang."
        );

        Self::run_command("updating apt", Command::new("apt-get").arg("update")).await?;

        Self::run_command(
            "installing clang",
            Command::new("apt-get")
                .arg("-y")
                .arg("install")
                .arg("clang"),
        )
        .await?;

        Ok(())
    }
}

fn dylib_path(name: &str, definition: &str) -> String {
    let mut hasher = DefaultHasher::new();
    definition.hash(&mut hasher);
    let hash = BASE64_STANDARD_NO_PAD.encode(hasher.finish().to_le_bytes());

    format!("udfs/{}_{}.{}", name, hash, PLATFORM_FILE_EXTENSION)
}

#[tonic::async_trait]
impl CompilerGrpc for CompileService {
    async fn build_udf(
        &self,
        request: Request<BuildUdfReq>,
    ) -> Result<Response<BuildUdfResp>, Status> {
        // only allow one request to be active at a given time
        let _guard = self.lock.lock().await;

        self.check_cc()
            .await
            .map_err(|e| Status::failed_precondition(e.to_string()))?;
        self.check_cargo()
            .await
            .map_err(|e| Status::failed_precondition(e.to_string()))?;

        let req = request.into_inner();
        let udf_crate = req
            .udf_crate
            .ok_or_else(|| Status::failed_precondition("missing udf_crate field"))?;

        let path = dylib_path(&udf_crate.name, &udf_crate.definition);
        let canonical_url = self.storage.canonical_url_for(&path);

        // exit early if udf is already compiled
        if self.storage.exists(path.as_str()).await.is_ok_and(|x| x) {
            info!("UDF {} already compiled, skipping", udf_crate.name);
            return Ok(Response::new(BuildUdfResp {
                errors: vec![],
                udf_path: Some(canonical_url),
            }));
        }

        let start = Instant::now();

        let name = udf_crate.name.clone();
        self.write_udf_crate(udf_crate)
            .await
            .map_err(|e| Status::internal(format!("Writing UDFs failed: {}", e)))?;

        let cargo_command = if req.save { "build" } else { "check" };

        info!("{}ing udf", cargo_command);
        let output = Command::new(&*self.cargo_path.lock().await)
            .current_dir(&self.build_dir)
            .arg(cargo_command)
            .arg("--release")
            .arg("--message-format=json")
            .output()
            .await
            .map_err(|e| {
                Status::internal(format!(
                    "Failed to run cargo, will not be able to compile UDFs: {e}"
                ))
            })?;

        info!(
            "Finished running cargo {} on udfs crate {} after {:.2}s, exit code: {:?}",
            cargo_command,
            &name,
            start.elapsed().as_secs_f32(),
            output.status.code()
        );

        if output.status.success() {
            let udf_path = if req.save {
                // save dylib to storage
                let dylib = tokio::fs::read(
                    &self
                        .build_dir
                        .join("target/release/")
                        .join(format!("libudf.{}", PLATFORM_FILE_EXTENSION)),
                )
                .await?;

                self.storage.put(&path, dylib).await.map_err(|e| {
                    Status::internal(format!(
                        "Failed to write UDF library to artifact storage: {}",
                        e
                    ))
                })?;

                info!("Wrote UDF dylib to {}", canonical_url);
                Some(canonical_url)
            } else {
                None
            };

            return Ok(Response::new(BuildUdfResp {
                errors: vec![],
                udf_path,
            }));
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
            let line_json: serde_json::Result<Value> = serde_json::from_str(line);
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

        return Ok(Response::new(BuildUdfResp {
            errors,
            udf_path: None,
        }));
    }

    async fn get_udf_path(
        &self,
        request: Request<GetUdfPathReq>,
    ) -> Result<Response<GetUdfPathResp>, Status> {
        let req = request.into_inner();

        let path = dylib_path(&req.name, &req.definition);
        let canonical_url = self.storage.canonical_url_for(&path);

        let exists =
            self.storage.exists(path.as_str()).await.map_err(|e| {
                Status::internal(format!("Failed to read from storage system: {}", e))
            })?;

        Ok(Response::new(GetUdfPathResp {
            udf_path: exists.then_some(canonical_url),
        }))
    }
}
