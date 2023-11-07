use crate::states::fatal;
use anyhow::{anyhow, Result};
use arroyo_datastream::{parse_type, Operator, Program, WasmBehavior};
use arroyo_rpc::grpc::compiler_grpc_client::CompilerGrpcClient;
use arroyo_rpc::grpc::CompileQueryReq;
use arroyo_types::REMOTE_COMPILER_ENDPOINT_ENV;
use petgraph::Direction;
use proc_macro2::TokenStream;
use quote::{format_ident, quote};
use std::io::ErrorKind;
use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::{fs, io};
use syn::{parse_quote, parse_str};
use tokio::process::Command;
use tonic::{Code, Request};
use tracing::info;

const OUTPUT_PATH: &str = "/tmp/arroyo_binaries";

#[derive(Debug, Clone)]
pub struct CompiledProgram {
    pub pipeline_path: String,
    pub wasm_path: String,
}

pub struct ProgramCompiler {
    name: String,
    job_id: String,
    program: Program,
}

impl ProgramCompiler {
    pub fn new(name: impl Into<String>, job_id: impl Into<String>, program: Program) -> Self {
        Self {
            name: name.into(),
            job_id: job_id.into(),
            program,
        }
    }
    fn get_source_dir() -> String {
        std::env::var("SOURCE_DIR")
            .ok()
            .unwrap_or_else(|| env!("CARGO_MANIFEST_DIR").to_string())
    }

    pub async fn compile(&self) -> Result<CompiledProgram> {
        if let Ok(endpoint) = std::env::var(REMOTE_COMPILER_ENDPOINT_ENV) {
            info!("Compiling remotely on {}", endpoint);
            self.compile_remote(endpoint).await
        } else {
            info!("Compiling locally");
            self.compile_local().await
        }
    }

    pub async fn compile_remote(&self, endpoint: String) -> Result<CompiledProgram> {
        let req = CompileQueryReq {
            job_id: self.job_id.clone(),
            types: self.compile_types().to_string(),
            pipeline: self.compile_pipeline_main(&self.name, &self.program.get_hash()),
            udfs: self.compile_udf_lib(),
            wasm_fns: self.compile_wasm_lib().to_string(),
        };

        let mut client = CompilerGrpcClient::connect(endpoint)
            .await
            .map_err(|e| io::Error::new(ErrorKind::Other, format!("{}", e)))?;

        let req = Request::new(req);
        let resp = client
            .compile_query(req)
            .await
            .map_err(|e| match e.code() {
                Code::Unimplemented => fatal(
                    "Compilation failed for this query. See the controller logs for more details.",
                    anyhow!("compilation request failed: {}", e.message()),
                )
                .into(),
                _ => {
                    anyhow!("compilation request failed: {:?}", e.message())
                }
            })?;

        let resp = resp.into_inner();

        Ok(CompiledProgram {
            pipeline_path: resp.pipeline_path,
            wasm_path: resp.wasm_fns_path,
        })
    }

    pub async fn compile_local(&self) -> Result<CompiledProgram> {
        let bin_dir = PathBuf::from_str(OUTPUT_PATH)
            .unwrap()
            .join(self.program.get_hash());

        tokio::fs::create_dir_all(&bin_dir)
            .await
            .unwrap_or_else(|e| panic!("Failed to create output bin dir {:?}: {:?}", &bin_dir, e));

        let api_dir = PathBuf::from(Self::get_source_dir());
        let arroyo_dir = api_dir.parent().unwrap();

        let mut dir = std::env::temp_dir();
        let program = format!("program_{}", self.name);
        dir.push(&program);

        fs::create_dir_all(&dir)?;

        let types = self.compile_types().to_string();
        let main = self.compile_pipeline_main(&self.name, &self.program.get_hash());
        let wasm = self.compile_wasm_lib().to_string();
        let udfs = self.compile_udf_lib();

        let workspace_toml = r#"
[workspace]
members = [
  "pipeline",
  "types",
]

exclude = [
  "wasm-fns",
]
[workspace.dependencies]
arrow = { version = "46.0.0" }
arrow-buffer = { version = "46.0.0" }
arrow-array = { version = "46.0.0" }
arrow-schema = { version = "46.0.0" }
parquet = { version = "46.0.0" }

[patch.crates-io]
parquet = {git = 'https://github.com/ArroyoSystems/arrow-rs', branch = '46.0.0/parquet_bytes'}
arrow = {git = 'https://github.com/ArroyoSystems/arrow-rs', branch = '46.0.0/parquet_bytes'}
arrow-buffer = {git = 'https://github.com/ArroyoSystems/arrow-rs', branch = '46.0.0/parquet_bytes'}
arrow-array = {git = 'https://github.com/ArroyoSystems/arrow-rs', branch = '46.0.0/parquet_bytes'}
arrow-schema = {git = 'https://github.com/ArroyoSystems/arrow-rs', branch = '46.0.0/parquet_bytes'}
object_store = {git = 'https://github.com/ArroyoSystems/arrow-rs', branch = 'object_store/put_part_api'}
deltalake = {git = "https://github.com/ArroyoSystems/delta-rs", branch = "dynamo_pin_0_16"}
"#;

        // NOTE: These must be kept in sync with the Cargo configs in docker/build_base and build_dir/Cargo.toml
        let workspace_cargo = dir.join("Cargo.toml");
        fs::write(workspace_cargo, workspace_toml)?;

        let types_toml = format!(
            r#"
[package]
name = "types"
version = "1.0.0"
edition = "2021"

[dependencies]
bincode = "=2.0.0-rc.3"
bincode_derive = "=2.0.0-rc.3"
arroyo-types = {{ path = "{}/arroyo-types" }}
"#,
            arroyo_dir.to_string_lossy()
        );
        Self::create_subproject(&dir, "types", &types_toml, "lib.rs", types).await?;

        let pipeline_toml = format!(
            r#"
[package]
name = "pipeline"
version = "1.0.0"
edition = "2021"

[dependencies]
types = {{ path = "../types" }}
udfs = {{ path = "../udfs" }}
petgraph = "0.6"
chrono = "0.4"
bincode = "=2.0.0-rc.3"
bincode_derive = "=2.0.0-rc.3"
serde = "1.0"
serde_json = "1.0"
arrow = {{ workspace = true }}
parquet = {{ workspace = true }}
arrow-array = {{ workspace = true }}
arrow-schema = {{ workspace = true }}
arroyo-types = {{ path = "{}/arroyo-types" }}
arroyo-worker = {{ path = "{}/arroyo-worker"{}}}
"#,
            arroyo_dir.to_string_lossy(),
            arroyo_dir.to_string_lossy(),
            if cfg!(feature = "kafka-sasl") {
                ", features = [\"kafka-sasl\"]"
            } else {
                ""
            }
        );
        Self::create_subproject(&dir, "pipeline", &pipeline_toml, "main.rs", main).await?;

        let udfs_toml = r#"
[package]
name = "udfs"
version = "1.0.0"
edition = "2021"

[dependencies]
chrono = "0.4"
serde = "1.0"
serde_json = "1.0"
regex = "1"
        "#;

        Self::create_subproject(&dir, "udfs", &udfs_toml, "lib.rs", udfs).await?;

        let wasmfns_toml = format!(
            r#"
[package]
name = "wasm-fns"
version = "1.0.0"
edition = "2021"

[lib]
crate-type = ["cdylib"]

[dependencies]
types = {{ path = "../types" }}

bincode = "=2.0.0-rc.3"
bincode_derive = "=2.0.0-rc.3"
arroyo-types = {{ path = "{}/arroyo-types" }}
wasm-bindgen = "0.2"
serde_json = "1.0"
[package.metadata.wasm-pack.profile.release]
wasm-opt = false
"#,
            arroyo_dir.to_string_lossy()
        );
        Self::create_subproject(&dir, "wasm-fns", &wasmfns_toml, "lib.rs", wasm).await?;

        let result = Command::new("cargo")
            .current_dir(&dir)
            .env("RUSTFLAGS", "-C target-cpu=native")
            .arg("build")
            .arg("--release")
            .output()
            .await
            .map_err(|e| {
                anyhow!(
                    "Failed to run `cargo`; is rust and cargo installed? {:?}",
                    e
                )
            })?;

        if !result.status.success() {
            return Err(fatal("Compilation failed for this query. We have been notified and are looking into the problem.",
                  anyhow!("Compilation Failed: {}", String::from_utf8_lossy(&result.stderr))).into());
        }

        let result = Command::new("wasm-pack")
            .arg("build")
            .current_dir(&dir.join("wasm-fns"))
            .output()
            .await
            .map_err(|e| anyhow!(
                "Failed to run `wasm-pack`; you may need to run `$cargo install wasm-pack`: {:?}", e))?;

        if !result.status.success() {
            return Err(fatal("Compilation failed for this query. We have been notified and are looking into the problem.",
                             anyhow!("Wasm Compilation Failed: {}", String::from_utf8_lossy(&result.stderr))).into());
        }

        let pipeline = dir.join("target/release/pipeline");
        tokio::fs::copy(pipeline, &bin_dir.join("pipeline")).await?;

        let wasm = dir.join("wasm-fns/pkg/wasm_fns_bg.wasm");
        tokio::fs::copy(wasm, &bin_dir.join("wasm_fns_bg.wasm")).await?;

        Ok(CompiledProgram {
            pipeline_path: format!("file://{}/pipeline", bin_dir.to_string_lossy()),
            wasm_path: format!("file://{}/wasm_fns_bg.wasm", bin_dir.to_string_lossy()),
        })
    }

    async fn create_subproject(
        base: &Path,
        name: &str,
        cargo: &str,
        src_file: &str,
        body: String,
    ) -> io::Result<()> {
        let dir = base.join(name);
        fs::create_dir_all(&dir)?;

        let cargo_path = dir.join("Cargo.toml");
        fs::write(cargo_path, cargo)?;

        let src = dir.join("src");
        fs::create_dir_all(&src)?;
        let src_file_path = src.join(src_file);
        fs::write(src_file_path, body)?;
        Ok(())
    }

    fn compile_types(&self) -> TokenStream {
        let structs: Vec<_> = self
            .program
            .types
            .iter()
            .map(|t| {
                let def: TokenStream = parse_str(t).unwrap();
                quote! {
                    #[derive(Clone, Debug, bincode::Encode, bincode::Decode, PartialEq,  PartialOrd)]
                    #def
                }
            })
            .collect();

        quote! {
            use std::time::SystemTime;

            #(#structs )*
        }
    }

    fn compile_pipeline_main(&self, name: &str, hash: &str) -> String {
        let imports = quote! {
            #![allow(warnings)]
            use petgraph::graph::DiGraph;
            use arroyo_worker::operators::*;
            use arroyo_worker::connectors;
            use arroyo_worker::operators::sinks::*;
            use arroyo_worker::operators::sinks;
            use arroyo_worker::operators::joins::*;
            use arroyo_worker::operators::windows::*;
            use arroyo_worker::engine::{Program, SubtaskNode};
            use arroyo_worker::{LogicalEdge, LogicalNode};
            use types::*;
            use chrono;
            use std::time::SystemTime;
            use std::str::FromStr;
            use serde::{Deserialize, Serialize};
        };
        info!("{}", self.program.dot());

        let other_defs: Vec<TokenStream> = self
            .program
            .other_defs
            .iter()
            .map(|t| parse_str(t).unwrap())
            .collect();

        let make_graph_function = self.program.make_graph_function();

        prettyplease::unparse(&parse_quote! {
            #imports

            #make_graph_function

            pub fn main() {
                let graph = make_graph();

                arroyo_worker::WorkerServer::new(#name, #hash, graph).start().unwrap();
            }

            #(#other_defs )*
        })
    }

    fn compile_udf_lib(&self) -> String {
        let udfs: Vec<TokenStream> = self
            .program
            .udfs
            .iter()
            .map(|t| parse_str(t).unwrap())
            .collect();

        prettyplease::unparse(&parse_quote! {
            use std::time::{SystemTime, Duration};
            use std::str::FromStr;

            #(#udfs )*
        })
    }

    fn compile_wasm_lib(&self) -> TokenStream {
        let wasm_fns: Vec<_> = self
            .program
            .graph
            .node_indices()
            .filter_map(|idx| {
                let node = self.program.graph.node_weight(idx).unwrap();
                if let Operator::FusedWasmUDFs { name, udfs } = &node.operator {
                    Some((idx, (name, udfs)))
                } else {
                    None
                }
            })
            .map(|(idx, (name, udfs))| {
                let in_edge = self
                    .program
                    .graph
                    .edges_directed(idx, Direction::Incoming)
                    .next()
                    .expect("wasm nodes must have an input");
                let _out_edge = self
                    .program
                    .graph
                    .edges_directed(idx, Direction::Outgoing)
                    .next()
                    .expect("wasm nodes must have an output");

                let in_k = parse_type(&in_edge.weight().key);
                let in_t = parse_type(&in_edge.weight().value);

                let mut defs = vec![];
                let mut fused_body = vec![quote! {
                    let record_0: arroyo_types::Record<#in_k, #in_t> = get_record(len);
                }];

                for (i, udf) in udfs.iter().enumerate() {
                    let body: TokenStream = parse_str(&udf.def.body).unwrap();
                    let name = format_ident!("{}", udf.def.name);

                    let cur_record = format_ident!("record_{}", i);
                    let next_record = format_ident!("record_{}", i + 1);

                    let call = quote!{ #name(&#cur_record.key, &#cur_record.value) };

                    fused_body.push(match udf.behavior {
                        WasmBehavior::Map => {
                            quote! {
                                let #next_record = arroyo_types::Record {
                                    timestamp: #cur_record.timestamp,
                                    key: #cur_record.key,
                                    value: #call,
                                };
                            }
                        }
                        WasmBehavior::OptMap => {
                            quote! {
                                let v = #call;
                                if v.is_none() {
                                    return;
                                }

                                let #next_record = arroyo_types::Record {
                                    timestamp: #cur_record.timestamp,
                                    key: #cur_record.key,
                                    value: v.unwrap(),
                                };
                            }
                        }
                        WasmBehavior::Filter => {
                            quote! {
                                if !#call {
                                    return;
                                }

                                let #next_record = #cur_record;
                            }
                        }
                        WasmBehavior::Timestamp => {
                            quote! {
                                let #next_record = arroyo_types::Record {
                                    timestamp: #call,
                                    key: #cur_record.key,
                                    value: #cur_record.value,
                                };
                            }
                        }
                        WasmBehavior::KeyBy => {
                            quote! {
                                let #next_record = arroyo_types::Record {
                                    timestamp: #cur_record.timestamp,
                                    key: Some(#call),
                                    value: #cur_record.value,
                                };
                            }
                        }
                    });

                    let key_arg = format_ident!("{}", udf.def.key_arg);
                    let key_arg_type = parse_type(&udf.def.key_arg_type);
                    let value_arg = format_ident!("{}", udf.def.value_arg);
                    let value_arg_type = parse_type(&udf.def.value_arg_type);

                    let ret_type = parse_type(&udf.def.ret_type);

                    defs.push(quote! {
                        fn #name(#key_arg: &Option<#key_arg_type>, #value_arg: &#value_arg_type) -> #ret_type {
                            #body
                        }
                    });
                }

                let process_name = format_ident!("{}_process", name);
                let last_record = format_ident!("record_{}", udfs.len());
                quote! {
                    #(#defs
                    )*

                    #[no_mangle]
                    pub extern "C" fn #process_name(len: u32) {
                        #(#fused_body
                        )*

                        send_record(#last_record);
                    }
                }
            })
            .collect();

        parse_quote! {
            use types::*;
            use std::time::SystemTime;

            extern "C" {
                pub fn send(s: *const u8, length: u32) -> i32;
                pub fn fetch_data(s: *const u8) -> i32;
            }

            fn get_record<K: arroyo_types::Key, T: arroyo_types::Data>(len: u32) -> arroyo_types::Record<K, T> {
                let mut data = vec![0u8; len as usize];
                unsafe { fetch_data(data.as_mut_ptr()) };
                let (record, _): (arroyo_types::Record<K, T>, usize) =
                    bincode::decode_from_slice(&data[..], bincode::config::standard()).unwrap();
                record
            }

            fn send_record<K: arroyo_types::Key, T: arroyo_types::Data>(record: arroyo_types::Record<K, T>) {
                let encoded = bincode::encode_to_vec(record, bincode::config::standard()).unwrap();
                unsafe { send(encoded.as_ptr(), encoded.len() as u32) };
            }

            #(#wasm_fns )*
        }
    }
}
