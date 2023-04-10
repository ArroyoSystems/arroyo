use crate::states::fatal;
use anyhow::{anyhow, Result};
use arroyo_datastream::{
    AggregateBehavior, EdgeType, Operator, Program, SlidingAggregatingTopN,
    SlidingWindowAggregator, TumblingTopN, TumblingWindowAggregator, WasmBehavior, WatermarkType,
    WindowType,
};
use arroyo_rpc::grpc::compiler_grpc_client::CompilerGrpcClient;
use arroyo_rpc::grpc::CompileQueryReq;
use arroyo_types::{to_micros, REMOTE_COMPILER_ENDPOINT_ENV};
use petgraph::Direction;
use proc_macro2::TokenStream;
use quote::{format_ident, quote};
use std::io::ErrorKind;
use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::{fs, io};
use syn::{parse_str, GenericArgument, PathArguments, Type, TypePath};
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

fn parse_type(s: &str) -> Type {
    let s = s
        .replace("arroyo_bench::", "")
        .replace("pipeline::", "")
        .replace("alloc::string::", "")
        .replace("alloc::vec::", "");
    parse_str(&s).expect(&s)
}

fn extract_container_type(name: &str, t: &Type) -> Option<Type> {
    if let Type::Path(TypePath { path, .. }) = t {
        let last = path.segments.last()?;
        if last.ident == format_ident!("{}", name) {
            if let PathArguments::AngleBracketed(args) = &last.arguments {
                if let GenericArgument::Type(t) = args.args.first()? {
                    return Some(t.clone());
                }
            }
        }
    }
    None
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
            self.compile_remote(endpoint).await
        } else {
            self.compile_local().await
        }
    }

    pub async fn compile_remote(&self, endpoint: String) -> Result<CompiledProgram> {
        let req = CompileQueryReq {
            job_id: self.job_id.clone(),
            types: self.compile_types().to_string(),
            pipeline: self
                .compile_pipeline_main(&self.name, &self.program.get_hash())
                .to_string(),
            wasm_fns: self.compile_wasm_lib().to_string(),
        };

        let mut client = CompilerGrpcClient::connect(endpoint)
            .await
            .map_err(|e| io::Error::new(ErrorKind::Other, format!("{}", e)))?;

        let req = Request::new(req);
        let resp = client
            .compile_query(req).await
            .map_err(|e| match e.code() {
                Code::Unimplemented => {
                    fatal("Compilation failed for this query. We have been notified and are looking into the problem.",
                          anyhow!("compilation request failed: {}", e.message())).into()
                }
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
        let main = self
            .compile_pipeline_main(&self.name, &self.program.get_hash())
            .to_string();
        let wasm = self.compile_wasm_lib().to_string();

        let workspace_toml = r#"
[workspace]
members = [
  "pipeline",
  "types",
]

exclude = [
  "wasm-fns",
]
"#;

        // NOTE: These must be kept in sync with the Cargo configs in ops/query-compiler/build-base
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
petgraph = "0.6"
bincode = "=2.0.0-rc.3"
bincode_derive = "=2.0.0-rc.3"
serde = "1.0"
serde_json = "1.0"
arroyo-types = {{ path = "{}/arroyo-types" }}
arroyo-worker = {{ path = "{}/arroyo-worker" }}
"#,
            arroyo_dir.to_string_lossy(),
            arroyo_dir.to_string_lossy()
        );
        Self::create_subproject(&dir, "pipeline", &pipeline_toml, "main.rs", main).await?;

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
wee_alloc = "0.4.5"
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
            .await?;

        if !result.status.success() {
            return Err(fatal("Compilation failed for this query. We have been notified and are looking into the problem.",
                  anyhow!("Compilation Failed: {}", String::from_utf8_lossy(&result.stderr))).into());
        }

        let result = Command::new("wasm-pack")
            .arg("build")
            .current_dir(&dir.join("wasm-fns"))
            .output()
            .await?;

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
        fs::write(&cargo_path, cargo)?;

        let src = dir.join("src");
        fs::create_dir_all(&src)?;
        let src_file_path = src.join(src_file);
        fs::write(&src_file_path, body)?;

        Self::rustfmt(&src_file_path).await;

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

    fn compile_pipeline_main(&self, name: &str, hash: &str) -> TokenStream {
        let imports = quote! {
            use petgraph::graph::DiGraph;
            use arroyo_worker::operators::*;
            use arroyo_worker::operators::sources::*;
            use arroyo_worker::operators::sinks::*;
            use arroyo_worker::operators::sinks;
            use arroyo_worker::operators::joins::*;
            use arroyo_worker::operators::windows::*;
            use arroyo_worker::engine::{Program, SubtaskNode};
            use arroyo_worker::{LogicalEdge, LogicalNode};
            use types::*;
            use std::time::SystemTime;
            use std::str::FromStr;
            use serde::{Deserialize, Serialize};
        };
        info!("{}", self.program.dot());

        let nodes: Vec<_> = self.program.graph.node_indices().map(|idx| {
            let node = self.program.graph.node_weight(idx).unwrap();
            let description = format!("{:?}", node);
            let input = self.program.graph.edges_directed(idx, Direction::Incoming).next();
            let output = self.program.graph.edges_directed(idx, Direction::Outgoing).next();
            let body = match &node.operator {
                Operator::FileSource { dir, delay } => {
                    let dir = dir.to_string_lossy();
                    let delay = delay.as_millis() as u64;
                    let out_t = parse_type(&output.unwrap().weight().value);
                    quote! {
                        Box::new(FileSourceFunc::<#out_t>::from_dir(&std::path::PathBuf::from(#dir), std::time::Duration::from_millis(#delay)))
                    }
                }
                Operator::ImpulseSource { start_time, spec, total_events } => {
                    let start_time = to_micros(*start_time);
                    let (interval, spec) = match spec {
                        arroyo_datastream::ImpulseSpec::Delay(d) => {
                            let micros = d.as_micros() as u64;
                            (
                                quote! { Duration::from_micros(#micros) },
                                quote!{ sources::ImpulseSpec::Delay(Duration::from_micros(#micros)) }
                            )
                        }
                        arroyo_datastream::ImpulseSpec::EventsPerSecond(eps) => {
                            (
                                quote!{ None },
                                quote!{ sources::ImpulseSpec::EventsPerSecond(#eps) },
                            )
                        },
                    };

                    let limit = total_events.unwrap_or(usize::MAX);
                    quote! {
                        Box::new(ImpulseSourceFunc::new(
                            #interval,
                            #spec,
                            #limit,
                            arroyo_types::from_micros(#start_time),
                        ))
                    }
                }
                Operator::KafkaSource { topic, bootstrap_servers, offset_mode, schema_registry, messages_per_second, client_configs } => {
                    let offset_mode = format!("{:?}", offset_mode);
                    let offset_mode = format_ident!("{}", offset_mode);
                    let out_t = parse_type(&output.unwrap().weight().value);
                    let bootstrap_servers = bootstrap_servers.join(",");
                    let serialization_mode = if *schema_registry {
                        format_ident!("JsonSchemaRegistry")
                    } else {
                        format_ident!("Json")
                    };
                    let client_configs: Vec<_> = client_configs.iter().map(|(key, val)| quote!((#key, #val))).collect();

                    quote! {
                        Box::new(sources::kafka::KafkaSourceFunc::<#out_t>::new(
                            #bootstrap_servers,
                            #topic,
                            sources::kafka::OffsetMode::#offset_mode,
                            sources::kafka::SerializationMode::#serialization_mode,
                            #messages_per_second,
                            vec![#(#client_configs),*]))
                    }
                }
                Operator::FusedWasmUDFs { name, udfs: _ } => {
                    let in_k = parse_type(&input.unwrap().weight().key);
                    let in_t = parse_type(&input.unwrap().weight().value);
                    let out_k = parse_type(&output.unwrap().weight().key);
                    let out_t = parse_type(&output.unwrap().weight().value);

                    let name = format!("{}_process", name);
                    quote! {
                        Box::new(WasmOperator::<#in_k, #in_t, #out_k, #out_t>::new(#name).unwrap())
                    }
                }
                Operator::Window { typ, agg, flatten } => {
                    let in_k = parse_type(&input.unwrap().weight().key);
                    let in_t = parse_type(&input.unwrap().weight().value);
                    let out_t = parse_type(&output.unwrap().weight().value);

                    let agg = match agg {
                        None => quote!{ WindowOperation::Aggregate(aggregators::vec_aggregator) },
                        Some(arroyo_datastream::WindowAgg::Count) => quote!{ WindowOperation::Aggregate(aggregators::count_aggregator) },
                        Some(arroyo_datastream::WindowAgg::Min) => quote!{ WindowOperation::Aggregate(aggregators::min_aggregator) },
                        Some(arroyo_datastream::WindowAgg::Max) => quote!{ WindowOperation::Aggregate(aggregators::max_aggregator) },
                        Some(arroyo_datastream::WindowAgg::Sum) => quote!{ WindowOperation::Aggregate(aggregators::sum_aggregator) },
                        Some(arroyo_datastream::WindowAgg::Expression {
                            expression,
                            ..
                        }) => {
                            let expr: syn::Expr = parse_str(expression).unwrap();
                            let operation = if *flatten {
                                format_ident!("Flatten")
                            } else {
                                format_ident!("Aggregate")
                            };

                            quote! {
                                WindowOperation::#operation(|mut arg: Vec<_>| {
                                    #expr
                                })
                            }
                        },
                    };

                    match typ {
                        WindowType::Tumbling { width } => {
                            let width = width.as_millis() as u64;

                            quote! {
                                Box::new(KeyedWindowFunc::<#in_k, #in_t, #out_t, TumblingWindowAssigner>::
                                    tumbling_window(std::time::Duration::from_millis(#width), #agg))
                            }
                        }
                        WindowType::Sliding { width, slide } => {
                            let width = width.as_millis() as u64;
                            let slide = slide.as_millis() as u64;

                            quote! {
                                Box::new(KeyedWindowFunc::<#in_k, #in_t, #out_t, SlidingWindowAssigner>::
                                    sliding_window(std::time::Duration::from_millis(#width),
                                        std::time::Duration::from_millis(#slide), #agg))
                            }
                        }
                        WindowType::Instant => {
                            quote! {
                                Box::new(KeyedWindowFunc::<#in_k, #in_t, #out_t, InstantWindowAssigner>::
                                    instant_window(#agg))
                            }
                        }
                    }
                }
                Operator::ConsoleSink => {
                    let in_k = parse_type(&input.unwrap().weight().key);
                    let in_t = parse_type(&input.unwrap().weight().value);
                    quote! {
                        Box::new(ConsoleSink::<#in_k, #in_t>::new())
                     }
                }
                Operator::GrpcSink => {
                    let in_k = parse_type(&input.unwrap().weight().key);
                    let in_t = parse_type(&input.unwrap().weight().value);
                    quote! {
                        Box::new(GrpcSink::<#in_k, #in_t>::new())
                     }
                }
                Operator::FileSink { dir } => {
                    let in_k = parse_type(&input.unwrap().weight().key);
                    let in_t = parse_type(&input.unwrap().weight().value);

                    let dir = dir.to_string_lossy();
                    quote! {
                        Box::new(FileSink::<#in_k, #in_t>::new(std::path::PathBuf::from(#dir)))
                    }
                }
                Operator::NullSink => {
                    let in_k = parse_type(&input.unwrap().weight().key);
                    let in_t = parse_type(&input.unwrap().weight().value);
                    quote! {
                        Box::new(NullSink::<#in_k, #in_t>::new())
                     }
                }
                Operator::Watermark(watermark) => {
                    let in_k = parse_type(&input.unwrap().weight().key);
                    let in_t = parse_type(&input.unwrap().weight().value);

                    match watermark {
                        WatermarkType::Periodic { period, max_lateness } => {
                            let period = period.as_millis() as u64;
                            let max_lateness = max_lateness.as_millis() as u64;
                            quote! {
                                Box::new(PeriodicWatermarkGenerator::<#in_k, #in_t>::new(
                                    std::time::Duration::from_millis(#period),
                                    std::time::Duration::from_millis(#max_lateness)))
                            }
                        }
                    }
                }
                Operator::GlobalKey => {
                    let in_k = parse_type(&input.unwrap().weight().key);
                    let in_t = parse_type(&input.unwrap().weight().value);
                    quote! {
                        Box::new(ToGlobalOperator::<#in_k, #in_t>::new())
                    }
                }
                Operator::WindowJoin { window } => {
                    let mut inputs: Vec<_> = self.program.graph.edges_directed(idx, Direction::Incoming)
                        .collect();
                    inputs.sort_by_key(|e| e.weight().typ.clone());
                    assert_eq!(2, inputs.len(), "WindowJoin should have 2 inputs, but has {}", inputs.len());
                    assert_eq!(inputs[0].weight().key, inputs[1].weight().key, "WindowJoin inputs must have the same key type");

                    let in_k = parse_type(&inputs[0].weight().key);
                    let in_t1 = parse_type(&inputs[0].weight().value);
                    let in_t2 = parse_type(&inputs[1].weight().value);

                    match window {
                        WindowType::Tumbling { width } => {
                            let width = width.as_millis() as u64;
                            quote! {
                                Box::new(WindowedHashJoin::<#in_k, #in_t1, #in_t2, TumblingWindowAssigner, TumblingWindowAssigner>::
                                    tumbling_window(std::time::Duration::from_millis(#width)))
                            }
                        }
                        WindowType::Sliding { width, slide } => {
                            let width = width.as_millis() as u64;
                            let slide = slide.as_millis() as u64;
                            quote! {
                                Box::new(WindowedHashJoin::<#in_k, #in_t1, #in_t2, SlidingWindowAssigner, SlidingWindowAssigner>::
                                    sliding_window(std::time::Duration::from_millis(#width),
                                        std::time::Duration::from_millis(#slide)))
                            }
                        }
                        WindowType::Instant => {
                            quote! {
                                Box::new(WindowedHashJoin::<#in_k, #in_t1, #in_t2, InstantWindowAssigner, InstantWindowAssigner>::
                                    instant_window())
                            }
                        }
                    }
                }
                Operator::KafkaSink { topic, bootstrap_servers, client_configs } => {
                    let in_k = parse_type(&input.unwrap().weight().key);
                    let in_t = parse_type(&input.unwrap().weight().value);

                    let bootstrap_servers = bootstrap_servers.join(",");
                    let client_configs: Vec<_> = client_configs.iter().map(|(key, val)| quote!((#key, #val))).collect();
                    quote! {
                        Box::new(sinks::kafka::KafkaSinkFunc::<#in_k, #in_t>::new(
                            #bootstrap_servers,
                            #topic,
                        vec![#(#client_configs ),*]))
                    }
                }
                Operator::NexmarkSource{first_event_rate, num_events}  => {
                    match *num_events {
                        Some(events) => {
                            quote! {
                                Box::new(sources::nexmark::NexmarkSourceFunc::new(#first_event_rate, Some(#events)))
                            }
                        }
                        None => {
                            quote! {
                                Box::new(sources::nexmark::NexmarkSourceFunc::new(#first_event_rate, None))
                            }
                        }
                    }
                    }
                Operator::Count => {
                    let in_k = parse_type(&input.unwrap().weight().key);
                    let in_t = parse_type(&input.unwrap().weight().value);

                    let in_t = extract_container_type("Vec", &in_t).expect("Input to count is not a Vec");
                    quote! {
                        Box::new(CountOperator::<#in_k, #in_t>::new())
                    }
                },
                Operator::Aggregate(behavior) => {
                    let in_k = parse_type(&input.unwrap().weight().key);
                    let in_t = parse_type(&input.unwrap().weight().value);
                    let in_t = extract_container_type("Vec", &in_t).expect("Input to aggregate is not a Vec");

                    let behavior = match behavior {
                        AggregateBehavior::Max => format_ident!("Max"),
                        AggregateBehavior::Min => format_ident!("Min"),
                        AggregateBehavior::Sum => format_ident!("Sum"),
                    };

                    quote! {
                        Box::new(AggregateOperator::<#in_k, #in_t>::new(AggregateBehavior::#behavior))
                    }
                },
                Operator::FlattenOperator { name } => {
                    let k = parse_type(&output.unwrap().weight().key);
                    let t = parse_type(&output.unwrap().weight().value);
                 quote! {
                    Box::new(FlattenOperator::<#k, #t>::new(#name.to_string()))
                }
            },
                Operator::ExpressionOperator { name, expression, return_type } => {
                    let expr : syn::Expr = parse_str(expression).expect(expression);
                    let in_k = parse_type(&input.unwrap().weight().key);
                    let in_t = parse_type(&input.unwrap().weight().value);
                    let out_k = parse_type(&output.unwrap().weight().key);
                    let out_t = parse_type(&output.unwrap().weight().value);
                    let func: syn::ExprClosure = parse_str(&quote!(|record, _| {#expr}).to_string()).unwrap();
                    match return_type {
                        arroyo_datastream::ExpressionReturnType::Predicate => {
                            quote! {
                                Box::new(FilterOperator::<#in_k, #in_t>{
                                    name: #name.to_string(),
                                    predicate_fn: Box::new(#func),
                                })
                            }
                        },
                        arroyo_datastream::ExpressionReturnType::Record => {
                    quote! {
                        Box::new(MapOperator::<#in_k, #in_t, #out_k, #out_t> {
                            name: #name.to_string(),
                            map_fn: Box::new(#func),
                        })
                    }
                        },
                        arroyo_datastream::ExpressionReturnType::OptionalRecord => {
                            quote! {
                                Box::new(OptionMapOperator::<#in_k, #in_t, #out_k, #out_t> {
                                    name: #name.to_string(),
                                    map_fn: Box::new(#func),
                                })
                            }
                        },
                    }
                },
                Operator::FlatMapOperator { name, expression, return_type } => {
                    let expr : syn::Expr = parse_str(expression).expect(expression);
                    let in_k = parse_type(&input.unwrap().weight().key);
                    let in_t = parse_type(&input.unwrap().weight().value);
                    let in_t = extract_container_type("Vec", &in_t).expect("Input to aggregate is not a Vec");
                    let out_k = parse_type(&output.unwrap().weight().key);
                    let out_t = parse_type(&output.unwrap().weight().value);
                    let closure = match return_type {
                        arroyo_datastream::ExpressionReturnType::Predicate => {
                            quote! {
                                |record, _| {
                                    if #expr {
                                        Some(record)
                                    } else {
                                        None
                                    }
                                }
                            }
                        },
                        arroyo_datastream::ExpressionReturnType::Record => {
                            quote! {
                                |record, _| {
                                    Some(#expr)
                                }
                            }
                        },
                        arroyo_datastream::ExpressionReturnType::OptionalRecord => {
                            quote! {
                                |record, _| {
                                    #expr
                                }
                            }
                        }
                    };
                    let func: syn::ExprClosure = parse_str(&quote!(#closure).to_string()).unwrap();
                    quote! {
                        Box::new(FlatMapOperator::<#in_k, #in_t, #out_k, #out_t> {
                            name: #name.to_string(),
                            map_fn: Box::new(#func),
                        })
                    }
                },
                Operator::SlidingWindowAggregator(SlidingWindowAggregator{
                    width,slide,aggregator,bin_merger,
                    in_memory_add,in_memory_remove,bin_type,mem_type}) => {
                    let in_k = parse_type(&input.unwrap().weight().key);
                    let in_t = parse_type(&input.unwrap().weight().value);
                    let out_t = parse_type(&output.unwrap().weight().value);
                    let bin_t = parse_type(bin_type);
                    let mem_t = parse_type(mem_type);
                    let width = width.as_millis() as u64;
                    let slide = slide.as_millis() as u64;
                    let aggregator: syn::ExprClosure = parse_str(aggregator).unwrap();
                    let bin_merger: syn::ExprClosure = parse_str(bin_merger).unwrap();
                    let in_memory_add: syn::ExprClosure = parse_str(in_memory_add).unwrap();
                    let in_memory_remove: syn::ExprClosure = parse_str(in_memory_remove).unwrap();

                    quote!{
                        Box::new(arroyo_worker::operators::aggregating_window::AggregatingWindowFunc::<#in_k, #in_t, #bin_t, #mem_t, #out_t>::
                        new(std::time::Duration::from_millis(#width),
                        std::time::Duration::from_millis(#slide),
                            #aggregator,
                            #bin_merger,
                            #in_memory_add,
                            #in_memory_remove))
                    }
                },
                Operator::TumblingWindowAggregator(TumblingWindowAggregator { width, aggregator, bin_merger, bin_type }) => {
                    let in_k = parse_type(&input.unwrap().weight().key);
                    let in_t = parse_type(&input.unwrap().weight().value);
                    let out_t = parse_type(&output.unwrap().weight().value);
                    let bin_t = parse_type(bin_type);
                    let width = width.as_millis() as u64;
                    let aggregator: syn::ExprClosure = parse_str(aggregator).unwrap();
                    let bin_merger: syn::ExprClosure = parse_str(bin_merger).unwrap();
                    quote!{
                        Box::new(arroyo_worker::operators::tumbling_aggregating_window::
                            TumblingAggregatingWindowFunc::<#in_k, #in_t, #bin_t, #out_t>::
                        new(std::time::Duration::from_millis(#width),
                            #aggregator,
                            #bin_merger))
                    }
                },
                Operator::TumblingTopN(
                        TumblingTopN {
                            width,
                            max_elements,
                            extractor,
                            partition_key_type,
                            converter,
                        },
                    ) => {
                        let in_k = parse_type(&input.unwrap().weight().key);
                        let in_t = parse_type(&input.unwrap().weight().value);
                        let out_t = parse_type(&output.unwrap().weight().value);
                        let pk_type = parse_type(partition_key_type);
                        let width = width.as_millis() as u64;
                        let extractor: syn::ExprClosure = parse_str(extractor).expect(extractor);
                        let converter: syn::ExprClosure = parse_str(converter).unwrap();
                        quote! {
                        Box::new(arroyo_worker::operators::tumbling_top_n_window::
                            TumblingTopNWindowFunc::<#in_k, #in_t, #pk_type, #out_t>::
                        new(std::time::Duration::from_millis(#width),
                            #max_elements,
                            #extractor,
                            #converter))
                    }
                    }

                Operator::SlidingAggregatingTopN(
                    SlidingAggregatingTopN {
                        width,
                        slide,
                        bin_merger,
                        in_memory_add,
                        in_memory_remove,
                        partitioning_func,
                        extractor,
                        aggregator,
                        bin_type,
                        mem_type,
                        sort_key_type,
                        max_elements,
                    },
                ) => {
                    let in_k = parse_type(&input.unwrap().weight().key);
                    let in_t = parse_type(&input.unwrap().weight().value);
                    let out_t = parse_type(&output.unwrap().weight().value);
                    let out_k = parse_type(&output.unwrap().weight().key);
                    let bin_t = parse_type(bin_type);
                    let mem_t = parse_type(mem_type);
                    let width = width.as_millis() as u64;
                    let slide = slide.as_millis() as u64;
                    let bin_merger: syn::ExprClosure = parse_str(bin_merger).unwrap();
                    let in_memory_add: syn::ExprClosure = parse_str(in_memory_add).unwrap();
                    let in_memory_remove: syn::ExprClosure =
                        parse_str(in_memory_remove).unwrap();
                    let partitioning_func: syn::ExprClosure =
                        parse_str(partitioning_func).unwrap();
                    let sort_key_type = parse_type(sort_key_type);
                    let extractor: syn::ExprClosure = parse_str(extractor).unwrap();
                    let aggregator: syn::ExprClosure = parse_str(aggregator).expect(aggregator);

                    quote! {
                    Box::new(arroyo_worker::operators::sliding_top_n_aggregating_window::
                        SlidingAggregatingTopNWindowFunc::<#in_k, #in_t, #bin_t, #mem_t, #out_k, #sort_key_type, #out_t>::
                    new(std::time::Duration::from_millis(#width),
                    std::time::Duration::from_millis(#slide),
                        #bin_merger,
                        #in_memory_add,
                        #in_memory_remove,
                        #partitioning_func,
                        #extractor,
                        #aggregator,
                        #max_elements))
                }
                }
            };

            (node.operator_id.clone(), description, body, node.parallelism)
        }).collect();

        let node_defs: Vec<_> = nodes
            .iter()
            .map(|(id, description, body, parallelism)| {
                let ident = format_ident!("{}", id);
                quote! {
                    let #ident = graph.add_node(
                        LogicalNode {
                            id: #id.to_string(),
                            description: #description.to_string(),
                            create_fn: Box::new(|subtask_idx: usize, parallelism: usize| {
                                SubtaskNode {
                                    id: #id.to_string(),
                                    subtask_idx,
                                    parallelism,
                                    node: #body
                                }
                            }),
                            initial_parallelism: #parallelism,
                        }
                    );
                }
            })
            .collect();

        let edge_defs: Vec<_> = self
            .program
            .graph
            .edge_indices()
            .map(|idx| {
                let e = self.program.graph.edge_weight(idx).unwrap();
                let (source, dest) = self.program.graph.edge_endpoints(idx).unwrap();
                let source = format_ident!(
                    "{}",
                    self.program.graph.node_weight(source).unwrap().operator_id
                );
                let dest = format_ident!(
                    "{}",
                    self.program.graph.node_weight(dest).unwrap().operator_id
                );
                let typ = match e.typ {
                    EdgeType::Forward => {
                        quote! { LogicalEdge::Forward }
                    }
                    EdgeType::Shuffle => {
                        quote! { LogicalEdge::Shuffle }
                    }
                    EdgeType::ShuffleJoin(order) => {
                        quote! { LogicalEdge::ShuffleJoin(#order) }
                    }
                };

                quote! {
                    graph.add_edge(#source, #dest, #typ);
                }
            })
            .collect();

        let other_defs: Vec<TokenStream> = self
            .program
            .other_defs
            .iter()
            .map(|t| parse_str(t).unwrap())
            .collect();

        quote! {
            #imports

            pub fn make_graph() -> DiGraph<LogicalNode, LogicalEdge> {
                let mut graph: DiGraph<LogicalNode, LogicalEdge> = DiGraph::new();
                #(#node_defs )*

                #(#edge_defs )*

                graph
            }

            pub fn main() {
                let graph = make_graph();

                arroyo_worker::WorkerServer::new(#name, #hash, graph).start().unwrap();
            }

            #(#other_defs )*
        }
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

        quote! {
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

    async fn rustfmt(file: &Path) {
        Command::new("rustfmt").arg(file).output().await.unwrap();
    }
}

#[cfg(test)]
mod tests {
    use quote::quote;
    use syn::parse_str;

    use super::extract_container_type;

    #[test]
    fn test_extract_vec_type() {
        let v = extract_container_type("Vec", &parse_str("Vec<Vec<u8>>").unwrap()).unwrap();
        assert_eq!("Vec < u8 >", quote! { #v }.to_string());

        let v = extract_container_type("Vec", &parse_str("Vec<String>").unwrap()).unwrap();
        assert_eq!("String", quote! { #v }.to_string());

        assert_eq!(
            None,
            extract_container_type("Vec", &parse_str("HashMap<String, u8>").unwrap())
        );
    }
}
