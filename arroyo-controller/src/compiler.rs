use crate::cargo_toml;
use crate::states::fatal;
use anyhow::{anyhow, Result};
use arroyo_datastream::{parse_type, Operator, Program, WasmBehavior};
use arroyo_rpc::grpc::compiler_grpc_client::CompilerGrpcClient;
use arroyo_rpc::grpc::{CompileQueryReq, UdfCrate};
use arroyo_types::REMOTE_COMPILER_ENDPOINT_ENV;
use petgraph::Direction;
use proc_macro2::TokenStream;
use quote::{format_ident, quote};
use std::io;
use std::io::ErrorKind;
use syn::{parse_quote, parse_str};

use tonic::{Code, Request};
use tracing::info;

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

    pub async fn compile(&self) -> Result<CompiledProgram> {
        let endpoint = std::env::var(REMOTE_COMPILER_ENDPOINT_ENV)
            .unwrap_or_else(|_| panic!("Must set {}", REMOTE_COMPILER_ENDPOINT_ENV));

        let req = CompileQueryReq {
            job_id: self.job_id.clone(),
            types: self.compile_types().to_string(),
            pipeline: self.compile_pipeline_main(&self.name, &self.program.get_hash()),
            udf_crates: self.compile_udf_lib()?,
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

    fn compile_udf_lib(&self) -> Result<Vec<UdfCrate>> {
        self.program
            .udfs
            .iter()
            .map(|udf| {
                Ok(UdfCrate {
                    name: udf.name.to_string(),
                    definition: udf.definition.to_string(),
                    cargo_toml: cargo_toml(&udf.name, &udf.dependencies),
                })
            })
            .collect()
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
