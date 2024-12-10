#![allow(clippy::new_without_default)]
#![allow(clippy::comparison_chain)]

pub mod logical;
pub mod optimizers;

use arroyo_rpc::config::{config, DefaultSink};
use arroyo_rpc::grpc::api;
use bincode::{Decode, Encode};
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::fmt::{Debug, Formatter};
use std::time::Duration;
use syn::parse_quote;

// quote a duration as a syn::Expr
pub fn duration_to_syn_expr(duration: Duration) -> syn::Expr {
    let secs = duration.as_secs();
    let nanos = duration.subsec_nanos();

    parse_quote!(std::time::Duration::new(#secs, #nanos))
}

#[derive(Copy, Clone, Debug, Eq, PartialEq, Encode, Decode, Serialize, Deserialize)]
pub enum WasmBehavior {
    Map,
    OptMap,
    Filter,
    Timestamp,
    KeyBy,
}

#[derive(Clone, Encode, Decode, Serialize, Deserialize, PartialEq, Eq)]
pub struct WasmDef {
    pub name: String,
    pub key_arg: String,
    pub key_arg_type: String,
    pub value_arg: String,
    pub value_arg_type: String,
    pub ret_type: String,
    pub body: String,
}

pub enum OpBiFunc {
    Wasm(WasmDef),
}

#[derive(Clone, Encode, Decode, Serialize, Deserialize, PartialEq, Eq)]
pub struct WasmUDF {
    pub behavior: WasmBehavior,
    pub def: WasmDef,
}

#[derive(Clone, Encode, Decode, Serialize, Deserialize, PartialEq, Eq, Hash, PartialOrd)]
pub enum WindowType {
    Tumbling { width: Duration },
    Sliding { width: Duration, slide: Duration },
    Instant,
    Session { gap: Duration },
}

fn format_duration(duration: Duration) -> String {
    let secs = duration.as_secs_f64();
    let secsi = duration.as_secs();
    if secs < 1.0 {
        format!("{}ms", duration.as_millis())
    } else if secs.floor() != secs {
        format!("{}s", secs)
    } else if secsi % (60 * 60 * 24) == 0 {
        format!("{}d", secsi / 60 / 60 / 24)
    } else if secsi % (60 * 60) == 0 {
        format!("{}h", secsi / 60 / 60)
    } else if secsi % 60 == 0 {
        format!("{}m", secsi / 60)
    } else {
        format!("{}s", secsi)
    }
}

impl Debug for WindowType {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Tumbling { width } => {
                write!(f, "TumblingWindow({})", format_duration(*width))
            }
            Self::Sliding { width, slide } => {
                write!(
                    f,
                    "SlidingWindow(size: {}, slide: {})",
                    format_duration(*width),
                    format_duration(*slide)
                )
            }
            Self::Instant => {
                write!(f, "InstantWindow")
            }
            Self::Session { gap } => {
                write!(f, "SessionWindow({})", format_duration(*gap))
            }
        }
    }
}

#[derive(Copy, Clone, Encode, Decode, Serialize, Deserialize, Debug, PartialEq, Eq)]
pub enum OffsetMode {
    Earliest,
    Latest,
}

impl From<arroyo_rpc::grpc::api::OffsetMode> for OffsetMode {
    fn from(offset_mode: arroyo_rpc::grpc::api::OffsetMode) -> Self {
        match offset_mode {
            arroyo_rpc::grpc::api::OffsetMode::Earliest => Self::Earliest,
            arroyo_rpc::grpc::api::OffsetMode::Latest => Self::Latest,
        }
    }
}

#[derive(Clone, Encode, Decode, Serialize, Deserialize, PartialEq, Eq)]
pub enum WindowAgg {
    Count,
    Max,
    Min,
    Sum,
    Expression { name: String, expression: String },
}

impl std::fmt::Debug for WindowAgg {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Count => write!(f, "Count"),
            Self::Max => write!(f, "Max"),
            Self::Min => write!(f, "Min"),
            Self::Sum => write!(f, "Sum"),
            Self::Expression { name, .. } => write!(f, "{}", name),
        }
    }
}

#[derive(Debug, Clone, Encode, Decode, Serialize, Deserialize, PartialEq, Eq)]
pub enum AggregateBehavior {
    Max,
    Min,
    Sum,
}
#[derive(Debug, Clone, Encode, Decode, Serialize, Deserialize, PartialEq, Eq)]
pub struct SlidingWindowAggregator {
    pub width: Duration,
    pub slide: Duration,
    // fn(&MemA) -> OutT
    pub aggregator: String,
    // fn(&T, Option<&BinA>) -> BinA
    pub bin_merger: String,
    // fn(Option<MemA>, BinA) -> MemA
    pub in_memory_add: String,
    // fn(MemA, BinA) -> Option<MemA>
    pub in_memory_remove: String,
    pub bin_type: String,
    pub mem_type: String,
}

#[derive(Debug, Clone, Encode, Decode, Serialize, Deserialize, PartialEq, Eq)]
pub struct TumblingWindowAggregator {
    pub width: Duration,
    // fn(&MemA) -> OutT
    pub aggregator: String,
    // fn(&T, Option<&BinA>) -> BinA
    pub bin_merger: String,
    pub bin_type: String,
}

#[derive(Debug, Clone, Encode, Decode, Serialize, Deserialize, PartialEq, Eq)]
pub struct TumblingTopN {
    pub width: Duration,
    pub max_elements: usize,
    pub extractor: String,
    pub partition_key_type: String,
    pub converter: String,
}

#[derive(Debug, Clone, Encode, Decode, Serialize, Deserialize, PartialEq, Eq)]
pub struct SlidingAggregatingTopN {
    pub width: Duration,
    pub slide: Duration,
    // fn(&T, Option<&BinA>) -> BinA
    pub bin_merger: String,
    // fn(Option<MemA>, BinA) -> MemA
    pub in_memory_add: String,
    // fn(MemA, BinA) -> Option<MemA>
    pub in_memory_remove: String,
    // fn(&K) -> PK
    pub partitioning_func: String,
    // fn(&K, &MemA) -> SK
    pub extractor: String,
    // fn(&K, &MemA) -> OutT
    pub aggregator: String,
    pub bin_type: String,
    pub mem_type: String,
    pub sort_key_type: String,
    pub max_elements: usize,
}

#[derive(Debug, Clone, Encode, Decode, Serialize, Deserialize, PartialEq, Eq)]
pub struct NonWindowAggregator {
    pub expiration: Duration,
    // fn(&BinA) -> OutT
    pub aggregator: String,
    // fn(&T, Option<&BinA>) -> Option<BinA>
    pub bin_merger: String,
    // BinA
    pub bin_type: String,
}

#[derive(Copy, Clone, Debug, Encode, Decode, Serialize, Deserialize, PartialEq)]
pub enum ImpulseSpec {
    Delay(Duration),
    EventsPerSecond(f32),
}

pub fn default_sink() -> api::ConnectorOp {
    match config().pipeline.default_sink {
        DefaultSink::Preview => api::ConnectorOp {
            connector: "preview".to_string(),
            config: json!({
                "connection": {},
                "table": {},
                "connection_schema": {
                    "fields": [],
                }
            })
            .to_string(),
            description: "PreviewSink".to_string(),
        },
        DefaultSink::Stdout => api::ConnectorOp {
            connector: "stdout".to_string(),
            config: json!({
                "connection": {},
                "table": {},
                "connection_schema": {
                    "fields": [],
                }
            })
            .to_string(),
            description: "StdoutSink".to_string(),
        },
    }
}
