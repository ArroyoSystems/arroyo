#![allow(clippy::new_without_default)]
#![allow(clippy::comparison_chain)]

use std::cell::RefCell;
use std::collections::hash_map::DefaultHasher;
use std::collections::{HashMap, HashSet};
use std::fmt::{Debug, Formatter};
use std::hash::Hasher;
use std::marker::PhantomData;
use std::ops::Add;
use std::path::PathBuf;
use std::rc::Rc;
use std::str::FromStr;
use std::time::{Duration, SystemTime};

use arroyo_rpc::grpc::api::create_pipeline_req::Config;
use arroyo_rpc::grpc::api::impulse_source::Spec;
use arroyo_rpc::grpc::api::operator::Operator as GrpcOperator;
use arroyo_rpc::grpc::api::{self as GrpcApi, ExpressionAggregator, Flatten, ProgramEdge};
use arroyo_types::nexmark::Event;
use arroyo_types::{from_micros, to_micros, Data, GlobalKey, ImpulseEvent, Key};
use bincode::{Decode, Encode};
use petgraph::graph::{DiGraph, NodeIndex};
use petgraph::visit::EdgeRef;
use proc_macro2::Ident;
use serde::{Deserialize, Serialize};
use syn::parse_str;
use tonic::{Request, Status};

use anyhow::{anyhow, bail, Result};
use prost::Message;

use crate::Operator::FusedWasmUDFs;
use arroyo_rpc::grpc::api::{
    Aggregator, BuiltinSink, CreateJobReq, CreatePipelineReq, JobEdge, JobGraph, JobNode,
    PipelineProgram, ProgramNode, StopType, UpdateJobReq, WasmFunction,
};
use petgraph::{Direction, Graph};
use rand::distributions::Alphanumeric;
use rand::rngs::SmallRng;
use rand::{Rng, SeedableRng};

pub trait ArroyoData {
    fn get_def() -> String;
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

#[derive(Clone, Encode, Decode, Serialize, Deserialize, PartialEq, Eq)]
pub enum WindowType {
    Tumbling { width: Duration },
    Sliding { width: Duration, slide: Duration },
    Instant,
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
        }
    }
}

#[derive(Clone, Encode, Decode, Serialize, Deserialize, PartialEq, Eq)]
pub enum WatermarkType {
    Periodic {
        period: Duration,
        max_lateness: Duration,
    },
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

#[derive(Copy, Clone, Encode, Decode, Serialize, Deserialize, PartialEq)]
pub enum ImpulseSpec {
    Delay(Duration),
    EventsPerSecond(f32),
}

#[derive(Clone, Encode, Decode, Serialize, Deserialize, PartialEq)]
pub enum Operator {
    FileSource {
        dir: PathBuf,
        delay: Duration,
    },
    ImpulseSource {
        start_time: SystemTime,
        spec: ImpulseSpec,
        total_events: Option<usize>,
    },
    KafkaSource {
        topic: String,
        bootstrap_servers: Vec<String>,
        offset_mode: OffsetMode,
        schema_registry: bool,
        messages_per_second: u32,
        client_configs: HashMap<String, String>,
    },
    FusedWasmUDFs {
        name: String,
        udfs: Vec<WasmUDF>,
    },
    Window {
        typ: WindowType,
        agg: Option<WindowAgg>,
        flatten: bool,
    },
    Count,
    Aggregate(AggregateBehavior),
    Watermark(WatermarkType),
    GlobalKey,
    ConsoleSink,
    GrpcSink,
    NullSink,
    FileSink {
        dir: PathBuf,
    },
    WindowJoin {
        window: WindowType,
    },
    KafkaSink {
        topic: String,
        bootstrap_servers: Vec<String>,
        client_configs: HashMap<String, String>,
    },
    NexmarkSource {
        first_event_rate: u64,
        num_events: Option<u64>,
    },
    ExpressionOperator {
        name: String,
        expression: String,
        return_type: ExpressionReturnType,
    },
    FlattenOperator {
        name: String,
    },
    FlatMapOperator {
        name: String,
        expression: String,
        return_type: ExpressionReturnType,
    },
    SlidingWindowAggregator(SlidingWindowAggregator),
    TumblingWindowAggregator(TumblingWindowAggregator),
    TumblingTopN(TumblingTopN),
    SlidingAggregatingTopN(SlidingAggregatingTopN),
}

#[derive(Clone, Encode, Decode, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub enum ExpressionReturnType {
    Predicate,
    Record,
    OptionalRecord,
}

impl ExpressionReturnType {
    pub fn receiver_ident(&self) -> Ident {
        match self {
            ExpressionReturnType::Predicate => parse_str("predicate").unwrap(),
            ExpressionReturnType::Record => parse_str("record").unwrap(),
            ExpressionReturnType::OptionalRecord => parse_str("record").unwrap(),
        }
    }
}

impl Debug for Operator {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        fn write_behavior(f: &mut Formatter<'_>, udf: &WasmUDF) -> std::fmt::Result {
            match udf.behavior {
                WasmBehavior::Map => write!(f, "Map({})", udf.def.name),
                WasmBehavior::OptMap => write!(f, "OptMap({})", udf.def.name),
                WasmBehavior::Filter => write!(f, "Filter({})", udf.def.name),
                WasmBehavior::Timestamp => write!(f, "Timestamp({})", udf.def.name),
                WasmBehavior::KeyBy => write!(f, "KeyBy({})", udf.def.name),
            }
        }

        match self {
            Operator::FileSource { dir, .. } => write!(f, "FileSource<{:?}>", dir),
            Operator::ImpulseSource { .. } => write!(f, "ImpulseSource"),
            Operator::KafkaSource { topic, .. } => write!(f, "KafkaSource<{}>", topic),
            Operator::FusedWasmUDFs { udfs, .. } => {
                if udfs.len() == 1 {
                    write_behavior(f, &udfs[0])
                } else if udfs.len() > 1 {
                    write!(f, "[")?;
                    for udf in &udfs[0..udfs.len() - 1] {
                        write_behavior(f, udf)?;
                        write!(f, "→")?;
                    }
                    write_behavior(f, &udfs[udfs.len() - 1])?;

                    write!(f, "]")
                } else {
                    Ok(())
                }
            }
            Operator::Aggregate(b) => write!(f, "{:?}", b),
            Operator::Count => write!(f, "Count"),
            Operator::Window { typ, agg, .. } => write!(f, "{:?}->{:?}", typ, agg),
            Operator::ConsoleSink => write!(f, "ConsoleSink"),
            Operator::GrpcSink => write!(f, "GrpcSink"),
            Operator::FileSink { .. } => write!(f, "FileSink"),
            Operator::Watermark(_) => write!(f, "Watermark"),
            Operator::WindowJoin { window } => write!(f, "WindowJoin({:?})", window),
            Operator::NullSink => write!(f, "NullSink"),
            Operator::KafkaSink { topic, .. } => write!(f, "KafkaSink<{}>", topic),
            Operator::NexmarkSource {
                first_event_rate,
                num_events,
            } => match num_events {
                Some(events) => {
                    write!(
                        f,
                        "BoundedNexmarkSource<qps: {}, total_runtime: {}s>",
                        first_event_rate,
                        events / first_event_rate
                    )
                }
                None => {
                    write!(f, "UnboundedNexmarkSource<qps: {}>", first_event_rate)
                }
            },
            Operator::GlobalKey => write!(f, "GlobalKey"),
            Operator::FlattenOperator { name } => write!(f, "flatten<{}>", name),
            Operator::ExpressionOperator {
                name,
                expression: _,
                return_type,
            } => write!(f, "expression<{}:{:?}>", name, return_type),
            Operator::FlatMapOperator {
                name,
                expression: _,
                return_type,
            } => write!(f, "flat_map<{}:{:?}>", name, return_type),
            Operator::SlidingWindowAggregator(SlidingWindowAggregator { width, slide, .. }) => {
                write!(
                    f,
                    "SlidingWindowAggregator<{:?}>",
                    WindowType::Sliding {
                        width: *width,
                        slide: *slide
                    }
                )
            }
            Operator::TumblingWindowAggregator(TumblingWindowAggregator { width, .. }) => write!(
                f,
                "TumblingWindowAggregator<{:?}>",
                WindowType::Tumbling { width: *width }
            ),
            Operator::TumblingTopN(TumblingTopN {
                width,
                max_elements,
                ..
            }) => write!(
                f,
                "TumblingTopN<{:?}, {:?}>",
                WindowType::Tumbling { width: *width },
                *max_elements
            ),
            Operator::SlidingAggregatingTopN(SlidingAggregatingTopN { width, slide, .. }) => {
                write!(
                    f,
                    "SlidingAggregatingTopN<{:?}>",
                    WindowType::Sliding {
                        width: *width,
                        slide: *slide
                    }
                )
            }
        }
    }
}

pub struct NexmarkSource {
    pub first_event_rate: u64,
    pub num_events: Option<u64>,
}

impl Source<Event> for NexmarkSource {
    fn as_operator(&self) -> Operator {
        Operator::NexmarkSource {
            first_event_rate: self.first_event_rate,
            num_events: self.num_events,
        }
    }
}

#[derive(Clone, Encode, Decode, serde::Serialize, serde::Deserialize, PartialEq)]
pub struct StreamNode {
    pub operator_id: String,
    pub operator: Operator,
    pub parallelism: usize,
}

impl Debug for StreamNode {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}:{:?}", self.operator_id, self.operator)
    }
}

#[derive(Clone, Debug, Encode, Decode, Serialize, Deserialize, Eq, PartialEq, PartialOrd, Ord)]
pub enum EdgeType {
    Forward,
    Shuffle,
    ShuffleJoin(usize),
}

#[derive(Clone, Encode, Decode, Serialize, Deserialize)]
pub struct StreamEdge {
    pub key: String,
    pub value: String,
    pub typ: EdgeType,
}

impl StreamEdge {
    pub fn unkeyed_edge(value: impl ToString, typ: EdgeType) -> StreamEdge {
        StreamEdge {
            key: "()".to_string(),
            value: value.to_string(),
            typ,
        }
    }

    pub fn keyed_edge(key: impl ToString, value: impl ToString, typ: EdgeType) -> StreamEdge {
        StreamEdge {
            key: key.to_string(),
            value: value.to_string(),
            typ,
        }
    }
}

impl Debug for StreamEdge {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let arrow = match self.typ {
            EdgeType::Forward => "→",
            EdgeType::Shuffle => "⤨",
            EdgeType::ShuffleJoin(0) => "-left→",
            EdgeType::ShuffleJoin(1) => "-right→",
            EdgeType::ShuffleJoin(_) => unimplemented!(),
        };
        write!(f, "{} {} {}", self.key, arrow, self.value)
    }
}

pub trait Source<T: Data> {
    fn as_operator(&self) -> Operator;
}

pub struct FileSource {
    dir: PathBuf,
    delay: Duration,
}

impl FileSource {
    pub fn from_dir(dir: PathBuf, delay: Duration) -> FileSource {
        FileSource { dir, delay }
    }
}

impl Source<String> for FileSource {
    fn as_operator(&self) -> Operator {
        Operator::FileSource {
            dir: self.dir.clone(),
            delay: self.delay,
        }
    }
}

pub struct ImpulseSource {
    start_time: SystemTime,
    interval: Duration,
}

impl ImpulseSource {
    pub fn with_interval(interval: Duration) -> ImpulseSource {
        ImpulseSource {
            start_time: SystemTime::now(),
            interval,
        }
    }

    pub fn with_interval_and_start(interval: Duration, start_time: SystemTime) -> ImpulseSource {
        ImpulseSource {
            start_time,
            interval,
        }
    }
}

impl Source<ImpulseEvent> for ImpulseSource {
    fn as_operator(&self) -> Operator {
        Operator::ImpulseSource {
            start_time: self.start_time,
            spec: ImpulseSpec::Delay(self.interval),
            total_events: None,
        }
    }
}

pub struct KafkaSource {
    topic: String,
    bootstrap_servers: Vec<String>,
    offset_mode: OffsetMode,
    messages_per_second: u32,
}

impl KafkaSource {
    pub fn new(topic: &str, bootstrap_servers: Vec<&str>, offset_mode: OffsetMode) -> Self {
        Self {
            topic: topic.to_string(),
            bootstrap_servers: bootstrap_servers.iter().map(|s| s.to_string()).collect(),
            offset_mode,
            messages_per_second: 10_000,
        }
    }

    pub fn new_with_schema_registry(
        topic: &str,
        bootstrap_servers: Vec<&str>,
        offset_mode: OffsetMode,
    ) -> Self {
        Self {
            topic: topic.to_string(),
            bootstrap_servers: bootstrap_servers.iter().map(|s| s.to_string()).collect(),
            offset_mode,
            messages_per_second: 10_000,
        }
    }
}

impl Source<Vec<u8>> for KafkaSource {
    fn as_operator(&self) -> Operator {
        Operator::KafkaSource {
            topic: self.topic.clone(),
            bootstrap_servers: self.bootstrap_servers.clone(),
            offset_mode: self.offset_mode,
            schema_registry: false,
            messages_per_second: self.messages_per_second,
            client_configs: HashMap::default(),
        }
    }
}

pub struct Stream<T> {
    _t: PhantomData<T>,
    graph: Rc<RefCell<DiGraph<StreamNode, StreamEdge>>>,
    last_node: Option<NodeIndex>,
    parallelism: usize,
}

pub trait BiFunc<K, T1, T2> {
    fn as_operator(&self, behavior: WasmBehavior) -> Operator;
}

impl<T: Data> Stream<T> {
    pub fn new() -> Stream<()> {
        Self::with_parallelism(1)
    }

    pub fn with_parallelism(parallelism: usize) -> Stream<()> {
        Stream {
            _t: PhantomData,
            graph: Rc::new(RefCell::new(DiGraph::new())),
            last_node: None,
            parallelism,
        }
    }

    pub fn source<T2: Data, S: Source<T2>>(&mut self, source: S) -> Stream<T2> {
        let operator = source.as_operator();
        let mut graph = (*self.graph).borrow_mut();
        let count = graph.node_count();
        let index = graph.add_node(StreamNode {
            operator_id: format!("node_{}", count),
            operator,
            parallelism: self.parallelism,
        });
        Stream {
            _t: PhantomData,
            graph: self.graph.clone(),
            last_node: Some(index),
            parallelism: self.parallelism,
        }
    }

    fn add_node<T2: Data>(&mut self, operator: Operator) -> Stream<T2> {
        let index = {
            let mut graph = (*self.graph).borrow_mut();
            let count = graph.node_count();
            graph.add_node(StreamNode {
                operator_id: format!("node_{}", count),
                operator,
                parallelism: self.parallelism,
            })
        };

        let last_parallelism = (*self.graph)
            .borrow()
            .node_weight(self.last_node.expect("graph must start with a source node"))
            .unwrap()
            .parallelism;

        let edge = StreamEdge {
            key: std::any::type_name::<()>().to_string(),
            value: std::any::type_name::<T>().to_string(),
            typ: if last_parallelism == self.parallelism {
                EdgeType::Forward
            } else {
                EdgeType::Shuffle
            },
        };

        (*self.graph)
            .borrow_mut()
            .add_edge(self.last_node.unwrap(), index, edge);

        Stream {
            _t: PhantomData,
            graph: self.graph.clone(),
            last_node: Some(index),
            parallelism: self.parallelism,
        }
    }

    pub fn map<T2: Data, F: BiFunc<(), T, T2>>(&mut self, f: F) -> Stream<T2> {
        self.add_node(f.as_operator(WasmBehavior::Map))
    }

    pub fn opt_map<T2: Data, F: BiFunc<(), T, Option<T2>>>(&mut self, f: F) -> Stream<T2> {
        self.add_node(f.as_operator(WasmBehavior::OptMap))
    }

    pub fn filter<F: BiFunc<(), T, bool>>(&mut self, f: F) -> Stream<T> {
        self.add_node(f.as_operator(WasmBehavior::Filter))
    }

    pub fn timestamp<F: BiFunc<(), T, SystemTime>>(&mut self, f: F) -> Stream<T> {
        self.add_node(f.as_operator(WasmBehavior::Timestamp))
    }

    pub fn watermark(&mut self, watermark: WatermarkType) -> Stream<T> {
        self.add_node(Operator::Watermark(watermark))
    }

    pub fn count(&mut self) -> Stream<usize> {
        self.add_node(Operator::Count)
    }

    pub fn max<N: Add<Output = N> + Ord + Copy + Data>(&mut self) -> Stream<Option<N>> {
        self.add_node(Operator::Aggregate(AggregateBehavior::Max))
    }

    pub fn min<N: Add<Output = N> + Ord + Copy + Data>(&mut self) -> Stream<Option<N>> {
        self.add_node(Operator::Aggregate(AggregateBehavior::Min))
    }

    pub fn sum<N: Add<Output = N> + Ord + Copy + Data>(&mut self) -> Stream<Option<N>> {
        self.add_node(Operator::Aggregate(AggregateBehavior::Sum))
    }

    pub fn key_by<K: Key, F: BiFunc<(), T, K>>(&mut self, f: F) -> KeyedStream<K, T> {
        let s = self.add_node::<T>(f.as_operator(WasmBehavior::KeyBy));

        KeyedStream {
            _t: PhantomData,
            graph: s.graph.clone(),
            last_node: s.last_node,
            parallelism: s.parallelism,
        }
    }

    pub fn rescale(&mut self, parallelism: usize) -> Stream<T> {
        Stream {
            _t: PhantomData,
            graph: self.graph.clone(),
            last_node: self.last_node,
            parallelism,
        }
    }

    pub fn sink<S: KeyedSink<(), T>>(&mut self, s: S) -> Stream<()> {
        self.add_node(s.as_operator())
    }

    pub fn into_program(self) -> Program {
        Program {
            types: vec![],
            other_defs: vec![],
            graph: self.graph.take(),
        }
    }
}

pub struct KeyedStream<K: Key, T: Data> {
    _t: PhantomData<(K, T)>,
    graph: Rc<RefCell<DiGraph<StreamNode, StreamEdge>>>,
    last_node: Option<NodeIndex>,
    parallelism: usize,
}

pub trait KeyedWindowFun<K: Key, T: Data> {
    fn as_operator(&self) -> Operator;
}

pub struct TumblingWindow<K: Key, T: Data> {
    width: Duration,
    _t: PhantomData<(K, T)>,
}

impl<K: Key, T: Data> TumblingWindow<K, T> {
    pub fn new(width: Duration) -> TumblingWindow<K, T> {
        TumblingWindow {
            width,
            _t: PhantomData,
        }
    }
}

impl<K: Key, T: Data> KeyedWindowFun<K, T> for TumblingWindow<K, T> {
    fn as_operator(&self) -> Operator {
        Operator::Window {
            typ: WindowType::Tumbling { width: self.width },
            agg: None,
            flatten: false,
        }
    }
}

pub struct SlidingWindow<K: Key, T: Data> {
    width: Duration,
    slide: Duration,
    _t: PhantomData<(K, T)>,
}

impl<K: Key, T: Data> SlidingWindow<K, T> {
    pub fn new(width: Duration, slide: Duration) -> SlidingWindow<K, T> {
        SlidingWindow {
            width,
            slide,
            _t: PhantomData,
        }
    }
}

impl<K: Key, T: Data> KeyedWindowFun<K, T> for SlidingWindow<K, T> {
    fn as_operator(&self) -> Operator {
        Operator::Window {
            typ: WindowType::Sliding {
                width: self.width,
                slide: self.slide,
            },
            agg: None,
            flatten: false,
        }
    }
}

pub struct InstantWindow<K: Key, T: Data> {
    _t: PhantomData<(K, T)>,
}

impl<K: Key, T: Data> InstantWindow<K, T> {
    pub fn new() -> InstantWindow<K, T> {
        InstantWindow { _t: PhantomData }
    }
}

impl<K: Key, T: Data> Default for InstantWindow<K, T> {
    fn default() -> Self {
        Self {
            _t: Default::default(),
        }
    }
}

impl<K: Key, T: Data> KeyedWindowFun<K, T> for InstantWindow<K, T> {
    fn as_operator(&self) -> Operator {
        Operator::Window {
            typ: WindowType::Instant,
            agg: None,
            flatten: false,
        }
    }
}

pub trait KeyedSink<K: Key, T: Data> {
    fn as_operator(&self) -> Operator;
}

pub struct KeyedConsoleSink<K: Key, T: Data> {
    _t: PhantomData<(K, T)>,
}

impl<K: Key, T: Data> KeyedConsoleSink<K, T> {
    pub fn new() -> KeyedConsoleSink<K, T> {
        KeyedConsoleSink { _t: PhantomData }
    }
}

impl<K: Key, T: Data> KeyedSink<K, T> for KeyedConsoleSink<K, T> {
    fn as_operator(&self) -> Operator {
        Operator::ConsoleSink
    }
}

pub struct KeyedGrpcSink<K: Key, T: Data> {
    _t: PhantomData<(K, T)>,
}

impl<K: Key, T: Data> KeyedGrpcSink<K, T> {
    pub fn new() -> Self {
        Self { _t: PhantomData }
    }
}

impl<K: Key, T: Data> KeyedSink<K, T> for KeyedGrpcSink<K, T> {
    fn as_operator(&self) -> Operator {
        Operator::GrpcSink
    }
}

pub struct KeyedFileSink<K: Key, T: Data> {
    _t: PhantomData<(K, T)>,
    dir: PathBuf,
}

impl<K: Key, T: Data> KeyedFileSink<K, T> {
    pub fn new(dir: PathBuf) -> KeyedFileSink<K, T> {
        KeyedFileSink {
            _t: PhantomData,
            dir,
        }
    }
}

impl<K: Key, T: Data> KeyedSink<K, T> for KeyedFileSink<K, T> {
    fn as_operator(&self) -> Operator {
        Operator::FileSink {
            dir: self.dir.clone(),
        }
    }
}

pub struct KeyedKafkaSink {
    topic: String,
    bootstrap_servers: Vec<String>,
}

impl KeyedKafkaSink {
    pub fn new(topic: String, bootstrap_servers: Vec<String>) -> KeyedKafkaSink {
        KeyedKafkaSink {
            topic,
            bootstrap_servers,
        }
    }
}
impl KeyedSink<Vec<u8>, Vec<u8>> for KeyedKafkaSink {
    fn as_operator(&self) -> Operator {
        Operator::KafkaSink {
            topic: self.topic.clone(),
            bootstrap_servers: self.bootstrap_servers.clone(),
            client_configs: HashMap::default(),
        }
    }
}

pub struct NullSink<K: Key, T: Data> {
    _t: PhantomData<(K, T)>,
}

impl<K: Key, T: Data> NullSink<K, T> {
    pub fn new() -> NullSink<K, T> {
        NullSink { _t: PhantomData }
    }
}

impl<K: Key, T: Data> KeyedSink<K, T> for NullSink<K, T> {
    fn as_operator(&self) -> Operator {
        Operator::NullSink
    }
}

impl<K: Key, T: Data> KeyedStream<K, T> {
    fn add_node<K2: Key, T2: Data>(
        &mut self,
        operator: Operator,
        mut edge: EdgeType,
    ) -> KeyedStream<K2, T2> {
        let index = {
            let mut graph = (*self.graph).borrow_mut();
            let count = graph.node_count();

            graph.add_node(StreamNode {
                operator_id: format!("node_{}", count),
                operator,
                parallelism: self.parallelism,
            })
        };

        let last_parallelism = (*self.graph)
            .borrow()
            .node_weight(self.last_node.expect("graph must start with a source node"))
            .unwrap()
            .parallelism;

        if last_parallelism != self.parallelism {
            edge = EdgeType::Shuffle;
        }

        let edge = StreamEdge {
            key: std::any::type_name::<K>().to_string(),
            value: std::any::type_name::<T>().to_string(),
            typ: edge,
        };

        (*self.graph).borrow_mut().add_edge(
            self.last_node.expect("graph must start with a source node"),
            index,
            edge,
        );

        KeyedStream {
            _t: PhantomData,
            graph: self.graph.clone(),
            last_node: Some(index),
            parallelism: self.parallelism,
        }
    }

    pub fn map<T2: Data, F: BiFunc<K, T, T2>>(&mut self, f: F) -> KeyedStream<K, T2> {
        self.add_node(f.as_operator(WasmBehavior::Map), EdgeType::Forward)
    }

    pub fn opt_map<T2: Data, F: BiFunc<K, T, Option<T2>>>(&mut self, f: F) -> KeyedStream<K, T2> {
        self.add_node(f.as_operator(WasmBehavior::OptMap), EdgeType::Forward)
    }

    pub fn filter<F: BiFunc<K, T, bool>>(&mut self, f: F) -> KeyedStream<K, T> {
        self.add_node(f.as_operator(WasmBehavior::Filter), EdgeType::Forward)
    }

    pub fn timestamp<F: BiFunc<K, T, SystemTime>>(&mut self, f: F) -> KeyedStream<K, T> {
        self.add_node(f.as_operator(WasmBehavior::Timestamp), EdgeType::Forward)
    }

    pub fn key_by<K2: Key, F: BiFunc<K, T, K2>>(&mut self, f: F) -> KeyedStream<K2, T> {
        self.add_node(f.as_operator(WasmBehavior::KeyBy), EdgeType::Forward)
    }

    pub fn count(&mut self) -> KeyedStream<K, usize> {
        self.add_node(Operator::Count, EdgeType::Forward)
    }

    pub fn max<N: Add<Output = N> + Ord + Copy + Data>(&mut self) -> KeyedStream<K, Option<N>> {
        self.add_node(
            Operator::Aggregate(AggregateBehavior::Max),
            EdgeType::Forward,
        )
    }

    pub fn min<N: Add<Output = N> + Ord + Copy + Data>(&mut self) -> KeyedStream<K, Option<N>> {
        self.add_node(
            Operator::Aggregate(AggregateBehavior::Min),
            EdgeType::Forward,
        )
    }

    pub fn sum<N: Add<Output = N> + Ord + Copy + Data>(&mut self) -> KeyedStream<K, Option<N>> {
        self.add_node(
            Operator::Aggregate(AggregateBehavior::Sum),
            EdgeType::Forward,
        )
    }

    pub fn global_key(&mut self) -> KeyedStream<GlobalKey, T> {
        self.add_node(Operator::GlobalKey, EdgeType::Forward)
    }

    pub fn window<W: KeyedWindowFun<K, T>>(&mut self, w: W) -> KeyedStream<K, Vec<T>> {
        self.add_node(w.as_operator(), EdgeType::Shuffle)
    }

    pub fn sink<S: KeyedSink<K, T>>(&mut self, s: S) -> KeyedStream<(), ()> {
        self.add_node(s.as_operator(), EdgeType::Forward)
    }

    pub fn rescale(&mut self, parallelism: usize) -> KeyedStream<K, T> {
        KeyedStream {
            _t: PhantomData,
            graph: self.graph.clone(),
            last_node: self.last_node,
            parallelism,
        }
    }

    pub fn window_join<T2: Data, W: KeyedWindowFun<K, T>>(
        &mut self,
        other: KeyedStream<K, T2>,
        window: W,
    ) -> KeyedStream<K, (Vec<T>, Vec<T2>)> {
        let idx_map = if Rc::ptr_eq(&self.graph, &other.graph) {
            // TODO: handle the case that the indexes are potentially out of order
            assert!(self.last_node.unwrap().index() < other.last_node.unwrap().index());
            None
        } else {
            Some(add_all(
                &mut (*self.graph).borrow_mut(),
                &(*other.graph).borrow(),
            ))
        };

        let join_op = if let Operator::Window { typ, .. } = window.as_operator() {
            Operator::WindowJoin { window: typ }
        } else {
            unreachable!()
        };

        let join_node = StreamNode {
            operator_id: format!("node_{}", (*self.graph).borrow().node_count()),
            operator: join_op,
            parallelism: self.parallelism,
        };

        let new_idx = (*self.graph).borrow_mut().add_node(join_node);

        let edge1 = StreamEdge {
            key: std::any::type_name::<K>().to_string(),
            value: std::any::type_name::<T>().to_string(),
            typ: EdgeType::ShuffleJoin(0),
        };

        let edge2 = StreamEdge {
            key: std::any::type_name::<K>().to_string(),
            value: std::any::type_name::<T2>().to_string(),
            typ: EdgeType::ShuffleJoin(1),
        };

        let node2_idx = idx_map
            .map(|m| *m.get(&other.last_node.unwrap()).unwrap())
            .or(other.last_node)
            .unwrap();
        (*self.graph)
            .borrow_mut()
            .add_edge(self.last_node.unwrap(), new_idx, edge1);
        (*self.graph)
            .borrow_mut()
            .add_edge(node2_idx, new_idx, edge2);

        KeyedStream {
            _t: PhantomData,
            graph: self.graph.clone(),
            last_node: Some(new_idx),
            parallelism: self.parallelism,
        }
    }

    pub fn into_program(self) -> Program {
        Program {
            types: vec![],
            other_defs: vec![],
            graph: self.graph.take(),
        }
    }
}

fn add_all<V: Clone, E: Clone>(
    graph: &mut Graph<V, E>,
    other: &Graph<V, E>,
) -> HashMap<NodeIndex, NodeIndex> {
    let mut idx_map = HashMap::new();
    for idx in other.node_indices() {
        let node = other.node_weight(idx).unwrap();
        idx_map.insert(idx, graph.add_node(node.clone()));
    }

    for idx in other.edge_indices() {
        let (from, to) = other.edge_endpoints(idx).unwrap();
        let edge = other.edge_weight(idx).unwrap();
        graph.add_edge(
            *idx_map.get(&from).unwrap(),
            *idx_map.get(&to).unwrap(),
            edge.clone(),
        );
    }

    idx_map
}

#[derive(Debug)]
pub struct WasmFunc<K: Key, T1: Data, T2: Data, F: Fn(&Option<K>, &T1) -> T2> {
    _t: PhantomData<(K, T1, T2, F)>,
    name: String,
    key_arg: Option<String>,
    value_arg: Option<String>,
    body: String,
}

impl<K: Key, T1: Data, T2: Data, F: Fn(&Option<K>, &T1) -> T2> WasmFunc<K, T1, T2, F> {
    pub fn new(
        name: &str,
        key_arg: Option<&str>,
        value_arg: Option<&str>,
        body: &str,
        _f: F,
    ) -> WasmFunc<K, T1, T2, F> {
        WasmFunc {
            _t: PhantomData,
            key_arg: key_arg.map(|a| a.to_string()),
            value_arg: value_arg.map(|a| a.to_string()),
            name: name.to_string(),
            body: body.to_string(),
        }
    }
}

impl<K: Key, T1: Data, T2: Data, F: Fn(&Option<K>, &T1) -> T2> BiFunc<K, T1, T2>
    for WasmFunc<K, T1, T2, F>
{
    fn as_operator(&self, behavior: WasmBehavior) -> Operator {
        FusedWasmUDFs {
            name: self.name.clone(),
            udfs: vec![WasmUDF {
                behavior,
                def: WasmDef {
                    name: self.name.clone(),
                    key_arg: self
                        .key_arg
                        .as_ref()
                        .cloned()
                        .unwrap_or_else(|| "_".to_string()),
                    key_arg_type: std::any::type_name::<K>().to_string(),
                    value_arg: self
                        .value_arg
                        .as_ref()
                        .cloned()
                        .unwrap_or_else(|| "_".to_string()),
                    value_arg_type: std::any::type_name::<T1>().to_string(),
                    ret_type: std::any::type_name::<T2>().to_string(),
                    body: self.body.clone(),
                },
            }],
        }
    }
}

#[derive(Encode, Decode, Clone, Debug)]
pub struct Program {
    pub types: Vec<String>,
    pub other_defs: Vec<String>,
    #[bincode(with_serde)]
    pub graph: DiGraph<StreamNode, StreamEdge>,
}

impl Program {
    pub fn from_stream<K: Key, T: Data>(s: KeyedStream<K, T>) -> Program {
        Program {
            types: vec![],
            other_defs: vec![],
            graph: s.graph.take(),
        }
    }

    pub fn with_type(mut self, dt: String) -> Program {
        self.types.push(dt);
        self
    }

    pub fn with_other_def(mut self, def: String) -> Program {
        self.other_defs.push(def);
        self
    }

    pub fn dot(&self) -> String {
        format!("{:?}", petgraph::dot::Dot::with_config(&self.graph, &[]))
    }

    pub fn validate_graph(&self) -> Vec<String> {
        let mut errors = vec![];
        // check that if we have a window function, we also have a watermark assigner
        if self.graph.node_weights().any(|n| {
            matches!(n.operator, Operator::Window { .. })
                || matches!(n.operator, Operator::WindowJoin { .. })
        }) && self
            .graph
            .node_weights()
            .all(|n| !matches!(n.operator, Operator::Watermark(..)))
        {
            errors.push(
                "Graph contains window but no watermark assigner; no elements will be produced"
                    .to_string(),
            )
        }

        errors
    }

    pub fn update_parallelism(&mut self, overrides: &HashMap<String, usize>) {
        for node in self.graph.node_weights_mut() {
            if let Some(p) = overrides.get(&node.operator_id) {
                node.parallelism = *p;
            }
        }
    }

    pub fn task_count(&self) -> usize {
        // TODO: this can be cached
        self.graph.node_weights().map(|nw| nw.parallelism).sum()
    }

    pub fn sources(&self) -> HashSet<&str> {
        // TODO: this can be memoized
        self.graph
            .externals(Direction::Incoming)
            .map(|t| self.graph.node_weight(t).unwrap().operator_id.as_str())
            .collect()
    }

    pub fn get_hash(&self) -> String {
        let mut hasher = DefaultHasher::new();
        let bs = bincode::encode_to_vec(self, bincode::config::standard()).unwrap();
        for b in bs {
            hasher.write_u8(b);
        }

        let rng = SmallRng::seed_from_u64(hasher.finish());

        rng.sample_iter(&Alphanumeric)
            .take(16)
            .map(char::from)
            .map(|c| c.to_ascii_lowercase())
            .collect()
    }

    pub async fn run_async(&self, name: &str, restore: Option<String>) -> Result<String, Status> {
        let mut client = arroyo_rpc::api_client().await;

        let checkpoint_interval_micros = u64::from_str(
            &std::env::var("CHECKPOINT_INTERVAL").unwrap_or_else(|_| "10".to_string()),
        )
        .unwrap()
            * 1_000_000;

        if let Some(restore_from) = restore {
            client
                .update_job(Request::new(UpdateJobReq {
                    job_id: restore_from.to_string(),
                    stop: Some(StopType::None as i32),
                    checkpoint_interval_micros: Some(checkpoint_interval_micros),
                    parallelism: None,
                }))
                .await?;
            Ok(restore_from)
        } else {
            let proto_program: PipelineProgram = self
                .clone()
                .try_into()
                .map_err(|_err| Status::internal("failed to convert program to protobuf"))?;
            let res = client
                .create_pipeline(Request::new(CreatePipelineReq {
                    name: name.to_string(),
                    config: Some(Config::Program(proto_program.encode_to_vec())),
                }))
                .await?;

            let res = client
                .create_job(Request::new(CreateJobReq {
                    pipeline_id: res.into_inner().pipeline_id,
                    checkpoint_interval_micros,
                    preview: false,
                }))
                .await?;

            Ok(res.into_inner().job_id)
        }
    }

    pub fn as_job_graph(&self) -> JobGraph {
        let nodes = self
            .graph
            .node_weights()
            .map(|node| JobNode {
                node_id: node.operator_id.to_string(),
                operator: format!("{:?}", node),
                parallelism: node.parallelism as u32,
            })
            .collect();

        let edges = self
            .graph
            .edge_references()
            .map(|edge| {
                let src = self.graph.node_weight(edge.source()).unwrap();
                let target = self.graph.node_weight(edge.target()).unwrap();
                JobEdge {
                    src_id: src.operator_id.to_string(),
                    dest_id: target.operator_id.to_string(),
                    key_type: edge.weight().key.to_string(),
                    value_type: edge.weight().value.to_string(),
                    edge_type: format!("{:?}", edge.weight().typ),
                }
            })
            .collect();

        JobGraph { nodes, edges }
    }

    #[tokio::main]
    pub async fn run(&self, name: &str, restore: Option<String>) -> Result<String, Status> {
        self.run_async(name, restore).await
    }
}

impl TryFrom<Program> for PipelineProgram {
    type Error = anyhow::Error;

    fn try_from(program: Program) -> Result<Self> {
        let nodes: Vec<_> = program
            .graph
            .node_indices()
            .map(|index| {
                let node = program.graph.node_weight(index).unwrap().clone();
                ProgramNode {
                    node_index: index.index() as i32,
                    parallelism: node.parallelism as i32,
                    node_id: node.operator_id,
                    operator: Some(GrpcApi::Operator {
                        operator: Some(node.operator.into()),
                    }),
                }
            })
            .collect();

        let edges = program
            .graph
            .edge_indices()
            .map(|index| {
                let edge = program.graph.edge_weight(index).unwrap().clone();
                let (upstream, downstream) = program.graph.edge_endpoints(index).unwrap();
                ProgramEdge {
                    upstream_node: upstream.index() as i32,
                    downstream_node: downstream.index() as i32,
                    key_type: edge.key,
                    value_type: edge.value,
                    edge_type: match edge.typ {
                        EdgeType::Forward => GrpcApi::EdgeType::Forward,
                        EdgeType::Shuffle => GrpcApi::EdgeType::Shuffle,
                        EdgeType::ShuffleJoin(0) => GrpcApi::EdgeType::LeftJoin,
                        EdgeType::ShuffleJoin(1) => GrpcApi::EdgeType::RightJoin,
                        _ => todo!(),
                    }
                    .into(),
                }
            })
            .collect();

        Ok(PipelineProgram {
            types: program.types,
            other_defs: program.other_defs,
            nodes,
            edges,
        })
    }
}

impl From<Operator> for GrpcApi::operator::Operator {
    fn from(operator: Operator) -> Self {
        match operator {
            Operator::FileSource { dir, delay } => GrpcOperator::FileSource(GrpcApi::FileSource {
                dir: dir.to_string_lossy().to_string(),
                micros_delay: delay.as_micros() as u64,
            }),
            Operator::ImpulseSource {
                start_time,
                spec,
                total_events,
            } => GrpcOperator::ImpulseSource(GrpcApi::ImpulseSource {
                micros_start: to_micros(start_time),
                total_events: total_events.map(|total| total as u64),
                spec: Some(spec.into()),
            }),
            Operator::KafkaSource {
                topic,
                bootstrap_servers,
                offset_mode,
                schema_registry,
                messages_per_second,
                client_configs,
            } => GrpcOperator::KafkaSource(GrpcApi::KafkaSource {
                topic,
                bootstrap_servers,
                offset_mode: match offset_mode {
                    OffsetMode::Earliest => GrpcApi::OffsetMode::Earliest.into(),
                    OffsetMode::Latest => GrpcApi::OffsetMode::Latest.into(),
                },
                schema_registry,
                messages_per_second,
                client_configs,
            }),
            FusedWasmUDFs { name, udfs } => GrpcOperator::WasmUdfs(GrpcApi::WasmUdfs {
                name,
                wasm_functions: udfs.into_iter().map(|udf| udf.into()).collect(),
            }),
            Operator::Window { typ, agg, flatten } => {
                GrpcOperator::Window(GrpcApi::WindowOperator {
                    aggregator: match &agg {
                        Some(WindowAgg::Count) => Some(GrpcApi::Aggregator::CountAggregate.into()),
                        Some(WindowAgg::Max) => Some(GrpcApi::Aggregator::MaxAggregate.into()),
                        Some(WindowAgg::Min) => Some(GrpcApi::Aggregator::MinAggregate.into()),
                        Some(WindowAgg::Sum) => Some(GrpcApi::Aggregator::SumAggregate.into()),
                        Some(WindowAgg::Expression { .. }) => None,
                        None => None,
                    },
                    expression_aggregator: match agg {
                        Some(WindowAgg::Expression { name, expression }) => {
                            Some(GrpcApi::ExpressionAggregator { name, expression })
                        }
                        _ => None,
                    },
                    flatten,
                    window: Some(GrpcApi::Window {
                        window: Some(typ.into()),
                    }),
                })
            }
            Operator::Count => GrpcOperator::Aggregator(GrpcApi::Aggregator::CountAggregate.into()),
            Operator::Aggregate(AggregateBehavior::Min) => {
                GrpcOperator::Aggregator(GrpcApi::Aggregator::MinAggregate.into())
            }
            Operator::Aggregate(AggregateBehavior::Max) => {
                GrpcOperator::Aggregator(GrpcApi::Aggregator::MaxAggregate.into())
            }
            Operator::Aggregate(AggregateBehavior::Sum) => {
                GrpcOperator::Aggregator(GrpcApi::Aggregator::SumAggregate.into())
            }
            Operator::Watermark(WatermarkType::Periodic {
                period,
                max_lateness,
            }) => GrpcOperator::PeriodicWatermark(GrpcApi::PeriodicWatermark {
                period_micros: period.as_micros() as u64,
                max_lateness_micros: max_lateness.as_micros() as u64,
            }),
            Operator::GlobalKey => todo!(),
            Operator::ConsoleSink => GrpcOperator::BuiltinSink(GrpcApi::BuiltinSink::Log.into()),
            Operator::GrpcSink => GrpcOperator::BuiltinSink(GrpcApi::BuiltinSink::Web.into()),
            Operator::NullSink => GrpcOperator::BuiltinSink(GrpcApi::BuiltinSink::Null.into()),
            Operator::FileSink { dir } => GrpcOperator::FileSink(GrpcApi::FileSink {
                file_path: dir.to_string_lossy().to_string(),
            }),
            Operator::WindowJoin { window } => GrpcOperator::WindowJoin(GrpcApi::Window {
                window: Some(window.into()),
            }),
            Operator::KafkaSink {
                topic,
                bootstrap_servers,
                client_configs,
            } => GrpcOperator::KafkaSink(GrpcApi::KafkaSink {
                topic,
                bootstrap_servers,
                client_configs,
            }),
            Operator::NexmarkSource {
                first_event_rate,
                num_events: total_events,
            } => GrpcOperator::NexmarkSource(GrpcApi::NexmarkSource {
                first_event_rate,
                total_events,
            }),
            Operator::ExpressionOperator {
                name,
                expression,
                return_type,
            } => GrpcOperator::ExpressionOperator(GrpcApi::ExpressionOperator {
                name,
                expression,
                return_type: return_type.into(),
            }),
            Operator::FlattenOperator { name } => GrpcOperator::Flatten(Flatten { name }),
            Operator::FlatMapOperator {
                name,
                expression,
                return_type,
            } => GrpcOperator::FlattenExpressionOperator(GrpcApi::FlattenExpressionOperator {
                name,
                expression,
                return_type: return_type.into(),
            }),
            Operator::SlidingWindowAggregator(SlidingWindowAggregator {
                width,
                slide,
                aggregator,
                bin_merger,
                in_memory_add,
                in_memory_remove,
                bin_type,
                mem_type,
            }) => GrpcOperator::SlidingWindowAggregator(GrpcApi::SlidingWindowAggregator {
                width_micros: width.as_micros() as u64,
                slide_micros: slide.as_micros() as u64,
                aggregator,
                bin_merger,
                in_memory_add,
                in_memory_remove,
                bin_type,
                mem_type,
            }),
            Operator::TumblingWindowAggregator(TumblingWindowAggregator {
                width,
                aggregator,
                bin_merger,
                bin_type,
            }) => GrpcOperator::TumblingWindowAggregator(GrpcApi::TumblingWindowAggregator {
                width_micros: width.as_micros() as u64,
                aggregator,
                bin_merger,
                bin_type,
            }),
            Operator::TumblingTopN(TumblingTopN {
                width,
                max_elements,
                extractor,
                partition_key_type,
                converter,
            }) => GrpcOperator::TumblingTopN(GrpcApi::TumblingTopN {
                width_micros: width.as_micros() as u64,
                max_elements: max_elements as u64,
                extractor,
                partition_key_type,
                converter,
            }),
            Operator::SlidingAggregatingTopN(SlidingAggregatingTopN {
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
            }) => GrpcOperator::SlidingAggregatingTopN(GrpcApi::SlidingAggregatingTopN {
                width_micros: width.as_micros() as u64,
                slide_micros: slide.as_micros() as u64,
                bin_merger,
                in_memory_add,
                in_memory_remove,
                partitioning_func,
                extractor,
                aggregator,
                bin_type,
                mem_type,
                sort_key_type,
                max_elements: max_elements as u64,
            }),
        }
    }
}

impl From<ImpulseSpec> for GrpcApi::impulse_source::Spec {
    fn from(spec: ImpulseSpec) -> Self {
        match spec {
            ImpulseSpec::Delay(delay) => Spec::MicrosDelay(delay.as_micros() as u64),
            ImpulseSpec::EventsPerSecond(events_qops) => Spec::EventQps(events_qops),
        }
    }
}

impl From<WasmUDF> for WasmFunction {
    fn from(udf: WasmUDF) -> Self {
        WasmFunction {
            behavior: udf.behavior.into(),
            name: udf.def.name,
            key_arg: udf.def.key_arg,
            key_arg_type: udf.def.key_arg_type,
            value_arg: udf.def.value_arg,
            value_arg_type: udf.def.value_arg_type,
            return_type: udf.def.ret_type,
            body: udf.def.body,
        }
    }
}

impl From<ExpressionReturnType> for i32 {
    fn from(return_type: ExpressionReturnType) -> Self {
        match return_type {
            ExpressionReturnType::Predicate => GrpcApi::ExpressionReturnType::Predicate.into(),
            ExpressionReturnType::Record => GrpcApi::ExpressionReturnType::Record.into(),
            ExpressionReturnType::OptionalRecord => {
                GrpcApi::ExpressionReturnType::OptionalRecord.into()
            }
        }
    }
}

impl From<WasmBehavior> for i32 {
    fn from(agg: WasmBehavior) -> Self {
        match agg {
            WasmBehavior::Map => GrpcApi::WasmBehavior::Map.into(),
            WasmBehavior::OptMap => GrpcApi::WasmBehavior::OptMap.into(),
            WasmBehavior::Filter => GrpcApi::WasmBehavior::Filter.into(),
            WasmBehavior::Timestamp => GrpcApi::WasmBehavior::Timestamp.into(),
            WasmBehavior::KeyBy => GrpcApi::WasmBehavior::KeyBy.into(),
        }
    }
}

impl From<WindowType> for GrpcApi::window::Window {
    fn from(window_type: WindowType) -> Self {
        match window_type {
            WindowType::Tumbling { width } => {
                GrpcApi::window::Window::TumblingWindow(GrpcApi::TumblingWindow {
                    size_micros: width.as_micros() as u64,
                })
            }
            WindowType::Sliding { width, slide } => {
                GrpcApi::window::Window::SlidingWindow(GrpcApi::SlidingWindow {
                    size_micros: width.as_micros() as u64,
                    slide_micros: slide.as_micros() as u64,
                })
            }
            WindowType::Instant => {
                GrpcApi::window::Window::InstantWindow(GrpcApi::InstantWindow {})
            }
        }
    }
}

impl TryFrom<PipelineProgram> for Program {
    type Error = anyhow::Error;

    fn try_from(program: PipelineProgram) -> Result<Self> {
        let mut graph = DiGraph::with_capacity(program.nodes.len(), program.edges.len());
        let types = program.types;
        let other_defs = program.other_defs;
        let mut nodes: Vec<_> = vec![];
        for node in program.nodes {
            let node_pair = (
                StreamNode {
                    operator_id: node.node_id,
                    operator: node
                        .operator
                        .ok_or_else(|| anyhow!("missing operator on program node"))?
                        .try_into()?,
                    parallelism: node.parallelism as usize,
                },
                node.node_index,
            );
            nodes.push(node_pair);
        }
        nodes.sort_by_key(|(_node, index)| *index);
        let mut node_indices = Vec::new();
        for (node, _) in nodes {
            node_indices.push(graph.add_node(node));
        }
        for edge in program.edges {
            graph.add_edge(
                node_indices[edge.upstream_node as usize],
                node_indices[edge.downstream_node as usize],
                edge.into(),
            );
        }
        Ok(Program {
            types,
            other_defs,
            graph,
        })
    }
}

impl TryFrom<arroyo_rpc::grpc::api::Operator> for Operator {
    type Error = anyhow::Error;

    fn try_from(operator: arroyo_rpc::grpc::api::Operator) -> Result<Self> {
        let result = match operator.operator {
            Some(operator) => match operator {
                GrpcOperator::FileSource(file_source) => Operator::FileSource {
                    dir: PathBuf::from(&file_source.dir).canonicalize()?,
                    delay: Duration::from_micros(file_source.micros_delay),
                },
                GrpcOperator::ImpulseSource(impulse_source) => {
                    let spec = match impulse_source.spec {
                        Some(Spec::MicrosDelay(micros_delay)) => {
                            ImpulseSpec::Delay(Duration::from_micros(micros_delay))
                        }
                        Some(Spec::EventQps(event_qps)) => ImpulseSpec::EventsPerSecond(event_qps),
                        None => bail!("impulse source {:?} missing spec", impulse_source),
                    };
                    Operator::ImpulseSource {
                        start_time: from_micros(impulse_source.micros_start),
                        spec,
                        total_events: impulse_source.total_events.map(|events| events as usize),
                    }
                }
                GrpcOperator::KafkaSource(kafka_source) => {
                    let offset_mode = kafka_source.offset_mode().into();
                    Operator::KafkaSource {
                        topic: kafka_source.topic,
                        bootstrap_servers: kafka_source.bootstrap_servers,
                        offset_mode,
                        schema_registry: kafka_source.schema_registry,
                        messages_per_second: kafka_source.messages_per_second,
                        client_configs: kafka_source.client_configs,
                    }
                }
                GrpcOperator::WasmUdfs(wasm_udfs) => Operator::FusedWasmUDFs {
                    name: wasm_udfs.name,
                    udfs: wasm_udfs
                        .wasm_functions
                        .iter()
                        .map(|func| func.clone().into())
                        .collect(),
                },
                GrpcOperator::Window(window) => {
                    let mut agg = match window.aggregator() {
                        arroyo_rpc::grpc::api::Aggregator::None => None,
                        arroyo_rpc::grpc::api::Aggregator::CountAggregate => Some(WindowAgg::Count),
                        arroyo_rpc::grpc::api::Aggregator::MaxAggregate => Some(WindowAgg::Max),
                        arroyo_rpc::grpc::api::Aggregator::MinAggregate => Some(WindowAgg::Min),
                        arroyo_rpc::grpc::api::Aggregator::SumAggregate => Some(WindowAgg::Sum),
                    };

                    if let Some(ExpressionAggregator { name, expression }) =
                        window.expression_aggregator
                    {
                        agg = Some(WindowAgg::Expression { name, expression });
                    }

                    Operator::Window {
                        typ: window
                            .window
                            .ok_or_else(|| anyhow!("missing window type"))?
                            .into(),
                        agg,
                        flatten: window.flatten,
                    }
                }
                GrpcOperator::Aggregator(agg) => {
                    match Aggregator::from_i32(agg).ok_or_else(|| anyhow!("unable to map enum"))? {
                        Aggregator::None => panic!(),
                        Aggregator::CountAggregate => Operator::Count,
                        Aggregator::MaxAggregate => Operator::Aggregate(AggregateBehavior::Max),
                        Aggregator::MinAggregate => Operator::Aggregate(AggregateBehavior::Min),
                        Aggregator::SumAggregate => Operator::Aggregate(AggregateBehavior::Sum),
                    }
                }
                GrpcOperator::PeriodicWatermark(watermark) => {
                    Operator::Watermark(WatermarkType::Periodic {
                        period: Duration::from_micros(watermark.period_micros),
                        max_lateness: Duration::from_micros(watermark.max_lateness_micros),
                    })
                }
                GrpcOperator::BuiltinSink(sink) => {
                    match BuiltinSink::from_i32(sink)
                        .ok_or_else(|| anyhow!("unable to map enum"))?
                    {
                        BuiltinSink::Null => Operator::NullSink,
                        BuiltinSink::Web => Operator::GrpcSink,
                        BuiltinSink::Log => Operator::ConsoleSink,
                    }
                }
                GrpcOperator::FileSink(file) => Operator::FileSink {
                    dir: PathBuf::from(file.file_path),
                },
                GrpcOperator::WindowJoin(window) => Operator::WindowJoin {
                    window: window.into(),
                },
                GrpcOperator::KafkaSink(kafka_sink) => Operator::KafkaSink {
                    topic: kafka_sink.topic,
                    bootstrap_servers: kafka_sink.bootstrap_servers,
                    client_configs: kafka_sink.client_configs,
                },
                GrpcOperator::NexmarkSource(nexmark_source) => Operator::NexmarkSource {
                    first_event_rate: nexmark_source.first_event_rate,
                    num_events: nexmark_source.total_events,
                },
                GrpcOperator::ExpressionOperator(expression_operator) => {
                    let return_type = expression_operator.return_type().into();
                    Operator::ExpressionOperator {
                        name: expression_operator.name,
                        expression: expression_operator.expression,
                        return_type,
                    }
                }
                GrpcOperator::Flatten(Flatten { name }) => Operator::FlattenOperator { name },
                GrpcOperator::FlattenExpressionOperator(flatten_expression) => {
                    let return_type = flatten_expression.return_type().into();
                    Operator::FlatMapOperator {
                        name: flatten_expression.name,
                        expression: flatten_expression.expression,
                        return_type,
                    }
                }
                GrpcOperator::SlidingWindowAggregator(GrpcApi::SlidingWindowAggregator {
                    width_micros,
                    slide_micros,
                    aggregator,
                    bin_merger,
                    in_memory_add,
                    in_memory_remove,
                    bin_type,
                    mem_type,
                }) => Operator::SlidingWindowAggregator(SlidingWindowAggregator {
                    width: Duration::from_micros(width_micros),
                    slide: Duration::from_micros(slide_micros),
                    aggregator,
                    bin_merger,
                    in_memory_add,
                    in_memory_remove,
                    bin_type,
                    mem_type,
                }),
                GrpcOperator::TumblingWindowAggregator(GrpcApi::TumblingWindowAggregator {
                    width_micros,
                    aggregator,
                    bin_merger,
                    bin_type,
                }) => Operator::TumblingWindowAggregator(TumblingWindowAggregator {
                    width: Duration::from_micros(width_micros),
                    aggregator,
                    bin_merger,
                    bin_type,
                }),
                GrpcOperator::TumblingTopN(GrpcApi::TumblingTopN {
                    width_micros,
                    max_elements,
                    extractor,
                    partition_key_type,
                    converter,
                }) => Operator::TumblingTopN(TumblingTopN {
                    width: Duration::from_micros(width_micros),
                    max_elements: max_elements as usize,
                    extractor,
                    partition_key_type,
                    converter,
                }),
                GrpcOperator::SlidingAggregatingTopN(GrpcApi::SlidingAggregatingTopN {
                    width_micros,
                    slide_micros,
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
                }) => Operator::SlidingAggregatingTopN(SlidingAggregatingTopN {
                    width: Duration::from_micros(width_micros),
                    slide: Duration::from_micros(slide_micros),
                    bin_merger,
                    in_memory_add,
                    in_memory_remove,
                    partitioning_func,
                    extractor,
                    aggregator,
                    bin_type,
                    mem_type,
                    sort_key_type,
                    max_elements: max_elements as usize,
                }),
            },
            None => bail!("unset on operator {:?}", operator),
        };
        Ok(result)
    }
}

impl From<WasmFunction> for WasmUDF {
    fn from(wasm_function: WasmFunction) -> Self {
        WasmUDF {
            behavior: wasm_function.behavior().into(),
            def: WasmDef {
                name: wasm_function.name,
                key_arg: wasm_function.key_arg,
                key_arg_type: wasm_function.key_arg_type,
                value_arg: wasm_function.value_arg,
                value_arg_type: wasm_function.value_arg_type,
                ret_type: wasm_function.return_type,
                body: wasm_function.body,
            },
        }
    }
}
impl From<arroyo_rpc::grpc::api::WasmBehavior> for WasmBehavior {
    fn from(wasm_behavior: arroyo_rpc::grpc::api::WasmBehavior) -> Self {
        match wasm_behavior {
            arroyo_rpc::grpc::api::WasmBehavior::Map => Self::Map,
            arroyo_rpc::grpc::api::WasmBehavior::OptMap => Self::OptMap,
            arroyo_rpc::grpc::api::WasmBehavior::Filter => Self::Filter,
            arroyo_rpc::grpc::api::WasmBehavior::Timestamp => Self::Timestamp,
            arroyo_rpc::grpc::api::WasmBehavior::KeyBy => Self::KeyBy,
        }
    }
}

impl From<arroyo_rpc::grpc::api::Window> for WindowType {
    fn from(window: arroyo_rpc::grpc::api::Window) -> Self {
        match window.window {
            Some(arroyo_rpc::grpc::api::window::Window::SlidingWindow(sliding_window)) => {
                WindowType::Sliding {
                    width: Duration::from_micros(sliding_window.size_micros),
                    slide: Duration::from_micros(sliding_window.slide_micros),
                }
            }
            Some(arroyo_rpc::grpc::api::window::Window::TumblingWindow(tumbling_window)) => {
                WindowType::Tumbling {
                    width: Duration::from_micros(tumbling_window.size_micros),
                }
            }
            Some(arroyo_rpc::grpc::api::window::Window::InstantWindow(_)) => WindowType::Instant,
            None => todo!(),
        }
    }
}

impl From<arroyo_rpc::grpc::api::ExpressionReturnType> for ExpressionReturnType {
    fn from(return_type: arroyo_rpc::grpc::api::ExpressionReturnType) -> Self {
        match return_type {
            arroyo_rpc::grpc::api::ExpressionReturnType::UnusedErt => panic!(),
            arroyo_rpc::grpc::api::ExpressionReturnType::Predicate => {
                ExpressionReturnType::Predicate
            }
            arroyo_rpc::grpc::api::ExpressionReturnType::Record => ExpressionReturnType::Record,
            arroyo_rpc::grpc::api::ExpressionReturnType::OptionalRecord => {
                ExpressionReturnType::OptionalRecord
            }
        }
    }
}

impl From<arroyo_rpc::grpc::api::ProgramEdge> for StreamEdge {
    fn from(edge: arroyo_rpc::grpc::api::ProgramEdge) -> Self {
        let typ = match &edge.edge_type() {
            arroyo_rpc::grpc::api::EdgeType::Unused => unreachable!(),
            arroyo_rpc::grpc::api::EdgeType::Forward => EdgeType::Forward,
            arroyo_rpc::grpc::api::EdgeType::Shuffle => EdgeType::Shuffle,
            arroyo_rpc::grpc::api::EdgeType::LeftJoin => EdgeType::ShuffleJoin(0),
            arroyo_rpc::grpc::api::EdgeType::RightJoin => EdgeType::ShuffleJoin(1),
        };
        StreamEdge {
            key: edge.key_type,
            value: edge.value_type,
            typ,
        }
    }
}
