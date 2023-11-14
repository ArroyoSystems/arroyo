#![allow(clippy::new_without_default)]
#![allow(clippy::comparison_chain)]

use std::cell::RefCell;
use std::collections::hash_map::DefaultHasher;
use std::collections::{HashMap, HashSet};
use std::fmt::{Debug, Formatter};
use std::hash::Hasher;
use std::marker::PhantomData;
use std::ops::Add;
use std::rc::Rc;
use std::time::{Duration, SystemTime};

use arroyo_rpc::grpc::api::operator::Operator as GrpcOperator;
use arroyo_rpc::grpc::api::{self as GrpcApi, ExpressionAggregator, Flatten, ProgramEdge};
use arroyo_types::{Data, GlobalKey, JoinType, Key};
use bincode::{Decode, Encode};
use petgraph::graph::{DiGraph, NodeIndex};
use petgraph::visit::EdgeRef;
use proc_macro2::Ident;
use quote::format_ident;
use quote::quote;
use serde::{Deserialize, Serialize};
use syn::{parse_quote, parse_str, GenericArgument, PathArguments, Type, TypePath};

use anyhow::{anyhow, bail, Result};

use crate::Operator::FusedWasmUDFs;
use arroyo_rpc::grpc::api::{
    Aggregator, JobEdge, JobGraph, JobNode, PipelineProgram, PipelineProgramUdf, ProgramNode,
    WasmFunction,
};
use petgraph::{Direction, Graph};
use rand::distributions::Alphanumeric;
use rand::rngs::SmallRng;
use rand::{Rng, SeedableRng};
use regex::Regex;

pub fn parse_type(s: &str) -> Type {
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

// quote a duration as a syn::Expr
pub fn duration_to_syn_expr(duration: Duration) -> syn::Expr {
    let secs = duration.as_secs();
    let nanos = duration.subsec_nanos();

    parse_quote!(std::time::Duration::new(#secs, #nanos))
}

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

#[derive(Debug, Clone, Encode, Decode, Serialize, Deserialize, PartialEq, Eq)]
pub struct PeriodicWatermark {
    pub period: Duration,
    pub idle_time: Option<Duration>,
    pub strategy: WatermarkStrategy,
}

#[derive(Debug, Clone, Encode, Decode, Serialize, Deserialize, PartialEq, Eq)]
pub enum WatermarkStrategy {
    FixedLateness { max_lateness: Duration },
    Expression { expression: String },
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

#[derive(Clone, Encode, Decode, Serialize, Deserialize, PartialEq)]
pub struct ConnectorOp {
    // path of the operator that this will compile into (like `crate::sources::kafka::KafkaSource`)
    pub operator: String,
    // json-encoded config for the operator
    pub config: String,
    // description to be rendered in the pipeline graph
    pub description: String,
}

impl ConnectorOp {
    pub fn web_sink() -> Self {
        ConnectorOp {
            operator: "GrpcSink::<#in_k, #in_t>".to_string(),
            config: "{}".to_string(),
            description: "WebSink".to_string(),
        }
    }
}

impl From<GrpcApi::ConnectorOp> for ConnectorOp {
    fn from(c: GrpcApi::ConnectorOp) -> Self {
        ConnectorOp {
            operator: c.operator,
            config: c.config,
            description: c.description,
        }
    }
}

impl From<ConnectorOp> for GrpcApi::ConnectorOp {
    fn from(c: ConnectorOp) -> Self {
        GrpcApi::ConnectorOp {
            operator: c.operator,
            config: c.config,
            description: c.description,
        }
    }
}

#[derive(Clone, Encode, Decode, Serialize, Deserialize, PartialEq)]
pub enum Operator {
    ConnectorSource(ConnectorOp),
    ConnectorSink(ConnectorOp),
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
    Watermark(PeriodicWatermark),
    GlobalKey,
    WindowJoin {
        window: WindowType,
    },
    ExpressionOperator {
        name: String,
        expression: String,
        return_type: ExpressionReturnType,
    },
    FlattenOperator {
        name: String,
    },
    ArrayMapOperator {
        name: String,
        expression: String,
        return_type: ExpressionReturnType,
    },
    FlatMapOperator {
        name: String,
        expression: String,
    },
    SlidingWindowAggregator(SlidingWindowAggregator),
    TumblingWindowAggregator(TumblingWindowAggregator),
    TumblingTopN(TumblingTopN),
    SlidingAggregatingTopN(SlidingAggregatingTopN),
    JoinWithExpiration {
        left_expiration: Duration,
        right_expiration: Duration,
        join_type: JoinType,
    },
    UpdatingOperator {
        name: String,
        expression: String,
    },
    NonWindowAggregator(NonWindowAggregator),
    UpdatingKeyOperator {
        name: String,
        expression: String,
    },
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
            Operator::ConnectorSource(c) | Operator::ConnectorSink(c) => {
                write!(f, "{}", c.description)
            }
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
            Operator::Watermark(_) => write!(f, "Watermark"),
            Operator::WindowJoin { window } => write!(f, "WindowJoin({:?})", window),
            Operator::GlobalKey => write!(f, "GlobalKey"),
            Operator::FlattenOperator { name } => write!(f, "flatten<{}>", name),
            Operator::ExpressionOperator {
                name,
                expression: _,
                return_type,
            } => write!(f, "expression<{}:{:?}>", name, return_type),
            Operator::ArrayMapOperator {
                name,
                expression: _,
                return_type,
            } => write!(f, "array_map<{}:{:?}>", name, return_type),
            Operator::FlatMapOperator {
                name,
                expression: _,
            } => write!(f, "flat_map<{}>", name),
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
            Operator::JoinWithExpiration {
                left_expiration,
                right_expiration,
                join_type,
            } => write!(
                f,
                "JoinWithExpiration<left_expire: {:?}, right_expire: {:?}, join_type: {:?}>",
                left_expiration, right_expiration, join_type
            ),
            Operator::UpdatingOperator {
                name,
                expression: _,
            } => write!(f, "updating<{}>", name,),
            Operator::NonWindowAggregator(_) => write!(f, "NonWindowAggregator"),
            Operator::UpdatingKeyOperator {
                name,
                expression: _,
            } => write!(f, "updating_key<{}>", name),
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

    pub fn watermark(&mut self, watermark: PeriodicWatermark) -> Stream<T> {
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
            udfs: vec![],
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
            udfs: vec![],
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
pub struct ProgramUdf {
    pub name: String,
    pub definition: String,
    pub dependencies: String,
}

#[derive(Encode, Decode, Clone, Debug)]
pub struct Program {
    pub types: Vec<String>,
    pub udfs: Vec<ProgramUdf>,
    pub other_defs: Vec<String>,
    #[bincode(with_serde)]
    pub graph: DiGraph<StreamNode, StreamEdge>,
}

impl Program {
    pub fn from_stream<K: Key, T: Data>(s: KeyedStream<K, T>) -> Program {
        Program {
            types: vec![],
            other_defs: vec![],
            udfs: vec![],
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

    pub fn features(&self) -> HashSet<String> {
        let mut s = HashSet::new();

        for t in self.graph.node_weights() {
            match &t.operator {
                Operator::ConnectorSource(c) | Operator::ConnectorSink(c) => {
                    s.insert(
                        Regex::new("::<.*>$")
                            .unwrap()
                            .replace(&c.operator, "")
                            .to_string(),
                    );
                }
                Operator::Window { typ, .. } => {
                    s.insert(format!("{:?} window", typ));
                }
                Operator::WindowJoin { window } => {
                    s.insert(format!("{:?} window join", window));
                }
                Operator::SlidingWindowAggregator(_) => {
                    s.insert(format!("sliding window aggregator"));
                }
                Operator::TumblingWindowAggregator(_) => {
                    s.insert(format!("tumbling window aggregator"));
                }
                Operator::TumblingTopN(_) => {
                    s.insert(format!("tumbling top n"));
                }
                Operator::SlidingAggregatingTopN(_) => {
                    s.insert(format!("sliding aggregating top n"));
                }
                Operator::JoinWithExpiration { .. } => {
                    s.insert(format!("join with expiration"));
                }
                Operator::NonWindowAggregator(_) => {
                    s.insert(format!("non-window aggregator"));
                }
                _ => {}
            }
        }

        s
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

        for stream_node in self.graph.node_indices() {
            if let Err(error) = self.check_incoming_edges_for_node(stream_node) {
                errors.push(error.to_string());
            }
        }

        errors
    }

    fn check_incoming_edges_for_node(&self, node_index: NodeIndex) -> Result<()> {
        let node_name = &self.graph.node_weight(node_index).unwrap().operator_id;
        let incoming_edges: Vec<_> = self
            .graph
            .edges_directed(node_index, Direction::Incoming)
            .map(|e| e.weight())
            .collect();
        match incoming_edges.len() {
            0 => return Ok(()),
            1 => {
                let edge = incoming_edges[0];
                if matches!(edge.typ, EdgeType::ShuffleJoin(..)) {
                    bail!(
                        "Node {:?} has a shuffle join edge but no other incoming edges",
                        node_name
                    );
                }
            }
            2 => {
                let edge1 = incoming_edges[0];
                let edge2 = incoming_edges[1];
                // must either be two sides of a join or multiple direct and shuffle edges with the same type.
                if let (EdgeType::ShuffleJoin(edge_one), EdgeType::ShuffleJoin(edge_two)) =
                    (&edge1.typ, &edge2.typ)
                {
                    if edge_one == edge_two {
                        bail!(
                            "Node {:?} has two shuffle join edges with the same join index",
                            node_name
                        );
                    } else if edge1.key != edge2.key {
                        bail!(
                            "Node {:?} has two shuffle join edges with different key types",
                            node_name
                        );
                    }
                } else if edge1.typ != edge2.typ {
                    bail!(
                        "Node {:?} has two incoming edges with different types but isn't a join",
                        node_name
                    );
                } else if edge1.key != edge2.key || edge1.value != edge2.value {
                    bail!(
                        "Node {:?} has non-join two incoming edges with different key/value types",
                        node_name
                    );
                }
            }
            _ => {
                // check if there are any shuffle join edges.
                if incoming_edges
                    .iter()
                    .any(|e| matches!(e.typ, EdgeType::ShuffleJoin(_)))
                {
                    bail!(
                            "Node {:?} has more than two incoming edges, but some are shuffle join edges",
                            node_name
                        );
                }
                // check that the key and value are the same for all inputs
                if incoming_edges
                    .iter()
                    .map(|e| (e.key.as_str(), e.value.as_str()))
                    .collect::<HashSet<_>>()
                    .len()
                    > 1
                {
                    bail!(
                            "Node {:?} has more than two incoming edges, but key/value types are not the same for all inputs",
                            node_name
                        );
                }
            }
        }
        Ok(())
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

    pub fn as_job_graph_rest(&self) -> JobGraph {
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

    pub fn make_graph_function(&self) -> syn::ItemFn {
        let nodes: Vec<_> = self.graph.node_indices().map(|idx| {
            let node = self.graph.node_weight(idx).unwrap();
            let description = format!("{:?}", node);
            let input = self.graph.edges_directed(idx, Direction::Incoming).next();
            let output = self.graph.edges_directed(idx, Direction::Outgoing).next();
            let body = match &node.operator {
                Operator::ConnectorSource(c)  => {
                    let out_k = parse_type(&output.unwrap().weight().key);
                    let out_t = parse_type(&output.unwrap().weight().value);

                    let strukt = parse_type(&c.operator);
                    let config = &c.config;
                    quote! {
                        Box::new(#strukt::<#out_k, #out_t>::from_config(#config))
                    }
                }
                Operator::ConnectorSink(c)  => {
                    // In c.operator, replace #in_k and #in_t with the actual types
                    let replaced_type = c.operator.replace("#in_k", &input.unwrap().weight().key);
                    let replaced_type = replaced_type.replace("#in_t", &input.unwrap().weight().value);

                    let strukt = parse_type(&replaced_type);
                    let config = &c.config;
                    quote! {
                        Box::new(#strukt::from_config(#config))
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
                        Some(WindowAgg::Count) => quote!{ WindowOperation::Aggregate(aggregators::count_aggregator) },
                        Some(WindowAgg::Min) => quote!{ WindowOperation::Aggregate(aggregators::min_aggregator) },
                        Some(WindowAgg::Max) => quote!{ WindowOperation::Aggregate(aggregators::max_aggregator) },
                        Some(WindowAgg::Sum) => quote!{ WindowOperation::Aggregate(aggregators::sum_aggregator) },
                        Some(WindowAgg::Expression {
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
                                WindowOperation::#operation(|key: &#in_k, window: arroyo_types::Window, mut arg: Vec<_>| {
                                    #expr
                                })
                            }
                        },
                    };

                    match typ {
                        WindowType::Tumbling { width } => {
                            let width = duration_to_syn_expr(*width);

                            quote! {
                                Box::new(KeyedWindowFunc::<#in_k, #in_t, #out_t, TumblingWindowAssigner>::
                                    tumbling_window(#width, #agg))
                            }
                        }
                        WindowType::Sliding { width, slide } => {
                            let width = duration_to_syn_expr(*width);
                            let slide = duration_to_syn_expr(*slide);

                            quote! {
                                Box::new(KeyedWindowFunc::<#in_k, #in_t, #out_t, SlidingWindowAssigner>::
                                    sliding_window(#width, #slide, #agg))
                            }
                        }
                        WindowType::Instant => {
                            quote! {
                                Box::new(KeyedWindowFunc::<#in_k, #in_t, #out_t, InstantWindowAssigner>::
                                    instant_window(#agg))
                            }
                        }
                        WindowType::Session { gap } => {
                            let gap = duration_to_syn_expr(*gap);
                            quote! {
                                Box::new(SessionWindowFunc::<#in_k, #in_t, #out_t>::new(
                                    #agg, #gap
                                ))
                            }
                        }
                    }
                }
                Operator::Watermark(watermark) => {
                    let in_k = parse_type(&input.unwrap().weight().key);
                    let in_t = parse_type(&input.unwrap().weight().value);

                    let period = duration_to_syn_expr(watermark.period);

                    let idle_time = match watermark.idle_time {
                        Some(d) => {
                            let micros = d.as_micros() as u64;
                            quote!(Some(std::time::Duration::from_micros(#micros)))
                        }
                        None => quote!(None),
                    };

                    match &watermark.strategy {
                        WatermarkStrategy::FixedLateness { max_lateness } => {
                            let max_lateness = duration_to_syn_expr(*max_lateness);

                            quote! {
                                Box::new(
                                    PeriodicWatermarkGenerator::<#in_k, #in_t>::
                                    fixed_lateness(#period, #idle_time, #max_lateness))
                            }
                        }
                        WatermarkStrategy::Expression { expression } => {
                            let expr: syn::Expr = parse_str(&expression).unwrap();
                            let watermark_function : syn::ExprClosure = parse_quote!(|record| {#expr});
                            quote! {
                                Box::new(
                                    PeriodicWatermarkGenerator::<#in_k, #in_t>::
                                    watermark_function(#period, #idle_time, Box::new(#watermark_function)))
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
                    let mut inputs: Vec<_> = self.graph.edges_directed(idx, Direction::Incoming)
                        .collect();
                    inputs.sort_by_key(|e| e.weight().typ.clone());
                    assert_eq!(2, inputs.len(), "WindowJoin should have 2 inputs, but has {}", inputs.len());
                    assert_eq!(inputs[0].weight().key, inputs[1].weight().key, "WindowJoin inputs must have the same key type");

                    let in_k = parse_type(&inputs[0].weight().key);
                    let in_t1 = parse_type(&inputs[0].weight().value);
                    let in_t2 = parse_type(&inputs[1].weight().value);

                    match window {
                        WindowType::Tumbling { width } => {
                            let width = duration_to_syn_expr(*width);
                            quote! {
                                Box::new(WindowedHashJoin::<#in_k, #in_t1, #in_t2, TumblingWindowAssigner, TumblingWindowAssigner>::
                                    tumbling_window(#width))
                            }
                        }
                        WindowType::Sliding { width, slide } => {
                            let width = duration_to_syn_expr(*width);
                            let slide = duration_to_syn_expr(*slide);
                            quote! {
                                Box::new(WindowedHashJoin::<#in_k, #in_t1, #in_t2, SlidingWindowAssigner, SlidingWindowAssigner>::
                                    sliding_window(#width, #slide))
                            }
                        }
                        WindowType::Instant => {
                            quote! {
                                Box::new(WindowedHashJoin::<#in_k, #in_t1, #in_t2, InstantWindowAssigner, InstantWindowAssigner>::
                                    instant_window())
                            }
                        }
                        WindowType::Session { .. } => {
                            unimplemented!("Session windows are not supported in joins")
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
                    let func: syn::ExprClosure = parse_quote!(|record, _| {#expr});
                    match return_type {
                        ExpressionReturnType::Predicate => {
                            quote! {
                                Box::new(FilterOperator::<#in_k, #in_t>{
                                    name: #name.to_string(),
                                    predicate_fn: Box::new(#func),
                                })
                            }
                        },
                        ExpressionReturnType::Record => {
                    quote! {
                        Box::new(MapOperator::<#in_k, #in_t, #out_k, #out_t> {
                            name: #name.to_string(),
                            map_fn: Box::new(#func),
                        })
                    }
                        },
                        ExpressionReturnType::OptionalRecord => {
                            quote! {
                                Box::new(OptionMapOperator::<#in_k, #in_t, #out_k, #out_t> {
                                    name: #name.to_string(),
                                    map_fn: Box::new(#func),
                                })
                            }
                        },
                    }
                },
                Operator::FlatMapOperator { name, expression } => {
                    let expr : syn::Expr = parse_str(expression).expect(expression);
                    let in_k = parse_type(&input.unwrap().weight().key);
                    let in_t = parse_type(&input.unwrap().weight().value);
                    let out_k = parse_type(&output.unwrap().weight().key);
                    let out_t = parse_type(&output.unwrap().weight().value);
                    let func: syn::ExprClosure = parse_quote!(|record, _| {#expr});
                    quote! {
                        Box::new(FlatMapOperator::<#in_k, #in_t, #out_k, #out_t>{
                            name: #name.to_string(),
                            flat_map: Box::new(#func),
                        })
                    }
                },
                Operator::ArrayMapOperator { name, expression, return_type } => {
                    let expr : syn::Expr = parse_str(expression).expect(expression);
                    let in_k = parse_type(&input.unwrap().weight().key);
                    let in_t = parse_type(&input.unwrap().weight().value);
                    let in_t = extract_container_type("Vec", &in_t).expect("Input to aggregate is not a Vec");
                    let out_k = parse_type(&output.unwrap().weight().key);
                    let out_t = parse_type(&output.unwrap().weight().value);
                    let closure = match return_type {
                        ExpressionReturnType::Predicate => {
                            quote! {
                                |record, _| {
                                    if #expr {
                                        Some(record.clone())
                                    } else {
                                        None
                                    }
                                }
                            }
                        },
                        ExpressionReturnType::Record => {
                            quote! {
                                |record, _| {
                                    Some(#expr)
                                }
                            }
                        },
                        ExpressionReturnType::OptionalRecord => {
                            quote! {
                                |record, _| {
                                    #expr
                                }
                            }
                        }
                    };
                    let func: syn::ExprClosure = parse_str(&quote!(#closure).to_string()).unwrap();
                    quote! {
                        Box::new(ArrayMapOperator::<#in_k, #in_t, #out_k, #out_t> {
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
                    let width = duration_to_syn_expr(*width);
                    let slide = duration_to_syn_expr(*slide);
                    let aggregator: syn::ExprClosure = parse_str(aggregator).unwrap();
                    let bin_merger: syn::ExprClosure = parse_str(bin_merger).unwrap();
                    let in_memory_add: syn::ExprClosure = parse_str(in_memory_add).unwrap();
                    let in_memory_remove: syn::ExprClosure = parse_str(in_memory_remove).unwrap();

                    quote!{
                        Box::new(arroyo_worker::operators::aggregating_window::AggregatingWindowFunc::<#in_k, #in_t, #bin_t, #mem_t, #out_t>::
                            new(#width,
                                #slide,
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
                    let width = duration_to_syn_expr(*width);
                    let aggregator: syn::ExprClosure = parse_str(aggregator).unwrap();
                    let bin_merger: syn::ExprClosure = parse_str(bin_merger).unwrap();
                    quote!{
                        Box::new(arroyo_worker::operators::tumbling_aggregating_window::
                            TumblingAggregatingWindowFunc::<#in_k, #in_t, #bin_t, #out_t>::
                        new(#width,
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
                        let width = duration_to_syn_expr(*width);
                        let extractor: syn::ExprClosure = parse_str(extractor).expect(extractor);
                        let converter: syn::ExprClosure = parse_str(converter).unwrap();
                        quote! {
                            Box::new(arroyo_worker::operators::tumbling_top_n_window::
                                TumblingTopNWindowFunc::<#in_k, #in_t, #pk_type, #out_t>::
                            new(#width,
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
                    let width = duration_to_syn_expr(*width);
                    let slide = duration_to_syn_expr(*slide);
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
                    new(#width,
                        #slide,
                        #bin_merger,
                        #in_memory_add,
                        #in_memory_remove,
                        #partitioning_func,
                        #extractor,
                        #aggregator,
                        #max_elements))
                }
                }
                Operator::JoinWithExpiration { left_expiration, right_expiration, join_type } => {
                    let mut inputs: Vec<_> = self.graph.edges_directed(idx, Direction::Incoming)
                        .collect();
                    inputs.sort_by_key(|e| e.weight().typ.clone());
                    assert_eq!(2, inputs.len(), "JoinWithExpiration should have 2 inputs, but has {}", inputs.len());
                    assert_eq!(inputs[0].weight().key, inputs[1].weight().key, "JoinWithExpiration inputs must have the same key type");

                    let in_k = parse_type(&inputs[0].weight().key);
                    let in_t1 = parse_type(&inputs[0].weight().value);
                    let in_t2 = parse_type(&inputs[1].weight().value);
                    let left_expiration = duration_to_syn_expr(*left_expiration);
                    let right_expiration = duration_to_syn_expr(*right_expiration);
                    match join_type {
                        arroyo_types::JoinType::Inner => quote!{
                            Box::new(arroyo_worker::operators::join_with_expiration::
                                inner_join::<#in_k, #in_t1, #in_t2>(#left_expiration, #right_expiration))
                        },
                        arroyo_types::JoinType::Left => quote!{
                            Box::new(arroyo_worker::operators::join_with_expiration::
                                left_join::<#in_k, #in_t1, #in_t2>(#left_expiration, #right_expiration))
                        },
                        arroyo_types::JoinType::Right => quote!{
                            Box::new(arroyo_worker::operators::join_with_expiration::
                                right_join::<#in_k, #in_t1, #in_t2>(#left_expiration, #right_expiration))
                        },
                        arroyo_types::JoinType::Full => quote!{
                            Box::new(arroyo_worker::operators::join_with_expiration::
                                full_join::<#in_k, #in_t1, #in_t2>(#left_expiration, #right_expiration))
                        },
                    }
                },
                Operator::UpdatingOperator { name, expression } => {
                    let expr : syn::Expr = parse_str(expression).expect(expression);
                    let in_k = parse_type(&input.unwrap().weight().key);
                    let in_t = parse_type(&input.unwrap().weight().value);
                    let out_t = parse_type(&output.unwrap().weight().value);
                    let func: syn::ExprClosure = parse_quote!(|arg| {
                        #expr});
                    quote!{
                        Box::new(arroyo_worker::operators::OptionMapOperator::<#in_k, #in_t, #in_k, #out_t>::
                            updating_operator(#name.to_string(), #func))
                    }
                },
                Operator::NonWindowAggregator(NonWindowAggregator { expiration, aggregator, bin_merger, bin_type }) => {
                    let in_k = parse_type(&input.unwrap().weight().key);
                    let in_t = parse_type(&input.unwrap().weight().value);
                    let updating_out_t = parse_type(&output.unwrap().weight().value);
                    let out_t = extract_container_type("UpdatingData", &updating_out_t).unwrap();
                    let bin_t = parse_type(bin_type);
                    let expiration = duration_to_syn_expr(*expiration);
                    let aggregator: syn::ExprClosure = parse_str(aggregator).unwrap();
                    let bin_merger: syn::ExprClosure = parse_str(bin_merger).unwrap();
                    quote!{
                        Box::new(arroyo_worker::operators::updating_aggregate::
                            UpdatingAggregateOperator::<#in_k, #in_t, #bin_t, #out_t>::
                        new(#expiration,
                            #aggregator,
                            #bin_merger))
                    }
                },
                Operator::UpdatingKeyOperator { name, expression } => {
                    let updating_in_t = parse_type(&input.unwrap().weight().value);
                    let in_t = extract_container_type("UpdatingData", &updating_in_t).unwrap();
                    let out_k = parse_type(&output.unwrap().weight().key);
                    let expr : syn::Expr = parse_str(expression).expect(expression);
                    quote! {
                        Box::new(arroyo_worker::operators::
                            KeyMapUpdatingOperator::<#in_t, #out_k>::
                        new(#name.to_string(), #expr))
                    }
                },
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
            .graph
            .edge_indices()
            .map(|idx: petgraph::stable_graph::EdgeIndex| {
                let e = self.graph.edge_weight(idx).unwrap();
                let (source, dest) = self.graph.edge_endpoints(idx).unwrap();
                let source =
                    format_ident!("{}", self.graph.node_weight(source).unwrap().operator_id);
                let dest = format_ident!("{}", self.graph.node_weight(dest).unwrap().operator_id);
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

        parse_quote! {
            pub fn make_graph() -> DiGraph<LogicalNode, LogicalEdge> {
                let mut graph: DiGraph<LogicalNode, LogicalEdge> = DiGraph::new();
                #(#node_defs )*

                #(#edge_defs )*

                graph
            }
        }
    }

    pub fn tasks_per_operator(&self) -> HashMap<String, usize> {
        let mut tasks_per_operator = HashMap::new();
        for node in self.graph.node_weights() {
            tasks_per_operator.insert(node.operator_id.clone(), node.parallelism);
        }
        tasks_per_operator
    }
}

impl Into<PipelineProgramUdf> for ProgramUdf {
    fn into(self) -> PipelineProgramUdf {
        PipelineProgramUdf {
            name: self.name,
            definition: self.definition,
            dependencies: self.dependencies,
        }
    }
}

impl Into<ProgramUdf> for PipelineProgramUdf {
    fn into(self) -> ProgramUdf {
        ProgramUdf {
            name: self.name,
            definition: self.definition,
            dependencies: self.dependencies,
        }
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
            udfs: program.udfs.into_iter().map(|udf| udf.into()).collect(),
            nodes,
            edges,
        })
    }
}

impl From<Operator> for GrpcApi::operator::Operator {
    fn from(operator: Operator) -> Self {
        match operator {
            Operator::ConnectorSource(c) => GrpcOperator::ConnectorSource(c.into()),
            Operator::ConnectorSink(c) => GrpcOperator::ConnectorSink(c.into()),
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
            Operator::Watermark(PeriodicWatermark {
                period,
                idle_time,
                strategy,
            }) => {
                let period_micros = period.as_micros() as u64;
                let idle_time_micros = idle_time.map(|t| t.as_micros() as u64);
                match strategy {
                    WatermarkStrategy::FixedLateness { max_lateness } => {
                        GrpcOperator::PeriodicWatermark(GrpcApi::PeriodicWatermark {
                            period_micros,
                            max_lateness_micros: max_lateness.as_micros() as u64,
                            idle_time_micros,
                        })
                    }
                    WatermarkStrategy::Expression { expression } => {
                        GrpcOperator::ExpressionWatermark(GrpcApi::ExpressionWatermark {
                            period_micros,
                            expression,
                            idle_time_micros,
                        })
                    }
                }
            }
            Operator::GlobalKey => todo!(),
            Operator::WindowJoin { window } => GrpcOperator::WindowJoin(GrpcApi::Window {
                window: Some(window.into()),
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
            Operator::FlatMapOperator { name, expression } => {
                GrpcOperator::FlatMapOperator(GrpcApi::FlatMapOperator { name, expression })
            }
            Operator::ArrayMapOperator {
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
            Operator::JoinWithExpiration {
                left_expiration,
                right_expiration,
                join_type,
            } => GrpcOperator::JoinWithExpiration(GrpcApi::JoinWithExpiration {
                left_expiration_micros: left_expiration.as_micros() as u64,
                right_expiration_micros: right_expiration.as_micros() as u64,
                join_type: match join_type {
                    JoinType::Inner => GrpcApi::JoinType::Inner,
                    JoinType::Left => GrpcApi::JoinType::Left,
                    JoinType::Right => GrpcApi::JoinType::Right,
                    JoinType::Full => GrpcApi::JoinType::Full,
                }
                .into(),
            }),
            Operator::UpdatingOperator { name, expression } => {
                GrpcOperator::UpdatingOperator(GrpcApi::UpdatingOperator { name, expression })
            }
            Operator::NonWindowAggregator(NonWindowAggregator {
                expiration,
                aggregator,
                bin_merger,
                bin_type,
            }) => GrpcOperator::NonWindowAggregator(GrpcApi::NonWindowAggregator {
                expiration_micros: expiration.as_micros() as u64,
                aggregator,
                bin_merger,
                bin_type,
            }),
            Operator::UpdatingKeyOperator { name, expression } => {
                GrpcOperator::UpdatingKeyOperator(GrpcApi::UpdatingKeyOperator { name, expression })
            }
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
            WindowType::Session { gap } => {
                GrpcApi::window::Window::SessionWindow(GrpcApi::SessionWindow {
                    gap_micros: gap.as_micros() as u64,
                })
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
        let udfs = program.udfs;
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
            udfs: udfs.into_iter().map(|udf| udf.into()).collect(),
            graph,
        })
    }
}

impl TryFrom<arroyo_rpc::grpc::api::Operator> for Operator {
    type Error = anyhow::Error;

    fn try_from(operator: arroyo_rpc::grpc::api::Operator) -> Result<Self> {
        let result = match operator.operator {
            Some(operator) => match operator {
                GrpcOperator::ConnectorSource(c) => Operator::ConnectorSource(c.into()),
                GrpcOperator::ConnectorSink(c) => Operator::ConnectorSink(c.into()),
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
                GrpcOperator::PeriodicWatermark(GrpcApi::PeriodicWatermark {
                    period_micros,
                    max_lateness_micros,
                    idle_time_micros,
                }) => Operator::Watermark(PeriodicWatermark {
                    period: Duration::from_micros(period_micros),
                    idle_time: idle_time_micros.map(Duration::from_micros),
                    strategy: WatermarkStrategy::FixedLateness {
                        max_lateness: Duration::from_micros(max_lateness_micros),
                    },
                }),
                GrpcOperator::ExpressionWatermark(GrpcApi::ExpressionWatermark {
                    period_micros,
                    expression,
                    idle_time_micros,
                }) => Operator::Watermark(PeriodicWatermark {
                    period: Duration::from_micros(period_micros),
                    idle_time: idle_time_micros.map(Duration::from_micros),
                    strategy: WatermarkStrategy::Expression { expression },
                }),
                GrpcOperator::WindowJoin(window) => Operator::WindowJoin {
                    window: window.into(),
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
                GrpcOperator::FlatMapOperator(GrpcApi::FlatMapOperator { name, expression }) => {
                    Operator::FlatMapOperator { name, expression }
                }
                GrpcOperator::FlattenExpressionOperator(flatten_expression) => {
                    let return_type = flatten_expression.return_type().into();
                    Operator::ArrayMapOperator {
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
                GrpcOperator::JoinWithExpiration(GrpcApi::JoinWithExpiration {
                    left_expiration_micros,
                    right_expiration_micros,
                    join_type,
                }) => Operator::JoinWithExpiration {
                    left_expiration: Duration::from_micros(left_expiration_micros),
                    right_expiration: Duration::from_micros(right_expiration_micros),
                    join_type: match GrpcApi::JoinType::from_i32(join_type) {
                        Some(GrpcApi::JoinType::Inner) => JoinType::Inner,
                        Some(GrpcApi::JoinType::Left) => JoinType::Left,
                        Some(GrpcApi::JoinType::Right) => JoinType::Right,
                        Some(GrpcApi::JoinType::Full) => JoinType::Full,
                        None => JoinType::Inner,
                    },
                },
                GrpcOperator::UpdatingOperator(GrpcApi::UpdatingOperator { name, expression }) => {
                    Operator::UpdatingOperator { name, expression }
                }
                GrpcOperator::NonWindowAggregator(GrpcApi::NonWindowAggregator {
                    expiration_micros,
                    aggregator,
                    bin_merger,
                    bin_type,
                }) => Operator::NonWindowAggregator(NonWindowAggregator {
                    expiration: Duration::from_micros(expiration_micros),
                    aggregator,
                    bin_merger,
                    bin_type,
                }),
                GrpcOperator::UpdatingKeyOperator(GrpcApi::UpdatingKeyOperator {
                    name,
                    expression,
                }) => Operator::UpdatingKeyOperator { name, expression },
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
            Some(arroyo_rpc::grpc::api::window::Window::SessionWindow(session)) => {
                WindowType::Session {
                    gap: Duration::from_micros(session.gap_micros),
                }
            }
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
        let t = extract_container_type("Vec", &parse_str("HashMap<String, u8>").unwrap());
        assert!(t.is_none())
    }
}
