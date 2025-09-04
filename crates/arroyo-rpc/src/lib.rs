pub mod api_types;
pub mod formats;
pub mod public_ids;
pub mod schema_resolver;
pub mod var_str;

use crate::config::{config, TlsConfig};
use crate::formats::{BadData, Format, Framing};
use crate::grpc::rpc::controller_grpc_client::ControllerGrpcClient;
use crate::grpc::rpc::{LoadCompactedDataReq, SubtaskCheckpointMetadata};
use anyhow::{anyhow, Context, Result};
use arrow::compute::kernels::cast_utils::parse_interval_day_time;
use arrow::row::{OwnedRow, RowConverter, RowParser, Rows, SortField};
use arrow_array::{Array, ArrayRef, BooleanArray};
use arrow_schema::{DataType, Field, Fields};
use arroyo_types::{CheckpointBarrier, HASH_SEEDS};
use datafusion::common::{
    not_impl_err, plan_datafusion_err, plan_err, DFSchema, Result as DFResult, ScalarValue,
    TableReference,
};
use datafusion::config::ConfigOptions;
use datafusion::error::DataFusionError;
use datafusion::logical_expr::sqlparser::ast::ValueWithSpan;
use datafusion::logical_expr::{AggregateUDF, ScalarUDF, TableSource, WindowUDF};
use datafusion::sql::planner::{ContextProvider, PlannerContext, SqlToRel};
use datafusion::sql::sqlparser::ast::{Expr, SqlOption, Value as SqlValue};
use datafusion::sql::sqlparser::dialect::PostgreSqlDialect;
use datafusion::sql::sqlparser::parser::Parser;
use datafusion::sql::sqlparser::tokenizer::Span;
use grpc::rpc::{StopMode, TableCheckpointMetadata, TaskCheckpointEventType};
use log::{debug, warn};
use regex::Regex;
use rustls::RootCertStore;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::net::{IpAddr, Ipv4Addr};
use std::num::{NonZero, NonZeroU64};
use std::str::FromStr;
use std::sync::{Arc, LazyLock, Mutex, OnceLock};
use std::time::Duration;
use std::{fs, time::SystemTime};
use tokio::sync::{
    mpsc::{channel, Receiver, Sender},
    oneshot,
};
use tonic::transport::{Channel, ClientTlsConfig, Endpoint, Identity};
use tonic::{
    metadata::{Ascii, MetadataValue},
    service::Interceptor,
};
use url::{Host, Url};

pub mod config;
pub mod df;

pub mod grpc {
    pub mod rpc {
        #![allow(clippy::derive_partial_eq_without_eq, deprecated)]
        tonic::include_proto!("arroyo_rpc");
    }

    pub mod api {
        #![allow(clippy::derive_partial_eq_without_eq, deprecated)]
        tonic::include_proto!("api");

        impl From<self::JoinType> for arroyo_types::JoinType {
            fn from(value: JoinType) -> Self {
                match value {
                    JoinType::Inner => arroyo_types::JoinType::Inner,
                    JoinType::Left => arroyo_types::JoinType::Left,
                    JoinType::Right => arroyo_types::JoinType::Right,
                    JoinType::Full => arroyo_types::JoinType::Full,
                }
            }
        }
    }

    pub const API_FILE_DESCRIPTOR_SET: &[u8] =
        tonic::include_file_descriptor_set!("api_descriptor");
}

static DB_BACKUP_NOTIFIER: OnceLock<Sender<oneshot::Sender<()>>> = OnceLock::new();

pub fn init_db_notifier() -> Receiver<oneshot::Sender<()>> {
    let (tx, rx) = channel(1);
    DB_BACKUP_NOTIFIER
        .set(tx)
        .expect("DB notifier was initialized multiple times!");
    rx
}

pub fn notify_db() -> Option<oneshot::Receiver<()>> {
    let (tx, rx) = oneshot::channel();
    DB_BACKUP_NOTIFIER.get()?.try_send(tx).ok().map(|_| rx)
}

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub enum EventLevel {
    Trace,
    Analytics,
}

pub trait EventLogger: Send + Sync {
    fn log_event(
        &self,
        name: &str,
        labels: Value,
        values: BTreeMap<String, f64>,
        level: EventLevel,
    );
    fn trace_enabled(&self) -> bool {
        false
    }
}

static EVENT_LOGGER: OnceLock<Arc<dyn EventLogger>> = OnceLock::new();

pub fn event_logger<'a>() -> Option<&'a Arc<dyn EventLogger>> {
    EVENT_LOGGER.get()
}

pub fn init_event_logger(event_logger: Arc<dyn EventLogger>) {
    let _ = EVENT_LOGGER.set(event_logger);
}

#[macro_export]
macro_rules! log_event {
    ($name:expr, { $($labels:tt)* }, [ $($key:expr => $value:expr),* $(,)? ]) => {
        if let Some(event_logger) = arroyo_rpc::event_logger() {
            let labels = serde_json::json!({ $($labels)* });
            let values = {
                let mut map = ::std::collections::BTreeMap::new();
                $(
                    map.insert($key.to_string(), $value as f64);
                )*
                map
            };
            event_logger.log_event($name, labels, values, arroyo_rpc::EventLevel::Analytics);
        }
    };
    ($name:expr, { $($labels:tt)* }) => {
        log_event!($name, { $($labels)* }, []);
    };
}

#[macro_export]
macro_rules! log_trace_event {
    ($name:expr, { $($labels:tt)* }, [ $($key:expr => $value:expr),* $(,)? ]) => {
        if let Some(event_logger) = arroyo_rpc::event_logger() {
            if event_logger.trace_enabled() {
                let labels = serde_json::json!({ $($labels)* });
                let values = {
                    let mut map = ::std::collections::BTreeMap::new();
                    $(
                        map.insert($key.to_string(), $value as f64);
                    )*
                    map
                };
                event_logger.log_event($name, labels, values, arroyo_rpc::EventLevel::Trace);
            }
        }
    };
    ($name:expr, { $($labels:tt)* }) => {
        log_trace_event!($name, { $($labels)* }, []);
    };
}

#[derive(Debug)]
pub enum ControlMessage {
    Checkpoint(CheckpointBarrier),
    Stop {
        mode: StopMode,
    },
    Commit {
        epoch: u32,
        commit_data: HashMap<String, HashMap<u32, Vec<u8>>>,
    },
    LoadCompacted {
        compacted: CompactionResult,
    },
    NoOp,
}

#[derive(Debug, Clone)]
pub struct CompactionResult {
    pub operator_id: String,
    pub compacted_tables: HashMap<String, TableCheckpointMetadata>,
}

impl From<LoadCompactedDataReq> for CompactionResult {
    fn from(req: LoadCompactedDataReq) -> Self {
        Self {
            operator_id: req.operator_id,
            compacted_tables: req.compacted_metadata,
        }
    }
}

#[derive(Debug, Clone)]
pub struct CheckpointCompleted {
    pub checkpoint_epoch: u32,
    pub node_id: u32,
    pub operator_id: String,
    pub subtask_metadata: SubtaskCheckpointMetadata,
}

#[derive(Debug, Clone)]
pub struct CheckpointEvent {
    pub checkpoint_epoch: u32,
    pub node_id: u32,
    pub operator_id: String,
    pub subtask_index: u32,
    pub time: SystemTime,
    pub event_type: TaskCheckpointEventType,
}

#[derive(Debug, Clone)]
pub enum ControlResp {
    CheckpointEvent(CheckpointEvent),
    CheckpointCompleted(CheckpointCompleted),
    TaskStarted {
        node_id: u32,
        task_index: usize,
        start_time: SystemTime,
    },
    TaskFinished {
        node_id: u32,
        task_index: usize,
    },
    TaskFailed {
        node_id: u32,
        task_index: usize,
        error: String,
    },
    Error {
        node_id: u32,
        operator_id: String,
        task_index: usize,
        message: String,
        details: String,
    },
}

pub struct FileAuthInterceptor {
    token: MetadataValue<Ascii>,
}

impl FileAuthInterceptor {
    pub fn load() -> Self {
        let path = format!("{}/.arroyo-token", std::env::var("HOME").unwrap());
        let token = fs::read_to_string(&path)
            .unwrap_or_else(|_| panic!("Expected auth token to be in {path}"));

        Self {
            token: token.trim().parse().unwrap(),
        }
    }
}

impl Interceptor for FileAuthInterceptor {
    fn call(
        &mut self,
        mut request: tonic::Request<()>,
    ) -> Result<tonic::Request<()>, tonic::Status> {
        request
            .metadata_mut()
            .insert("authorization", self.token.clone());

        Ok(request)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct RateLimit {
    pub messages_per_second: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct MetadataField {
    pub field_name: String,
    pub key: String,
    #[serde(default)]
    pub data_type: Option<DataType>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct OperatorConfig {
    pub connection: Value,
    pub table: Value,
    pub format: Option<Format>,
    pub bad_data: Option<BadData>,
    pub framing: Option<Framing>,
    pub rate_limit: Option<RateLimit>,
    #[serde(default)]
    pub metadata_fields: Vec<MetadataField>,
}

impl Default for OperatorConfig {
    fn default() -> Self {
        Self {
            connection: serde_json::from_str("{}").unwrap(),
            table: serde_json::from_str("{}").unwrap(),
            format: None,
            bad_data: None,
            framing: None,
            rate_limit: None,
            metadata_fields: vec![],
        }
    }
}

pub fn error_chain(e: anyhow::Error) -> String {
    e.chain()
        .map(|e| e.to_string())
        .collect::<Vec<_>>()
        .join(": ")
}

pub const TIMESTAMP_FIELD: &str = "_timestamp";
pub const UPDATING_META_FIELD: &str = "_updating_meta";

pub fn updating_meta_fields() -> Fields {
    static UPDATING_META_FIELDS: OnceLock<Fields> = OnceLock::new();

    UPDATING_META_FIELDS
        .get_or_init(|| {
            Fields::from(vec![
                Field::new("is_retract", DataType::Boolean, true),
                Field::new("id", DataType::FixedSizeBinary(16), true),
            ])
        })
        .clone()
}

pub fn updating_meta_field() -> Arc<Field> {
    static UPDATING_META_DATATYPE: OnceLock<Arc<Field>> = OnceLock::new();

    UPDATING_META_DATATYPE
        .get_or_init(|| {
            Arc::new(Field::new(
                UPDATING_META_FIELD,
                DataType::Struct(updating_meta_fields()),
                false,
            ))
        })
        .clone()
}
// need to handle the empty case as a row converter without sort fields emits empty Rows.
#[derive(Debug)]
pub enum Converter {
    RowConverter(RowConverter),
    Empty(RowConverter, Arc<dyn Array>),
}

impl Converter {
    pub fn new(sort_fields: Vec<SortField>) -> Result<Self> {
        if sort_fields.is_empty() {
            let array = Arc::new(BooleanArray::from(vec![false]));
            Ok(Self::Empty(
                RowConverter::new(vec![SortField::new(DataType::Boolean)])?,
                array,
            ))
        } else {
            Ok(Self::RowConverter(RowConverter::new(sort_fields)?))
        }
    }

    pub fn convert_columns(&self, columns: &[Arc<dyn Array>]) -> anyhow::Result<OwnedRow> {
        match self {
            Converter::RowConverter(row_converter) => {
                Ok(row_converter.convert_columns(columns)?.row(0).owned())
            }
            Converter::Empty(row_converter, array) => Ok(row_converter
                .convert_columns(&[array.clone()])?
                .row(0)
                .owned()),
        }
    }

    pub fn convert_all_columns(
        &self,
        columns: &[Arc<dyn Array>],
        num_rows: usize,
    ) -> anyhow::Result<Rows> {
        match self {
            Converter::RowConverter(row_converter) => Ok(row_converter.convert_columns(columns)?),
            Converter::Empty(row_converter, _array) => {
                let array = Arc::new(BooleanArray::from(vec![false; num_rows]));
                Ok(row_converter.convert_columns(&[array])?)
            }
        }
    }

    pub fn convert_rows(&self, rows: Vec<arrow::row::Row<'_>>) -> anyhow::Result<Vec<ArrayRef>> {
        match self {
            Converter::RowConverter(row_converter) => Ok(row_converter.convert_rows(rows)?),
            Converter::Empty(_row_converter, _array) => Ok(vec![]),
        }
    }

    pub fn convert_raw_rows(&self, row_bytes: Vec<&[u8]>) -> anyhow::Result<Vec<ArrayRef>> {
        match self {
            Converter::RowConverter(row_converter) => {
                let parser = row_converter.parser();
                let mut row_list = vec![];
                for bytes in row_bytes {
                    let row = parser.parse(bytes);
                    row_list.push(row);
                }
                Ok(row_converter.convert_rows(row_list)?)
            }
            Converter::Empty(_row_converter, _array) => Ok(vec![]),
        }
    }

    pub fn parser(&self) -> Option<RowParser> {
        match self {
            Converter::RowConverter(r) => Some(r.parser()),
            Converter::Empty(_, _) => None,
        }
    }
}

pub fn get_hasher() -> ahash::RandomState {
    ahash::RandomState::with_seeds(HASH_SEEDS[0], HASH_SEEDS[1], HASH_SEEDS[2], HASH_SEEDS[3])
}

#[derive(Default)]
pub struct EmptyContextProvider {
    config: ConfigOptions,
}
impl ContextProvider for EmptyContextProvider {
    fn get_table_source(&self, _: TableReference) -> DFResult<Arc<dyn TableSource>> {
        not_impl_err!("empty context provider has no table sources")
    }

    fn get_function_meta(&self, _: &str) -> Option<Arc<ScalarUDF>> {
        None
    }

    fn get_aggregate_meta(&self, _: &str) -> Option<Arc<AggregateUDF>> {
        None
    }

    fn get_window_meta(&self, _: &str) -> Option<Arc<WindowUDF>> {
        None
    }

    fn get_variable_type(&self, _: &[String]) -> Option<DataType> {
        None
    }

    fn options(&self) -> &ConfigOptions {
        &self.config
    }

    fn udf_names(&self) -> Vec<String> {
        vec![]
    }

    fn udaf_names(&self) -> Vec<String> {
        vec![]
    }

    fn udwf_names(&self) -> Vec<String> {
        vec![]
    }
}

pub fn contextless_sql_to_expr(
    expr: Expr,
    schema: Option<&DFSchema>,
) -> DFResult<datafusion::logical_expr::Expr> {
    let provider = EmptyContextProvider::default();
    let s = SqlToRel::new(&provider);
    s.sql_to_expr(
        expr,
        schema.unwrap_or(&DFSchema::empty()),
        &mut PlannerContext::new(),
    )
}

pub fn duration_from_sql(expr: Expr) -> DFResult<Duration> {
    let interval = match expr {
        Expr::Value(ValueWithSpan {
            value: SqlValue::SingleQuotedString(s),
            span: _,
        }) => {
            warn!(
                "Intervals in options should now be expressed with SQL interval syntax, but \
                used deprecated string syntax; this will be unsupported after Arroyo 0.14"
            );
            parse_interval_day_time(&s)
                // TODO: add diagnostic
                .map_err(|_| plan_datafusion_err!("expected an interval, but found `{}`", s))?
        }
        expr => {
            let expr = contextless_sql_to_expr(expr, None)
                .map_err(|e| plan_datafusion_err!("invalid expression: {}", e))?;

            match expr {
                datafusion::logical_expr::Expr::Literal(
                    ScalarValue::IntervalMonthDayNano(Some(m)),
                    _,
                ) => {
                    if m.months != 0 {
                        return plan_err!("months are not supported in this interval");
                    }
                    if m.days < 0 || m.nanoseconds < 0 {
                        return plan_err!("interval must not be negative");
                    }
                    return Ok(Duration::from_secs(m.days as u64 * 60 * 60 * 24)
                        + Duration::from_nanos(m.nanoseconds as u64));
                }
                datafusion::logical_expr::Expr::Literal(
                    ScalarValue::IntervalDayTime(Some(m)),
                    _,
                ) => m,
                e => {
                    return plan_err!("expected an interval, but found '{}'", e);
                }
            }
        }
    };

    if interval.days < 0 || interval.milliseconds < 0 {
        return plan_err!("interval must not be negative");
    }

    Ok(Duration::from_secs(interval.days as u64 * 60 * 60 * 24)
        + Duration::from_millis(interval.milliseconds as u64))
}

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub enum DataSizeUnit {
    Bytes,
    Kilobytes,
    Megabytes,
    Gigabytes,
    Terabytes,
    Petabytes,
    Exabytes,
}

impl FromStr for DataSizeUnit {
    type Err = DataFusionError;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        Ok(match s.to_lowercase().as_str() {
            "" | "b" => Self::Bytes,
            "k" | "kb" => Self::Kilobytes,
            "m" | "mb" => Self::Megabytes,
            "g" | "gb" => Self::Gigabytes,
            "t" | "tb" => Self::Terabytes,
            "p" | "pb" => Self::Petabytes,
            "e" | "eb" => Self::Exabytes,
            _ => {
                return plan_err!("invalid data size unit '{s}'");
            }
        })
    }
}

const REGEX: &str = r"^(\d+)([a-zA-Z]*)$";

impl DataSizeUnit {
    pub fn parse(s: &str) -> DFResult<(Self, u64)> {
        let regex = Regex::new(REGEX).unwrap();
        let captures = regex
            .captures(s)
            .ok_or_else(|| plan_datafusion_err!("invalid data size {}", s))?;
        let quantity: u64 = captures.get(1).unwrap().as_str().parse().unwrap();
        let unit = captures.get(2).unwrap().as_str();
        Ok((DataSizeUnit::from_str(unit)?, quantity))
    }

    pub fn multiplier(&self) -> u64 {
        match self {
            DataSizeUnit::Bytes => 1,
            DataSizeUnit::Kilobytes => 1024,
            DataSizeUnit::Megabytes => 1024 * 1024,
            DataSizeUnit::Gigabytes => 1024 * 1024 * 1024,
            DataSizeUnit::Terabytes => 1024 * 1024 * 1024 * 1024,
            DataSizeUnit::Petabytes => 1024 * 1024 * 1024 * 1024 * 1024,
            DataSizeUnit::Exabytes => 1024 * 1024 * 1024 * 1024 * 1024 * 1024,
        }
    }

    pub fn as_bytes(&self, value: u64) -> u64 {
        value * self.multiplier()
    }
}

pub struct ConnectorOptions {
    options: HashMap<String, Expr>,
}

impl TryFrom<&Vec<SqlOption>> for ConnectorOptions {
    type Error = datafusion::error::DataFusionError;

    fn try_from(value: &Vec<SqlOption>) -> Result<Self, Self::Error> {
        let mut options = HashMap::new();

        for option in value {
            let SqlOption::KeyValue { key, value } = &option else {
                return plan_err!(
                    "invalid with option: '{}'; expected an `=` delimited key-value pair",
                    option
                );
            };

            options.insert(key.value.to_string(), value.clone());
        }

        Ok(Self { options })
    }
}

impl ConnectorOptions {
    pub fn pull_struct<T: FromOpts>(&mut self) -> DFResult<T> {
        T::from_opts(self)
    }

    pub fn pull_opt_str(&mut self, name: &str) -> DFResult<Option<String>> {
        match self.options.remove(name) {
            Some(Expr::Value(ValueWithSpan {
                value: SqlValue::SingleQuotedString(s),
                span: _,
            })) => Ok(Some(s)),
            Some(e) => {
                plan_err!(
                    "expected with option '{}' to be a single-quoted string, but it was `{:?}`",
                    name,
                    e
                )
            }
            None => Ok(None),
        }
    }

    pub fn pull_str(&mut self, name: &str) -> DFResult<String> {
        self.pull_opt_str(name)?
            .ok_or_else(|| plan_datafusion_err!("required option '{}' not set", name))
    }

    pub fn pull_opt_bool(&mut self, name: &str) -> DFResult<Option<bool>> {
        match self.options.remove(name) {
            Some(Expr::Value(ValueWithSpan {
                value: SqlValue::Boolean(b),
                span: _,
            })) => Ok(Some(b)),
            Some(Expr::Value(ValueWithSpan {
                value: SqlValue::SingleQuotedString(s),
                span: _,
            })) => match s.as_str() {
                "true" | "yes" => Ok(Some(true)),
                "false" | "no" => Ok(Some(false)),
                _ => plan_err!(
                    "expected with option '{}' to be a boolean, but it was `'{}'`",
                    name,
                    s
                ),
            },
            Some(e) => {
                plan_err!(
                    "expected with option '{}' to be a boolean, but it was `{:?}`",
                    name,
                    e
                )
            }
            None => Ok(None),
        }
    }

    pub fn pull_opt_u64(&mut self, name: &str) -> DFResult<Option<u64>> {
        match self.options.remove(name) {
            Some(Expr::Value(ValueWithSpan {
                value: SqlValue::Number(s, _),
                span: _,
            }))
            | Some(Expr::Value(ValueWithSpan {
                value: SqlValue::SingleQuotedString(s),
                span: _,
            })) => s.parse::<u64>().map(Some).map_err(|_| {
                plan_datafusion_err!(
                    "expected with option '{}' to be an unsigned integer, but it was `{}`",
                    name,
                    s
                )
            }),
            Some(e) => {
                plan_err!(
                    "expected with option '{}' to be an unsigned integer, but it was `{:?}`",
                    name,
                    e
                )
            }
            None => Ok(None),
        }
    }

    pub fn pull_opt_nonzero_u64(&mut self, name: &str) -> DFResult<Option<NonZero<u64>>> {
        match self.pull_opt_u64(name)? {
            Some(0) => {
                plan_err!("expected with option '{name}' to be greater than 0, but it was 0")
            }
            Some(i) => Ok(Some(NonZeroU64::new(i).unwrap())),
            None => Ok(None),
        }
    }

    pub fn pull_opt_data_size_bytes(&mut self, name: &str) -> DFResult<Option<u64>> {
        self.pull_opt_str(name)?
            .map(|s| {
                let (unit, q) = DataSizeUnit::parse(&s)?;
                Ok(unit.as_bytes(q))
            })
            .transpose()
    }

    pub fn pull_opt_i64(&mut self, name: &str) -> DFResult<Option<i64>> {
        match self.options.remove(name) {
            Some(Expr::Value(ValueWithSpan {
                value: SqlValue::Number(s, _),
                span: _,
            }))
            | Some(Expr::Value(ValueWithSpan {
                value: SqlValue::SingleQuotedString(s),
                span: _,
            })) => s.parse::<i64>().map(Some).map_err(|_| {
                plan_datafusion_err!(
                    "expected with option '{}' to be an integer, but it was `{}`",
                    name,
                    s
                )
            }),
            Some(e) => {
                plan_err!(
                    "expected with option '{}' to be an integer, but it was `{:?}`",
                    name,
                    e
                )
            }
            None => Ok(None),
        }
    }

    pub fn pull_i64(&mut self, name: &str) -> DFResult<i64> {
        self.pull_opt_i64(name)?
            .ok_or_else(|| plan_datafusion_err!("required option '{}' not set", name))
    }

    pub fn pull_u64(&mut self, name: &str) -> DFResult<u64> {
        self.pull_opt_u64(name)?
            .ok_or_else(|| plan_datafusion_err!("required option '{}' not set", name))
    }

    pub fn pull_opt_f64(&mut self, name: &str) -> DFResult<Option<f64>> {
        match self.options.remove(name) {
            Some(Expr::Value(ValueWithSpan {
                value: SqlValue::Number(s, _),
                span: _,
            }))
            | Some(Expr::Value(ValueWithSpan {
                value: SqlValue::SingleQuotedString(s),
                span: _,
            })) => s.parse::<f64>().map(Some).map_err(|_| {
                plan_datafusion_err!(
                    "expected with option '{}' to be a double, but it was `{}`",
                    name,
                    s
                )
            }),
            Some(e) => {
                plan_err!(
                    "expected with option '{}' to be an double, but it was `{:?}`",
                    name,
                    e
                )
            }
            None => Ok(None),
        }
    }

    pub fn pull_f64(&mut self, name: &str) -> DFResult<f64> {
        self.pull_opt_f64(name)?
            .ok_or_else(|| plan_datafusion_err!("required option '{}' not set", name))
    }

    pub fn pull_bool(&mut self, name: &str) -> DFResult<bool> {
        self.pull_opt_bool(name)?
            .ok_or_else(|| plan_datafusion_err!("required option '{}' not set", name))
    }

    pub fn pull_opt_duration(&mut self, name: &str) -> DFResult<Option<Duration>> {
        match self.options.remove(name) {
            Some(e) => {
                Ok(Some(duration_from_sql(e).map_err(|e| {
                    e.context(format!("in with clause '{name}'"))
                })?))
            }
            None => Ok(None),
        }
    }

    pub fn pull_opt_field(&mut self, name: &str) -> DFResult<Option<String>> {
        match self.options.remove(name) {
            Some(Expr::Value(ValueWithSpan {
                value: SqlValue::SingleQuotedString(s),
                span: _,
            })) => {
                warn!("Referred to a field in `{name}` with a string—this is deprecated and will be unsupported after Arroyo 0.14");
                Ok(Some(s))
            }
            Some(Expr::Identifier(ident)) => Ok(Some(ident.value)),
            Some(e) => {
                plan_err!(
                    "expected with option '{}' to be a field, but it was `{:?}`",
                    name,
                    e
                )
            }
            None => Ok(None),
        }
    }

    pub fn pull_opt_array(&mut self, name: &str) -> Option<Vec<Expr>> {
        Some(match self.options.remove(name)? {
            Expr::Value(ValueWithSpan {
                value: SqlValue::SingleQuotedString(s),
                span,
            }) => s
                .split(",")
                .map(|p| {
                    Expr::Value(ValueWithSpan {
                        value: SqlValue::SingleQuotedString(p.to_string()),
                        span,
                    })
                })
                .collect(),
            Expr::Array(a) => a.elem,
            e => vec![e],
        })
    }

    pub fn pull_opt_parsed<T: FromStr>(&mut self, name: &str) -> DFResult<Option<T>> {
        Ok(match self.pull_opt_str(name)? {
            Some(s) => Some(
                s.parse()
                    .map_err(|_| plan_datafusion_err!("invalid value '{s}' for {name}"))?,
            ),
            None => None,
        })
    }

    pub fn keys(&self) -> impl Iterator<Item = &String> {
        self.options.keys()
    }

    pub fn keys_with_prefix<'a, 'b>(
        &'a self,
        prefix: &'b str,
    ) -> impl Iterator<Item = &'a String> + 'b
    where
        'a: 'b,
    {
        self.options.keys().filter(move |k| k.starts_with(prefix))
    }

    pub fn insert_str(
        &mut self,
        name: impl Into<String>,
        value: impl Into<String>,
    ) -> DFResult<Option<String>> {
        let name = name.into();
        let value = value.into();
        let existing = self.pull_opt_str(&name)?;
        self.options.insert(
            name,
            Expr::Value(ValueWithSpan {
                value: SqlValue::SingleQuotedString(value),
                span: Span::empty(),
            }),
        );
        Ok(existing)
    }

    pub fn is_empty(&self) -> bool {
        self.options.is_empty()
    }

    pub fn contains_key(&self, key: &str) -> bool {
        self.options.contains_key(key)
    }
}

pub fn parse_expr(sql: &str) -> anyhow::Result<Expr> {
    let dialect = PostgreSqlDialect {};
    let parser = Parser::new(&dialect);
    let mut parser = parser.try_with_sql(sql)?;
    Ok(parser.parse_expr()?)
}

static INTERN: OnceLock<Mutex<HashSet<&'static str>>> = OnceLock::new();

pub fn intern(s: &str) -> &'static str {
    let boxed = s.to_owned().into_boxed_str(); // one heap alloc
    let static_ref: &'static str = Box::leak(boxed); // never freed
    INTERN
        .get_or_init(|| Mutex::new(HashSet::new()))
        .lock()
        .unwrap()
        .insert(static_ref);
    static_ref
}

#[macro_export]
macro_rules! retry {
    ($e:expr, $max_retries:expr, $base:expr, $max_delay:expr, |$err_var:ident| $error_handler:expr, $retry_if:expr) => {{
        use rand::Rng;
        use std::time::Duration;
        let mut retries: u32 = 0;
        loop {
            match $e {
                Ok(value) => break Ok(value),
                Err(e) => {
                    if retries < $max_retries && $retry_if(&e) {
                        retries += 1;
                        {
                            let $err_var = e;
                            $error_handler;
                        }
                        let tmp = $max_delay.min($base * (2u32.pow(retries)));
                        let backoff = tmp / 2
                            + Duration::from_micros(
                                rand::rng().random_range(0..tmp.as_micros() as u64 / 2),
                            );

                        tokio::time::sleep(backoff).await;
                    } else {
                        break Err(e);
                    }
                }
            }
        }
    }};

    ($e:expr, $max_retries:expr, $base:expr, $max_delay:expr, |$err_var:ident| $error_handler:expr) => {
        retry!(
            $e,
            $max_retries,
            $base,
            $max_delay,
            |$err_var| $error_handler,
            |_| true
        )
    };
}

pub fn native_cert_store() -> Arc<RootCertStore> {
    static CERTS: LazyLock<Arc<RootCertStore>> = LazyLock::new(|| {
        let mut roots = RootCertStore::empty();
        let result = rustls_native_certs::load_native_certs();
        for error in result.errors {
            warn!("Errored while loading native certs: {error:?}");
        }

        for cert in result.certs {
            if let Err(e) = roots.add(cert) {
                warn!("Failed to add cert to store: {e:?}");
            }
        }

        Arc::new(roots)
    });

    CERTS.clone()
}

pub async fn grpc_channel_builder(
    our_name: &str,
    endpoint: String,
    our_tls: &Option<TlsConfig>,
    target_tls: &Option<TlsConfig>,
) -> Result<Endpoint> {
    let config = config();
    if let Some(target_tls) = config.get_tls_config(target_tls) {
        let mut endpoint = Url::parse(&endpoint)?;
        endpoint
            .set_scheme("https")
            .map_err(|_| anyhow!("invalid URL for gRPC endpoint: {}", endpoint))?;
        debug!("connecting to grpc endpoint via TLS {endpoint}");
        let b = Channel::from_shared(endpoint.to_string())?;
        let mut config_builder = ClientTlsConfig::new().with_enabled_roots();

        // Workaround for https://github.com/hyperium/tonic/issues/1696
        let host = endpoint
            .host()
            .ok_or_else(|| anyhow!("invalid host in endpoint {}", endpoint))?;
        if let Host::Ipv6(ip) = host {
            // this is an IPv6 address
            config_builder = config_builder.domain_name(ip.to_string());
        }

        if target_tls.mtls_ca_file.is_some() {
            let our_tls = config.get_tls_config(our_tls)
                .ok_or_else(|| anyhow!("mTLS is enabled for {endpoint}, but {our_name} service is not configured with TLS"))?
                .load()
                .await?;

            config_builder = config_builder.identity(Identity::from_pem(our_tls.cert, our_tls.key));
        }

        Ok(b.tls_config(config_builder).context("configuring TLS")?)
    } else {
        debug!("connecting to grpc endpoint {endpoint}");
        Ok(Channel::from_shared(endpoint.to_string())?)
    }
}

/// Connect to a gRPC service with optional TLS
pub async fn connect_grpc(
    our_name: &str,
    endpoint: String,
    our_tls: &Option<TlsConfig>,
    tls: &Option<TlsConfig>,
) -> Result<Channel> {
    Ok(grpc_channel_builder(our_name, endpoint, our_tls, tls)
        .await?
        .connect()
        .await?)
}

pub async fn controller_client(
    our_name: &str,
    our_tls: &Option<TlsConfig>,
) -> Result<ControllerGrpcClient<Channel>> {
    let endpoint = config().controller_endpoint();
    let channel = connect_grpc(our_name, endpoint, our_tls, &config().controller.tls).await?;
    Ok(ControllerGrpcClient::new(channel))
}

pub fn local_address(bind_address: IpAddr) -> String {
    if let Some(hostname) = config().hostname.clone() {
        hostname
    } else {
        let local_ip = if bind_address.is_loopback() {
            IpAddr::V4(Ipv4Addr::LOCALHOST)
        } else if bind_address.is_ipv4() {
            local_ip_address::local_ip().expect("could not determine worker ipv4 address")
        } else {
            local_ip_address::local_ipv6().expect("could not determine worker ipv6 address")
        };

        if local_ip.is_ipv4() {
            local_ip.to_string()
        } else {
            format!("[{local_ip}]")
        }
    }
}

pub trait FromOpts: Sized {
    fn from_opts(opts: &mut ConnectorOptions) -> std::result::Result<Self, DataFusionError>;
}

#[cfg(test)]
mod tests {
    use crate::{parse_expr, DataSizeUnit};

    #[test]
    fn test_parse_expr() {
        let sql = "concat(1 + hello, 'blah')";
        let parsed = parse_expr(sql).unwrap();
        assert_eq!(parsed.to_string(), sql);
    }

    #[test]
    fn test_data_size_parser() {
        assert_eq!(
            (DataSizeUnit::Bytes, 10),
            DataSizeUnit::parse("10").unwrap()
        );
        assert_eq!(
            (DataSizeUnit::Kilobytes, 54),
            DataSizeUnit::parse("54K").unwrap()
        );
        assert_eq!(
            (DataSizeUnit::Gigabytes, 999921),
            DataSizeUnit::parse("999921gb").unwrap()
        );

        assert!(DataSizeUnit::parse("-14G").is_err());
        assert!(DataSizeUnit::parse("G").is_err());
    }
}
