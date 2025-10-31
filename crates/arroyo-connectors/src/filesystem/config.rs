use crate::filesystem::sink::iceberg::transforms;
use anyhow::{anyhow, bail};
use arrow::datatypes::{DataType, Schema};
use arroyo_rpc::var_str::VarStr;
use arroyo_rpc::{ConnectorOptions, FromOpts, TIMESTAMP_FIELD};
use arroyo_storage::BackendConfig;
use datafusion::common::{plan_datafusion_err, plan_err, DFSchema, DataFusionError, Result, ScalarValue};
use datafusion::prelude::{col, concat, lit, to_char, Expr};
use datafusion::sql::sqlparser::ast::{Expr as SqlExpr, FunctionArg, FunctionArgExpr, FunctionArguments, Value, ValueWithSpan};
use datafusion_expr::ExprSchemable;
use iceberg::spec::{PartitionSpec, SchemaRef};
use regex::Regex;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fmt::{Display, Formatter};
use std::num::NonZeroU64;
use datafusion::logical_expr::sqlparser::ast::Function;
use datafusion::sql::sqlparser;
use strum_macros::EnumString;
use core::slice::Iter;

const MINIMUM_PART_SIZE: u64 = 5 * 1024 * 1024;

/// Rolling policy for file sinks (when & why to close a file and open a new one).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema, Default)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
#[schemars(title = "File Rolling Policy")]
pub struct RollingPolicy {
    /// Files will be rolled after reaching this number of bytes.
    #[schemars(
        title = "File Size",
        description = "Files will be rolled after reaching this number of bytes"
    )]
    pub file_size_bytes: Option<u64>,

    /// Number of seconds to wait before rolling over to a new file.
    #[schemars(
        title = "Interval Seconds",
        description = "Number of seconds to wait before rolling over to a new file",
        range(min = 1)
    )]
    pub interval_seconds: Option<u64>,

    /// Number of seconds of inactivity to wait before rolling over to a new file.
    #[schemars(
        title = "Inactivity Seconds",
        description = "Number of seconds of inactivity to wait before rolling over to a new file",
        range(min = 1)
    )]
    pub inactivity_seconds: Option<u64>,
}

impl FromOpts for RollingPolicy {
    fn from_opts(opts: &mut ConnectorOptions) -> Result<Self, DataFusionError> {
        Ok(Self {
            file_size_bytes: opts.pull_opt_data_size_bytes("rolling_policy.file_size")?,
            interval_seconds: opts
                .pull_opt_duration("rolling_policy.interval")?
                .map(|f| f.as_secs()),
            inactivity_seconds: opts
                .pull_opt_duration("rolling_policy.inactivity_interval")?
                .map(|f| f.as_secs()),
        })
    }
}

/// Multipart‑upload tuning for object stores that need it.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema, Default)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
#[schemars(title = "Multipart Upload Settings")]
pub struct MultipartConfig {
    /// Target size for each part of the multipart upload, in bytes.
    #[schemars(
        title       = "Target Part Size",
        description = "Target size for each part of the multipart upload, in bytes",
        range(min = MINIMUM_PART_SIZE)
    )]
    pub target_part_size_bytes: Option<u64>,

    /// Maximum number of parts allowed by the object store.
    #[schemars(
        title = "Max Parts",
        description = "Maximum number of parts to upload in a multipart upload"
    )]
    pub max_parts: Option<NonZeroU64>,
}

impl FromOpts for MultipartConfig {
    fn from_opts(opts: &mut ConnectorOptions) -> std::result::Result<Self, DataFusionError> {
        Ok(Self {
            target_part_size_bytes: opts
                .pull_opt_data_size_bytes("multipart.target_part_size")?
                .map(|p| {
                    if p < MINIMUM_PART_SIZE {
                        plan_err!("multipart.target_part_size must be >= {MINIMUM_PART_SIZE}")
                    } else {
                        Ok(p)
                    }
                })
                .transpose()?,
            max_parts: opts.pull_opt_nonzero_u64("multipart.max_parts")?,
        })
    }
}

/// Enable / disable hash‑shuffle on partition keys.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema, Default)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub struct PartitionShuffle {
    /// If enabled, shuffle by partition keys to reduce the number of files;
    /// may cause backlog on skewed data.
    #[schemars(
        title = "Enable partition shuffling",
        description = "If enabled, we will shuffle by the partition keys, which can \
                       reduce the number of files a sink produces; however this may \
                       cause backlog if data is skewed"
    )]
    #[serde(default)]
    pub enabled: bool,
}

impl FromOpts for PartitionShuffle {
    fn from_opts(opts: &mut ConnectorOptions) -> Result<Self, DataFusionError> {
        Ok(Self {
            enabled: opts
                .pull_opt_bool("shuffle_by_partition.enabled")?
                .unwrap_or_default(),
        })
    }
}

/// Data‑layout partitioning for sinks.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema, Default)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub struct PartitioningConfig {
    /// Date / time format string (e.g. `%Y/%m/%d`).
    #[schemars(
        title = "Time Partition Pattern",
        description = "The pattern of the date string"
    )]
    pub time_pattern: Option<String>,

    /// Field names used for partitioning (bucketed layout).
    #[schemars(
        title = "Partition Fields",
        description = "Fields to partition the data by"
    )]
    #[serde(default)]
    pub fields: Vec<String>,

    /// Partition shuffle settings (see [`PartitionShuffle`]).
    #[schemars(
        title = "Partition shuffle settings",
        description = "Advanced tuning for hash shuffling of partition keys"
    )]
    pub shuffle_by_partition: PartitionShuffle,
}

impl PartitioningConfig {
    pub fn partition_expr(&self, schema: &Schema) -> anyhow::Result<Option<Expr>> {
        Ok(match (&self.time_pattern, &self.fields) {
            (None, fields) if !fields.is_empty() => {
                Some(Self::field_logical_expression(schema, fields)?)
            }
            (None, _) => None,
            (Some(pattern), fields) if !fields.is_empty() => Some(
                Self::partition_string_for_fields_and_time(schema, fields, pattern)?,
            ),
            (Some(pattern), _) => Some(Self::timestamp_logical_expression(pattern)),
        })
    }

    fn partition_string_for_fields_and_time(
        schema: &Schema,
        partition_fields: &[String],
        time_partition_pattern: &str,
    ) -> anyhow::Result<Expr> {
        let field_function = Self::field_logical_expression(schema, partition_fields)?;
        let time_function = Self::timestamp_logical_expression(time_partition_pattern);
        let function = concat(vec![
            time_function,
            Expr::Literal(ScalarValue::Utf8(Some("/".to_string())), None),
            field_function,
        ]);
        Ok(function)
    }

    fn field_logical_expression(
        schema: &Schema,
        partition_fields: &[String],
    ) -> anyhow::Result<Expr> {
        let columns_as_string = partition_fields
            .iter()
            .map(|field| {
                let field = schema.field_with_name(field).map_err(|e| {
                    anyhow!(
                        "partition field '{field}' does not exist in the schema ({:?})",
                        e
                    )
                })?;
                let column_expr = col(field.name());
                let expr = match field.data_type() {
                    DataType::Utf8 => column_expr,
                    _ => Expr::Cast(datafusion::logical_expr::Cast {
                        expr: Box::new(column_expr),
                        data_type: DataType::Utf8,
                    }),
                };
                Ok((field.name(), expr))
            })
            .collect::<anyhow::Result<Vec<_>>>()?;

        let function = concat(
            columns_as_string
                .into_iter()
                .enumerate()
                .flat_map(|(i, (name, expr))| {
                    let preamble = if i == 0 {
                        format!("{name}=")
                    } else {
                        format!("/{name}=")
                    };
                    vec![Expr::Literal(ScalarValue::Utf8(Some(preamble)), None), expr]
                })
                .collect(),
        );

        Ok(function)
    }

    fn timestamp_logical_expression(time_partition_pattern: &str) -> Expr {
        to_char(col(TIMESTAMP_FIELD), lit(time_partition_pattern))
    }
}

impl FromOpts for PartitioningConfig {
    fn from_opts(opts: &mut ConnectorOptions) -> Result<Self, DataFusionError> {
        let fields = opts
            .pull_opt_array("partitioning.fields")
            .map(|fields| {
                fields.into_iter().map(|f| {
                    Ok(match f {
                        SqlExpr::Value(ValueWithSpan{ value: Value::SingleQuotedString(s), span: _}) => s,
                        SqlExpr::Identifier(ident) => ident.value,
                        expr => {
                            return plan_err!("invalid expression in `partitioning.fields`: {}; expected a column identifier", expr);
                        }
                    })
                }).collect::<Result<_>>()
            })
            .transpose()?
            .unwrap_or_default();

        Ok(Self {
            time_pattern: opts.pull_opt_str("partitioning.time_pattern")?,
            fields,
            shuffle_by_partition: opts.pull_struct()?,
        })
    }
}

/// Filename generation strategy.
#[derive(
    Debug, Copy, Clone, PartialEq, Serialize, Deserialize, JsonSchema, Default, EnumString,
)]
#[strum(serialize_all = "snake_case")]
#[serde(rename_all = "snake_case")]
#[schemars(title = "Filename Strategy")]
pub enum FilenameStrategy {
    Serial,
    #[default]
    Uuid,
    UuidV7,
    Ulid,
}

/// Controls filename prefix/suffix and strategy.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema, Default)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
#[schemars(title = "File Naming")]
pub struct NamingConfig {
    /// The prefix to use in file names (e.g. `prefix-<uuid>.parquet`).
    #[schemars(
        title = "Filename Prefix",
        description = "The prefix to use in file name. i.e prefix-<uuid>.parquet"
    )]
    pub prefix: Option<String>,

    /// Override the default suffix (e.g. `.parquet`) – use with caution.
    #[schemars(
        title = "Filename Suffix",
        description = "This will overwrite the default file suffix. i.e .parquet, use with caution"
    )]
    pub suffix: Option<String>,

    pub strategy: Option<FilenameStrategy>,
}

impl FromOpts for NamingConfig {
    fn from_opts(opts: &mut ConnectorOptions) -> Result<Self, DataFusionError> {
        Ok(Self {
            prefix: opts.pull_opt_str("filename.prefix")?,
            suffix: opts.pull_opt_str("filename.suffix")?,
            strategy: opts.pull_opt_parsed("filename.strategy")?,
        })
    }
}

/* -------------------------------------------------------------------- */
/*                     File‑system source & sink                        */
/* -------------------------------------------------------------------- */

/// Compression methods recognised by the FileSystem source.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema, EnumString, Default)]
#[strum(serialize_all = "snake_case")]
#[serde(rename_all = "snake_case")]
#[schemars(title = "Compression format")]
pub enum SourceFileCompressionFormat {
    #[default]
    None,
    Zstd,
    Gzip,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
#[schemars(title = "Source")]
pub struct FileSystemSource {
    /// URI of the folder to read from.
    #[schemars(title = "Path", description = "URI of the folder to read from")]
    pub path: String,

    /// Compression format of the files in the source path.
    #[schemars(description = "Compression format of the files in the source path")]
    pub compression_format: SourceFileCompressionFormat,

    /// Regex matching pattern (searches recursively under `path`).
    #[schemars(
        title = "File Regex Pattern",
        description = "Regex pattern for files to include in source. Will search everything under the source path."
    )]
    pub regex_pattern: Option<String>,

    #[schemars(
        title = "Storage Options",
        description = "See the FileSystem connector docs for the full list of options"
    )]
    #[serde(default)]
    pub storage_options: HashMap<String, String>,
}

fn pull_storage_options(opts: &mut ConnectorOptions) -> Result<HashMap<String, String>> {
    let storage_keys: Vec<_> = opts.keys_with_prefix("storage.").cloned().collect();

    let storage_options = storage_keys
        .iter()
        .map(|k| {
            Ok((
                k.trim_start_matches("storage.").to_string(),
                opts.pull_str(k)?,
            ))
        })
        .collect::<Result<HashMap<String, String>>>()?;

    Ok(storage_options)
}

fn pull_path(opts: &mut ConnectorOptions) -> Result<String, DataFusionError> {
    let path = opts.pull_str("path")?;
    if let Err(e) = BackendConfig::parse_url(&path, true) {
        return plan_err!("invalid path '{path}': {:?}", e);
    }

    Ok(path)
}

impl FromOpts for FileSystemSource {
    fn from_opts(opts: &mut ConnectorOptions) -> Result<Self, DataFusionError> {
        let regex_pattern = opts.pull_opt_str("source.regex_pattern")?;

        if let Some(regex) = &regex_pattern {
            if let Err(e) = Regex::new(regex) {
                return plan_err!("could not parse regex_pattern '{regex}': {:?}", e);
            }
        }

        Ok(Self {
            path: pull_path(opts)?,
            compression_format: opts
                .pull_opt_parsed("source.compression")?
                .unwrap_or_default(),
            regex_pattern,
            storage_options: pull_storage_options(opts)?,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
#[schemars(title = "Sink")]
pub struct FileSystemSink {
    /// URI of the folder to write to.
    #[schemars(title = "Path", description = "URI of the folder to write to")]
    pub path: String,

    #[schemars(
        title = "Storage Options",
        description = "See the FileSystem connector docs for the full list of options"
    )]
    #[serde(default)]
    pub storage_options: HashMap<String, String>,

    #[serde(default)]
    pub rolling_policy: RollingPolicy,

    #[serde(default)]
    pub file_naming: NamingConfig,

    #[serde(default)]
    pub partitioning: PartitioningConfig,

    #[serde(default)]
    pub multipart: MultipartConfig,
}

impl FromOpts for FileSystemSink {
    fn from_opts(opts: &mut ConnectorOptions) -> Result<Self, DataFusionError> {
        Ok(Self {
            path: pull_path(opts)?,
            storage_options: pull_storage_options(opts)?,
            rolling_policy: opts.pull_struct()?,
            file_naming: opts.pull_struct()?,
            partitioning: opts.pull_struct()?,
            multipart: opts.pull_struct()?,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "type", rename_all = "snake_case")]
#[schemars(title = "Table Type")]
pub enum FileSystemTableType {
    Source(FileSystemSource),
    Sink(FileSystemSink),
}

/// Top‑level FileSystem table definition (source or sink).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub struct FileSystemTable {
    pub table_type: FileSystemTableType,
}

impl FromOpts for FileSystemTable {
    fn from_opts(opts: &mut ConnectorOptions) -> Result<Self, DataFusionError> {
        Ok(Self {
            table_type: match opts.pull_str("type")?.as_str() {
                "source" => FileSystemTableType::Source(opts.pull_struct()?),
                "sink" => FileSystemTableType::Sink(opts.pull_struct()?),
                _ => {
                    return plan_err!("type must be one of 'source' or 'sink'");
                }
            },
        })
    }
}

/* -------------------------------------------------------------------- */
/*                           Delta Lake sink                            */
/* -------------------------------------------------------------------- */

/// Delta‑Lake sink definition.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub struct DeltaLakeSink {
    #[schemars(title = "Path", description = "URI of the DeltaLake table to write to")]
    pub path: String,

    #[schemars(
        title = "Storage Options",
        description = "See the FileSystem connector docs for the full list of options"
    )]
    #[serde(default)]
    pub storage_options: HashMap<String, String>,

    #[serde(default)]
    pub rolling_policy: RollingPolicy,

    #[serde(default)]
    pub file_naming: NamingConfig,

    #[serde(default)]
    pub partitioning: PartitioningConfig,

    #[serde(default)]
    pub multipart: MultipartConfig,
}

impl FromOpts for DeltaLakeSink {
    fn from_opts(opts: &mut ConnectorOptions) -> Result<Self, DataFusionError> {
        Ok(Self {
            path: pull_path(opts)?,
            storage_options: pull_storage_options(opts)?,
            rolling_policy: opts.pull_struct()?,
            file_naming: opts.pull_struct()?,
            partitioning: opts.pull_struct()?,
            multipart: opts.pull_struct()?,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum DeltaLakeTableType {
    Sink(DeltaLakeSink),
}

/// Wrapper allowing future extension of Delta‑Lake table types.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub struct DeltaLakeTable {
    pub table_type: DeltaLakeTableType,
}

impl FromOpts for DeltaLakeTable {
    fn from_opts(opts: &mut ConnectorOptions) -> Result<Self, DataFusionError> {
        Ok(Self {
            table_type: match opts.pull_str("type")?.as_str() {
                "source" => {
                    return plan_err!("DeltaLake sources are not yet supported");
                }
                "sink" => DeltaLakeTableType::Sink(opts.pull_struct()?),
                _ => {
                    return plan_err!("type must be one of 'source' or 'sink'");
                }
            },
        })
    }
}

/* -------------------------------------------------------------------- */
/*                       Iceberg profile & sink                         */
/* -------------------------------------------------------------------- */

/// REST catalog connector for Apache Iceberg.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub struct IcebergRestCatalog {
    /// Base URL for the REST catalog.
    #[schemars(
        url,
        description = "Base URL for the REST catalog",
        example = "http://localhost:8001/catalog"
    )]
    pub url: String,

    /// Warehouse to connect to.
    #[schemars(
        description = "The Warehouse to connect to",
        example = "\"my_warehouse\""
    )]
    pub warehouse: Option<String>,

    /// Token to use for auth against the REST catalog
    #[schemars(description = "Authentication token", example = "\"asdfkj2h34kjhkj\"")]
    pub token: Option<VarStr>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum IcebergCatalog {
    Rest(IcebergRestCatalog),
}

/// Main Iceberg profile (wraps the catalog).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub struct IcebergProfile {
    pub catalog: IcebergCatalog,
}

impl FromOpts for IcebergProfile {
    fn from_opts(opts: &mut ConnectorOptions) -> std::result::Result<Self, DataFusionError> {
        let catalog_type = opts.pull_str("catalog.type")?;
        let warehouse = opts.pull_opt_str("catalog.warehouse")?;
        match catalog_type.as_str() {
            "rest" => Ok(Self {
                catalog: IcebergCatalog::Rest(IcebergRestCatalog {
                    url: opts.pull_str("catalog.rest.url")?,
                    warehouse,
                    token: opts.pull_opt_str("catalog.rest.token")?.map(VarStr::new),
                }),
            }),
            s => {
                plan_err!("unsupported Iceberg catalog.type '{}'", s)
            }
        }
    }
}

/// Iceberg partitioning transforms
#[derive(Debug, Copy, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "transform", rename_all = "snake_case")]
pub enum Transform {
    /// Source value, unmodified
    Identity,
    /// Hash of value, mod `N`.
    Bucket { arg0: u32 },
    /// Value truncated to width `W`
    Truncate { arg0: u32 },
    /// Extract a date or timestamp year, as years from 1970
    Year,
    /// Extract a date or timestamp month, as months from 1970-01-01
    Month,
    /// Extract a date or timestamp day, as days from 1970-01-01
    Day,
    /// Extract a timestamp hour, as hours from 1970-01-01 00:00:00
    Hour,
    /// Always produces `null`
    Void,
}

impl Transform {
    pub fn name(&self) -> &'static str {
        match self {
            Transform::Identity => "identity",
            Transform::Bucket { .. } => "bucket",
            Transform::Truncate { .. } => "truncate",
            Transform::Year => "year",
            Transform::Month => "month",
            Transform::Day => "day",
            Transform::Hour => "hour",
            Transform::Void => "void",
        }
    }
}

impl From<Transform> for iceberg::spec::Transform {
    fn from(value: Transform) -> Self {
        use iceberg::spec::Transform as ITransform;

        match value {
            Transform::Identity => ITransform::Identity,
            Transform::Bucket { arg0: n } => ITransform::Bucket(n),
            Transform::Truncate { arg0: width } => ITransform::Truncate(width),
            Transform::Year => ITransform::Year,
            Transform::Month => ITransform::Month,
            Transform::Day => ITransform::Day,
            Transform::Hour => ITransform::Hour,
            Transform::Void => ITransform::Void,
        }
    }
}

/// Iceberg partitioning field configuration
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub struct IcebergPartitioningField {
    pub name: Option<String>,
    pub field: String,
    #[serde(flatten)]
    pub transform_fn: Transform,
}

impl IcebergPartitioningField {
    pub fn name(&self) -> String {
        if let Some(name) = &self.name {
            return name.clone();
        }

        let t = match self.transform_fn {
            Transform::Identity => "identity".to_string(),
            Transform::Bucket { arg0: n } => format!("bucket_{n}"),
            Transform::Truncate { arg0: width } => format!("truncate_{width}"),
            Transform::Year => "year".to_string(),
            Transform::Month => "month".to_string(),
            Transform::Day => "day".to_string(),
            Transform::Hour => "hour".to_string(),
            Transform::Void => "void".to_string(),
        };

        format!("{}_{}", self.field, t)
    }
}

impl TryFrom<&sqlparser::ast::Function> for IcebergPartitioningField {
    type Error = DataFusionError;

    fn try_from(value: &Function) -> Result<Self, Self::Error> {
        let FunctionArguments::List(args) = &value.args else {
            return plan_err!("expected arg list");
        };

        fn take_arg<'a, 'b>(f: &'a str, expected: u32, iter: &'b mut Iter<FunctionArg>) -> Result<&'b sqlparser::ast::Expr, DataFusionError> {
            let arg = iter.next().ok_or_else(||
                plan_datafusion_err!("Iceberg PARTITION BY function '{}' expects {} arguments", f, expected))?;
            let FunctionArg::Unnamed(FunctionArgExpr::Expr(e)) = arg else {
                return plan_err!("unexpected argument to PARTITION BY fucntion '{}'", f);
            };

            Ok(e)
        }

        fn take_u32<'a, 'b>(f: &'a str, expected: u32, iter: &'b mut Iter<FunctionArg>) -> Result<u32, DataFusionError> {
            let sqlparser::ast::Expr::Value(value) = take_arg(f, expected, iter)? else {
                return plan_err!("expected a second argument for '{}' in iceberg PARTITION BY", f);
            };

            let Value::Number(n, _) = &value.value else {
                return plan_err!("expected number as second argument for '{}' in iceberg PARTITION BY", f);
            };

            let n: u32 = n.parse().map_err(|_| plan_datafusion_err!("expected u32 as second argument for '{}' in iceberg PARTITION BY", f))?;

            Ok(n)
        }

        let mut arg_iter = args.args.iter();

        let name = value.name.to_string();

        let sqlparser::ast::Expr::Identifier(ident) = take_arg(&name, 1, &mut arg_iter)? else {
            return plan_err!("expected field identifier as first argument for '{}' in iceberg PARTITION BY", name);
        };

        let field = ident.value.to_string();

        let transform = match name.as_str() {
            "identity" => Transform::Identity,
            "bucket" => Transform::Bucket {  arg0: take_u32(&name, 2, &mut arg_iter)? },
            "truncate" => Transform::Truncate {  arg0: take_u32(&name, 2, &mut arg_iter)? },
            "year" => Transform::Year,
            "month" => Transform::Month,
            "day" => Transform::Day,
            "hour" => Transform::Hour,
            "void" => Transform::Void,
            _ => {
                return plan_err!("unsupported iceberg transform function '{}' in PARTITION BY", name);
            }
        };

        Ok(Self {
            name: None,
            field,
            transform_fn: transform,
        })
    }
}

impl Display for IcebergPartitioningField {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}({}", self.transform_fn.name(), self.field)?;
        if let Transform::Bucket { arg0: i } | Transform::Truncate { arg0: i } = self.transform_fn {
            write!(f, ", {i}")?;
        }
        write!(f, ")")
    }
}

/// Iceberg partitioning configuration
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema, Default)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub struct IcebergPartitioning {
    /// Field names used for partitioning (bucketed layout).
    #[schemars(
        title = "Partition Fields",
        description = "Fields to partition the data by"
    )]
    #[serde(default)]
    pub fields: Vec<IcebergPartitioningField>,

    /// Partition shuffle settings (see [`PartitionShuffle`]).
    #[schemars(
        title = "Partition shuffle settings",
        description = "Advanced tuning for hash shuffling of partition keys"
    )]
    #[serde(default)]
    pub shuffle_by_partition: PartitionShuffle,
}

impl FromOpts for IcebergPartitioning {
    fn from_opts(opts: &mut ConnectorOptions) -> std::result::Result<Self, DataFusionError> {
        let mut fields = vec![];

        for expr in opts.partitions() {
            let sqlparser::ast::Expr::Function(f) = expr else {
                return plan_err!("unexpected expression in PARTITION BY: {:?}", expr);
            };

            fields.push(f.try_into()?);
        }

        Ok(Self {
            fields,
            shuffle_by_partition: opts.pull_struct()?,
        })
    }
}

impl IcebergPartitioning {
    pub fn as_partition_spec(&self, schema: SchemaRef) -> anyhow::Result<PartitionSpec> {
        let mut builder = PartitionSpec::builder(schema.clone());

        for f in &self.fields {
            builder = builder.add_partition_field(&f.field, f.name(), f.transform_fn.into())?;
        }

        Ok(builder.build()?)
    }

    pub fn partition_expr(&self, schema: &Schema) -> anyhow::Result<Option<Vec<Expr>>> {
        let exprs: Vec<_> = self
            .fields
            .iter()
            .map(|f| match f.transform_fn {
                Transform::Identity => transforms::fns::ice_identity(),
                Transform::Bucket { arg0: n } => transforms::fns::ice_bucket(col(&f.field), lit(n)),
                Transform::Truncate { arg0: width } => {
                    transforms::fns::ice_bucket(col(&f.field), lit(width))
                }
                Transform::Year => transforms::fns::ice_year(col(&f.field)),
                Transform::Month => transforms::fns::ice_month(col(&f.field)),
                Transform::Day => transforms::fns::ice_day(col(&f.field)),
                Transform::Hour => transforms::fns::ice_hour(col(&f.field)),
                Transform::Void => lit(ScalarValue::Null),
            })
            .collect();

        let dfschema = DFSchema::try_from(schema.clone())?;

        for (expr, field) in exprs.iter().zip(&self.fields) {
            let field = schema.field_with_name(&field.field).map_err(|e| {
                anyhow!(
                    "partition field '{field}' does not exist in the schema ({:?})",
                    e
                )
            })?;

            if expr.data_type_and_nullable(&dfschema).is_err() {
                bail!("partition transform {} has invalid types", field);
            }
        }

        if exprs.is_empty() {
            Ok(None)
        } else {
            Ok(Some(exprs))
        }
    }
}

fn default_namespace() -> String {
    "default".into()
}

/// Iceberg sink definition.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub struct IcebergSink {
    /// Table namespace, optionally dot-separated
    #[schemars(title = "Namespace", description = "Table namespace")]
    #[serde(default = "default_namespace")]
    pub namespace: String,

    /// Table identifier
    #[schemars(title = "Table Name", description = "Table name")]
    pub table_name: String,

    #[schemars(title = "Partitioning", description = "Iceberg partitioning config")]
    #[serde(default)]
    pub partitioning: IcebergPartitioning,

    /// Optional path controlling where parquet files will be written, if creating the table
    #[schemars(title = "Location Path", description = "Data file location")]
    pub location_path: Option<String>,

    #[schemars(
        title = "Storage Options",
        description = "See the FileSystem connector docs for the full list of options"
    )]
    #[serde(default)]
    pub storage_options: HashMap<String, String>,

    #[serde(default)]
    pub rolling_policy: RollingPolicy,

    #[serde(default)]
    pub file_naming: NamingConfig,

    #[serde(default)]
    pub multipart: MultipartConfig,
}

impl FromOpts for IcebergSink {
    fn from_opts(opts: &mut ConnectorOptions) -> Result<Self, DataFusionError> {
        Ok(Self {
            namespace: opts
                .pull_opt_str("namespace")?
                .unwrap_or_else(|| "default".to_string()),
            table_name: opts.pull_str("table_name")?,
            partitioning: opts.pull_struct()?,
            location_path: opts.pull_opt_str("location_path")?,
            storage_options: pull_storage_options(opts)?,
            rolling_policy: opts.pull_struct()?,
            file_naming: opts.pull_struct()?,
            multipart: opts.pull_struct()?,
        })
    }
}

/// Wrapper allowing future extension of Iceberg table types.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum IcebergTable {
    Sink(IcebergSink),
}

impl FromOpts for IcebergTable {
    fn from_opts(opts: &mut ConnectorOptions) -> Result<Self, DataFusionError> {
        Ok(match opts.pull_str("type")?.as_str() {
            "source" => {
                return plan_err!("Iceberg sources are not yet supported");
            }
            "sink" => IcebergTable::Sink(opts.pull_struct()?),
            _ => {
                return plan_err!("type must be 'sink'");
            }
        })
    }
}
