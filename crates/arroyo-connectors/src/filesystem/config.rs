use arroyo_rpc::var_str::VarStr;
use arroyo_rpc::{ConnectorOptions, FromOpts};
use arroyo_storage::BackendConfig;
use datafusion::common::{plan_err, DataFusionError, Result};
use datafusion::sql::sqlparser::ast::{Expr as SqlExpr, Value, ValueWithSpan};
use regex::Regex;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::num::NonZeroU64;
use strum_macros::EnumString;

const MINIMUM_PART_SIZE: u64 = 5 * 1024 * 1024;

/// Rolling policy for file sinks (when & why to close a file and open a new one).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema, Default)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
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
#[serde(rename_all = "camelCase", deny_unknown_fields)]
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
#[serde(rename_all = "camelCase", deny_unknown_fields)]
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
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct PartitioningConfig {
    /// Date / time format string (e.g. `%Y/%m/%d`).
    #[schemars(
        title = "Time Partition Pattern",
        description = "The pattern of the date string"
    )]
    pub time_partition_pattern: Option<String>,

    /// Field names used for partitioning (bucketed layout).
    #[schemars(
        title = "Partition Fields",
        description = "Fields to partition the data by"
    )]
    #[serde(default)]
    pub partition_fields: Vec<String>,

    /// Partition shuffle settings (see [`PartitionShuffle`]).
    #[schemars(
        title = "Partition shuffle settings",
        description = "Advanced tuning for hash shuffling of partition keys"
    )]
    pub shuffle_by_partition: PartitionShuffle,
}

impl FromOpts for PartitioningConfig {
    fn from_opts(opts: &mut ConnectorOptions) -> Result<Self, DataFusionError> {
        let partition_fields = opts
            .pull_opt_array("partition_fields")
            .map(|fields| {
                fields.into_iter().map(|f| {
                    Ok(match f {
                        SqlExpr::Value(ValueWithSpan{ value: Value::SingleQuotedString(s), span: _}) => s,
                        SqlExpr::Identifier(ident) => ident.value,
                        expr => {
                            return plan_err!("invalid expression in `partition_fields`: {}; expected a column identifier", expr);
                        }
                    })
                }).collect::<Result<_>>()
            })
            .transpose()?
            .unwrap_or_default();

        Ok(Self {
            time_partition_pattern: opts.pull_opt_str("time_partition_pattern")?,
            partition_fields,
            shuffle_by_partition: opts.pull_struct()?,
        })
    }
}

/// Filename generation strategy.
#[derive(
    Debug, Copy, Clone, PartialEq, Serialize, Deserialize, JsonSchema, Default, EnumString,
)]
#[strum(serialize_all = "camelCase")]
#[serde(rename_all = "camelCase")]
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
#[serde(rename_all = "camelCase", deny_unknown_fields)]
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
#[strum(serialize_all = "camelCase")]
#[serde(rename_all = "camelCase")]
#[schemars(title = "Compression format")]
pub enum SourceFileCompressionFormat {
    #[default]
    None,
    Zstd,
    Gzip,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
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
        let regex_pattern = opts.pull_opt_str("regex_pattern")?;

        if let Some(regex) = &regex_pattern {
            if let Err(e) = Regex::new(regex) {
                return plan_err!("could not parse regex_pattern '{regex}': {:?}", e);
            }
        }

        Ok(Self {
            path: pull_path(opts)?,
            compression_format: opts.pull_opt_parsed("compression")?.unwrap_or_default(),
            regex_pattern,
            storage_options: pull_storage_options(opts)?,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
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
#[serde(untagged)]
#[schemars(title = "Table Type")]
pub enum FileSystemTableType {
    Source(FileSystemSource),
    Sink(FileSystemSink),
}

/// Top‑level FileSystem table definition (source or sink).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
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
#[serde(rename_all = "camelCase", deny_unknown_fields)]
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
#[serde(untagged)]
pub enum DeltaLakeTableType {
    Sink(DeltaLakeSink),
}

/// Wrapper allowing future extension of Delta‑Lake table types.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
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
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct IcebergRestCatalog {
    /// Base URL for the REST catalog.
    #[schemars(
        url,
        description = "Base URL for the REST catalog",
        example = "http://localhost:8001/iceberg"
    )]
    pub url: String,

    /// Warehouse to connect to.
    #[schemars(
        description = "The Warehouse to connect to",
        example = "16ba210d70caae96ecb1f6e17afe6f3b_my-bucket"
    )]
    pub warehouse: Option<String>,

    /// Token to use for auth against the REST catalog
    #[schemars(description = "Authentication token", example = "\"asdfkj2h34kjhkj\"")]
    pub token: Option<VarStr>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(untagged)]
pub enum IcebergCatalog {
    Rest(IcebergRestCatalog),
}

/// Main Iceberg profile (wraps the catalog).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
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

fn default_namespace() -> String {
    "default".into()
}

/// Iceberg sink definition.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct IcebergSink {
    /// Table namespace, optionally dot-separated
    #[schemars(title = "Namespace", description = "Table namespace")]
    #[serde(default = "default_namespace")]
    pub namespace: String,

    /// Table identifier
    #[schemars(title = "Table Name", description = "Table name")]
    pub table_name: String,

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
            location_path: opts.pull_opt_str("location_path")?,
            storage_options: pull_storage_options(opts)?,
            rolling_policy: opts.pull_struct()?,
            file_naming: opts.pull_struct()?,
            multipart: opts.pull_struct()?,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(untagged)]
pub enum IcebergTableType {
    Sink(IcebergSink),
}

/// Wrapper allowing future extension of Iceberg table types.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct IcebergTable {
    pub table_type: IcebergTableType,
}

impl FromOpts for IcebergTable {
    fn from_opts(opts: &mut ConnectorOptions) -> Result<Self, DataFusionError> {
        Ok(Self {
            table_type: match opts.pull_str("type")?.as_str() {
                "source" => {
                    return plan_err!("DeltaLake sources are not yet supported");
                }
                "sink" => IcebergTableType::Sink(opts.pull_struct()?),
                _ => {
                    return plan_err!("type must be one of 'source' or 'sink'");
                }
            },
        })
    }
}
