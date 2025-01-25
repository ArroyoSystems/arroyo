pub mod delta;
mod sink;
mod source;

use anyhow::{anyhow, bail, Result};
use arroyo_storage::BackendConfig;
use datafusion::sql::sqlparser::ast::{Expr as SqlExpr, Value};
use regex::Regex;
use std::collections::{HashMap, HashSet};
use typify::import_types;

use crate::EmptyConfig;
use arroyo_operator::connector::Connection;
use arroyo_rpc::api_types::connections::{
    ConnectionProfile, ConnectionSchema, ConnectionType, TestSourceMessage,
};
use arroyo_rpc::formats::Format;
use arroyo_rpc::{ConnectorOptions, OperatorConfig};
use serde::{Deserialize, Serialize};

use crate::filesystem::source::FileSystemSourceFunc;
use arroyo_operator::connector::Connector;
use arroyo_operator::operator::ConstructedOperator;

use self::sink::{
    JsonFileSystemSink, LocalJsonFileSystemSink, LocalParquetFileSystemSink, ParquetFileSystemSink,
};

const TABLE_SCHEMA: &str = include_str!("./table.json");
const ICON: &str = include_str!("./filesystem.svg");

import_types!(schema = "src/filesystem/table.json");

pub struct FileSystemConnector {}

impl Connector for FileSystemConnector {
    type ProfileT = EmptyConfig;

    type TableT = FileSystemTable;

    fn name(&self) -> &'static str {
        "filesystem"
    }

    fn metadata(&self) -> arroyo_rpc::api_types::connections::Connector {
        arroyo_rpc::api_types::connections::Connector {
            id: "filesystem".to_string(),
            name: "FileSystem".to_string(),
            icon: ICON.to_string(),
            description: "Read or write to a filesystem or object store like S3".to_string(),
            enabled: true,
            source: true,
            sink: true,
            testing: true,
            hidden: false,
            custom_schemas: true,
            connection_config: None,
            table_config: TABLE_SCHEMA.to_owned(),
        }
    }

    fn test(
        &self,
        _: &str,
        _: Self::ProfileT,
        table: Self::TableT,
        _: Option<&ConnectionSchema>,
        tx: tokio::sync::mpsc::Sender<TestSourceMessage>,
    ) {
        let mut failed = false;
        let mut message = "Successfully validated connection".to_string();
        match table.table_type {
            TableType::Source { regex_pattern, .. } => {
                let r = regex_pattern
                    .as_ref()
                    .map(|pattern| Regex::new(pattern))
                    .transpose();
                if let Err(e) = r {
                    failed = true;
                    message = format!(
                        "Invalid regex pattern: {}, {}",
                        regex_pattern.as_ref().unwrap(),
                        e
                    );
                }
            }
            TableType::Sink { .. } => {
                // TODO: implement sink testing
            }
        }

        tokio::task::spawn(async move {
            let message = TestSourceMessage {
                error: failed,
                done: true,
                message,
            };
            tx.send(message).await.unwrap();
        });
    }

    fn table_type(&self, _: Self::ProfileT, table: Self::TableT) -> ConnectionType {
        match table.table_type {
            TableType::Source { .. } => ConnectionType::Source,
            TableType::Sink { .. } => ConnectionType::Sink,
        }
    }

    fn from_config(
        &self,
        id: Option<i64>,
        name: &str,
        config: Self::ProfileT,
        table: Self::TableT,
        schema: Option<&ConnectionSchema>,
    ) -> anyhow::Result<Connection> {
        let (description, connection_type) = match table.table_type {
            TableType::Source { .. } => ("FileSystem".to_string(), ConnectionType::Source),
            TableType::Sink {
                ref write_path,
                ref format_settings,
                ref file_settings,
                ..
            } => {
                // confirm commit style is Direct
                let Some(CommitStyle::Direct) = file_settings
                    .as_ref()
                    .ok_or_else(|| anyhow!("no file_settings"))?
                    .commit_style
                else {
                    bail!("commit_style must be Direct");
                };

                let backend_config = BackendConfig::parse_url(write_path, true)?;
                let is_local = backend_config.is_local();
                let description = match (format_settings, is_local) {
                    (Some(FormatSettings::Parquet { .. }), true) => {
                        "LocalFileSystem<Parquet>".to_string()
                    }
                    (Some(FormatSettings::Parquet { .. }), false) => {
                        "FileSystem<Parquet>".to_string()
                    }
                    (Some(FormatSettings::Json { .. }), true) => {
                        "LocalFileSystem<JSON>".to_string()
                    }
                    (Some(FormatSettings::Json { .. }), false) => "FileSystem<JSON>".to_string(),
                    (None, _) => bail!("have to have some format settings"),
                };
                (description, ConnectionType::Sink)
            }
        };

        let schema = schema
            .map(|s| s.to_owned())
            .ok_or_else(|| anyhow!("no schema defined for FileSystem connection"))?;

        let format = schema
            .format
            .as_ref()
            .map(|t| t.to_owned())
            .ok_or_else(|| anyhow!("'format' must be set for FileSystem connection"))?;

        let config = OperatorConfig {
            connection: serde_json::to_value(config).unwrap(),
            table: serde_json::to_value(table).unwrap(),
            rate_limit: None,
            format: Some(format),
            bad_data: schema.bad_data.clone(),
            framing: schema.framing.clone(),
            metadata_fields: schema.metadata_fields(),
        };

        Ok(Connection::new(
            id,
            self.name(),
            name.to_string(),
            connection_type,
            schema,
            &config,
            description,
        ))
    }

    fn from_options(
        &self,
        name: &str,
        options: &mut ConnectorOptions,
        schema: Option<&ConnectionSchema>,
        _: Option<&ConnectionProfile>,
    ) -> anyhow::Result<Connection> {
        match options.pull_opt_str("type")? {
            Some(t) if t == "source" => {
                let (storage_url, storage_options) = get_storage_url_and_options(options)?;
                let compression_format = options
                    .pull_opt_str("compression_format")?
                    .map(|format| format.as_str().try_into().map_err(|err: &str| anyhow!(err)))
                    .transpose()?
                    .unwrap_or(CompressionFormat::None);
                let matching_pattern = options.pull_opt_str("source.regex-pattern")?;
                self.from_config(
                    None,
                    name,
                    EmptyConfig {},
                    FileSystemTable {
                        table_type: TableType::Source {
                            path: storage_url,
                            storage_options,
                            compression_format: Some(compression_format),
                            regex_pattern: matching_pattern,
                        },
                    },
                    schema,
                )
            }
            Some(t) if t == "sink" => {
                let table = file_system_sink_from_options(options, schema, CommitStyle::Direct)?;

                self.from_config(None, name, EmptyConfig {}, table, schema)
            }
            Some(t) => bail!("unknown type: {}", t),
            None => bail!("must have type set"),
        }
    }

    fn make_operator(
        &self,
        _: Self::ProfileT,
        table: Self::TableT,
        config: OperatorConfig,
    ) -> Result<ConstructedOperator> {
        match &table.table_type {
            TableType::Source { .. } => Ok(ConstructedOperator::from_source(Box::new(
                FileSystemSourceFunc {
                    table: table.table_type.clone(),
                    format: config
                        .format
                        .ok_or_else(|| anyhow!("format required for FileSystem source"))?,
                    framing: config.framing.clone(),
                    bad_data: config.bad_data.clone(),
                    file_states: HashMap::new(),
                },
            ))),
            TableType::Sink {
                file_settings: _,
                format_settings,
                storage_options: _,
                write_path,
                shuffle_by_partition,
            } => {
                let backend_config = BackendConfig::parse_url(write_path, true)?;
                match (format_settings, backend_config.is_local()) {
                    (Some(FormatSettings::Parquet { .. }), true) => {
                        Ok(ConstructedOperator::from_operator(Box::new(
                            LocalParquetFileSystemSink::new(write_path.to_string(), table, config),
                        )))
                    }
                    (Some(FormatSettings::Parquet { .. }), false) => {
                        Ok(ConstructedOperator::from_operator(Box::new(
                            ParquetFileSystemSink::new(table, config),
                        )))
                    }
                    (Some(FormatSettings::Json { .. }), true) => {
                        Ok(ConstructedOperator::from_operator(Box::new(
                            LocalJsonFileSystemSink::new(write_path.to_string(), table, config),
                        )))
                    }
                    (Some(FormatSettings::Json { .. }), false) => {
                        Ok(ConstructedOperator::from_operator(Box::new(
                            JsonFileSystemSink::new(table, config),
                        )))
                    }
                    (None, _) => bail!("have to have some format settings"),
                }
            }
        }
    }
}

fn get_storage_url_and_options(
    opts: &mut ConnectorOptions,
) -> Result<(String, HashMap<String, String>)> {
    let storage_url = opts.pull_str("path")?;
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

    BackendConfig::parse_url(&storage_url, true)?;
    Ok((storage_url, storage_options))
}

pub fn file_system_sink_from_options(
    opts: &mut ConnectorOptions,
    schema: Option<&ConnectionSchema>,
    commit_style: CommitStyle,
) -> Result<FileSystemTable> {
    let schema = schema.ok_or_else(|| anyhow!("FileSystem connector requires a schema"))?;

    let (storage_url, storage_options) = get_storage_url_and_options(opts)?;

    let inactivity_rollover_seconds = opts.pull_opt_i64("inactivity_rollover_seconds")?;
    let max_parts = opts.pull_opt_i64("max_parts")?;
    let rollover_seconds = opts.pull_opt_i64("rollover_seconds")?;
    let target_file_size = opts.pull_opt_i64("target_file_size")?;
    let target_part_size = opts.pull_opt_i64("target_part_size")?;
    let prefix = opts.pull_opt_str("filename.prefix")?;
    let suffix = opts.pull_opt_str("filename.suffix")?;
    let strategy = opts
        .pull_opt_str("filename.strategy")?
        .map(|value| {
            FilenameStrategy::try_from(&value)
                .map_err(|_err| anyhow!("{} is not a valid Filenaming Strategy", value))
        })
        .transpose()?;

    let schema_fields: HashSet<_> = schema
        .fields
        .iter()
        .map(|f| f.field_name.as_str())
        .collect();

    let partition_fields: Option<Vec<_>> = opts
        .pull_opt_array("partition_fields")
        .map(|fields| {
            if !fields.is_empty() && schema.inferred.unwrap_or_default() {
                bail!("cannot use `partition_fields` option for an inferred schema; supply the fields of the sink within the CREATE TABLE statement");
            }

            fields.into_iter().map(|f| {
                let f = match f {
                    SqlExpr::Value(Value::SingleQuotedString(s)) => s,
                    SqlExpr::Identifier(ident) => ident.value,
                    expr => {
                        bail!("invalid expression in `partition_fields`: {}; expected a column identifier", expr);
                    }
                };

                if !schema_fields.contains(f.as_str()) {
                    bail!("partition field '{}' does not exist in the schema", f);
                }
                Ok(f)
            }).collect::<Result<_>>()
        })
        .transpose()?;

    let time_partition_pattern = opts.pull_opt_str("time_partition_pattern")?;

    let partitioning = if time_partition_pattern.is_some() || partition_fields.is_some() {
        Some(Partitioning {
            time_partition_pattern,
            partition_fields: partition_fields.unwrap_or_default(),
        })
    } else {
        None
    };

    let file_naming = if prefix.is_some() || suffix.is_some() || strategy.is_some() {
        Some(FileNaming {
            prefix,
            suffix,
            strategy,
        })
    } else {
        None
    };

    let file_settings = Some(FileSettings {
        inactivity_rollover_seconds,
        max_parts,
        rollover_seconds,
        target_file_size,
        target_part_size,
        partitioning,
        commit_style: Some(commit_style),
        file_naming,
    });

    let shuffle_by_partition = opts.pull_opt_bool("shuffle_by_partition.enabled")?
        .unwrap_or_default();

    let format_settings = match schema.format.as_ref().ok_or(anyhow!(
        "filesystem sink requires a format, such as json or parquet"
    ))? {
        Format::Parquet(..) => {
            let compression = opts
                .pull_opt_str("parquet_compression")?
                .map(|value| {
                    Compression::try_from(&value).map_err(|_err| {
                        anyhow!("{} is not a valid parquet_compression argument", value)
                    })
                })
                .transpose()?;
            let row_batch_size = opts.pull_opt_i64("parquet_row_batch_size")?;
            let row_group_size = opts.pull_opt_i64("parquet_row_group_size")?;
            Some(FormatSettings::Parquet {
                compression,
                row_batch_size,
                row_group_size,
            })
        }
        Format::Json(..) => Some(FormatSettings::Json {
            json_format: JsonFormat::Json,
        }),
        other => bail!("Unsupported format: {:?}", other),
    };
    Ok(FileSystemTable {
        table_type: TableType::Sink {
            file_settings,
            format_settings,
            write_path: storage_url,
            storage_options,
            shuffle_by_partition: Some(PartitionShuffleSettings {
                enabled: Some(shuffle_by_partition)
            })
        },
    })
}
