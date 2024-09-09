pub mod delta;
mod sink;
mod source;

use anyhow::{anyhow, bail, Result};
use arroyo_storage::BackendConfig;
use regex::Regex;
use std::collections::HashMap;

use typify::import_types;

use arroyo_operator::connector::Connection;
use arroyo_rpc::api_types::connections::{
    ConnectionProfile, ConnectionSchema, ConnectionType, TestSourceMessage,
};
use arroyo_rpc::formats::Format;
use arroyo_rpc::OperatorConfig;
use serde::{Deserialize, Serialize};

use crate::{pull_opt, pull_option_to_i64, EmptyConfig};

use crate::filesystem::source::FileSystemSourceFunc;
use arroyo_operator::connector::Connector;
use arroyo_operator::operator::OperatorNode;

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
        };

        Ok(Connection {
            id,
            connector: self.name(),
            name: name.to_string(),
            connection_type,
            schema,
            config: serde_json::to_string(&config).unwrap(),
            description,
        })
    }

    fn from_options(
        &self,
        name: &str,
        options: &mut HashMap<String, String>,
        schema: Option<&ConnectionSchema>,
        _profile: Option<&ConnectionProfile>,
    ) -> anyhow::Result<Connection> {
        match options.remove("type") {
            Some(t) if t == "source" => {
                let (storage_url, storage_options) = get_storage_url_and_options(options)?;
                let compression_format = options
                    .remove("compression_format")
                    .map(|format| format.as_str().try_into().map_err(|err: &str| anyhow!(err)))
                    .transpose()?
                    .unwrap_or(CompressionFormat::None);
                let matching_pattern = options.remove("source.regex-pattern");
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
    ) -> Result<OperatorNode> {
        match &table.table_type {
            TableType::Source { .. } => {
                Ok(OperatorNode::from_source(Box::new(FileSystemSourceFunc {
                    table: table.table_type.clone(),
                    format: config
                        .format
                        .ok_or_else(|| anyhow!("format required for FileSystem source"))?,
                    framing: config.framing.clone(),
                    bad_data: config.bad_data.clone(),
                    file_states: HashMap::new(),
                })))
            }
            TableType::Sink {
                file_settings: _,
                format_settings,
                storage_options: _,
                write_path,
            } => {
                let backend_config = BackendConfig::parse_url(write_path, true)?;
                match (format_settings, backend_config.is_local()) {
                    (Some(FormatSettings::Parquet { .. }), true) => {
                        Ok(OperatorNode::from_operator(Box::new(
                            LocalParquetFileSystemSink::new(write_path.to_string(), table, config),
                        )))
                    }
                    (Some(FormatSettings::Parquet { .. }), false) => {
                        Ok(OperatorNode::from_operator(Box::new(
                            ParquetFileSystemSink::new(table, config),
                        )))
                    }
                    (Some(FormatSettings::Json { .. }), true) => {
                        Ok(OperatorNode::from_operator(Box::new(
                            LocalJsonFileSystemSink::new(write_path.to_string(), table, config),
                        )))
                    }
                    (Some(FormatSettings::Json { .. }), false) => Ok(OperatorNode::from_operator(
                        Box::new(JsonFileSystemSink::new(table, config)),
                    )),
                    (None, _) => bail!("have to have some format settings"),
                }
            }
        }
    }
}

fn get_storage_url_and_options(
    opts: &mut HashMap<String, String>,
) -> Result<(String, HashMap<String, String>)> {
    let storage_url = pull_opt("path", opts)?;
    let storage_options: HashMap<String, String> = opts
        .iter()
        .filter(|(k, _)| k.starts_with("storage."))
        .map(|(k, v)| (k.trim_start_matches("storage.").to_string(), v.to_string()))
        .collect();
    opts.retain(|k, _| !k.starts_with("storage."));
    BackendConfig::parse_url(&storage_url, true)?;
    Ok((storage_url, storage_options))
}

pub fn file_system_sink_from_options(
    opts: &mut std::collections::HashMap<String, String>,
    schema: Option<&ConnectionSchema>,
    commit_style: CommitStyle,
) -> Result<FileSystemTable> {
    let (storage_url, storage_options) = get_storage_url_and_options(opts)?;

    let inactivity_rollover_seconds = pull_option_to_i64("inactivity_rollover_seconds", opts)?;
    let max_parts = pull_option_to_i64("max_parts", opts)?;
    let rollover_seconds = pull_option_to_i64("rollover_seconds", opts)?;
    let target_file_size = pull_option_to_i64("target_file_size", opts)?;
    let target_part_size = pull_option_to_i64("target_part_size", opts)?;
    let prefix = opts.remove("filename.prefix");
    let suffix = opts.remove("filename.suffix");
    let strategy = opts
        .remove("filename.strategy")
        .map(|value| {
            FilenameStrategy::try_from(&value)
                .map_err(|_err| anyhow!("{} is not a valid Filenaming Strategy", value))
        })
        .transpose()?;

    let partition_fields: Vec<_> = opts
        .remove("partition_fields")
        .map(|fields| fields.split(',').map(|f| f.to_string()).collect())
        .unwrap_or_default();

    let time_partition_pattern = opts.remove("time_partition_pattern");

    let partitioning = if time_partition_pattern.is_some() || !partition_fields.is_empty() {
        Some(Partitioning {
            time_partition_pattern,
            partition_fields,
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

    let format_settings = match schema.as_ref().unwrap().format.as_ref().ok_or(anyhow!(
        "filesystem sink requires a format, such as json or parquet"
    ))? {
        Format::Parquet(..) => {
            let compression = opts
                .remove("parquet_compression")
                .map(|value| {
                    Compression::try_from(&value).map_err(|_err| {
                        anyhow!("{} is not a valid parquet_compression argument", value)
                    })
                })
                .transpose()?;
            let row_batch_size = pull_option_to_i64("parquet_row_batch_size", opts)?;
            let row_group_size = pull_option_to_i64("parquet_row_group_size", opts)?;
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
        },
    })
}
