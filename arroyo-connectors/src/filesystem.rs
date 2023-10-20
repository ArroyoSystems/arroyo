use anyhow::{anyhow, bail, Result};
use arroyo_storage::BackendConfig;
use axum::response::sse::Event;
use std::convert::Infallible;
use typify::import_types;

use arroyo_rpc::api_types::connections::{ConnectionSchema, ConnectionType, TestSourceMessage};
use arroyo_rpc::formats::Format;
use arroyo_rpc::OperatorConfig;
use serde::{Deserialize, Serialize};

use crate::{pull_opt, pull_option_to_i64, Connection, EmptyConfig};

use super::Connector;

const TABLE_SCHEMA: &str = include_str!("../../connector-schemas/filesystem/table.json");

import_types!(schema = "../connector-schemas/filesystem/table.json");

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
            icon: "".to_string(),
            description: "Read (S3 only) or write to a filesystem (like S3)".to_string(),
            enabled: true,
            source: true,
            sink: true,
            testing: false,
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
        _: Self::TableT,
        _: Option<&ConnectionSchema>,
        tx: tokio::sync::mpsc::Sender<Result<Event, Infallible>>,
    ) {
        tokio::task::spawn(async move {
            let message = TestSourceMessage {
                error: false,
                done: true,
                message: "Successfully validated connection".to_string(),
            };
            tx.send(Ok(Event::default().json_data(message).unwrap()))
                .await
                .unwrap();
        });
    }

    fn table_type(&self, _: Self::ProfileT, table: Self::TableT) -> ConnectionType {
        match table.type_ {
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
    ) -> anyhow::Result<crate::Connection> {
        let (description, operator, connection_type) = match table.type_ {
            TableType::Source { .. } => (
                "FileSystem<S3>".to_string(),
                "connectors::filesystem::source::FileSystemSourceFunc",
                ConnectionType::Source,
            ),
            TableType::Sink {
                ref format_settings,
                ref write_target,
                ..
            } => {
                let backend_config = BackendConfig::parse_url(&write_target.path, true)?;
                let is_local = match &backend_config {
                    BackendConfig::Local { .. } => true,
                    _ => false,
                };
                let (description, operator) = match (format_settings, is_local) {
                    (Some(FormatSettings::Parquet { .. }), true) => (
                        "LocalFileSystem<Parquet>".to_string(),
                        "connectors::filesystem::LocalParquetFileSystemSink::<#in_k, #in_t, #in_tRecordBatchBuilder>"
                    ),
                    (Some(FormatSettings::Parquet { .. }), false) => (
                        "FileSystem<Parquet>".to_string(),
                        "connectors::filesystem::ParquetFileSystemSink::<#in_k, #in_t, #in_tRecordBatchBuilder>"
                    ),
                    (Some(FormatSettings::Json {  }), true) => (
                        "LocalFileSystem<JSON>".to_string(),
                        "connectors::filesystem::LocalJsonFileSystemSink::<#in_k, #in_t>"
                    ),
                    (Some(FormatSettings::Json {  }), false) => (
                        "FileSystem<JSON>".to_string(),
                        "connectors::filesystem::JsonFileSystemSink::<#in_k, #in_t>"
                    ),
                    (None, _) => bail!("have to have some format settings"),
                };
                (description, operator, ConnectionType::Sink)
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
            framing: schema.framing.clone(),
        };

        Ok(Connection {
            id,
            name: name.to_string(),
            connection_type,
            schema,
            operator: operator.to_string(),
            config: serde_json::to_string(&config).unwrap(),
            description,
        })
    }

    fn from_options(
        &self,
        name: &str,
        opts: &mut std::collections::HashMap<String, String>,
        schema: Option<&ConnectionSchema>,
    ) -> anyhow::Result<crate::Connection> {
        match opts.remove("type") {
            Some(t) if t == "source" => {
                if let (Some(bucket), Some(prefix), Some(region), Some(compression_format)) = (
                    opts.remove("bucket"),
                    opts.remove("prefix"),
                    opts.remove("region"),
                    opts.remove("compression_format"),
                ) {
                    self.from_config(
                        None,
                        name,
                        EmptyConfig {},
                        FileSystemTable {
                            type_: TableType::Source {
                                read_source: S3 {
                                    bucket,
                                    compression_format: compression_format
                                        .as_str()
                                        .try_into()
                                        .map_err(|_| {
                                            anyhow::anyhow!(
                                                "unsupported compression format: {}",
                                                compression_format
                                            )
                                        })?,
                                    prefix,
                                    region,
                                },
                            },
                        },
                        schema,
                    )
                } else {
                    bail!("Source S3 must have bucket, prefix, region, and compression_format set")
                }
            }
            Some(t) if t == "sink" => {
                let storage_options: std::collections::HashMap<String, String> = opts
                    .iter()
                    .filter(|(k, _)| k.starts_with("storage."))
                    .map(|(k, v)| (k.trim_start_matches("storage.").to_string(), v.to_string()))
                    .collect();
                opts.retain(|k, _| !k.starts_with("storage."));

                let storage_url = pull_opt("path", opts)?;
                BackendConfig::parse_url(&storage_url, true)?;

                let inactivity_rollover_seconds =
                    pull_option_to_i64("inactivity_rollover_seconds", opts)?;
                let max_parts = pull_option_to_i64("max_parts", opts)?;
                let rollover_seconds = pull_option_to_i64("rollover_seconds", opts)?;
                let target_file_size = pull_option_to_i64("target_file_size", opts)?;
                let target_part_size = pull_option_to_i64("target_part_size", opts)?;

                let partition_fields: Vec<_> = opts
                    .remove("partition_fields")
                    .map(|fields| fields.split(',').map(|f| f.to_string()).collect())
                    .unwrap_or_default();

                let time_partition_pattern = opts.remove("time_partition_pattern");

                let partitioning =
                    if time_partition_pattern.is_some() || !partition_fields.is_empty() {
                        Some(Partitioning {
                            time_partition_pattern,
                            partition_fields,
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
                });

                let format_settings = match schema
                    .ok_or(anyhow!("require schema"))?
                    .format
                    .as_ref()
                    .ok_or(anyhow!(
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
                    Format::Json(..) => Some(FormatSettings::Json {}),
                    other => bail!("Unsupported format: {:?}", other),
                };

                self.from_config(
                    None,
                    name,
                    EmptyConfig {},
                    FileSystemTable {
                        type_: TableType::Sink {
                            write_target: FolderUrl {
                                path: storage_url,
                                storage_options,
                            },
                            file_settings,
                            format_settings,
                        },
                    },
                    schema,
                )
            }
            Some(t) => bail!("unknown type: {}", t),
            None => bail!("must have type set"),
        }
    }
}
