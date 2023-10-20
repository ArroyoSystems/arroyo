use anyhow::{anyhow, bail, Result};
use arroyo_storage::BackendConfig;
use axum::response::sse::Event;
use std::convert::Infallible;
use typify::import_types;

use arroyo_rpc::formats::Format;
use arroyo_rpc::types::{ConnectionSchema, ConnectionType, TestSourceMessage};
use arroyo_rpc::OperatorConfig;
use serde::{Deserialize, Serialize};

use crate::{pull_option_to_i64, Connection, EmptyConfig};

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

    fn metadata(&self) -> arroyo_rpc::types::Connector {
        arroyo_rpc::types::Connector {
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
                "FileSystem<Plain>".to_string(),
                "connectors::filesystem",
                ConnectionType::Source,
            ),
            TableType::Sink {
                ref format_settings,
                ref write_target,
                ..
            } => {
                let is_local = match write_target {
                    Some(Destination::FolderUri { path }) => path.starts_with("file:/"),
                    Some(Destination::S3Bucket { .. }) => false,
                    Some(Destination::LocalFilesystem { .. }) => true,
                    None => false,
                };
                let (desc, op) = match (format_settings, is_local) {
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
                (desc, op, ConnectionType::Sink)
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
                                read_source: Some(S3 {
                                    bucket: Some(bucket),
                                    compression_format: Some(
                                        compression_format.as_str().try_into().map_err(|_| {
                                            anyhow::anyhow!(
                                                "unsupported compression format: {}",
                                                compression_format
                                            )
                                        })?,
                                    ),
                                    prefix: Some(prefix),
                                    region: Some(region),
                                }),
                            },
                        },
                        schema,
                    )
                } else {
                    bail!("Source S3 must have bucket, prefix, region, and compression_format set")
                }
            }
            Some(t) if t == "sink" => {
                let write_target = if let Some(path) = opts.remove("path") {
                    if let BackendConfig::Local(local_config) =
                        BackendConfig::parse_url(&path, false)?
                    {
                        Destination::LocalFilesystem {
                            local_directory: local_config.path,
                        }
                    } else {
                        Destination::FolderUri { path }
                    }
                } else if let (Some(s3_bucket), Some(s3_directory), Some(aws_region)) = (
                    opts.remove("s3_bucket"),
                    opts.remove("s3_directory"),
                    opts.remove("aws_region"),
                ) {
                    Destination::S3Bucket {
                        s3_bucket,
                        s3_directory,
                        aws_region,
                    }
                } else {
                    bail!("Target for filesystem connector incorrectly specified. Should be a URI path or a triple of s3_bucket, s3_directory, and aws_region");
                };

                let inactivity_rollover_seconds =
                    pull_option_to_i64("inactivity_rollover_seconds", opts)?;
                let max_parts = pull_option_to_i64("max_parts", opts)?;
                let rollover_seconds = pull_option_to_i64("rollover_seconds", opts)?;
                let target_file_size = pull_option_to_i64("target_file_size", opts)?;
                let target_part_size = pull_option_to_i64("target_part_size", opts)?;

                let file_settings = Some(FileSettings {
                    inactivity_rollover_seconds,
                    max_parts,
                    rollover_seconds,
                    target_file_size,
                    target_part_size,
                });
                let format_settings = match schema
                    .ok_or(anyhow!("require schema"))?
                    .format
                    .as_ref()
                    .unwrap()
                {
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
                            file_settings,
                            format_settings,
                            write_target: Some(write_target),
                        },
                    },
                    schema,
                )
            }
            _ => bail!("invalid type"),
        }
    }
}
