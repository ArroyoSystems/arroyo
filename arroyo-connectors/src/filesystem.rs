use anyhow::{anyhow, bail};
use arroyo_rpc::grpc::{
    self,
    api::{ConnectionSchema, Format, TestSourceMessage},
};
use typify::import_types;

use serde::{Deserialize, Serialize};

use crate::{serialization_mode, Connection, ConnectionType, EmptyConfig, OperatorConfig};

use super::Connector;

const TABLE_SCHEMA: &str = include_str!("../../connector-schemas/filesystem/table.json");

import_types!(schema = "../connector-schemas/filesystem/table.json");

pub struct FileSystemConnector {}

impl Connector for FileSystemConnector {
    type ConfigT = EmptyConfig;

    type TableT = FileSystemTable;

    fn name(&self) -> &'static str {
        "filesystem"
    }

    fn metadata(&self) -> grpc::api::Connector {
        grpc::api::Connector {
            id: "filesystem".to_string(),
            name: "FileSystem Sink".to_string(),
            icon: "".to_string(),
            description: "Write to a filesystem (S3)".to_string(),
            enabled: true,
            source: false,
            sink: true,
            testing: false,
            hidden: true,
            custom_schemas: true,
            connection_config: None,
            table_config: TABLE_SCHEMA.to_owned(),
        }
    }

    fn test(
        &self,
        _: &str,
        _: Self::ConfigT,
        _: Self::TableT,
        _: Option<&arroyo_rpc::grpc::api::ConnectionSchema>,
        tx: tokio::sync::mpsc::Sender<
            Result<arroyo_rpc::grpc::api::TestSourceMessage, tonic::Status>,
        >,
    ) {
        tokio::task::spawn(async move {
            tx.send(Ok(TestSourceMessage {
                error: false,
                done: true,
                message: "Successfully validated connection".to_string(),
            }))
            .await
            .unwrap();
        });
    }

    fn table_type(&self, _: Self::ConfigT, _: Self::TableT) -> grpc::api::TableType {
        return grpc::api::TableType::Source;
    }

    fn from_config(
        &self,
        id: Option<i64>,
        name: &str,
        config: Self::ConfigT,
        table: Self::TableT,
        schema: Option<&ConnectionSchema>,
    ) -> anyhow::Result<crate::Connection> {
        let is_local = match &table.write_target {
            Destination::FolderUri { path } => path.starts_with("file://"),
            Destination::S3Bucket { .. } => false,
            Destination::LocalFilesystem { .. } => true,
        };
        let (description, operator) = match (&table.format_settings, is_local) {
            (Some(FormatSettings::Parquet { .. }), true) => ("LocalFileSystem<Parquet>".to_string(), "connectors::filesystem::LocalParquetFileSystemSink::<#in_k, #in_t, #in_tRecordBatchBuilder>"),
            (Some(FormatSettings::Parquet { .. }), false) => ("FileSystem<Parquet>".to_string(), "connectors::filesystem::ParquetFileSystemSink::<#in_k, #in_t, #in_tRecordBatchBuilder>"),
            (Some(FormatSettings::Json {  }), true) => ("LocalFileSystem<JSON>".to_string(), "connectors::filesystem::LocalJsonFileSystemSink::<#in_k, #in_t>"),
            (Some(FormatSettings::Json {  }), false) => ("FileSystem<JSON>".to_string(), "connectors::filesystem::JsonFileSystemSink::<#in_k, #in_t>"),
            (None, _) => bail!("have to have some format settings"),
        };

        let config = OperatorConfig {
            connection: serde_json::to_value(config).unwrap(),
            table: serde_json::to_value(table).unwrap(),
            rate_limit: None,
            serialization_mode: Some(serialization_mode(schema.as_ref().unwrap())),
        };

        Ok(Connection {
            id,
            name: name.to_string(),
            connection_type: ConnectionType::Sink,
            schema: schema
                .map(|s| s.to_owned())
                .ok_or_else(|| anyhow!("No schema defined for FileSystemSink"))?,
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
        let write_target = if let Some(path) = opts.remove("path") {
            Destination::FolderUri { path }
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
            bail!("Only s3 targets are supported currently");
        };

        let inactivity_rollover_seconds = opts
            .remove("inactivity_rollover_seconds")
            .map(|value| value.parse::<i64>().map_err(|e| anyhow::anyhow!(e)))
            .transpose()?;
        let max_parts = opts
            .remove("max_parts")
            .map(|value| value.parse::<i64>().map_err(|e| anyhow::anyhow!(e)))
            .transpose()?;
        let rollover_seconds = opts
            .remove("rollover_seconds")
            .map(|value| value.parse::<i64>().map_err(|e| anyhow::anyhow!(e)))
            .transpose()?;
        let target_file_size = opts
            .remove("target_file_size")
            .map(|value| value.parse::<i64>().map_err(|e| anyhow::anyhow!(e)))
            .transpose()?;
        let target_part_size = opts
            .remove("target_part_size")
            .map(|value| value.parse::<i64>().map_err(|e| anyhow::anyhow!(e)))
            .transpose()?;

        let file_settings = Some(FileSettings {
            inactivity_rollover_seconds,
            max_parts,
            rollover_seconds,
            target_file_size,
            target_part_size,
        });
        let format_settings = match schema.ok_or(anyhow!("require schema"))?.format() {
            Format::ParquetFormat => {
                let compression = opts
                    .remove("parquet_compression")
                    .map(|value| Compression::try_from(value).map_err(|e| anyhow::anyhow!(e)))
                    .transpose()?;
                let row_batch_size = opts
                    .remove("parquet_row_batch_size")
                    .map(|value| value.parse::<i64>().map_err(|e| anyhow::anyhow!(e)))
                    .transpose()?;
                let row_group_size = opts
                    .remove("parquet_row_group_size")
                    .map(|value| value.parse::<i64>().map_err(|e| anyhow::anyhow!(e)))
                    .transpose()?;
                Some(FormatSettings::Parquet {
                    compression,
                    row_batch_size,
                    row_group_size,
                })
            }
            Format::JsonFormat => Some(FormatSettings::Json {}),
            other => bail!("Unsupported format: {:?}", other),
        };

        self.from_config(
            None,
            name,
            EmptyConfig {},
            FileSystemTable {
                write_target,
                file_settings,
                format_settings,
            },
            schema,
        )
    }
}
