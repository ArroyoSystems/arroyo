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
            name: "FileSystem Sink".to_string(),
            icon: "".to_string(),
            description: "Write to a filesystem (like S3)".to_string(),
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

    fn table_type(&self, _: Self::ProfileT, _: Self::TableT) -> ConnectionType {
        return ConnectionType::Source;
    }

    fn from_config(
        &self,
        id: Option<i64>,
        name: &str,
        config: Self::ProfileT,
        table: Self::TableT,
        schema: Option<&ConnectionSchema>,
    ) -> anyhow::Result<crate::Connection> {
        // confirm commit style is Direct
        if let Some(CommitStyle::Direct) = table
            .file_settings
            .as_ref()
            .ok_or_else(|| anyhow!("no file_settings"))?
            .commit_style
        {
            // ok
        } else {
            bail!("commit_style must be Direct");
        }

        let backend_config = BackendConfig::parse_url(&table.write_target.path, true)?;
        let is_local = match &backend_config {
            BackendConfig::Local { .. } => true,
            _ => false,
        };
        let (description, operator) = match (&table.format_settings, is_local) {
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
            connection_type: ConnectionType::Sink,
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
        let table = file_system_table_from_options(opts, schema, CommitStyle::Direct)?;

        self.from_config(None, name, EmptyConfig {}, table, schema)
    }
}

pub fn file_system_table_from_options(
    opts: &mut std::collections::HashMap<String, String>,
    schema: Option<&ConnectionSchema>,
    commit_style: CommitStyle,
) -> Result<FileSystemTable> {
    let storage_options: std::collections::HashMap<String, String> = opts
        .iter()
        .filter(|(k, _)| k.starts_with("storage."))
        .map(|(k, v)| (k.trim_start_matches("storage.").to_string(), v.to_string()))
        .collect();
    opts.retain(|k, _| !k.starts_with("storage."));

    let storage_url = pull_opt("path", opts)?;
    BackendConfig::parse_url(&storage_url, true)?;

    let inactivity_rollover_seconds = pull_option_to_i64("inactivity_rollover_seconds", opts)?;
    let max_parts = pull_option_to_i64("max_parts", opts)?;
    let rollover_seconds = pull_option_to_i64("rollover_seconds", opts)?;
    let target_file_size = pull_option_to_i64("target_file_size", opts)?;
    let target_part_size = pull_option_to_i64("target_part_size", opts)?;

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

    let file_settings = Some(FileSettings {
        inactivity_rollover_seconds,
        max_parts,
        rollover_seconds,
        target_file_size,
        target_part_size,
        partitioning,
        commit_style: Some(commit_style),
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
    Ok(FileSystemTable {
        write_target: FolderUrl {
            path: storage_url,
            storage_options,
        },
        file_settings,
        format_settings,
    })
}
