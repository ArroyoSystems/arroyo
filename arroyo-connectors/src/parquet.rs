use anyhow::{anyhow, bail};
use arroyo_rpc::grpc::{
    self,
    api::{ConnectionSchema, TestSourceMessage},
};
use typify::import_types;

use serde::{Deserialize, Serialize};

use crate::{
    pull_opt, serialization_mode, Connection, ConnectionType, EmptyConfig, OperatorConfig,
};

use super::Connector;

const TABLE_SCHEMA: &str = include_str!("../../connector-schemas/parquet/table.json");

import_types!(schema = "../connector-schemas/parquet/table.json");
const ICON: &str = include_str!("../resources/parquet.svg");

pub struct ParquetConnector {}

impl Connector for ParquetConnector {
    type ConfigT = EmptyConfig;

    type TableT = ParquetTable;

    fn name(&self) -> &'static str {
        "parquet"
    }

    fn metadata(&self) -> grpc::api::Connector {
        grpc::api::Connector {
            id: "parquet".to_string(),
            name: "Parquet Sink".to_string(),
            icon: ICON.to_string(),
            description: "Write to Parquet".to_string(),
            enabled: true,
            source: false,
            sink: true,
            testing: false,
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
        let description = format!("Parquet<>");

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
                .ok_or_else(|| anyhow!("No schema defined for SSE source"))?,
            operator: "connectors::parquet::ParquetSink::<#in_k, #in_t, #in_tRecordBatchBuilder>"
                .to_string(),
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
        let write_target = if let (Some(s3_bucket), Some(s3_directory)) =
            (opts.remove("s3_bucket"), opts.remove("s3_directory"))
        {
            Destination::S3Bucket {
                s3_bucket,
                s3_directory,
            }
        } else {
            bail!("Only s3 targets are supported currently")
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
        let format_settings = Some(FormatSettings {
            compression,
            row_batch_size,
            row_group_size,
        });

        self.from_config(
            None,
            name,
            EmptyConfig {},
            ParquetTable {
                write_target,
                file_settings,
                format_settings,
            },
            schema,
        )
    }
}
