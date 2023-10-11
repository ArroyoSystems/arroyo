use anyhow::{anyhow, bail, Result};
use axum::response::sse::Event;
use std::convert::Infallible;
use typify::import_types;

use arroyo_rpc::types::{ConnectionSchema, ConnectionType, TestSourceMessage};
use arroyo_rpc::OperatorConfig;
use serde::{Deserialize, Serialize};

use crate::{pull_opt, Connection, EmptyConfig};

use super::Connector;

const TABLE_SCHEMA: &str = include_str!("../../connector-schemas/s3/table.json");

import_types!(schema = "../connector-schemas/s3/table.json");

pub struct S3Connector {}

impl Connector for S3Connector {
    type ProfileT = EmptyConfig;

    type TableT = S3Table;

    fn name(&self) -> &'static str {
        "s3"
    }

    fn metadata(&self) -> arroyo_rpc::types::Connector {
        arroyo_rpc::types::Connector {
            id: "s3".to_string(),
            name: "S3".to_string(),
            icon: "".to_string(),
            description: "Read from an S3 prefix".to_string(),
            enabled: true,
            source: true,
            sink: false,
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
        let schema = schema
            .map(|s| s.to_owned())
            .ok_or_else(|| anyhow!("no schema defined for S3 Source connection"))?;

        let format = schema
            .format
            .as_ref()
            .map(|t| t.to_owned())
            .ok_or_else(|| anyhow!("'format' must be set for S3 Source connection"))?;
        let operator = "connectors::filesystem::s3::source::S3SourceFunc".to_string();

        let config = OperatorConfig {
            connection: serde_json::to_value(config).unwrap(),
            table: serde_json::to_value(table).unwrap(),
            rate_limit: None,
            framing: None,
            format: Some(format),
        };

        Ok(Connection {
            id,
            name: name.to_string(),
            connection_type: ConnectionType::Source,
            schema,
            operator,
            config: serde_json::to_string(&config).unwrap(),
            description: "S3 Source".to_string(),
        })
    }

    fn from_options(
        &self,
        name: &str,
        opts: &mut std::collections::HashMap<String, String>,
        schema: Option<&ConnectionSchema>,
    ) -> anyhow::Result<crate::Connection> {
        let bucket = pull_opt("bucket", opts)?;
        let prefix = pull_opt("prefix", opts)?;
        let region = pull_opt("region", opts)?;
        let compression_format = match pull_opt("compression_format", opts)?.as_str() {
            "zstd" => CompressionFormat::Zstd,
            "gzip" => CompressionFormat::Gzip,
            "none" => CompressionFormat::None,
            other => bail!("unsupported compression format {}", other),
        };

        self.from_config(
            None,
            name,
            EmptyConfig {},
            S3Table {
                bucket,
                prefix,
                region,
                compression_format,
            },
            schema,
        )
    }
}
