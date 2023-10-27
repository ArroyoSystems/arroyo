use anyhow::{anyhow, bail, Result};
use arroyo_storage::BackendConfig;
use axum::response::sse::Event;
use std::convert::Infallible;

use arroyo_rpc::api_types::connections::{ConnectionSchema, ConnectionType, TestSourceMessage};
use arroyo_rpc::OperatorConfig;

use crate::filesystem::{
    file_system_table_from_options, CommitStyle, FileSystemTable, FormatSettings,
};
use crate::{Connection, EmptyConfig};

use super::Connector;

const TABLE_SCHEMA: &str = include_str!("../../connector-schemas/filesystem/table.json");

pub struct DeltaLakeConnector {}

impl Connector for DeltaLakeConnector {
    type ProfileT = EmptyConfig;

    type TableT = FileSystemTable;

    fn name(&self) -> &'static str {
        "delta"
    }

    fn metadata(&self) -> arroyo_rpc::api_types::connections::Connector {
        arroyo_rpc::api_types::connections::Connector {
            id: "delta".to_string(),
            name: "Delta Lake".to_string(),
            icon: "".to_string(),
            description: "Write to a Delta Lake table".to_string(),
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
        // confirm commit style is DeltaLake
        if let Some(CommitStyle::DeltaLake) = table
            .file_settings
            .as_ref()
            .ok_or_else(|| anyhow!("no file_settings"))?
            .commit_style
        {
            // ok
        } else {
            bail!("commit_style must be DeltaLake");
        }

        let backend_config = BackendConfig::parse_url(&table.write_target.path, true)?;
        let is_local = match &backend_config {
            BackendConfig::Local { .. } => true,
            _ => false,
        };
        let (description, operator) = match (&table.format_settings, is_local) {
            (Some(FormatSettings::Parquet { .. }), true) => (
                "LocalDeltaLake<Parquet>".to_string(),
                "connectors::filesystem::LocalParquetFileSystemSink::<#in_k, #in_t, #in_tRecordBatchBuilder>"
            ),
            (Some(FormatSettings::Parquet { .. }), false) => (
                "DeltaLake<Parquet>".to_string(),
                "connectors::filesystem::ParquetFileSystemSink::<#in_k, #in_t, #in_tRecordBatchBuilder>"
            ),
            _ => bail!("Delta Lake sink only supports Parquet format"),
        };

        let schema = schema
            .map(|s| s.to_owned())
            .ok_or_else(|| anyhow!("no schema defined for Delta Lake sink"))?;

        let format = schema
            .format
            .as_ref()
            .map(|t| t.to_owned())
            .ok_or_else(|| anyhow!("'format' must be set for Delta Lake connection"))?;

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
        let table = file_system_table_from_options(opts, schema, CommitStyle::DeltaLake)?;

        self.from_config(None, name, EmptyConfig {}, table, schema)
    }
}
