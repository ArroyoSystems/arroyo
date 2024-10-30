use anyhow::{anyhow, bail};
use arrow::datatypes::DataType;
use arroyo_operator::connector::Connection;
use arroyo_storage::BackendConfig;
use std::collections::HashMap;

use arroyo_rpc::api_types::connections::{
    ConnectionProfile, ConnectionSchema, ConnectionType, TestSourceMessage,
};
use arroyo_rpc::OperatorConfig;

use crate::filesystem::{
    file_system_sink_from_options, CommitStyle, FileSystemTable, FormatSettings, TableType,
};
use crate::EmptyConfig;

use arroyo_operator::connector::Connector;
use arroyo_operator::operator::OperatorNode;

use super::sink::{LocalParquetFileSystemSink, ParquetFileSystemSink};

const TABLE_SCHEMA: &str = include_str!("./table.json");

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
        tx: tokio::sync::mpsc::Sender<TestSourceMessage>,
    ) {
        tokio::task::spawn(async move {
            let message = TestSourceMessage {
                error: false,
                done: true,
                message: "Successfully validated connection".to_string(),
            };
            tx.send(message).await.unwrap();
        });
    }

    fn table_type(&self, _: Self::ProfileT, _: Self::TableT) -> ConnectionType {
        ConnectionType::Source
    }

    fn from_config(
        &self,
        id: Option<i64>,
        name: &str,
        config: Self::ProfileT,
        table: Self::TableT,
        schema: Option<&ConnectionSchema>,
        _metadata_fields: Option<HashMap<String, (String, DataType)>>,
    ) -> anyhow::Result<arroyo_operator::connector::Connection> {
        let TableType::Sink {
            write_path,
            file_settings,
            format_settings,
            ..
        } = &table.table_type
        else {
            bail!("Delta Lake connector only supports sink tables");
        };
        // confirm commit style is DeltaLake
        if let Some(CommitStyle::DeltaLake) = file_settings
            .as_ref()
            .ok_or_else(|| anyhow!("no file_settings"))?
            .commit_style
        {
            // ok
        } else {
            bail!("commit_style must be DeltaLake");
        }

        let backend_config = BackendConfig::parse_url(write_path, true)?;
        let is_local = backend_config.is_local();
        let description = match (&format_settings, is_local) {
            (Some(FormatSettings::Parquet { .. }), true) => "LocalDeltaLake<Parquet>".to_string(),
            (Some(FormatSettings::Parquet { .. }), false) => "DeltaLake<Parquet>".to_string(),
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
            bad_data: schema.bad_data.clone(),
            framing: schema.framing.clone(),
            additional_fields: None,
        };

        Ok(Connection {
            id,
            connector: self.name(),
            name: name.to_string(),
            connection_type: ConnectionType::Sink,
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
        _metadata_fields: Option<HashMap<String, (String, DataType)>>,
    ) -> anyhow::Result<Connection> {
        let table = file_system_sink_from_options(options, schema, CommitStyle::DeltaLake)?;

        self.from_config(None, name, EmptyConfig {}, table, schema, None)
    }

    fn make_operator(
        &self,
        _: Self::ProfileT,
        table: Self::TableT,
        config: OperatorConfig,
    ) -> anyhow::Result<OperatorNode> {
        let TableType::Sink {
            write_path,
            file_settings,
            format_settings,
            ..
        } = &table.table_type
        else {
            bail!("Delta Lake connector only supports sink tables");
        };
        // confirm commit style is DeltaLake
        if let Some(CommitStyle::DeltaLake) = file_settings
            .as_ref()
            .ok_or_else(|| anyhow!("no file_settings"))?
            .commit_style
        {
            // ok
        } else {
            bail!("commit_style must be DeltaLake");
        }

        let backend_config = BackendConfig::parse_url(write_path, true)?;
        let is_local = backend_config.is_local();
        match (&format_settings, is_local) {
            (Some(FormatSettings::Parquet { .. }), true) => {
                Ok(OperatorNode::from_operator(Box::new(
                    LocalParquetFileSystemSink::new(write_path.to_string(), table, config),
                )))
            }
            (Some(FormatSettings::Parquet { .. }), false) => Ok(OperatorNode::from_operator(
                Box::new(ParquetFileSystemSink::new(table, config)),
            )),
            _ => bail!("Delta Lake sink only supports Parquet format"),
        }
    }
}
