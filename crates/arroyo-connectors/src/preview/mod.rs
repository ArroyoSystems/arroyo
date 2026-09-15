mod operator;

use anyhow::{anyhow, bail};
use parquet::format::KeyValue;
use arroyo_rpc::config::config;
use arroyo_rpc::{ConnectorOptions, OperatorConfig};

use arroyo_operator::connector::Connection;
use arroyo_rpc::api_types::connections::{
    ConnectionProfile, ConnectionSchema, ConnectionType, TestSourceMessage,
};
use serde::{Deserialize, Serialize};
use std::str::FromStr;
use tokio::sync::mpsc::Sender;

use crate::EmptyConfig;

use crate::preview::operator::PreviewSink;
use arroyo_operator::connector::Connector;
use arroyo_operator::operator::ConstructedOperator;

pub struct PreviewConnector {}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default, rename_all = "snake_case", deny_unknown_fields)]
pub struct PreviewTable {
    pub path: String,
    pub flush_interval_millis: Option<u64>,
}

impl Default for PreviewTable {
    fn default() -> Self {
        Self {
            path: config().preview_url.clone(),
            flush_interval_millis: Some(
                config().pipeline.preview_output_flush_interval.as_millis() as u64,
            ),
        }
    }
}

pub struct PreviewMetadata {
    pub start_row: u64,
    pub operator_id: String,
    pub subtask_idx: u64,
    pub timestamp_idx: u64,
}

impl From<PreviewMetadata> for Vec<KeyValue> {
    fn from(value: PreviewMetadata) -> Self {
        vec![
            KeyValue::new("dev.arroyo.preview.start_row".to_string(), value.start_row.to_string()),
            KeyValue::new("dev.arroyo.preview.operator_id".to_string(), value.operator_id),
            KeyValue::new("dev.arroyo.preview.subtask_idx".to_string(), value.subtask_idx.to_string()),
            KeyValue::new("dev.arroyo.preview.timestamp_idx".to_string(), value.timestamp_idx.to_string()),
        ]
    }
}

fn extract_metadata<T>(metadata: &[KeyValue], key: &str) -> anyhow::Result<T>
where
    T: FromStr,
    T::Err: std::fmt::Display,
{
    let value = metadata
        .iter()
        .find(|value| value.key == key)
        .ok_or_else(|| anyhow!("missing {key} metadata"))?
        .value
        .as_deref()
        .ok_or_else(|| anyhow!("{key} metadata has no value"))?;

    value
        .parse()
        .map_err(|error| anyhow!("invalid {key} metadata: {error}"))
}

impl TryFrom<Vec<KeyValue>> for PreviewMetadata {
    type Error = anyhow::Error;

    fn try_from(value: Vec<KeyValue>) -> Result<Self, Self::Error> {
        Ok(Self {
            start_row: extract_metadata(&value, "dev.arroyo.preview.start_row")?,
            operator_id: extract_metadata(&value, "dev.arroyo.preview.operator_id")?,
            subtask_idx: extract_metadata(&value, "dev.arroyo.preview.subtask_idx")?,
            timestamp_idx: extract_metadata(&value, "dev.arroyo.preview.timestamp_idx")?,
        })
    }
}


impl Connector for PreviewConnector {
    type ProfileT = EmptyConfig;
    type TableT = PreviewTable;
    fn name(&self) -> &'static str {
        "preview"
    }

    fn metadata(&self) -> arroyo_rpc::api_types::connections::Connector {
        arroyo_rpc::api_types::connections::Connector {
            id: "preview".to_string(),
            name: "Preview".to_string(),
            icon: "".to_string(),
            description: "Preview outputs in the console".to_string(),
            enabled: true,
            source: false,
            sink: true,
            testing: false,
            hidden: true,
            custom_schemas: false,
            connection_config: None,
            table_config: "{}".to_string(),
        }
    }

    fn table_type(&self, _: Self::ProfileT, _: Self::TableT) -> ConnectionType {
        ConnectionType::Sink
    }

    fn test(
        &self,
        _: &str,
        _: Self::ProfileT,
        _: Self::TableT,
        _: Option<&ConnectionSchema>,
        tx: Sender<TestSourceMessage>,
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

    fn from_options(
        &self,
        _: &str,
        _: &mut ConnectorOptions,
        _: Option<&ConnectionSchema>,
        _: Option<&ConnectionProfile>,
    ) -> anyhow::Result<Connection> {
        bail!("Preview connector cannot be created in SQL");
    }

    fn from_config(
        &self,
        id: Option<i64>,
        name: &str,
        config: Self::ProfileT,
        table: Self::TableT,
        schema: Option<&ConnectionSchema>,
    ) -> anyhow::Result<Connection> {
        let description = "PreviewSink".to_string();

        let schema = schema
            .map(|s| s.to_owned())
            .ok_or_else(|| anyhow!("no schema defined for preview connection"))?;

        let config = OperatorConfig {
            connection: serde_json::to_value(config).unwrap(),
            table: serde_json::to_value(table).unwrap(),
            rate_limit: None,
            format: None,
            bad_data: schema.bad_data.clone(),
            framing: schema.framing.clone(),
            metadata_fields: schema.metadata_fields(),
        };

        Ok(Connection::new(
            id,
            self.name(),
            name.to_string(),
            ConnectionType::Sink,
            schema,
            &config,
            description,
        ))
    }

    fn make_operator(
        &self,
        _: Self::ProfileT,
        table: Self::TableT,
        _: OperatorConfig,
    ) -> anyhow::Result<ConstructedOperator> {
        if table.path.is_empty() {
            bail!("preview output path must not be empty");
        }
        if table.flush_interval_millis == Some(0) {
            bail!("preview output flush interval must be greater than zero");
        }
        Ok(ConstructedOperator::from_operator(Box::new(
            PreviewSink::new(table),
        )))
    }
}
