mod operator;

use anyhow::bail;
use arroyo_operator::connector::{Connection, Connector};
use arroyo_operator::operator::ConstructedOperator;
use arroyo_rpc::api_types::connections::{
    ConnectionProfile, ConnectionSchema, FieldType, TestSourceMessage,
};
use arroyo_rpc::{ConnectorOptions, OperatorConfig};
use serde::{Deserialize, Serialize};
use std::time::{Duration, SystemTime};
use typify::import_types;

use crate::impulse::operator::{ImpulseSourceFunc, ImpulseSourceState, ImpulseSpec};
use crate::{ConnectionType, EmptyConfig, source_field};

const TABLE_SCHEMA: &str = include_str!("./table.json");

import_types!(schema = "src/impulse/table.json");
const ICON: &str = include_str!("./impulse.svg");

pub fn impulse_schema() -> ConnectionSchema {
    ConnectionSchema {
        format: None,
        framing: None,
        bad_data: None,
        fields: vec![
            source_field("counter", FieldType::Int64),
            source_field("subtask_index", FieldType::Int64),
        ],
        definition: None,
        inferred: None,
        primary_keys: Default::default(),
    }
}

pub struct ImpulseConnector {}

impl Connector for ImpulseConnector {
    type ProfileT = EmptyConfig;
    type TableT = ImpulseTable;

    fn name(&self) -> &'static str {
        "impulse"
    }

    fn metadata(&self) -> arroyo_rpc::api_types::connections::Connector {
        arroyo_rpc::api_types::connections::Connector {
            id: "impulse".to_string(),
            name: "Impulse".to_string(),
            icon: ICON.to_string(),
            description: "Periodic demo source".to_string(),
            enabled: true,
            source: true,
            sink: false,
            testing: false,
            hidden: false,
            custom_schemas: false,
            connection_config: None,
            table_config: TABLE_SCHEMA.to_string(),
        }
    }

    fn table_type(&self, _: Self::ProfileT, _: Self::TableT) -> ConnectionType {
        ConnectionType::Source
    }

    fn get_schema(
        &self,
        _: Self::ProfileT,
        _: Self::TableT,
        _: Option<&ConnectionSchema>,
    ) -> Option<ConnectionSchema> {
        Some(impulse_schema())
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

    fn from_options(
        &self,
        name: &str,
        options: &mut ConnectorOptions,
        schema: Option<&ConnectionSchema>,
        _profile: Option<&ConnectionProfile>,
    ) -> anyhow::Result<Connection> {
        let event_rate = options.pull_f64("event_rate")?;

        let event_time_interval = options.pull_opt_i64("event_time_interval")?;
        let message_count = options.pull_opt_i64("message_count")?;

        // validate the schema
        if let Some(s) = schema
            && !s.fields.is_empty()
            && s.fields != impulse_schema().fields
        {
            bail!("invalid schema for impulse source");
        }

        self.from_config(
            None,
            name,
            EmptyConfig {},
            ImpulseTable {
                event_rate,
                event_time_interval,
                message_count,
            },
            None,
        )
    }

    fn from_config(
        &self,
        id: Option<i64>,
        name: &str,
        config: Self::ProfileT,
        table: Self::TableT,
        _: Option<&ConnectionSchema>,
    ) -> anyhow::Result<Connection> {
        let description = format!(
            "{}Impulse<{} eps{}>",
            if table.message_count.is_some() {
                "Bounded"
            } else {
                ""
            },
            table.event_rate,
            table
                .event_time_interval
                .map(|t| format!(", {:?}", Duration::from_nanos(t as u64)))
                .unwrap_or("".to_string())
        );

        let config = OperatorConfig {
            connection: serde_json::to_value(config).unwrap(),
            table: serde_json::to_value(table).unwrap(),
            rate_limit: None,
            format: None,
            bad_data: None,
            framing: None,
            metadata_fields: vec![],
        };

        Ok(Connection::new(
            id,
            self.name(),
            name.to_string(),
            ConnectionType::Source,
            impulse_schema(),
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
        Ok(ConstructedOperator::from_source(Box::new(
            ImpulseSourceFunc {
                interval: table
                    .event_time_interval
                    .map(|i| Duration::from_nanos(i as u64)),
                spec: ImpulseSpec::EventsPerSecond(table.event_rate as f32),
                limit: table
                    .message_count
                    .map(|n| n as usize)
                    .unwrap_or(usize::MAX),
                state: ImpulseSourceState {
                    counter: 0,
                    start_time: SystemTime::now(),
                },
            },
        )))
    }
}
