mod operator;
#[cfg(test)]
mod test;

use anyhow::bail;
use arrow::datatypes::{Field, Schema, TimeUnit};
use arroyo_operator::connector::{Connection, Connector};
use arroyo_operator::operator::ConstructedOperator;
use arroyo_rpc::api_types::connections::{
    ConnectionProfile, ConnectionSchema, ConnectionType, TestSourceMessage,
};
use arroyo_rpc::{ConnectorOptions, OperatorConfig};
use serde::{Deserialize, Serialize};
use typify::import_types;

use crate::nexmark::operator::NexmarkSourceFunc;
use crate::EmptyConfig;

const TABLE_SCHEMA: &str = include_str!("./table.json");
const ICON: &str = include_str!("./nexmark.svg");

import_types!(schema = "src/nexmark/table.json");

pub(crate) fn person_fields() -> Vec<Field> {
    use arrow::datatypes::DataType::*;

    vec![
        Field::new("id", Int64, false),
        Field::new("name", Utf8, false),
        Field::new("email_address", Utf8, false),
        Field::new("credit_card", Utf8, false),
        Field::new("city", Utf8, false),
        Field::new("state", Utf8, false),
        Field::new("datetime", Timestamp(TimeUnit::Nanosecond, None), false),
        Field::new("extra", Utf8, false),
    ]
}

pub(crate) fn auction_fields() -> Vec<Field> {
    use arrow::datatypes::DataType::*;

    vec![
        Field::new("id", Int64, false),
        Field::new("description", Utf8, false),
        Field::new("item_name", Utf8, false),
        Field::new("initial_bid", Int64, false),
        Field::new("reserve", Int64, false),
        Field::new("datetime", Timestamp(TimeUnit::Nanosecond, None), false),
        Field::new("expires", Timestamp(TimeUnit::Nanosecond, None), false),
        Field::new("seller", Int64, false),
        Field::new("category", Int64, false),
        Field::new("extra", Utf8, false),
    ]
}

pub(crate) fn bid_fields() -> Vec<Field> {
    use arrow::datatypes::DataType::*;

    vec![
        Field::new("auction", Int64, false),
        Field::new("bidder", Int64, false),
        Field::new("price", Int64, false),
        Field::new("channel", Utf8, false),
        Field::new("url", Utf8, false),
        Field::new("datetime", Timestamp(TimeUnit::Nanosecond, None), false),
        Field::new("extra", Utf8, false),
    ]
}

fn arrow_schema() -> Schema {
    use arrow::datatypes::DataType::Struct;
    Schema::new(vec![
        Field::new("person", Struct(person_fields().into()), true),
        Field::new("auction", Struct(auction_fields().into()), true),
        Field::new("bid", Struct(bid_fields().into()), true),
    ])
}

pub fn nexmark_schema() -> ConnectionSchema {
    ConnectionSchema {
        format: None,
        bad_data: None,
        framing: None,
        struct_name: None,
        fields: arrow_schema()
            .fields
            .iter()
            .map(|f| (**f).clone().try_into().unwrap())
            .collect(),
        definition: None,
        inferred: None,
        primary_keys: Default::default(),
    }
}

pub struct NexmarkConnector {}

impl Connector for NexmarkConnector {
    type ProfileT = EmptyConfig;
    type TableT = NexmarkTable;

    fn name(&self) -> &'static str {
        "nexmark"
    }

    fn metadata(&self) -> arroyo_rpc::api_types::connections::Connector {
        arroyo_rpc::api_types::connections::Connector {
            id: "nexmark".to_string(),
            name: "Nexmark".to_string(),
            icon: ICON.to_string(),
            description: "Demo source for a simulated auction website".to_string(),
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
        Some(nexmark_schema())
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

        let runtime = options.pull_opt_f64("runtime")?;

        if let Some(schema) = schema {
            if !schema.fields.is_empty() && schema.fields != nexmark_schema().fields {
                bail!("invalid schema for nexmark source; omit fields to rely on inference");
            }
        }

        self.from_config(
            None,
            name,
            EmptyConfig {},
            NexmarkTable {
                event_rate,
                runtime,
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
            "{}Nexmark<{} eps>",
            if table.runtime.is_some() {
                "Bounded"
            } else {
                ""
            },
            table.event_rate
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

        Ok(Connection {
            id,
            connector: self.name(),
            name: name.to_string(),
            connection_type: ConnectionType::Source,
            schema: nexmark_schema(),
            config: serde_json::to_string(&config).unwrap(),
            description,
        })
    }

    fn make_operator(
        &self,
        _: Self::ProfileT,
        table: Self::TableT,
        _: OperatorConfig,
    ) -> anyhow::Result<ConstructedOperator> {
        Ok(ConstructedOperator::from_source(Box::new(
            NexmarkSourceFunc::from_config(&table),
        )))
    }
}
