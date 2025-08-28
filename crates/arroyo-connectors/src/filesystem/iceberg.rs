use crate::filesystem::config::{
    FileSystemSink, IcebergProfile, IcebergSink, IcebergTable, PartitioningConfig,
};
use crate::filesystem::{make_sink, sink, TableFormat};
use crate::render_schema;
use anyhow::anyhow;
use arroyo_operator::connector::Connection;
use arroyo_operator::connector::Connector;
use arroyo_operator::operator::ConstructedOperator;
use arroyo_rpc::api_types::connections::{
    ConnectionProfile, ConnectionSchema, ConnectionType, TestSourceMessage,
};
use arroyo_rpc::{ConnectorOptions, OperatorConfig};
use datafusion::common::plan_datafusion_err;
use std::collections::HashMap;

pub struct IcebergConnector {}

impl Connector for IcebergConnector {
    type ProfileT = IcebergProfile;

    type TableT = IcebergTable;

    fn name(&self) -> &'static str {
        "iceberg"
    }

    fn metadata(&self) -> arroyo_rpc::api_types::connections::Connector {
        arroyo_rpc::api_types::connections::Connector {
            id: "iceberg".to_string(),
            name: "Iceberg".to_string(),
            icon: "".to_string(),
            description: "Write to an Iceberg table".to_string(),
            enabled: true,
            source: false,
            sink: true,
            testing: false,
            hidden: true,
            custom_schemas: true,
            connection_config: Some(render_schema::<Self::ProfileT>()),
            table_config: render_schema::<Self::TableT>(),
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
        ConnectionType::Sink
    }

    fn from_config(
        &self,
        id: Option<i64>,
        name: &str,
        config: Self::ProfileT,
        table: Self::TableT,
        schema: Option<&ConnectionSchema>,
    ) -> anyhow::Result<Connection> {
        let schema = schema
            .map(|s| s.to_owned())
            .ok_or_else(|| anyhow!("no schema defined for Iceberg connection"))?;

        let format = schema
            .format
            .as_ref()
            .map(|t| t.to_owned())
            .ok_or_else(|| anyhow!("'format' must be set for Iceberg connection"))?;

        let (description, connection_type, partition_fields) = match &table {
            IcebergTable::Sink(IcebergSink {
                namespace,
                table_name,
                ..
            }) => {
                let description = format!("IcebergSink<{format}, {namespace}.{table_name}>");

                (description, ConnectionType::Sink, None)
            }
        };

        let config = OperatorConfig {
            connection: serde_json::to_value(config).unwrap(),
            table: serde_json::to_value(table).unwrap(),
            rate_limit: None,
            format: Some(format),
            bad_data: schema.bad_data.clone(),
            framing: schema.framing.clone(),
            metadata_fields: schema.metadata_fields(),
        };

        Ok(Connection::new(
            id,
            self.name(),
            name.to_string(),
            connection_type,
            schema,
            &config,
            description,
        )
        .with_partition_fields(partition_fields))
    }

    fn from_options(
        &self,
        name: &str,
        options: &mut ConnectorOptions,
        schema: Option<&ConnectionSchema>,
        profile: Option<&ConnectionProfile>,
    ) -> anyhow::Result<Connection> {
        let profile = profile
            .map(|p| {
                serde_json::from_value(p.config.clone()).map_err(|e| {
                    plan_datafusion_err!("invalid config for profile '{}' in database: {}", p.id, e)
                })
            })
            .unwrap_or_else(|| options.pull_struct())?;

        self.from_config(None, name, profile, options.pull_struct()?, schema)
    }

    fn make_operator(
        &self,
        profile: Self::ProfileT,
        table: Self::TableT,
        config: OperatorConfig,
    ) -> anyhow::Result<ConstructedOperator> {
        match table {
            IcebergTable::Sink(sink) => {
                let tf = sink::iceberg::IcebergTable::new(&profile.catalog, &sink)?;
                make_sink(
                    FileSystemSink {
                        // in iceberg, the path and storage options come from the catalog
                        path: "".to_string(),
                        storage_options: HashMap::new(),
                        rolling_policy: sink.rolling_policy,
                        file_naming: sink.file_naming,
                        partitioning: PartitioningConfig::default(),
                        multipart: sink.multipart,
                    },
                    config,
                    TableFormat::Iceberg(Box::new(tf)),
                )
            }
        }
    }
}
