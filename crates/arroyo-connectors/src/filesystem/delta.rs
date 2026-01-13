use anyhow::anyhow;
use arroyo_operator::connector::Connection;
use arroyo_storage::BackendConfig;

use arroyo_rpc::api_types::connections::{
    ConnectionProfile, ConnectionSchema, ConnectionType, TestSourceMessage,
};
use arroyo_rpc::{ConnectorOptions, OperatorConfig};

use crate::{EmptyConfig, render_schema};

use crate::filesystem::config::{
    DeltaLakeSink, DeltaLakeTable, DeltaLakeTableType, FileSystemSink,
};
use crate::filesystem::sink::partitioning::PartitionerMode;
use crate::filesystem::{TableFormat, make_sink};
use arroyo_operator::connector::Connector;
use arroyo_operator::operator::ConstructedOperator;

pub struct DeltaLakeConnector {}

impl Connector for DeltaLakeConnector {
    type ProfileT = EmptyConfig;

    type TableT = DeltaLakeTable;

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
    ) -> anyhow::Result<arroyo_operator::connector::Connection> {
        let schema = schema
            .map(|s| s.to_owned())
            .ok_or_else(|| anyhow!("no schema defined for DeltaLake connection"))?;

        let format = schema
            .format
            .as_ref()
            .map(|t| t.to_owned())
            .ok_or_else(|| anyhow!("'format' must be set for DeltaLake connection"))?;

        let (description, connection_type, partition_fields) = match &table.table_type {
            DeltaLakeTableType::Sink(DeltaLakeSink {
                path, partitioning, ..
            }) => {
                BackendConfig::parse_url(path, true)?;

                let description = format!("DeltaLakeSink<{format}, {path}>");

                let exprs = partitioning
                    .partition_expr(&schema.arroyo_schema().schema)?
                    .map(|p| vec![p]);

                let partitioner = if partitioning.shuffle_by_partition.enabled {
                    exprs
                } else {
                    None
                };

                (description, ConnectionType::Sink, partitioner)
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
        .with_partition_exprs(partition_fields))
    }

    fn from_options(
        &self,
        name: &str,
        options: &mut ConnectorOptions,
        schema: Option<&ConnectionSchema>,
        _: Option<&ConnectionProfile>,
    ) -> anyhow::Result<Connection> {
        self.from_config(None, name, EmptyConfig {}, options.pull_struct()?, schema)
    }

    fn make_operator(
        &self,
        _: Self::ProfileT,
        table: Self::TableT,
        config: OperatorConfig,
    ) -> anyhow::Result<ConstructedOperator> {
        match table.table_type {
            DeltaLakeTableType::Sink(sink) => {
                let partitioning = sink.partitioning.clone();
                make_sink(
                    FileSystemSink {
                        path: sink.path,
                        storage_options: sink.storage_options,
                        rolling_policy: sink.rolling_policy,
                        file_naming: sink.file_naming,
                        partitioning: sink.partitioning,
                        multipart: sink.multipart,
                    },
                    config,
                    TableFormat::Delta {},
                    PartitionerMode::FileConfig(partitioning),
                    None,
                )
            }
        }
    }
}
