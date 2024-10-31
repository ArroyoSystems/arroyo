use crate::operator::OperatorNode;
use anyhow::{anyhow, bail};
use arrow::datatypes::{DataType, Field};
use arroyo_rpc::api_types::connections::{
    ConnectionProfile, ConnectionSchema, ConnectionType, TestSourceMessage,
};
use arroyo_rpc::OperatorConfig;
use arroyo_types::DisplayAsSql;
use serde::de::DeserializeOwned;
use serde::ser::Serialize;
use serde_json::value::Value;
use std::collections::HashMap;
use tokio::sync::mpsc::Sender;
use tokio::sync::oneshot;

#[derive(Debug, Clone)]
pub struct Connection {
    pub id: Option<i64>,
    pub connector: &'static str,
    pub name: String,
    pub connection_type: ConnectionType,
    pub schema: ConnectionSchema,
    pub config: String,
    pub description: String,
}

pub struct MetadataDef {
    pub name: &'static str,
    pub data_type: DataType,
}

#[allow(clippy::wrong_self_convention)]
pub trait Connector: Send {
    type ProfileT: DeserializeOwned + Serialize;
    type TableT: DeserializeOwned + Serialize;

    fn name(&self) -> &'static str;

    #[allow(unused)]
    fn config_description(&self, config: Self::ProfileT) -> String {
        "".to_string()
    }

    fn parse_config(&self, s: &serde_json::Value) -> Result<Self::ProfileT, serde_json::Error> {
        serde_json::from_value(s.clone())
    }

    fn parse_table(&self, s: &serde_json::Value) -> Result<Self::TableT, serde_json::Error> {
        serde_json::from_value(s.clone())
    }

    fn metadata(&self) -> arroyo_rpc::api_types::connections::Connector;

    fn metadata_defs(&self) -> &'static [MetadataDef] {
        &[]
    }

    fn table_type(&self, config: Self::ProfileT, table: Self::TableT) -> ConnectionType;

    #[allow(unused)]
    fn get_schema(
        &self,
        config: Self::ProfileT,
        table: Self::TableT,
        schema: Option<&ConnectionSchema>,
    ) -> Option<ConnectionSchema> {
        schema.cloned()
    }

    #[allow(unused)]
    fn test_profile(
        &self,
        profile: Self::ProfileT,
    ) -> Option<tokio::sync::oneshot::Receiver<TestSourceMessage>> {
        None
    }

    #[allow(unused)]
    fn get_autocomplete(
        &self,
        profile: Self::ProfileT,
    ) -> oneshot::Receiver<anyhow::Result<HashMap<String, Vec<String>>>> {
        let (tx, rx) = oneshot::channel();
        tx.send(Ok(HashMap::new())).unwrap();
        rx
    }

    fn test(
        &self,
        name: &str,
        config: Self::ProfileT,
        table: Self::TableT,
        schema: Option<&ConnectionSchema>,
        tx: Sender<TestSourceMessage>,
    );

    fn from_options(
        &self,
        name: &str,
        options: &mut HashMap<String, String>,
        schema: Option<&ConnectionSchema>,
        profile: Option<&ConnectionProfile>,
    ) -> anyhow::Result<Connection>;

    fn from_config(
        &self,
        id: Option<i64>,
        name: &str,
        config: Self::ProfileT,
        table: Self::TableT,
        schema: Option<&ConnectionSchema>,
    ) -> anyhow::Result<Connection>;

    #[allow(unused)]
    fn make_operator(
        &self,
        profile: Self::ProfileT,
        table: Self::TableT,
        config: OperatorConfig,
    ) -> anyhow::Result<OperatorNode>;
}
#[allow(clippy::type_complexity)]
#[allow(clippy::wrong_self_convention)]
pub trait ErasedConnector: Send {
    fn name(&self) -> &'static str;

    fn metadata(&self) -> arroyo_rpc::api_types::connections::Connector;

    fn metadata_defs(&self) -> &'static [MetadataDef];

    fn validate_config(&self, s: &serde_json::Value) -> Result<(), serde_json::Error>;

    fn validate_table(&self, s: &serde_json::Value) -> Result<(), serde_json::Error>;

    fn table_type(
        &self,
        config: &serde_json::Value,
        table: &serde_json::Value,
    ) -> Result<ConnectionType, serde_json::Error>;

    fn config_description(&self, s: &serde_json::Value) -> Result<String, serde_json::Error>;

    fn get_schema(
        &self,
        config: &serde_json::Value,
        table: &serde_json::Value,
        schema: Option<&ConnectionSchema>,
    ) -> Result<Option<ConnectionSchema>, serde_json::Error>;

    /// Returns a map of autocomplete values from key names (with paths separated by dots) to values that should
    /// be used to autocomplete them.
    #[allow(unused)]
    fn get_autocomplete(
        &self,
        profile: &serde_json::Value,
    ) -> Result<oneshot::Receiver<anyhow::Result<HashMap<String, Vec<String>>>>, serde_json::Error>;

    fn test_profile(
        &self,
        profile: &serde_json::Value,
    ) -> Result<Option<oneshot::Receiver<TestSourceMessage>>, serde_json::Error>;

    fn test(
        &self,
        name: &str,
        config: &serde_json::Value,
        table: &serde_json::Value,
        schema: Option<&ConnectionSchema>,
        tx: Sender<TestSourceMessage>,
    ) -> Result<(), serde_json::Error>;

    fn from_options(
        &self,
        name: &str,
        options: &mut HashMap<String, String>,
        schema: Option<&ConnectionSchema>,
        profile: Option<&ConnectionProfile>,
    ) -> anyhow::Result<Connection>;

    fn from_config(
        &self,
        id: Option<i64>,
        name: &str,
        config: &serde_json::Value,
        table: &serde_json::Value,
        schema: Option<&ConnectionSchema>,
    ) -> anyhow::Result<Connection>;

    fn make_operator(&self, config: OperatorConfig) -> anyhow::Result<OperatorNode>;
}

impl<C: Connector> ErasedConnector for C {
    fn name(&self) -> &'static str {
        self.name()
    }

    fn metadata(&self) -> arroyo_rpc::api_types::connections::Connector {
        self.metadata()
    }

    fn metadata_defs(&self) -> &'static [MetadataDef] {
        self.metadata_defs()
    }

    fn config_description(&self, s: &serde_json::Value) -> Result<String, serde_json::Error> {
        Ok(self.config_description(self.parse_config(s)?))
    }

    fn validate_config(&self, config: &serde_json::Value) -> Result<(), serde_json::Error> {
        self.parse_config(config)?;
        Ok(())
    }

    fn validate_table(&self, table: &serde_json::Value) -> Result<(), serde_json::Error> {
        self.parse_table(table)?;
        Ok(())
    }

    fn table_type(
        &self,
        config: &serde_json::Value,
        table: &serde_json::Value,
    ) -> Result<ConnectionType, serde_json::Error> {
        Ok(self.table_type(self.parse_config(config)?, self.parse_table(table)?))
    }

    fn get_schema(
        &self,
        config: &serde_json::Value,
        table: &serde_json::Value,
        schema: Option<&ConnectionSchema>,
    ) -> Result<Option<ConnectionSchema>, serde_json::Error> {
        Ok(self.get_schema(self.parse_config(config)?, self.parse_table(table)?, schema))
    }

    fn get_autocomplete(
        &self,
        profile: &Value,
    ) -> Result<oneshot::Receiver<anyhow::Result<HashMap<String, Vec<String>>>>, serde_json::Error>
    {
        Ok(self.get_autocomplete(self.parse_config(profile)?))
    }

    fn test_profile(
        &self,
        profile: &serde_json::Value,
    ) -> Result<Option<tokio::sync::oneshot::Receiver<TestSourceMessage>>, serde_json::Error> {
        Ok(self.test_profile(self.parse_config(profile)?))
    }

    fn test(
        &self,
        name: &str,
        config: &serde_json::Value,
        table: &serde_json::Value,
        schema: Option<&ConnectionSchema>,
        tx: Sender<TestSourceMessage>,
    ) -> Result<(), serde_json::Error> {
        self.test(
            name,
            self.parse_config(config)?,
            self.parse_table(table)?,
            schema,
            tx,
        );

        Ok(())
    }

    fn from_options(
        &self,
        name: &str,
        options: &mut HashMap<String, String>,
        schema: Option<&ConnectionSchema>,
        profile: Option<&ConnectionProfile>,
    ) -> anyhow::Result<Connection> {
        if let Some(schema) = schema {
            for sf in schema.fields.iter() {
                if let Some(key) = &sf.metadata_key {
                    let field = self
                        .metadata_defs()
                        .iter()
                        .find(|f| f.name == key)
                        .ok_or_else(|| {
                            anyhow!(
                                "unknown metadata field '{}' for {} connector '{}'",
                                key,
                                self.name(),
                                name
                            )
                        })?;

                    let arrow_field: Field = sf.clone().into();

                    if !field.data_type.equals_datatype(arrow_field.data_type()) {
                        bail!("incorrect data type for metadata field '{}'; expected {}, but found {}",
                        arrow_field.name(), DisplayAsSql(&field.data_type), DisplayAsSql(arrow_field.data_type()));
                    }
                }
            }
        }

        self.from_options(name, options, schema, profile)
    }

    fn from_config(
        &self,
        id: Option<i64>,
        name: &str,
        config: &serde_json::Value,
        table: &serde_json::Value,
        schema: Option<&ConnectionSchema>,
    ) -> anyhow::Result<Connection> {
        self.from_config(
            id,
            name,
            self.parse_config(config)?,
            self.parse_table(table)?,
            schema,
        )
    }

    fn make_operator(&self, config: OperatorConfig) -> anyhow::Result<OperatorNode> {
        self.make_operator(
            self.parse_config(&config.connection).map_err(|e| {
                anyhow!(
                    "invalid profile config for operator {}: {:?}",
                    self.name(),
                    e
                )
            })?,
            self.parse_table(&config.table).map_err(|e| {
                anyhow!("invalid table config for operator {}: {:?}", self.name(), e)
            })?,
            config,
        )
    }
}
