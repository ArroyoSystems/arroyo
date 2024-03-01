use std::borrow::Cow;
use std::collections::HashMap;

use arroyo_rpc::var_str::VarStr;
use rdkafka::Offset;
use serde::{Deserialize, Serialize};
use typify::import_types;

pub mod sink;
pub mod source;

import_types!(
    schema = "../connector-schemas/kafka/connection.json",
    convert = {
        {type = "string", format = "var-str"} = VarStr
    }
);
import_types!(schema = "../connector-schemas/kafka/table.json");

impl KafkaTable {
    pub fn subject(&self) -> Cow<str> {
        match &self.value_subject {
            None => Cow::Owned(format!("{}-value", self.topic)),
            Some(s) => Cow::Borrowed(s),
        }
    }
}

impl SourceOffset {
    fn get_offset(&self) -> Offset {
        match self {
            SourceOffset::Earliest => Offset::Beginning,
            SourceOffset::Latest => Offset::End,
            SourceOffset::Group => Offset::Stored,
        }
    }
}

pub fn client_configs(connection: &KafkaConfig, table: &KafkaTable) -> HashMap<String, String> {
    let mut client_configs: HashMap<String, String> = HashMap::new();

    match &connection.authentication {
        KafkaConfigAuthentication::None {} => {}
        KafkaConfigAuthentication::Sasl {
            mechanism,
            password,
            protocol,
            username,
        } => {
            client_configs.insert("sasl.mechanism".to_string(), mechanism.to_string());
            client_configs.insert("security.protocol".to_string(), protocol.to_string());
            client_configs.insert(
                "sasl.username".to_string(),
                username
                    .sub_env_vars()
                    .expect("Missing env-vars for Kafka username"),
            );
            client_configs.insert(
                "sasl.password".to_string(),
                password
                    .sub_env_vars()
                    .expect("Missing env-vars for Kafka password"),
            );
        }
    };

    client_configs.extend(
        table
            .client_configs
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string())),
    );

    client_configs
}
