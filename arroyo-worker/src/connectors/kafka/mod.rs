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
        {type = "string", format = "var-str"} = VarStr,
        {type = "string", format = "var-str", isSensitive = true} = VarStr
    }
);
import_types!(schema = "../connector-schemas/kafka/table.json");

impl SourceOffset {
    fn get_offset(&self) -> Offset {
        match self {
            SourceOffset::Earliest => Offset::Beginning,
            SourceOffset::Latest => Offset::End,
        }
    }
}

pub fn client_configs(connection: &KafkaConfig) -> HashMap<String, String> {
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
            client_configs.insert("sasl.username".to_string(), username.to_string());
            client_configs.insert(
                "sasl.password".to_string(),
                password
                    .sub_env_vars()
                    .expect("Failed to substitute env vars"),
            );
        }
    };

    client_configs
}
