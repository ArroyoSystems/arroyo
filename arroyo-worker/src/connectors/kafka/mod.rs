use serde::{Deserialize, Serialize};
use typify::import_types;

pub mod sink;
pub mod source;

import_types!(schema = "../connector-schemas/kafka_config.json");
import_types!(schema = "../connector-schemas/kafka_table.json");
