use serde::{Deserialize, Serialize};
use typify::import_types;
pub mod sink;

import_types!(schema = "../connector-schemas/redis/connection.json");
import_types!(schema = "../connector-schemas/redis/table.json");
