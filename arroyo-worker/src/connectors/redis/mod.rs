use typify::import_types;
use serde::{Deserialize, Serialize};
pub mod sink;

import_types!(schema = "../connector-schemas/redis/connection.json");
import_types!(schema = "../connector-schemas/redis/table.json");
