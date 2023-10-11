use serde::{Deserialize, Serialize};
use typify::import_types;

pub mod source;

import_types!(schema = "../connector-schemas/s3/table.json");
