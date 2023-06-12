use serde::{Deserialize, Serialize};
use typify::import_types;

pub mod kafka;

import_types!(schema = "../connector-schemas/common.json",);
