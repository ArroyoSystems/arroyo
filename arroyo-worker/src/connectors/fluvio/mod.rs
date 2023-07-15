use fluvio::Offset;
use serde::{Deserialize, Serialize};
use typify::import_types;

pub mod sink;
pub mod source;

import_types!(schema = "../connector-schemas/fluvio/table.json");

impl SourceOffset {
    pub fn offset(&self) -> Offset {
        match self {
            SourceOffset::Earliest => Offset::beginning(),
            SourceOffset::Latest => Offset::end(),
        }
    }
}
