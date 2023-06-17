use rdkafka::Offset;
use serde::{Deserialize, Serialize};
use typify::import_types;

pub mod sink;
pub mod source;

import_types!(schema = "../connector-schemas/kafka/connection.json");
import_types!(schema = "../connector-schemas/kafka/table.json");


impl SourceOffset {
    fn get_offset(&self) -> Offset {
        match self {
            SourceOffset::Earliest => Offset::Beginning,
            SourceOffset::Latest => Offset::End,
        }
    }
}
