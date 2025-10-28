use arrow_array::Datum;
use datafusion::common::ScalarValue;
use serde::{Deserialize, Serialize};
use smallvec::SmallVec;
use std::borrow::Cow;
use std::hash::{Hash, Hasher};

#[derive(Clone, PartialEq, Eq, Hash, Ord, PartialOrd)]
pub enum PartValue {
    Null,
    Bool(bool),
    I32(i32),
    I64(i64),
    Date(i32),      // days since epoch
    Timestamp(i64), // micros since epoch (UTC)
    Str(String),
    Bytes(Vec<u8>),
}

/// A single component
#[derive(Clone, PartialEq, Eq, Hash)]
pub struct PartComponent {
    pub key: Cow<'static, str>, // e.g. "year", "ds", "user_id", "bucket_16", "ts_hour"
    pub value: PartValue,
}

/// Canonical, scheme-agnostic partition
#[derive(Clone, PartialEq, Eq, Hash)]
pub struct Partition {
    pub scheme: PartitionSchemeId,
    pub parts: SmallVec<[PartComponent; 4]>,
}

impl Partition {
    pub fn unpartitioned() -> Self {
        Self {
            scheme: PartitionSchemeId::Unpartitioned,
            parts: SmallVec::new(),
        }
    }

    /// Stable path like hive: key1=val1/key2=val2...
    pub fn to_hive_path(&self) -> String {
        let mut b = String::new();
        for PartComponent { key, value } in &self.parts {
            b.push_str(key);
            b.push('=');
            match value {
                PartValue::Null => {}
                PartValue::Bool(true) => b.push_str("true"),
                PartValue::Bool(false) => b.push_str("false"),
                PartValue::I32(i) => {
                    todo!()
                }
                PartValue::I64(_) => {}
                PartValue::Date(_) => {}
                PartValue::Timestamp(_) => {}
                PartValue::Str(_) => {}
                PartValue::Bytes(_) => {}
            }
        }
        b
    }

    /// Stable binary key for routing / object-store prefixes
    pub fn to_key_bytes(&self) -> Vec<u8> {
        Vec::new()
    }
}

/// Logical scheme identifier (cheap to compare/log/serde)
#[derive(Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum PartitionSchemeId {
    Unpartitioned,
    Hive,
    Iceberg,
}

/// How a scheme derives components from a record; object-safe and testable
pub trait PartitionScheme {
    fn id(&self) -> PartitionSchemeId;
    /// Given a record (row/Arrow struct), compute the partition
    fn derive(&self, row: &dyn RowAccess) -> Partition;
    /// Parse from a path to a Partition (for discovery/listing)
    fn parse_path(&self, path: &str) -> Option<Partition>;
}

/// Minimal row accessor to avoid hard deps; implement for Arrow, serde_json::Value, etc.
pub trait RowAccess {
    fn get_bool(&self, field: &str) -> Option<bool>;
    fn get_i32(&self, field: &str) -> Option<i32>;
    fn get_i64(&self, field: &str) -> Option<i64>;
    fn get_str(&self, field: &str) -> Option<Cow<'_, str>>;
    fn get_ts_micros(&self, field: &str) -> Option<i64>;
    // …
}
