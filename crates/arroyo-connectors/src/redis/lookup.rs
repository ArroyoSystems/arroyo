use crate::redis::sink::GeneralConnection;
use crate::redis::RedisClient;
use arrow::array::{Array, ArrayRef, AsArray, RecordBatch};
use arrow::datatypes::DataType;
use arroyo_formats::de::{ArrowDeserializer, FieldValueType};
use arroyo_operator::connector::LookupConnector;
use arroyo_rpc::errors::SourceError;
use arroyo_rpc::MetadataField;
use arroyo_types::LOOKUP_KEY_INDEX_FIELD;
use async_trait::async_trait;
use redis::aio::ConnectionLike;
use redis::{cmd, Value};
use std::collections::HashMap;

pub struct RedisLookup {
    pub(crate) deserializer: ArrowDeserializer,
    pub(crate) client: RedisClient,
    pub(crate) connection: Option<GeneralConnection>,
    pub(crate) metadata_fields: Vec<MetadataField>,
}

#[async_trait]
impl LookupConnector for RedisLookup {
    fn name(&self) -> String {
        "RedisLookup".to_string()
    }

    async fn lookup(&mut self, keys: &[ArrayRef]) -> Option<Result<RecordBatch, SourceError>> {
        if self.connection.is_none() {
            self.connection = Some(self.client.get_connection().await.unwrap());
        }

        assert_eq!(keys.len(), 1, "redis lookup can only have a single key");
        assert_eq!(
            *keys[0].data_type(),
            DataType::Utf8,
            "redis lookup key must be a string"
        );

        let connection = self.connection.as_mut().unwrap();

        let mut mget = cmd("mget");

        let keys = keys[0].as_string::<i32>();

        for k in keys {
            mget.arg(k.unwrap());
        }

        let Value::Array(vs) = connection.req_packed_command(&mget).await.unwrap() else {
            panic!("value was not an array");
        };

        assert_eq!(
            vs.len(),
            keys.len(),
            "Redis sent back the wrong number of values"
        );

        let mut additional = HashMap::new();

        for (idx, (v, k)) in vs.iter().zip(keys).enumerate() {
            additional.insert(
                LOOKUP_KEY_INDEX_FIELD,
                FieldValueType::UInt64(Some(idx as u64)),
            );
            for m in &self.metadata_fields {
                additional.insert(
                    m.field_name.as_str(),
                    match m.key.as_str() {
                        "key" => FieldValueType::String(Some(k.unwrap())),
                        k => unreachable!("Invalid metadata key '{}'", k),
                    },
                );
            }

            let errors = match v {
                Value::Nil => {
                    self.deserializer.deserialize_null(Some(&additional));
                    vec![]
                }
                Value::SimpleString(s) => {
                    self.deserializer
                        .deserialize_without_timestamp(s.as_bytes(), Some(&additional))
                        .await
                }
                Value::BulkString(v) => {
                    self.deserializer
                        .deserialize_without_timestamp(v, Some(&additional))
                        .await
                }
                v => {
                    panic!("unexpected type {v:?}");
                }
            };

            if !errors.is_empty() {
                return Some(Err(errors.into_iter().next().unwrap()));
            }
        }

        let (batch, mut errors) = self.deserializer.flush_buffer();
        if let Some(error) = errors.pop() {
            Some(Err(error))
        } else {
            batch.map(Ok)
        }
    }
}
