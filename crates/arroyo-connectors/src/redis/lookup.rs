use std::collections::HashMap;
use crate::redis::sink::GeneralConnection;
use crate::redis::RedisClient;
use arrow::array::{Array, ArrayRef, AsArray, RecordBatch};
use arrow::datatypes::DataType;
use arroyo_formats::de::{ArrowDeserializer, FieldValueType};
use arroyo_operator::connector::LookupConnector;
use arroyo_types::{SourceError, LOOKUP_KEY_INDEX_FIELD};
use async_trait::async_trait;
use redis::aio::ConnectionLike;
use redis::{cmd, Value};
use arroyo_rpc::MetadataField;

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
            println!("GETTTING {:?}", k);
        }

        let Value::Array(vs) = connection.req_packed_command(&mget).await.unwrap() else {
            panic!("value was not an array");
        };

        assert_eq!(vs.len(), keys.len(), "Redis sent back the wrong number of values");

        let mut additional = HashMap::new();
        
        for (idx, (v, k)) in vs.iter().zip(keys).enumerate() {
            additional.insert(LOOKUP_KEY_INDEX_FIELD, FieldValueType::UInt64(idx as u64));
            for m in &self.metadata_fields {
                additional.insert(m.field_name.as_str(), match m.key.as_str() {
                    "key" => FieldValueType::String(k.unwrap()),
                    k => unreachable!("Invalid metadata key '{}'", k)
                });
            }

            let errors = match v {
                Value::Nil => {
                    println!("GOt null");
                    vec![]
                }
                Value::SimpleString(s) => {
                    println!("Got {:?}", s);
                    self.deserializer
                        .deserialize_without_timestamp(s.as_bytes(), Some(&additional))
                        .await
                }
                Value::BulkString(v) => {
                    println!("Got {:?}", String::from_utf8(v.clone()));
                    self.deserializer
                        .deserialize_without_timestamp(&v, Some(&additional))
                        .await
                }
                v => {
                    panic!("unexpected type {:?}", v);
                }
            };
            
            if !errors.is_empty() {
                return Some(Err(errors.into_iter().next().unwrap()));
            }
        }

        self.deserializer.flush_buffer()
    }
}
