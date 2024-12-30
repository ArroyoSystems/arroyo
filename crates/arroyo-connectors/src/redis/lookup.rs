use crate::redis::sink::GeneralConnection;
use crate::redis::RedisClient;
use arrow::array::{ArrayRef, AsArray, RecordBatch};
use arrow::datatypes::DataType;
use arroyo_formats::de::ArrowDeserializer;
use arroyo_operator::connector::LookupConnector;
use arroyo_types::SourceError;
use async_trait::async_trait;
use redis::aio::ConnectionLike;
use redis::{cmd, Value};
use std::time::SystemTime;

pub struct RedisLookup {
    pub(crate) deserializer: ArrowDeserializer,
    pub(crate) client: RedisClient,
    pub(crate) connection: Option<GeneralConnection>,
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

        for k in keys[0].as_string::<i32>() {
            mget.arg(k.unwrap());
        }

        let Value::Array(vs) = connection.req_packed_command(&mget).await.unwrap() else {
            panic!("value was not an array");
        };

        for v in vs {
            match v {
                Value::Nil => {
                    self.deserializer.deserialize_slice("null".as_bytes(), SystemTime::now(), None)
                        .await;
                }
                Value::SimpleString(s) => {
                    self.deserializer
                        .deserialize_slice(s.as_bytes(), SystemTime::now(), None)
                        .await;
                }
                v => {
                    panic!("unexpected type {:?}", v);
                }
            }
        }

        self.deserializer.flush_buffer()
    }
}
