use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};
use arrow::array::{ArrayRef, AsArray, OffsetSizeTrait, RecordBatch};
use arrow::compute::StringArrayType;
use arrow::datatypes::DataType;
use async_trait::async_trait;
use futures::future::OptionFuture;
use futures::stream::FuturesOrdered;
use futures::StreamExt;
use redis::{cmd, AsyncCommands, Pipeline, RedisFuture, RedisResult, Value};
use redis::aio::ConnectionLike;
use arroyo_formats::de::ArrowDeserializer;
use crate::LookupConnector;
use crate::redis::{RedisClient, RedisConnector};
use crate::redis::sink::GeneralConnection;

pub struct RedisLookup {
    deserializer: ArrowDeserializer,
    client: RedisClient,
    connection: Option<GeneralConnection>,
}

// pub enum RedisFutureOrNull<'a> {
//     RedisFuture(RedisFuture<'a, String>),
//     Null
// }
// 
// impl <'a> Future for RedisFutureOrNull<'a> {
//     type Output = RedisResult<Option<String>>;
// 
//     fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
//         match self {
//             RedisFutureOrNull::RedisFuture(f) => (**f).poll().map(|t| Some(t)),
//             RedisFutureOrNull::Null => Poll::Ready(None),
//         }
//     }
// }

#[async_trait]
impl LookupConnector for RedisLookup {
    fn name(&self) -> String {
        "RedisLookup".to_string()
    }

    async fn lookup(&mut self, keys: &[ArrayRef]) -> RecordBatch {
        if self.connection.is_none() {
            self.connection = Some(self.client.get_connection().await.unwrap());
        }

        assert_eq!(keys.len(), 1, "redis lookup can only have a single key");
        assert_eq!(*keys[0].data_type(), DataType::Utf8, "redis lookup key must be a string");


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
                Value::Nil => {}
                Value::SimpleString(s) => {
                    self.deserializer.deserialize_slice()
                }
                v => {
                    panic!("unexpected type {:?}", v);
                }
            }
        }
        
        
        todo!()
    }
}