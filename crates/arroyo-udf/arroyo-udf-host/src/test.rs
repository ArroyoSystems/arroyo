use crate::{AsyncUdfDylib, AsyncUdfDylibInterface, SyncUdfDylib};
use arrow::array::{Array, ArrayRef, Int32Array, StringArray, UInt64Array};
use arrow::datatypes::DataType;
use datafusion::logical_expr::{ColumnarValue, ScalarUDFImpl};
use std::sync::Arc;

mod test_udf_1 {
    use crate as arroyo_udf_host;
    use arroyo_udf_macros::local_udf;

    #[local_udf]
    fn my_udf(x: i32, y: String) -> Option<String> {
        if x < 5 {
            None
        } else {
            Some(format!("{x}-{y}"))
        }
    }
}

#[test]
fn test_udf() {
    let udf = test_udf_1::__local().config;
    let sync_udf: SyncUdfDylib = (&udf).try_into().unwrap();
    let result = sync_udf
        .invoke(&[
            ColumnarValue::Array(Arc::new(Int32Array::from(vec![1, 10, 20]))),
            ColumnarValue::Array(Arc::new(StringArray::from(vec!["a", "b", "c"]))),
        ])
        .unwrap();

    let ColumnarValue::Array(a) = result else {
        panic!("not an array");
    };

    let result = a.as_any().downcast_ref::<StringArray>().unwrap();
    assert_eq!(result.len(), 3);
    assert!(result.is_null(0));
    assert_eq!(result.value(1), "10-b");
    assert_eq!(result.value(2), "20-c");
}

mod test_udaf {
    use crate as arroyo_udf_host;
    use arroyo_udf_macros::local_udf;

    #[local_udf]
    fn my_udaf(x: Vec<u64>) -> u64 {
        x.len() as u64
    }
}

#[test]
fn test_udaf() {
    let udf = test_udaf::__local().config;
    let sync_udf: SyncUdfDylib = (&udf).try_into().unwrap();
    let result = sync_udf
        .invoke_udaf(&[Arc::new(UInt64Array::from(vec![1, 10, 20])) as ArrayRef])
        .unwrap();

    assert_eq!(result, ScalarValue::UInt64(Some(3)));
}

mod test_async_udf {
    use arroyo_udf_macros::udf;
    use std::time::Duration;

    #[udf(ordered, timeout = "5s", allowed_in_flight = 100)]
    async fn my_udf(a: u64, b: String) -> u32 {
        tokio::time::sleep(Duration::from_millis(10)).await;
        a as u32 + b.len() as u32
    }
}

use arrow::array::PrimitiveArray;
use arrow::datatypes::{UInt32Type, UInt64Type};
use datafusion::common::ScalarValue;
use std::time::Duration;
use tokio::time::Instant;

#[tokio::test]
async fn test_async() {
    let interface = AsyncUdfDylibInterface {
        __start: test_async_udf::__start,
        __send: test_async_udf::__send,
        __drain_results: test_async_udf::__drain_results,
        __stop_runtime: test_async_udf::__stop_runtime,
    };
    let mut udf = AsyncUdfDylib::new("my_udf".to_string(), DataType::Utf8, interface);
    udf.start(false, Duration::from_secs(1), 100);

    let arg1 = PrimitiveArray::<UInt64Type>::from(vec![2]);
    let arg2 = StringArray::from(vec!["hello"]);
    udf.send(5, vec![arg1.to_data(), arg2.to_data()])
        .await
        .unwrap();

    let start_time = Instant::now();
    loop {
        match udf.drain_results().unwrap() {
            Some((ids, values)) => {
                let values = PrimitiveArray::<UInt32Type>::from(values);
                assert_eq!(values.len(), 1);
                assert_eq!(values.value(0), 7);
                assert_eq!(ids.len(), 1);
                assert_eq!(ids.value(0), 5);
                return;
            }
            None => {
                if start_time.elapsed() > Duration::from_secs(1) {
                    panic!("no results after 1 second");
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        }
    }
}
