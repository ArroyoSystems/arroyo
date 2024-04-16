use crate::{AsyncUdfDylib, AsyncUdfDylibInterface, SyncUdfDylib, UdfDylibInterface};
use arrow::array::{Array, Int32Array, StringArray};
use arrow::datatypes::DataType;
use datafusion::logical_expr::{ColumnarValue, ScalarUDFImpl, Signature, Volatility};
use std::sync::Arc;

mod test_udf_1 {
    use arroyo_udf_macros::udf;
    #[udf]
    fn my_udf(x: i32, y: String) -> Option<String> {
        if x < 5 {
            None
        } else {
            Some(format!("{x}-{y}"))
        }
    }
}

#[test]
fn test() {
    let interface = UdfDylibInterface {
        run: test_udf_1::run,
    };

    let udf = SyncUdfDylib::new(
        "my_udf".to_string(),
        Signature::exact(vec![DataType::Int32, DataType::Utf8], Volatility::Volatile),
        DataType::Utf8,
        interface,
    );

    let result = udf
        .invoke(&vec![
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
use std::time::Duration;
use tokio::time::Instant;

#[tokio::test]
async fn test_async() {
    let interface = AsyncUdfDylibInterface {
        start: test_async_udf::start,
        send: test_async_udf::send,
        drain_results: test_async_udf::drain_results,
        stop_runtime: test_async_udf::stop_runtime,
    };
    let mut udf = AsyncUdfDylib::new("my_udf".to_string(), DataType::Utf8, interface);
    udf.start(false, Duration::from_secs(1));

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
