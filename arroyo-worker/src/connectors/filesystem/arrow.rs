use std::time::{Duration, SystemTime};

use arrow::datatypes::{DataType, TimeUnit};
use arrow_array::{
    types::{
        ArrowPrimitiveType, TimestampMicrosecondType, TimestampMillisecondType,
        TimestampNanosecondType, TimestampSecondType,
    },
    Array, ArrayRef, PrimitiveArray,
};

pub fn extract_native_value_from_arrow_array_nullable<T: ArrowPrimitiveType>(
    array: &ArrayRef,
    index: usize,
) -> Option<T::Native> {
    let array = array
        .as_any()
        .downcast_ref::<PrimitiveArray<T>>()
        .unwrap()
        .clone();
    if array.is_null(index) {
        None
    } else {
        Some(array.value(index))
    }
}

pub fn extract_native_value_from_arrow_array<T: ArrowPrimitiveType>(
    array: &ArrayRef,
    index: usize,
) -> T::Native {
    let array = array
        .as_any()
        .downcast_ref::<PrimitiveArray<T>>()
        .unwrap()
        .clone();
    array.value(index)
}

pub fn extract_timestamp_from_arrow_array_nullable(
    array: &ArrayRef,
    index: usize,
) -> Option<SystemTime> {
    match array.data_type() {
        DataType::Timestamp(grain, None) => match grain {
            TimeUnit::Second => {
                extract_native_value_from_arrow_array_nullable::<TimestampSecondType>(array, index)
                    .map(|seconds| SystemTime::UNIX_EPOCH + Duration::from_secs(seconds as u64))
            }
            TimeUnit::Millisecond => extract_native_value_from_arrow_array_nullable::<
                TimestampMillisecondType,
            >(array, index)
            .map(|milliseconds| {
                SystemTime::UNIX_EPOCH + Duration::from_millis(milliseconds as u64)
            }),
            TimeUnit::Microsecond => extract_native_value_from_arrow_array_nullable::<
                TimestampMicrosecondType,
            >(array, index)
            .map(|microseconds| {
                SystemTime::UNIX_EPOCH + Duration::from_micros(microseconds as u64)
            }),
            TimeUnit::Nanosecond => extract_native_value_from_arrow_array_nullable::<
                TimestampNanosecondType,
            >(array, index)
            .map(|nanoseconds| SystemTime::UNIX_EPOCH + Duration::from_nanos(nanoseconds as u64)),
        },
        other => unreachable!("data type {:?} can't be converted to a SystemTime", other),
    }
}

pub fn extract_timestamp_from_arrow_array(array: &ArrayRef, index: usize) -> SystemTime {
    match array.data_type() {
        DataType::Timestamp(grain, None) => match grain {
            TimeUnit::Second => {
                SystemTime::UNIX_EPOCH
                    + Duration::from_secs(
                        extract_native_value_from_arrow_array::<TimestampSecondType>(array, index)
                            as u64,
                    )
            }
            TimeUnit::Millisecond => {
                SystemTime::UNIX_EPOCH
                    + Duration::from_millis(extract_native_value_from_arrow_array::<
                        TimestampMillisecondType,
                    >(array, index) as u64)
            }
            TimeUnit::Microsecond => {
                SystemTime::UNIX_EPOCH
                    + Duration::from_micros(extract_native_value_from_arrow_array::<
                        TimestampMicrosecondType,
                    >(array, index) as u64)
            }
            TimeUnit::Nanosecond => {
                SystemTime::UNIX_EPOCH
                    + Duration::from_nanos(extract_native_value_from_arrow_array::<
                        TimestampNanosecondType,
                    >(array, index) as u64)
            }
        },
        other => unreachable!("data type {:?} can't be converted to a SystemTime", other),
    }
}

pub fn extract_string_from_arrow_array(array: &ArrayRef, index: usize) -> String {
    let array = array
        .as_any()
        .downcast_ref::<arrow::array::StringArray>()
        .unwrap()
        .clone();
    array.value(index).to_string()
}

pub fn extract_string_from_arrow_array_nullable(array: &ArrayRef, index: usize) -> Option<String> {
    let array = array
        .as_any()
        .downcast_ref::<arrow::array::StringArray>()
        .unwrap()
        .clone();
    if array.is_null(index) {
        None
    } else {
        Some(array.value(index).to_string())
    }
}

pub fn extract_bool_from_arrow_array_nullable(array: &ArrayRef, index: usize) -> Option<bool> {
    let array = array
        .as_any()
        .downcast_ref::<arrow::array::BooleanArray>()
        .unwrap()
        .clone();
    if array.is_null(index) {
        None
    } else {
        Some(array.value(index))
    }
}

pub fn extract_bool_from_arrow_array(array: &ArrayRef, index: usize) -> bool {
    let array = array
        .as_any()
        .downcast_ref::<arrow::array::BooleanArray>()
        .unwrap()
        .clone();
    array.value(index)
}
