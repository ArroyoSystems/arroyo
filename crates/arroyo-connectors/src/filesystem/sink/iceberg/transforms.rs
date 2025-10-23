// Adapted from https://github.com/JanKaul/iceberg-rust/blob/main/iceberg-rust/src/arrow/transform.rs
// Licensed under Apache2

use std::sync::Arc;
use arrow::array::{as_primitive_array, downcast_array, Array, ArrayRef, PrimitiveArray, StringArray};
use arrow::buffer::ScalarBuffer;
use arrow::compute::{binary, cast, date_part, unary, DatePart};
use arrow::datatypes::{DataType, Date32Type, Int16Type, Int32Type, Int64Type, TimeUnit, TimestampMicrosecondType};
use arrow::error::ArrowError;
use crate::filesystem::config::Transform;

static YEARS_BEFORE_UNIX_EPOCH: i32 = 1970;
static MICROS_IN_HOUR: i64 = 3_600_000_000;
static MICROS_IN_DAY: i64 = 86_400_000_000;


/// Applies an Iceberg partition transform to an Arrow array
///
/// # Arguments
/// * `array` - The Arrow array to transform
/// * `transform` - The Iceberg partition transform to apply
///
/// # Returns
/// * `Ok(ArrayRef)` - A new Arrow array containing the transformed values
/// * `Err(ArrowError)` - If the transform cannot be applied to the array's data type
///
/// # Supported Transforms
/// * Identity - Returns the input array unchanged
/// * Day - Extracts day from date32 or timestamp
/// * Month - Extracts month from date32 or timestamp
/// * Year - Extracts year from date32 or timestamp
/// * Hour - Extracts hour from timestamp
/// * Int16 - Truncate value
/// * Int32 - Truncate value
/// * Int64 - Truncate value
/// * Int32 - Use hash of value to repart it between bucket
/// * Int64 - Use hash of value to repart it between bucket
/// * Date32 - Use hash of value to repart it between bucket
/// * Time32 - Use hash of value to repart it between bucket
/// * Utf8 - Use hash of value to repart it between bucket
pub fn transform_arrow(array: ArrayRef, transform: &Transform) -> Result<ArrayRef, ArrowError> {
    match (array.data_type(), transform) {
        (_, Transform::Identity) => Ok(array),
        (DataType::Date32, Transform::Day) => cast(&array, &DataType::Int32),
        (DataType::Date32, Transform::Month) => {
            let year = date_part(as_primitive_array::<Date32Type>(&array), DatePart::Year)?;
            let month = date_part(as_primitive_array::<Date32Type>(&array), DatePart::Month)?;
            Ok(Arc::new(binary::<_, _, _, Int32Type>(
                as_primitive_array::<Int32Type>(&year),
                as_primitive_array::<Int32Type>(&month),
                datepart_to_months,
            )?))
        }
        (DataType::Date32, Transform::Year) => Ok(Arc::new(unary::<_, _, Int32Type>(
            as_primitive_array::<Int32Type>(&date_part(
                as_primitive_array::<Date32Type>(&array),
                DatePart::Year,
            )?),
            datepart_to_years,
        ))),
        (DataType::Timestamp(TimeUnit::Microsecond, None), Transform::Hour) => {
            Ok(Arc::new(unary::<_, _, Int32Type>(
                as_primitive_array::<Int64Type>(&cast(&array, &DataType::Int64)?),
                micros_to_hours,
            )) as Arc<dyn Array>)
        }
        (DataType::Timestamp(TimeUnit::Microsecond, None), Transform::Day) => {
            Ok(Arc::new(unary::<_, _, Int32Type>(
                as_primitive_array::<Int64Type>(&cast(&array, &DataType::Int64)?),
                micros_to_days,
            )) as Arc<dyn Array>)
        }
        (DataType::Timestamp(TimeUnit::Microsecond, None), Transform::Month) => {
            let year = date_part(
                as_primitive_array::<TimestampMicrosecondType>(&array),
                DatePart::Year,
            )?;
            let month = date_part(
                as_primitive_array::<TimestampMicrosecondType>(&array),
                DatePart::Month,
            )?;
            Ok(Arc::new(binary::<_, _, _, Int32Type>(
                as_primitive_array::<Int32Type>(&year),
                as_primitive_array::<Int32Type>(&month),
                datepart_to_months,
            )?))
        }
        (DataType::Timestamp(TimeUnit::Microsecond, None), Transform::Year) => {
            Ok(Arc::new(unary::<_, _, Int32Type>(
                as_primitive_array::<Int32Type>(&date_part(
                    as_primitive_array::<TimestampMicrosecondType>(&array),
                    DatePart::Year,
                )?),
                datepart_to_years,
            )))
        }
        (DataType::Int16, Transform::Truncate(m)) => Ok(Arc::<PrimitiveArray<Int16Type>>::new(
            unary(as_primitive_array::<Int16Type>(&array), |i| {
                i - i.rem_euclid(*m as i16)
            }),
        )),
        (DataType::Int32, Transform::Truncate(m)) => Ok(Arc::<PrimitiveArray<Int32Type>>::new(
            unary(as_primitive_array::<Int32Type>(&array), |i| {
                i - i.rem_euclid(*m as i32)
            }),
        )),
        (DataType::Int64, Transform::Truncate(m)) => Ok(Arc::<PrimitiveArray<Int64Type>>::new(
            unary(as_primitive_array::<Int64Type>(&array), |i| {
                i - i.rem_euclid(*m as i64)
            }),
        )),
        (DataType::Int32, Transform::Bucket(m)) => Ok(Arc::<PrimitiveArray<Int32Type>>::new(
            unary(as_primitive_array::<Int32Type>(&array), |i| {
                let mut buffer = std::io::Cursor::new((i as i64).to_le_bytes());
                (murmur3::murmur3_32(&mut buffer, 0).expect("murmur3 hash failled for some reason")
                    as i32)
                    .rem_euclid(*m as i32)
            }),
        )),
        (DataType::Int64, Transform::Bucket(m)) => Ok(Arc::<PrimitiveArray<Int32Type>>::new(
            unary(as_primitive_array::<Int64Type>(&array), |i| {
                let mut buffer = std::io::Cursor::new((i).to_le_bytes());
                (murmur3::murmur3_32(&mut buffer, 0).expect("murmur3 hash failled for some reason")
                    as i32)
                    .rem_euclid(*m as i32)
            }),
        )),
        (DataType::Date32, Transform::Bucket(m)) => {
            let temp = cast(&array, &DataType::Int32)?;

            Ok(Arc::<PrimitiveArray<Int32Type>>::new(unary(
                as_primitive_array::<Int32Type>(&temp),
                |i| {
                    let mut buffer = std::io::Cursor::new((i as i64).to_le_bytes());
                    (murmur3::murmur3_32(&mut buffer, 0)
                        .expect("murmur3 hash failled for some reason") as i32)
                        .rem_euclid(*m as i32)
                },
            )))
        }
        (DataType::Time32(TimeUnit::Millisecond), Transform::Bucket(m)) => {
            let temp = cast(&array, &DataType::Int32)?;

            Ok(Arc::<PrimitiveArray<Int32Type>>::new(unary(
                as_primitive_array::<Int32Type>(&temp),
                |i: i32| {
                    let mut buffer = std::io::Cursor::new((i as i64).to_le_bytes());
                    (murmur3::murmur3_32(&mut buffer, 0)
                        .expect("murmur3 hash failled for some reason") as i32)
                        .rem_euclid(*m as i32)
                },
            )))
        }
        (DataType::Utf8, Transform::Bucket(m)) => {
            let nulls = array.nulls();
            let local_array: StringArray = downcast_array::<StringArray>(&array);

            Ok(Arc::new(PrimitiveArray::<Int32Type>::new(
                ScalarBuffer::from_iter(local_array.iter().map(|a| {
                    if let Some(value) = a {
                        murmur3::murmur3_32(&mut value.as_bytes(), 0)
                            .expect("murmur3 hash failled for some reason")
                            as i32
                    } else {
                        0
                    }
                        .rem_euclid(*m as i32)
                })),
                nulls.cloned(),
            )))
        }
        _ => Err(ArrowError::ComputeError(
            "Failed to perform transform for datatype".to_string(),
        )),
    }
}

#[inline]
fn micros_to_days(a: i64) -> i32 {
    (a / MICROS_IN_DAY) as i32
}

#[inline]
fn micros_to_hours(a: i64) -> i32 {
    (a / MICROS_IN_HOUR) as i32
}

#[inline]
fn datepart_to_years(year: i32) -> i32 {
    year - YEARS_BEFORE_UNIX_EPOCH
}

#[inline]
fn datepart_to_months(year: i32, month: i32) -> i32 {
    12 * (year - YEARS_BEFORE_UNIX_EPOCH) + month
}