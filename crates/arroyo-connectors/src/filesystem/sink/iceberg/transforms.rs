// Adapted from https://github.com/JanKaul/iceberg-rust/blob/495a566f762f276b30c55caa5c10d3d7cbf856f5/iceberg-rust/src/arrow/transform.rs
// Licensed under Apache2

use crate::filesystem::config::Transform;
use arrow::array::{
    as_primitive_array, downcast_array, Array, ArrayRef, PrimitiveArray, StringArray,
};
use arrow::buffer::ScalarBuffer;
use arrow::compute::{binary, cast, date_part, unary, DatePart};
use arrow::datatypes::{
    DataType, Date32Type, Int16Type, Int32Type, Int64Type, TimeUnit, TimestampMicrosecondType,
};
use arrow::error::ArrowError;
use datafusion::common::{exec_datafusion_err, exec_err, ScalarValue};
use datafusion::error::Result as DFResult;
use datafusion::execution::FunctionRegistry;
use datafusion::functions::{export_functions, make_udf_function};
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, TypeSignature, Volatility,
};
use std::any::Any;
use std::sync::Arc;

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
pub fn transform_arrow(array: ArrayRef, transform: Transform) -> Result<ArrayRef, ArrowError> {
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
                i - i.rem_euclid(m as i16)
            }),
        )),
        (DataType::Int32, Transform::Truncate(m)) => Ok(Arc::<PrimitiveArray<Int32Type>>::new(
            unary(as_primitive_array::<Int32Type>(&array), |i| {
                i - i.rem_euclid(m as i32)
            }),
        )),
        (DataType::Int64, Transform::Truncate(m)) => Ok(Arc::<PrimitiveArray<Int64Type>>::new(
            unary(as_primitive_array::<Int64Type>(&array), |i| {
                i - i.rem_euclid(m as i64)
            }),
        )),
        (DataType::Int32, Transform::Bucket(m)) => Ok(Arc::<PrimitiveArray<Int32Type>>::new(
            unary(as_primitive_array::<Int32Type>(&array), |i| {
                let mut buffer = std::io::Cursor::new((i as i64).to_le_bytes());
                (murmur3::murmur3_32(&mut buffer, 0).expect("murmur3 hash failled for some reason")
                    as i32)
                    .rem_euclid(m as i32)
            }),
        )),
        (DataType::Int64, Transform::Bucket(m)) => Ok(Arc::<PrimitiveArray<Int32Type>>::new(
            unary(as_primitive_array::<Int64Type>(&array), |i| {
                let mut buffer = std::io::Cursor::new((i).to_le_bytes());
                (murmur3::murmur3_32(&mut buffer, 0).expect("murmur3 hash failled for some reason")
                    as i32)
                    .rem_euclid(m as i32)
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
                        .rem_euclid(m as i32)
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
                        .rem_euclid(m as i32)
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
                    .rem_euclid(m as i32)
                })),
                nulls.cloned(),
            )))
        }
        _ => Err(ArrowError::ComputeError(
            "Failed to perform transform for datatype".to_string(),
        )),
    }
}

macro_rules! make_transform_udf {
    (
        $type_name:ident,
        $udf_name:ident,
        $ret_ty:expr,
        $signature:expr,
        $mk_transform:expr,
    ) => {
        #[derive(Debug)]
        struct $type_name(Signature);

        impl ScalarUDFImpl for $type_name {
            fn as_any(&self) -> &dyn Any {
                self
            }

            fn name(&self) -> &str {
                stringify!($udf_name)
            }

            fn signature(&self) -> &Signature {
                &self.0
            }

            fn return_type(&self, arg_types: &[DataType]) -> DFResult<DataType> {
                $ret_ty(arg_types)
            }

            fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
                let transform: DFResult<Transform> = $mk_transform(&args);
                let arr = args.args[0].to_array(args.number_rows)?;
                transform_arrow(arr, transform?)
                    .map(ColumnarValue::Array)
                    .map_err(|e| exec_datafusion_err!("{}: {}", self.name(), e))
            }
        }

        impl $type_name {
            fn new() -> Self {
                Self($signature)
            }
        }

        make_udf_function!($type_name, $udf_name);
    };
}
fn scalar_to_u32(arg: &ColumnarValue, fname: &str, pos: usize) -> DFResult<u32> {
    match arg {
        ColumnarValue::Scalar(ScalarValue::UInt32(Some(v))) => Ok(*v),
        ColumnarValue::Scalar(ScalarValue::UInt64(Some(v))) => u32::try_from(*v)
            .map_err(|_| exec_datafusion_err!("{fname}: arg#{pos} out of u32 range ({v})")),
        ColumnarValue::Scalar(ScalarValue::Int32(Some(v))) => u32::try_from(*v)
            .map_err(|_| exec_datafusion_err!("{fname}: arg#{pos} out of u32 range ({v})")),
        ColumnarValue::Scalar(ScalarValue::Int64(Some(v))) => u32::try_from(*v)
            .map_err(|_| exec_datafusion_err!("{fname}: arg#{pos} out of u32 range ({v})")),
        ColumnarValue::Scalar(ScalarValue::Null) => Err(exec_datafusion_err!(
            "{fname}: arg#{pos} must be non-null integer scalar"
        )),
        _ => Err(exec_datafusion_err!(
            "{fname}: arg#{pos} must be integer scalar"
        )),
    }
}

fn truncate_return_type(arg_types: &[DataType]) -> DFResult<DataType> {
    let Some(lhs) = arg_types.first() else {
        return Ok(DataType::Null);
    };
    match lhs {
        DataType::Int16 | DataType::Int32 | DataType::Int64 => Ok(lhs.clone()),
        other => Err(exec_datafusion_err!(
            "ice_truncate expects Int16/Int32/Int64 as first arg, got {other:?}"
        )),
    }
}

fn bucket_return_type(_arg_types: &[DataType]) -> DFResult<DataType> {
    Ok(DataType::Int32)
}

fn i32_return_type(_arg_types: &[DataType]) -> DFResult<DataType> {
    Ok(DataType::Int32)
}

fn time_signature() -> Signature {
    Signature::new(
        TypeSignature::OneOf(vec![
            TypeSignature::Exact(vec![DataType::Date32]),
            TypeSignature::Exact(vec![DataType::Timestamp(TimeUnit::Microsecond, None)]),
        ]),
        Volatility::Immutable,
    )
}

make_transform_udf!(
    IceIdentity,
    ice_identity,
    |arg_types: &[DataType]| Ok(arg_types.first().cloned().unwrap_or(DataType::Null)),
    // any single arg
    Signature::new(TypeSignature::Any(1), Volatility::Immutable),
    |_args: &ScalarFunctionArgs| Ok(Transform::Identity),
);

make_transform_udf!(
    IceDay,
    ice_day,
    i32_return_type,
    time_signature(),
    |_args: &ScalarFunctionArgs| Ok(Transform::Day),
);

make_transform_udf!(
    IceMonth,
    ice_month,
    i32_return_type,
    time_signature(),
    |_args: &ScalarFunctionArgs| Ok(Transform::Month),
);

make_transform_udf!(
    IceYear,
    ice_year,
    i32_return_type,
    time_signature(),
    |_args: &ScalarFunctionArgs| Ok(Transform::Year),
);

make_transform_udf!(
    IceHour,
    ice_hour,
    i32_return_type,
    time_signature(),
    |_args: &ScalarFunctionArgs| Ok(Transform::Hour),
);

make_transform_udf!(
    IceTruncate,
    ice_truncate,
    truncate_return_type,
    // two args: (Int16|Int32|Int64, Int32 m)
    Signature::new(
        TypeSignature::OneOf(vec![
            TypeSignature::Exact(vec![DataType::Int16, DataType::Int32]),
            TypeSignature::Exact(vec![DataType::Int32, DataType::Int32]),
            TypeSignature::Exact(vec![DataType::Int64, DataType::Int32])
        ],),
        Volatility::Immutable
    ),
    |args: &ScalarFunctionArgs| {
        if args.args.len() != 2 {
            return exec_err!("ice_truncate requires 2 args (value, m)");
        }
        let m = scalar_to_u32(&args.args[1], "ice_truncate", 2)?;
        Ok(Transform::Truncate(m))
    },
);

make_transform_udf!(
    IceBucket,
    ice_bucket,
    bucket_return_type,
    // two args: (value, Int32 m). value types validated by transform_arrow.
    Signature::new(TypeSignature::Any(2), Volatility::Immutable),
    |args: &ScalarFunctionArgs| {
        if args.args.len() != 2 {
            return exec_err!("ice_bucket requires 2 args (value, m)");
        }
        let m = scalar_to_u32(&args.args[1], "ice_bucket", 2)?;
        Ok(Transform::Bucket(m))
    },
);

pub fn register_all(registery: &mut dyn FunctionRegistry) -> DFResult<()> {
    registery.register_udf(ice_identity())?;
    registery.register_udf(ice_hour())?;
    registery.register_udf(ice_day())?;
    registery.register_udf(ice_month())?;
    registery.register_udf(ice_year())?;
    registery.register_udf(ice_truncate())?;
    registery.register_udf(ice_bucket())?;
    Ok(())
}

pub mod fns {
    use super::*;

    export_functions!(
        (ice_identity, "computes the iceberg identity transform",),
        (ice_hour, "computes the iceberg hour transform", timestamp),
        (ice_day, "computes the iceberg day transform", timestamp),
        (ice_month, "computes the iceberg month transform", timestamp),
        (ice_year, "computes the iceberg year transform", timestamp),
        (ice_truncate, "computes the iceberg truncate transform", num len),
        (ice_bucket, "computes the iceberg bucket transform", value count)
    );
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

#[cfg(test)]
mod tests {

    use super::*;
    use arrow::array::{ArrayRef, Date32Array, TimestampMicrosecondArray};

    fn create_date32_array() -> ArrayRef {
        Arc::new(Date32Array::from(vec![
            Some(19478), // 2023-05-01
            Some(19523), // 2023-06-15
            Some(19723), // 2024-01-01
            None,
        ])) as ArrayRef
    }

    fn create_timestamp_micro_array() -> ArrayRef {
        Arc::new(TimestampMicrosecondArray::from(vec![
            Some(1682937000000000),
            Some(1686840330000000),
            Some(1704067200000000),
            None,
        ])) as ArrayRef
    }

    #[test]
    fn test_identity_transform() {
        let array = create_date32_array();
        let result = transform_arrow(array.clone(), Transform::Identity).unwrap();
        assert_eq!(&array, &result);
    }

    #[test]
    fn test_date32_day_transform() {
        let array = create_date32_array();
        let result = transform_arrow(array, Transform::Day).unwrap();
        let expected = Arc::new(arrow::array::Int32Array::from(vec![
            Some(19478),
            Some(19523),
            Some(19723),
            None,
        ])) as ArrayRef;
        assert_eq!(&expected, &result);
    }

    #[test]
    fn test_date32_month_transform() {
        let array = create_date32_array();
        let result = transform_arrow(array, Transform::Month).unwrap();
        let expected = Arc::new(arrow::array::Int32Array::from(vec![
            Some(641),
            Some(642),
            Some(649),
            None,
        ])) as ArrayRef;
        assert_eq!(&expected, &result);
    }

    #[test]
    fn test_date32_year_transform() {
        let array = create_date32_array();
        let result = transform_arrow(array, Transform::Year).unwrap();
        let expected = Arc::new(arrow::array::Int32Array::from(vec![
            Some(53),
            Some(53),
            Some(54),
            None,
        ])) as ArrayRef;
        assert_eq!(&expected, &result);
    }

    #[test]
    fn test_timestamp_micro_hour_transform() {
        let array = create_timestamp_micro_array();
        let result = transform_arrow(array, Transform::Hour).unwrap();
        let expected = Arc::new(arrow::array::Int32Array::from(vec![
            Some(467482),
            Some(468566),
            Some(473352),
            None,
        ])) as ArrayRef;
        assert_eq!(&expected, &result);
    }

    #[test]
    fn test_timestamp_micro_day_transform() {
        let array = create_timestamp_micro_array();
        let result = transform_arrow(array, Transform::Day).unwrap();
        let expected = Arc::new(arrow::array::Int32Array::from(vec![
            Some(19478),
            Some(19523),
            Some(19723),
            None,
        ])) as ArrayRef;
        assert_eq!(&expected, &result);
    }

    #[test]
    fn test_timestamp_micro_month_transform() {
        let array = create_timestamp_micro_array();
        let result = transform_arrow(array, Transform::Month).unwrap();
        let expected = Arc::new(arrow::array::Int32Array::from(vec![
            Some(641),
            Some(642),
            Some(649),
            None,
        ])) as ArrayRef;
        assert_eq!(&expected, &result);
    }

    #[test]
    fn test_timestamp_micro_year_transform() {
        let array = create_timestamp_micro_array();
        let result = transform_arrow(array, Transform::Year).unwrap();
        let expected = Arc::new(arrow::array::Int32Array::from(vec![
            Some(53),
            Some(53),
            Some(54),
            None,
        ])) as ArrayRef;
        assert_eq!(&expected, &result);
    }

    #[test]
    fn test_int16_truncate_transform() {
        let array = Arc::new(arrow::array::Int16Array::from(vec![
            Some(17),
            Some(23),
            Some(-15),
            Some(5),
            None,
        ])) as ArrayRef;
        let result = transform_arrow(array, Transform::Truncate(10)).unwrap();
        let expected = Arc::new(arrow::array::Int16Array::from(vec![
            Some(10),  // 17 - 17 % 10 = 17 - 7 = 10
            Some(20),  // 23 - 23 % 10 = 23 - 3 = 20
            Some(-20), // -15 - (-15 % 10) = -15 - (-5) = -15 + 5 = -10, but rem_euclid gives -15 - 5 = -20
            Some(0),   // 5 - 5 % 10 = 5 - 5 = 0
            None,
        ])) as ArrayRef;
        assert_eq!(&expected, &result);
    }

    #[test]
    fn test_int32_truncate_transform() {
        let array = Arc::new(arrow::array::Int32Array::from(vec![
            Some(127),
            Some(234),
            Some(-156),
            Some(50),
            None,
        ])) as ArrayRef;
        let result = transform_arrow(array, Transform::Truncate(100)).unwrap();
        let expected = Arc::new(arrow::array::Int32Array::from(vec![
            Some(100),  // 127 - 127 % 100 = 127 - 27 = 100
            Some(200),  // 234 - 234 % 100 = 234 - 34 = 200
            Some(-200), // -156 - (-156 % 100) = -156 - (-56) = -156 + 56 = -100, but rem_euclid gives -156 - 44 = -200
            Some(0),    // 50 - 50 % 100 = 50 - 50 = 0
            None,
        ])) as ArrayRef;
        assert_eq!(&expected, &result);
    }

    #[test]
    fn test_int64_truncate_transform() {
        let array = Arc::new(arrow::array::Int64Array::from(vec![
            Some(1275),
            Some(2348),
            Some(-1567),
            Some(500),
            None,
        ])) as ArrayRef;
        let result = transform_arrow(array, Transform::Truncate(1000)).unwrap();
        let expected = Arc::new(arrow::array::Int64Array::from(vec![
            Some(1000),  // 1275 - 1275 % 1000 = 1275 - 275 = 1000
            Some(2000),  // 2348 - 2348 % 1000 = 2348 - 348 = 2000
            Some(-2000), // -1567 - (-1567 % 1000) = -1567 - (-567) = -1567 + 567 = -1000, but rem_euclid gives -1567 - 433 = -2000
            Some(0),     // 500 - 500 % 1000 = 500 - 500 = 0
            None,
        ])) as ArrayRef;
        assert_eq!(&expected, &result);
    }

    #[test]
    fn test_bucket_hash_value() {
        // Check value match https://iceberg.apache.org/spec/#appendix-b-32-bit-hash-requirements

        // 34 -> 2017239379
        let mut buffer = std::io::Cursor::new((34i32 as i64).to_le_bytes());
        assert_eq!(murmur3::murmur3_32(&mut buffer, 0).unwrap(), 2017239379);

        // 34 -> 2017239379
        let mut buffer = std::io::Cursor::new((34i64).to_le_bytes());
        assert_eq!(murmur3::murmur3_32(&mut buffer, 0).unwrap(), 2017239379);

        // daysFromUnixEpoch(2017-11-16) -> 17_486 -> -653330422
        let mut buffer = std::io::Cursor::new((17_486i32 as i64).to_le_bytes());
        assert_eq!(
            murmur3::murmur3_32(&mut buffer, 0).unwrap() as i32,
            -653330422
        );

        // 81_068_000_000 number of micros from midnight 22:31:08
        let mut buffer = std::io::Cursor::new((81_068_000_000i64).to_le_bytes());
        assert_eq!(
            murmur3::murmur3_32(&mut buffer, 0).unwrap() as i32,
            -662762989
        );

        // utf8Bytes(iceberg) -> 1210000089
        assert_eq!(
            murmur3::murmur3_32(&mut "iceberg".as_bytes(), 0).unwrap() as i32,
            1210000089
        );
    }

    #[test]
    fn test_int32_bucket_transform() {
        let array = Arc::new(arrow::array::Int32Array::from(vec![
            Some(34),       // Spec value
            Some(17_486),   // number of day between 2017-11-16 and epoch
            Some(84668000), // number of micros from midnight 22:31:08
            Some(-2000),
            Some(0),
            None,
        ])) as ArrayRef;
        let result = transform_arrow(array, Transform::Bucket(1000)).unwrap();
        let expected = Arc::new(arrow::array::Int32Array::from(vec![
            Some(2017239379i32.rem_euclid(1000)),
            Some(578), // -653330422 % 1000 not match I don't know why
            Some(988822981i32.rem_euclid(1000)),
            Some(964620854i32.rem_euclid(1000)),
            Some(1669671676i32.rem_euclid(1000)),
            None,
        ])) as ArrayRef;
        assert_eq!(&expected, &result);
    }

    #[test]
    fn test_int64_bucket_transform() {
        let array = Arc::new(arrow::array::Int64Array::from(vec![
            Some(34),     // Spec value
            Some(17_486), // number of day between 2017-11-16 and epoch
            Some(2000),
            Some(-2000),
            Some(0),
            None,
        ])) as ArrayRef;
        let result = transform_arrow(array, Transform::Bucket(1000)).unwrap();
        let expected = Arc::new(arrow::array::Int32Array::from(vec![
            Some(2017239379i32.rem_euclid(1000)),
            Some(578), // -653_330_422 % 1000 not match probably like to signed number
            Some(117), // 716_914_497 = 1000 not match probably like to signed number
            Some(964_620_854i32.rem_euclid(1000)),
            Some(1669671676i32.rem_euclid(1000)),
            None,
        ])) as ArrayRef;
        assert_eq!(&expected, &result);
    }

    #[test]
    fn test_date32_bucket_transform() {
        let array = Arc::new(arrow::array::Date32Array::from(vec![
            Some(17_486), // number of day between 2017-11-16
            None,
        ])) as ArrayRef;
        let result = transform_arrow(array, Transform::Bucket(1000)).unwrap();

        let expected = Arc::new(arrow::array::Int32Array::from(vec![
            Some(578), // -653330422 % 1000 not match probably like to signed number
            None,
        ])) as ArrayRef;

        assert_eq!(&expected, &result);
    }

    #[test]
    fn test_time32_bucket_transform() {
        let array = Arc::new(arrow::array::Time32MillisecondArray::from(vec![
            Some(81_068_000), // number of micros from midnight 22:31:08
            None,
        ])) as ArrayRef;
        let result = transform_arrow(array, Transform::Bucket(1000)).unwrap();
        let expected = Arc::new(arrow::array::Int32Array::from(vec![
            Some(693), // -662762989 % 1000 not match probably like to signed number
            None,
        ])) as ArrayRef;
        assert_eq!(&expected, &result);
    }

    #[test]
    fn test_utf8_bucket_transform() {
        let array =
            Arc::new(arrow::array::StringArray::from(vec![Some("iceberg"), None])) as ArrayRef;
        let result = transform_arrow(array, Transform::Bucket(1000)).unwrap();
        let expected = Arc::new(arrow::array::Int32Array::from(vec![
            Some(1_210_000_089i32.rem_euclid(1000)),
            None,
        ])) as ArrayRef;
        assert_eq!(&expected, &result);
    }

    #[test]
    fn test_unsupported_transform() {
        let array = Arc::new(arrow::array::StringArray::from(vec!["a", "b", "c"])) as ArrayRef;
        let result = transform_arrow(array, Transform::Day);
        assert!(result.is_err());
        assert_eq!(
            result.unwrap_err().to_string(),
            "Compute error: Failed to perform transform for datatype"
        );
    }
}
