use std::{
    sync::Arc,
    time::{Duration, SystemTime},
};

use anyhow::bail;
use anyhow::Result;
use arrow::array;
use arrow::array::ArrayData;
use arrow::datatypes::{DataType, Field, IntervalMonthDayNanoType};
use arrow_array::make_array;

use arrow_schema::{IntervalUnit, TimeUnit, DECIMAL128_MAX_PRECISION, DECIMAL_DEFAULT_SCALE};
use datafusion::sql::sqlparser::ast::{
    ArrayElemTypeDef, DataType as SQLDataType, ExactNumberInfo, TimezoneInfo,
};
use datafusion_common::ScalarValue;
use datafusion_expr::ColumnarValue;

use arroyo_types::{ArroyoExtensionType, NullableType};
use syn::PathArguments::AngleBracketed;
use syn::{GenericArgument, Type};

/* this returns a duration with the same length as the postgres interval. */
pub fn interval_month_day_nanos_to_duration(serialized_value: i128) -> Duration {
    let (month, day, nanos) = IntervalMonthDayNanoType::to_parts(serialized_value);
    let years = month / 12;
    let extra_month = month % 12;
    let year_hours = 1461 * years * 24 / 4;
    let days_to_seconds = ((year_hours + 24 * (day + 30 * extra_month)) as u64) * 60 * 60;
    let nanos = nanos as u64;
    std::time::Duration::from_secs(days_to_seconds) + std::time::Duration::from_nanos(nanos)
}

pub fn rust_primitive_to_arrow(typ: &Type) -> Option<DataType> {
    match typ {
        Type::Path(pat) => {
            let path: Vec<String> = pat
                .path
                .segments
                .iter()
                .map(|s| s.ident.to_string())
                .collect();

            match path.join("::").as_str() {
                "bool" => Some(DataType::Boolean),
                "i8" => Some(DataType::Int8),
                "i16" => Some(DataType::Int16),
                "i32" => Some(DataType::Int32),
                "i64" => Some(DataType::Int64),
                "u8" => Some(DataType::UInt8),
                "u16" => Some(DataType::UInt16),
                "u32" => Some(DataType::UInt32),
                "u64" => Some(DataType::UInt64),
                "f16" => Some(DataType::Float16),
                "f32" => Some(DataType::Float32),
                "f64" => Some(DataType::Float64),
                "String" => Some(DataType::Utf8),
                "Vec<u8>" => Some(DataType::Binary),
                "SystemTime" | "std::time::SystemTime" => {
                    Some(DataType::Timestamp(TimeUnit::Microsecond, None))
                }
                "Duration" | "std::time::Duration" => {
                    Some(DataType::Duration(TimeUnit::Microsecond))
                }
                _ => None,
            }
        }
        _ => None,
    }
}

pub fn rust_to_arrow(typ: &Type) -> Option<NullableType> {
    match typ {
        Type::Path(pat) => {
            let last = pat.path.segments.last().unwrap();
            if last.ident == "Option" {
                let AngleBracketed(args) = &last.arguments else {
                    return None;
                };

                let GenericArgument::Type(inner) = args.args.first()? else {
                    return None;
                };

                Some(rust_to_arrow(inner)?.with_nullability(true))
            } else {
                Some(NullableType::not_null(rust_primitive_to_arrow(typ)?))
            }
        }
        _ => None,
    }
}

pub fn array_to_columnar_value(array: ArrayData, data_type: &DataType) -> ColumnarValue {
    if array.len() != 1 {
        return ColumnarValue::Array(make_array(array));
    }

    let scalar = match data_type {
        DataType::Utf8 => {
            ScalarValue::Utf8(Some(array::StringArray::from(array).value(0).to_string()))
        }
        DataType::Boolean => ScalarValue::Boolean(Some(array::BooleanArray::from(array).value(0))),
        DataType::Int8 => ScalarValue::Int8(Some(array::Int8Array::from(array).value(0))),
        DataType::Int16 => ScalarValue::Int16(Some(array::Int16Array::from(array).value(0))),
        DataType::Int32 => ScalarValue::Int32(Some(array::Int32Array::from(array).value(0))),
        DataType::Int64 => ScalarValue::Int64(Some(array::Int64Array::from(array).value(0))),
        DataType::UInt8 => ScalarValue::UInt8(Some(array::UInt8Array::from(array).value(0))),
        DataType::UInt16 => ScalarValue::UInt16(Some(array::UInt16Array::from(array).value(0))),
        DataType::UInt32 => ScalarValue::UInt32(Some(array::UInt32Array::from(array).value(0))),
        DataType::UInt64 => ScalarValue::UInt64(Some(array::UInt64Array::from(array).value(0))),
        DataType::Float32 => ScalarValue::Float32(Some(array::Float32Array::from(array).value(0))),
        DataType::Float64 => ScalarValue::Float64(Some(array::Float64Array::from(array).value(0))),
        _ => panic!("Unsupported DataType: {:?}", data_type),
    };
    ColumnarValue::Scalar(scalar)
}

// Pulled from DataFusion

pub(crate) fn convert_data_type(
    sql_type: &SQLDataType,
) -> Result<(DataType, Option<ArroyoExtensionType>)> {
    match sql_type {
        SQLDataType::Array(ArrayElemTypeDef::AngleBracket(inner_sql_type))
        | SQLDataType::Array(ArrayElemTypeDef::SquareBracket(inner_sql_type)) => {
            let (data_type, extension) = convert_simple_data_type(inner_sql_type)?;

            Ok((
                DataType::List(Arc::new(ArroyoExtensionType::add_metadata(
                    extension,
                    Field::new("field", data_type, true),
                ))),
                None,
            ))
        }
        SQLDataType::Array(ArrayElemTypeDef::None) => {
            bail!("Arrays with unspecified type is not supported")
        }
        other => convert_simple_data_type(other),
    }
}

fn convert_simple_data_type(
    sql_type: &SQLDataType,
) -> Result<(DataType, Option<ArroyoExtensionType>)> {
    if matches!(sql_type, SQLDataType::JSON) {
        return Ok((DataType::Utf8, Some(ArroyoExtensionType::JSON)));
    }

    let dt = match sql_type {
        SQLDataType::Boolean | SQLDataType::Bool => Ok(DataType::Boolean),
        SQLDataType::TinyInt(_) => Ok(DataType::Int8),
        SQLDataType::SmallInt(_) | SQLDataType::Int2(_) => Ok(DataType::Int16),
        SQLDataType::Int(_) | SQLDataType::Integer(_) | SQLDataType::Int4(_) => Ok(DataType::Int32),
        SQLDataType::BigInt(_) | SQLDataType::Int8(_) => Ok(DataType::Int64),
        SQLDataType::UnsignedTinyInt(_) => Ok(DataType::UInt8),
        SQLDataType::UnsignedSmallInt(_) | SQLDataType::UnsignedInt2(_) => Ok(DataType::UInt16),
        SQLDataType::UnsignedInt(_)
        | SQLDataType::UnsignedInteger(_)
        | SQLDataType::UnsignedInt4(_) => Ok(DataType::UInt32),
        SQLDataType::UnsignedBigInt(_) | SQLDataType::UnsignedInt8(_) => Ok(DataType::UInt64),
        SQLDataType::Float(_) => Ok(DataType::Float32),
        SQLDataType::Real | SQLDataType::Float4 => Ok(DataType::Float32),
        SQLDataType::Double | SQLDataType::DoublePrecision | SQLDataType::Float8 => {
            Ok(DataType::Float64)
        }
        SQLDataType::Char(_)
        | SQLDataType::Varchar(_)
        | SQLDataType::Text
        | SQLDataType::String(_) => Ok(DataType::Utf8),
        SQLDataType::Timestamp(None, TimezoneInfo::None) | SQLDataType::Datetime(_) => {
            Ok(DataType::Timestamp(TimeUnit::Nanosecond, None))
        }
        SQLDataType::Timestamp(Some(precision), TimezoneInfo::None) => match *precision {
            0 => Ok(DataType::Timestamp(TimeUnit::Second, None)),
            3 => Ok(DataType::Timestamp(TimeUnit::Millisecond, None)),
            6 => Ok(DataType::Timestamp(TimeUnit::Microsecond, None)),
            9 => Ok(DataType::Timestamp(TimeUnit::Nanosecond, None)),
            _ => bail!("unsupported precision {}", precision),
        },
        SQLDataType::Date => Ok(DataType::Date32),
        SQLDataType::Time(None, tz_info) => {
            if matches!(tz_info, TimezoneInfo::None)
                || matches!(tz_info, TimezoneInfo::WithoutTimeZone)
            {
                Ok(DataType::Time64(TimeUnit::Nanosecond))
            } else {
                // We don't support TIMETZ and TIME WITH TIME ZONE for now
                bail!("Unsupported SQL type {sql_type:?}")
            }
        }
        SQLDataType::Numeric(exact_number_info) | SQLDataType::Decimal(exact_number_info) => {
            let (precision, scale) = match *exact_number_info {
                ExactNumberInfo::None => (None, None),
                ExactNumberInfo::Precision(precision) => (Some(precision), None),
                ExactNumberInfo::PrecisionAndScale(precision, scale) => {
                    (Some(precision), Some(scale))
                }
            };
            make_decimal_type(precision, scale)
        }
        SQLDataType::Bytea => Ok(DataType::Binary),
        SQLDataType::Interval => Ok(DataType::Interval(IntervalUnit::MonthDayNano)),
        // Explicitly list all other types so that if sqlparser
        // adds/changes the `SQLDataType` the compiler will tell us on upgrade
        // and avoid bugs like https://github.com/apache/arrow-datafusion/issues/3059
        _ => bail!("Unsupported SQL type {sql_type:?}"),
    };

    Ok((dt?, None))
}

/// Returns a validated `DataType` for the specified precision and
/// scale
pub(crate) fn make_decimal_type(precision: Option<u64>, scale: Option<u64>) -> Result<DataType> {
    // postgres like behavior
    let (precision, scale) = match (precision, scale) {
        (Some(p), Some(s)) => (p as u8, s as i8),
        (Some(p), None) => (p as u8, 0),
        (None, Some(_)) => {
            bail!("Cannot specify only scale for decimal data type".to_string())
        }
        (None, None) => (DECIMAL128_MAX_PRECISION, DECIMAL_DEFAULT_SCALE),
    };

    // Arrow decimal is i128 meaning 38 maximum decimal digits
    if precision == 0 || precision > DECIMAL128_MAX_PRECISION || scale.unsigned_abs() > precision {
        bail!(
            "Decimal(precision = {precision}, scale = {scale}) should satisfy `0 < precision <= 38`, and `scale <= precision`."
        )
    } else {
        Ok(DataType::Decimal128(precision, scale))
    }
}

pub trait GetArrowType {
    fn arrow_type() -> DataType;
}

pub trait GetArrowSchema {
    fn arrow_schema() -> arrow::datatypes::Schema;
}

impl<T> GetArrowType for T
where
    T: GetArrowSchema,
{
    fn arrow_type() -> DataType {
        DataType::Struct(Self::arrow_schema().fields.clone())
    }
}

impl GetArrowType for bool {
    fn arrow_type() -> DataType {
        DataType::Boolean
    }
}

impl GetArrowType for i8 {
    fn arrow_type() -> DataType {
        DataType::Int8
    }
}

impl GetArrowType for i16 {
    fn arrow_type() -> DataType {
        DataType::Int16
    }
}

impl GetArrowType for i32 {
    fn arrow_type() -> DataType {
        DataType::Int32
    }
}

impl GetArrowType for i64 {
    fn arrow_type() -> DataType {
        DataType::Int64
    }
}

impl GetArrowType for u8 {
    fn arrow_type() -> DataType {
        DataType::UInt8
    }
}

impl GetArrowType for u16 {
    fn arrow_type() -> DataType {
        DataType::UInt16
    }
}

impl GetArrowType for u32 {
    fn arrow_type() -> DataType {
        DataType::UInt32
    }
}

impl GetArrowType for u64 {
    fn arrow_type() -> DataType {
        DataType::UInt64
    }
}

impl GetArrowType for f32 {
    fn arrow_type() -> DataType {
        DataType::Float32
    }
}

impl GetArrowType for f64 {
    fn arrow_type() -> DataType {
        DataType::Float64
    }
}

impl GetArrowType for String {
    fn arrow_type() -> DataType {
        DataType::Utf8
    }
}

impl GetArrowType for Vec<u8> {
    fn arrow_type() -> DataType {
        DataType::Binary
    }
}

impl GetArrowType for SystemTime {
    fn arrow_type() -> DataType {
        DataType::Timestamp(TimeUnit::Nanosecond, None)
    }
}
