use crate::config::Transform;
use arrow::array::ArrayRef;
use arrow::compute::cast;
use arrow::datatypes::{DataType, TimeUnit};
use arrow::error::ArrowError;
use datafusion::common::{ScalarValue, exec_datafusion_err, exec_err};
use datafusion::error::Result as DFResult;
use datafusion::execution::FunctionRegistry;
use datafusion::functions::{export_functions, make_udf_function};
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, TypeSignature, Volatility,
};
use iceberg::transform::create_transform_function;

/// The arrow type of the Iceberg type that a column of `data_type` has, where they differ:
/// Iceberg has no 8 or 16-bit integers, and stores unsigned 32-bit integers as longs (see
/// `iceberg::arrow::arrow_type_to_type`)
fn iceberg_arrow_type(data_type: &DataType) -> DataType {
    match data_type {
        DataType::Int8 | DataType::Int16 | DataType::UInt8 | DataType::UInt16 => DataType::Int32,
        DataType::UInt32 => DataType::Int64,
        other => other.clone(),
    }
}

/// Applies an Iceberg partition transform to an Arrow array.
///
/// Uses iceberg-rust's transforms, which also compute the partition values written to the
/// manifest, after casting the array to its Iceberg type. Identity returns the array unchanged.
pub fn transform_arrow(array: ArrayRef, transform: Transform) -> Result<ArrayRef, ArrowError> {
    if matches!(transform, Transform::Identity) {
        return Ok(array);
    }

    let array = cast(&array, &iceberg_arrow_type(array.data_type()))?;
    create_transform_function(&transform.into())
        .and_then(|f| f.transform(array))
        .map_err(|e| ArrowError::ComputeError(e.to_string()))
}

macro_rules! make_transform_udf {
    (
        $type_name:ident,
        $udf_name:ident,
        $ret_ty:expr,
        $signature:expr,
        $mk_transform:expr,
    ) => {
        #[derive(Debug, PartialEq, Eq, Hash)]
        struct $type_name(Signature);

        impl ScalarUDFImpl for $type_name {
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
                // iceberg-rust's result type can differ from the declared one (day returns Date32)
                transform_arrow(arr, transform?)
                    .and_then(|result| cast(&result, args.return_type()))
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
    match iceberg_arrow_type(lhs) {
        t @ (DataType::Int32
        | DataType::Int64
        | DataType::Decimal128(_, _)
        | DataType::Utf8
        | DataType::LargeUtf8
        | DataType::Binary) => Ok(t),
        _ => Err(exec_datafusion_err!(
            "ice_truncate expects an integer, decimal, string or binary first arg, got {lhs:?}"
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
    // two args: (value, Int32 m). Any value type, so that planning does not coerce it, which the
    // sink's compiled expression would not; value types validated by truncate_return_type.
    Signature::new(TypeSignature::Any(2), Volatility::Immutable),
    |args: &ScalarFunctionArgs| {
        if args.args.len() != 2 {
            return exec_err!("ice_truncate requires 2 args (value, m)");
        }
        let arg0 = scalar_to_u32(&args.args[1], "ice_truncate", 2)?;
        Ok(Transform::Truncate { arg0 })
    },
);

make_transform_udf!(
    IceBucket,
    ice_bucket,
    bucket_return_type,
    // two args: (value, Int32 m). value types validated by iceberg-rust's transform.
    Signature::new(TypeSignature::Any(2), Volatility::Immutable),
    |args: &ScalarFunctionArgs| {
        if args.args.len() != 2 {
            return exec_err!("ice_bucket requires 2 args (value, m)");
        }
        let arg0 = scalar_to_u32(&args.args[1], "ice_bucket", 2)?;
        Ok(Transform::Bucket { arg0 })
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
        (ice_identity, "computes the iceberg identity transform", value),
        (ice_hour, "computes the iceberg hour transform", timestamp),
        (ice_day, "computes the iceberg day transform", timestamp),
        (ice_month, "computes the iceberg month transform", timestamp),
        (ice_year, "computes the iceberg year transform", timestamp),
        (ice_truncate, "computes the iceberg truncate transform", num len),
        (ice_bucket, "computes the iceberg bucket transform", value count)
    );
}
