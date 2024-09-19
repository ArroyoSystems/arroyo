use crate::ArroyoSchemaProvider;
use arrow_array::builder::{ListBuilder, StringBuilder};
use arrow_array::cast::{as_string_array, AsArray};
use arrow_array::types::{Float64Type, Int64Type};
use arrow_array::{Array, ArrayRef, StringArray, UnionArray};
use arrow_schema::{DataType, Field, UnionFields, UnionMode};
use datafusion::common::{DataFusionError, ScalarValue};
use datafusion::common::{Result, TableReference};
use datafusion::execution::FunctionRegistry;
use datafusion::logical_expr::expr::{Alias, ScalarFunction};
use datafusion::logical_expr::{create_udf, ColumnarValue, LogicalPlan, Projection, Volatility};
use datafusion::prelude::{col, Expr};
use serde_json_path::JsonPath;
use std::fmt::Write;
use std::sync::{Arc, OnceLock};

const SERIALIZE_JSON_UNION: &str = "serialize_json_union";

pub fn register_json_functions(registry: &mut dyn FunctionRegistry) {
    registry
        .register_udf(Arc::new(create_udf(
            "get_first_json_object",
            vec![DataType::Utf8, DataType::Utf8],
            Arc::new(DataType::Utf8),
            Volatility::Immutable,
            Arc::new(get_first_json_object),
        )))
        .unwrap();

    registry
        .register_udf(Arc::new(create_udf(
            "extract_json",
            vec![DataType::Utf8, DataType::Utf8],
            Arc::new(DataType::List(Arc::new(Field::new(
                "item",
                DataType::Utf8,
                true,
            )))),
            Volatility::Immutable,
            Arc::new(extract_json),
        )))
        .unwrap();

    registry
        .register_udf(Arc::new(create_udf(
            "extract_json_string",
            vec![DataType::Utf8, DataType::Utf8],
            Arc::new(DataType::Utf8),
            Volatility::Immutable,
            Arc::new(extract_json_string),
        )))
        .unwrap();

    registry
        .register_udf(Arc::new(create_udf(
            SERIALIZE_JSON_UNION,
            vec![DataType::Union(union_fields(), UnionMode::Sparse)],
            Arc::new(DataType::Utf8),
            Volatility::Immutable,
            Arc::new(serialize_json_union),
        )))
        .unwrap();
}

fn parse_path(name: &str, path: &ScalarValue) -> Result<Arc<JsonPath>> {
    let path = match path {
        ScalarValue::Utf8(Some(s)) => JsonPath::parse(s)
            .map_err(|e| DataFusionError::Execution(format!("Invalid json path '{s}': {:?}", e)))?,
        ScalarValue::Utf8(None) => {
            return Err(DataFusionError::Execution(format!(
                "The path argument to {name} cannot be null"
            )));
        }
        _ => {
            return Err(DataFusionError::Execution(format!(
                "The path argument to {name} must be of type TEXT"
            )));
        }
    };

    Ok(Arc::new(path))
}

fn json_function<T, ArrayT, F, ToS>(
    name: &str,
    f: F,
    to_scalar: ToS,
    args: &[ColumnarValue],
) -> Result<ColumnarValue>
where
    ArrayT: Array + FromIterator<Option<T>> + 'static,
    F: Fn(serde_json::Value, &JsonPath) -> Option<T>,
    ToS: Fn(Option<T>) -> ScalarValue,
{
    assert_eq!(args.len(), 2);
    Ok(match (&args[0], &args[1]) {
        (ColumnarValue::Array(values), ColumnarValue::Scalar(path)) => {
            let path = parse_path(name, path)?;
            let vs = as_string_array(values);
            ColumnarValue::Array(Arc::new(
                vs.iter()
                    .map(|s| s.and_then(|s| f(serde_json::from_str(s).ok()?, &path)))
                    .collect::<ArrayT>(),
            ) as ArrayRef)
        }
        (ColumnarValue::Scalar(value), ColumnarValue::Scalar(path)) => {
            let path = parse_path(name, path)?;
            let ScalarValue::Utf8(ref value) = value else {
                return Err(DataFusionError::Execution(format!(
                    "The value argument to {name} must be of type TEXT"
                )));
            };

            let result = value
                .as_ref()
                .and_then(|v| f(serde_json::from_str(v).ok()?, &path));
            ColumnarValue::Scalar(to_scalar(result))
        }
        _ => {
            return Err(DataFusionError::Execution(
                "The path argument to {name} must be a literal".to_string(),
            ))
        }
    })
}

pub fn extract_json(args: &[ColumnarValue]) -> Result<ColumnarValue> {
    assert_eq!(args.len(), 2);

    let inner = |s, path: &JsonPath| {
        Some(
            path.query(&serde_json::from_str(s).ok()?)
                .iter()
                .map(|v| Some(v.to_string()))
                .collect::<Vec<Option<String>>>(),
        )
    };

    Ok(match (&args[0], &args[1]) {
        (ColumnarValue::Array(values), ColumnarValue::Scalar(path)) => {
            let path = parse_path("extract_json", path)?;
            let values = as_string_array(values);

            let mut builder = ListBuilder::with_capacity(StringBuilder::new(), values.len());

            let queried = values.iter().map(|s| s.and_then(|s| inner(s, &path)));

            for v in queried {
                builder.append_option(v);
            }

            ColumnarValue::Array(Arc::new(builder.finish()))
        }
        (ColumnarValue::Scalar(value), ColumnarValue::Scalar(path)) => {
            let path = parse_path("extract_json", path)?;
            let ScalarValue::Utf8(ref v) = value else {
                return Err(DataFusionError::Execution(
                    "The value argument to extract_json must be of type TEXT".to_string(),
                ));
            };

            let mut builder = ListBuilder::with_capacity(StringBuilder::new(), 1);
            let result = v.as_ref().and_then(|s| inner(s, &path));
            builder.append_option(result);

            ColumnarValue::Scalar(ScalarValue::List(Arc::new(builder.finish())))
        }
        _ => {
            return Err(DataFusionError::Execution(
                "The path argument to extract_json must be a literal".to_string(),
            ))
        }
    })
}
pub fn get_first_json_object(args: &[ColumnarValue]) -> Result<ColumnarValue> {
    json_function::<String, StringArray, _, _>(
        "get_first_json_object",
        |s, path| path.query(&s).first().map(|v| v.to_string()),
        |s| s.as_deref().into(),
        args,
    )
}

pub fn extract_json_string(args: &[ColumnarValue]) -> Result<ColumnarValue> {
    json_function::<String, StringArray, _, _>(
        "extract_json_string",
        |s, path| {
            path.query(&s)
                .first()
                .and_then(|v| v.as_str().map(|s| s.to_string()))
        },
        |s| s.as_deref().into(),
        args,
    )
}

// This code is vendored from
// https://github.com/datafusion-contrib/datafusion-functions-json/blob/main/src/common_union.rs
// as the `is_json_union` function is not public. It should be kept in sync with that code so
// that we are able to detect JSON unions and rewrite them to serialized JSON for sinks.
pub(crate) fn is_json_union(data_type: &DataType) -> bool {
    match data_type {
        DataType::Union(fields, UnionMode::Sparse) => fields == &union_fields(),
        _ => false,
    }
}

pub(crate) const TYPE_ID_NULL: i8 = 0;
const TYPE_ID_BOOL: i8 = 1;
const TYPE_ID_INT: i8 = 2;
const TYPE_ID_FLOAT: i8 = 3;
const TYPE_ID_STR: i8 = 4;
const TYPE_ID_ARRAY: i8 = 5;
const TYPE_ID_OBJECT: i8 = 6;

fn union_fields() -> UnionFields {
    static FIELDS: OnceLock<UnionFields> = OnceLock::new();
    FIELDS
        .get_or_init(|| {
            UnionFields::from_iter([
                (
                    TYPE_ID_NULL,
                    Arc::new(Field::new("null", DataType::Null, true)),
                ),
                (
                    TYPE_ID_BOOL,
                    Arc::new(Field::new("bool", DataType::Boolean, false)),
                ),
                (
                    TYPE_ID_INT,
                    Arc::new(Field::new("int", DataType::Int64, false)),
                ),
                (
                    TYPE_ID_FLOAT,
                    Arc::new(Field::new("float", DataType::Float64, false)),
                ),
                (
                    TYPE_ID_STR,
                    Arc::new(Field::new("str", DataType::Utf8, false)),
                ),
                (
                    TYPE_ID_ARRAY,
                    Arc::new(Field::new("array", DataType::Utf8, false)),
                ),
                (
                    TYPE_ID_OBJECT,
                    Arc::new(Field::new("object", DataType::Utf8, false)),
                ),
            ])
        })
        .clone()
}
// End vendored code

pub fn serialize_json_union(args: &[ColumnarValue]) -> Result<ColumnarValue> {
    assert_eq!(args.len(), 1);
    let array = match args.first().unwrap() {
        ColumnarValue::Array(a) => a.clone(),
        ColumnarValue::Scalar(s) => s.to_array_of_size(1)?,
    };

    let mut b = StringBuilder::with_capacity(array.len(), array.get_array_memory_size());

    write_union(&mut b, &array)?;

    Ok(ColumnarValue::Array(Arc::new(b.finish())))
}

fn write_union(b: &mut StringBuilder, array: &ArrayRef) -> Result<(), std::fmt::Error> {
    assert!(
        is_json_union(array.data_type()),
        "array item is not a valid JSON union"
    );
    let json_union = array.as_any().downcast_ref::<UnionArray>().unwrap();

    for i in 0..json_union.len() {
        if json_union.is_null(i) {
            b.append_null();
        } else {
            write_value(b, json_union.type_id(i), &json_union.value(i))?;
            b.append_value("");
        }
    }

    Ok(())
}

fn write_value(b: &mut StringBuilder, id: i8, a: &ArrayRef) -> Result<(), std::fmt::Error> {
    match id {
        TYPE_ID_NULL => write!(b, "null")?,
        TYPE_ID_BOOL => write!(b, "{}", a.as_boolean().value(0))?,
        TYPE_ID_INT => write!(b, "{}", a.as_primitive::<Int64Type>().value(0))?,
        TYPE_ID_FLOAT => write!(b, "{}", a.as_primitive::<Float64Type>().value(0))?,
        TYPE_ID_STR => {
            // assumes that this is already a valid (escaped) json string as the only way to
            // construct these values are by parsing (valid) JSON
            b.write_char('"')?;
            b.write_str(a.as_string::<i32>().value(0))?;
            b.write_char('"')?;
        }
        TYPE_ID_ARRAY => {
            // write_array(b, a.as_list::<i32>())?;
            b.write_str(a.as_string::<i32>().value(0))?;
        }
        TYPE_ID_OBJECT => {
            b.write_str(a.as_string::<i32>().value(0))?;
        }
        _ => unreachable!("invalid union type in JSON union: {}", id),
    }

    Ok(())
}

pub(crate) fn serialize_outgoing_json(
    registry: &ArroyoSchemaProvider,
    node: Arc<LogicalPlan>,
) -> LogicalPlan {
    let exprs = node
        .schema()
        .fields()
        .iter()
        .map(|f| {
            if is_json_union(f.data_type()) {
                Expr::Alias(Alias::new(
                    Expr::ScalarFunction(ScalarFunction::new_udf(
                        registry.udf(SERIALIZE_JSON_UNION).unwrap(),
                        vec![col(f.name())],
                    )),
                    Option::<TableReference>::None,
                    f.name(),
                ))
            } else {
                col(f.name())
            }
        })
        .collect();

    LogicalPlan::Projection(Projection::try_new(exprs, node).unwrap())
}

#[cfg(test)]
mod test {
    use arrow_array::builder::{ListBuilder, StringBuilder};
    use arrow_array::StringArray;
    use datafusion::common::ScalarValue;
    use std::sync::Arc;

    #[test]
    fn test_extract_json() {
        let input = Arc::new(StringArray::from(vec![
            r#"{"a": 1, "b": 2, "c": { "d": "hello" }}"#,
            r#"{"a": 3, "b": 4}"#,
            r#"{"a": 5, "b": 6}"#,
        ]));

        let path = "$.c.d";

        let result = super::extract_json(&[
            super::ColumnarValue::Array(input),
            super::ColumnarValue::Scalar(path.into()),
        ])
        .unwrap();

        let mut expected = ListBuilder::new(StringBuilder::new());
        expected.append_value(vec![Some("\"hello\"".to_string())]);
        expected.append_value(Vec::<Option<String>>::new());
        expected.append_value(Vec::<Option<String>>::new());
        if let super::ColumnarValue::Array(result) = result {
            assert_eq!(*result, expected.finish());
        } else {
            panic!("Expected array, got scalar");
        }

        let result = super::extract_json(&[
            super::ColumnarValue::Scalar(r#"{"a": 1, "b": 2, "c": { "d": "hello" }}"#.into()),
            super::ColumnarValue::Scalar(path.into()),
        ])
        .unwrap();

        let mut expected = ListBuilder::with_capacity(StringBuilder::new(), 1);
        expected.append_value(vec![Some("\"hello\"".to_string())]);

        if let super::ColumnarValue::Scalar(ScalarValue::List(result)) = result {
            assert_eq!(*result, expected.finish());
        } else {
            panic!("Expected scalar list");
        }
    }

    #[test]
    fn test_get_first_json_object() {
        let input = Arc::new(StringArray::from(vec![
            r#"{"a": 1, "b": 2}"#,
            r#"{"a": 3}"#,
            r#"{"a": 5, "b": 6}"#,
        ]));

        let path = "$.b";

        let result = super::get_first_json_object(&[
            super::ColumnarValue::Array(input),
            super::ColumnarValue::Scalar(path.into()),
        ])
        .unwrap();

        let expected = StringArray::from(vec![Some("2"), None, Some("6")]);

        if let super::ColumnarValue::Array(result) = result {
            assert_eq!(*result, expected);
        } else {
            panic!("Expected array, got scalar");
        }

        let result = super::get_first_json_object(&[
            super::ColumnarValue::Scalar(r#"{"a": 1, "b": 2, "c": { "d": "hello" }}"#.into()),
            super::ColumnarValue::Scalar("$.c.d".into()),
        ])
        .unwrap();

        let expected = ScalarValue::Utf8(Some("\"hello\"".to_string()));

        if let super::ColumnarValue::Scalar(result) = result {
            assert_eq!(result, expected);
        } else {
            panic!("Expected scalar");
        }
    }

    #[test]
    fn test_extract_json_string() {
        let input = Arc::new(StringArray::from(vec![
            r#"{"a": 1, "b": 2, "c": { "d": "hello" }}"#,
            r#"{"a": 3, "b": 4}"#,
            r#"{"a": 5, "b": 6}"#,
        ]));

        let path = "$.c.d";

        let result = super::extract_json_string(&[
            super::ColumnarValue::Array(input),
            super::ColumnarValue::Scalar(path.into()),
        ])
        .unwrap();

        let expected = StringArray::from(vec![Some("hello"), None, None]);

        if let super::ColumnarValue::Array(result) = result {
            assert_eq!(*result, expected);
        } else {
            panic!("Expected array, got scalar");
        }

        let result = super::extract_json_string(&[
            super::ColumnarValue::Scalar(r#"{"a": 1, "b": 2, "c": { "d": "hello" }}"#.into()),
            super::ColumnarValue::Scalar(path.into()),
        ])
        .unwrap();

        let expected = ScalarValue::Utf8(Some("hello".to_string()));

        if let super::ColumnarValue::Scalar(result) = result {
            assert_eq!(result, expected);
        } else {
            panic!("Expected scalar");
        }
    }
}
