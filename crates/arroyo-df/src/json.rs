use arrow_array::builder::{ListBuilder, StringBuilder};
use arrow_array::cast::as_string_array;
use arrow_array::{Array, ArrayRef, StringArray};
use arrow_schema::{DataType, Field};
use datafusion::common::Result;
use datafusion::common::{DataFusionError, ScalarValue};
use datafusion::logical_expr::{create_udf, ColumnarValue, ScalarUDF, Volatility};
use serde_json_path::JsonPath;
use std::collections::HashMap;
use std::sync::Arc;

pub fn get_json_functions() -> HashMap<String, Arc<ScalarUDF>> {
    let mut udfs = HashMap::new();

    udfs.insert(
        "get_first_json_object".to_string(),
        Arc::new(create_udf(
            "get_first_json_object",
            vec![DataType::Utf8, DataType::Utf8],
            Arc::new(DataType::Utf8),
            Volatility::Immutable,
            Arc::new(get_first_json_object),
        )),
    );

    udfs.insert(
        "extract_json".to_string(),
        Arc::new(create_udf(
            "extract_json",
            vec![DataType::Utf8, DataType::Utf8],
            Arc::new(DataType::List(Arc::new(Field::new(
                "item",
                DataType::Utf8,
                true,
            )))),
            Volatility::Immutable,
            Arc::new(extract_json),
        )),
    );

    udfs.insert(
        "extract_json_string".to_string(),
        Arc::new(create_udf(
            "extract_json_string",
            vec![DataType::Utf8, DataType::Utf8],
            Arc::new(DataType::Utf8),
            Volatility::Immutable,
            Arc::new(extract_json_string),
        )),
    );

    udfs
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
