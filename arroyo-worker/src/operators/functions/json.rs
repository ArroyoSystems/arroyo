use serde_json::Value;
use serde_json_path::JsonPath;

pub fn get_first_json_object(json_str: String, path: String) -> Option<String> {
    let value: Value = serde_json::from_str(&json_str).ok()?;
    let path = JsonPath::parse(&path).ok()?;
    path.query(&value).first().map(|v| v.to_string())
}

pub fn get_json_objects(json_str: String, path: String) -> Option<Vec<String>> {
    let value: Value = serde_json::from_str(&json_str).ok()?;
    let path = JsonPath::parse(&path).ok()?;
    Some(
        path.query(&value)
            .iter()
            .map(|v| v.to_string())
            .collect::<Vec<String>>(),
    )
}

pub fn extract_json_string(json_str: String, path: String) -> Option<String> {
    let value: Value = serde_json::from_str(&json_str).ok()?;
    let path = JsonPath::parse(&path).ok()?;
    let first_value = path.query(&value).first();
    match first_value {
        Some(Value::String(value)) => Some(value.to_string()),
        _ => None,
    }
}
