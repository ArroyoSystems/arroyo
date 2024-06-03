/*
[dependencies]
regex = "1.10.2"
serde_json = "1.0"
*/

#[udf]
fn parse_prom(s: String) -> Option<String> {
    use regex::Regex;
    use std::collections::HashMap;

    // use OnceLock to prevent re-compilation of the regexes on every request
    use std::sync::OnceLock;
    static METRIC_REGEX: OnceLock<Regex> = OnceLock::new();
    let metric_regex = METRIC_REGEX.get_or_init(|| {
        Regex::new(r"(?P<metric_name>\w+)\{(?P<labels>[^}]+)\}\s+(?P<metric_value>[\d.]+)").unwrap()
    });

    static LABEL_REGEX: OnceLock<Regex> = OnceLock::new();
    let label_regex = LABEL_REGEX.get_or_init(|| {
        regex::Regex::new(r##"(?P<label>[^,]+)="(?P<value>[^"]+)""##).unwrap()
    });

    // pull out the metric name, labels, and value
    let captures = metric_regex.captures(&s)?;

    let name = captures.name("metric_name").unwrap().as_str();
    let labels = captures.name("labels").unwrap().as_str();
    let value = captures.name("metric_value").unwrap().as_str();

    // parse out the label key-value pairs
    let labels: std::collections::HashMap<String, String> = label_regex.captures_iter(&labels)
        .map(|capture| (
            capture.name("label").unwrap().as_str().to_string(),
            capture.name("value").unwrap().as_str().to_string()
        ))
        .collect();

    // return the parsed result as JSON
    Some(serde_json::json!({
        "name": name,
        "labels": labels,
        "value": value
    }).to_string())
}
