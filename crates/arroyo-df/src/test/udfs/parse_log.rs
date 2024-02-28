/*
[dependencies]
access_log_parser = "0.8"
serde_json = "1"
*/

use access_log_parser::{parse, AccessLogError, LogType, LogEntry};
use serde_json::json;
use std::time::{UNIX_EPOCH, Duration};

pub fn parse_log(input: String) -> Option<String> {
    let LogEntry::CommonLog(entry) =
        parse(LogType::CommonLog, &input).ok()? else {
        return None
    };

    Some(json!({
        "ip": entry.ip,
        "user": entry.user,
        "timestamp": entry.timestamp.to_rfc3339(),
        "request": format!("{:?}", entry.request),
        "status_code": entry.status_code.as_u16()
    }).to_string())
}
