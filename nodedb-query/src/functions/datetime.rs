//! DateTime and duration scalar functions.

use crate::json_ops::{json_to_display_string, to_json_number};

pub(super) fn try_eval(name: &str, args: &[serde_json::Value]) -> Option<serde_json::Value> {
    let v = match name {
        "now" | "current_timestamp" => {
            let dt = nodedb_types::NdbDateTime::now();
            serde_json::Value::String(dt.to_iso8601())
        }
        "datetime" | "to_datetime" => args
            .first()
            .and_then(|v| match v {
                serde_json::Value::String(s) => nodedb_types::NdbDateTime::parse(s)
                    .map(|dt| serde_json::Value::String(dt.to_iso8601())),
                serde_json::Value::Number(n) => {
                    let micros = n.as_i64().unwrap_or(0);
                    Some(serde_json::Value::String(
                        nodedb_types::NdbDateTime::from_micros(micros).to_iso8601(),
                    ))
                }
                _ => None,
            })
            .unwrap_or(serde_json::Value::Null),
        "unix_secs" | "epoch_secs" => args
            .first()
            .and_then(|v| v.as_str())
            .and_then(nodedb_types::NdbDateTime::parse)
            .map_or(serde_json::Value::Null, |dt| {
                serde_json::Value::Number(dt.unix_secs().into())
            }),
        "unix_millis" | "epoch_millis" => args
            .first()
            .and_then(|v| v.as_str())
            .and_then(nodedb_types::NdbDateTime::parse)
            .map_or(serde_json::Value::Null, |dt| {
                serde_json::Value::Number(dt.unix_millis().into())
            }),
        "extract" | "date_part" => {
            let part = args.first().and_then(|v| v.as_str()).unwrap_or("");
            let dt = args
                .get(1)
                .and_then(|v| v.as_str())
                .and_then(nodedb_types::NdbDateTime::parse);
            match dt {
                Some(dt) => {
                    let c = dt.components();
                    let val: i64 = match part.to_lowercase().as_str() {
                        "year" | "y" => c.year as i64,
                        "month" | "mon" => c.month as i64,
                        "day" | "d" => c.day as i64,
                        "hour" | "h" => c.hour as i64,
                        "minute" | "min" | "m" => c.minute as i64,
                        "second" | "sec" | "s" => c.second as i64,
                        "microsecond" | "us" => c.microsecond as i64,
                        "epoch" => dt.unix_secs(),
                        "dow" | "dayofweek" => {
                            let days = dt.micros / 86_400_000_000;
                            (days + 4) % 7
                        }
                        _ => return Some(serde_json::Value::Null),
                    };
                    serde_json::Value::Number(val.into())
                }
                None => serde_json::Value::Null,
            }
        }
        "date_trunc" | "datetrunc" => {
            let part = args.first().and_then(|v| v.as_str()).unwrap_or("");
            let dt = args
                .get(1)
                .and_then(|v| v.as_str())
                .and_then(nodedb_types::NdbDateTime::parse);
            match dt {
                Some(dt) => {
                    let c = dt.components();
                    let truncated = match part.to_lowercase().as_str() {
                        "year" => nodedb_types::NdbDateTime::parse(&format!(
                            "{:04}-01-01T00:00:00Z",
                            c.year
                        )),
                        "month" => nodedb_types::NdbDateTime::parse(&format!(
                            "{:04}-{:02}-01T00:00:00Z",
                            c.year, c.month
                        )),
                        "day" => nodedb_types::NdbDateTime::parse(&format!(
                            "{:04}-{:02}-{:02}T00:00:00Z",
                            c.year, c.month, c.day
                        )),
                        "hour" => nodedb_types::NdbDateTime::parse(&format!(
                            "{:04}-{:02}-{:02}T{:02}:00:00Z",
                            c.year, c.month, c.day, c.hour
                        )),
                        "minute" => nodedb_types::NdbDateTime::parse(&format!(
                            "{:04}-{:02}-{:02}T{:02}:{:02}:00Z",
                            c.year, c.month, c.day, c.hour, c.minute
                        )),
                        "second" => nodedb_types::NdbDateTime::parse(&format!(
                            "{:04}-{:02}-{:02}T{:02}:{:02}:{:02}Z",
                            c.year, c.month, c.day, c.hour, c.minute, c.second
                        )),
                        _ => None,
                    };
                    truncated.map_or(serde_json::Value::Null, |t| {
                        serde_json::Value::String(t.to_iso8601())
                    })
                }
                None => serde_json::Value::Null,
            }
        }
        "date_add" | "datetime_add" => {
            let dt = args
                .first()
                .and_then(|v| v.as_str())
                .and_then(nodedb_types::NdbDateTime::parse);
            let dur = args
                .get(1)
                .and_then(|v| v.as_str())
                .and_then(nodedb_types::NdbDuration::parse);
            match (dt, dur) {
                (Some(dt), Some(dur)) => {
                    serde_json::Value::String(dt.add_duration(dur).to_iso8601())
                }
                _ => serde_json::Value::Null,
            }
        }
        "date_sub" | "datetime_sub" => {
            let dt = args
                .first()
                .and_then(|v| v.as_str())
                .and_then(nodedb_types::NdbDateTime::parse);
            let dur = args
                .get(1)
                .and_then(|v| v.as_str())
                .and_then(nodedb_types::NdbDuration::parse);
            match (dt, dur) {
                (Some(dt), Some(dur)) => {
                    serde_json::Value::String(dt.sub_duration(dur).to_iso8601())
                }
                _ => serde_json::Value::Null,
            }
        }
        "date_diff" | "datediff" => {
            let dt1 = args
                .first()
                .and_then(|v| v.as_str())
                .and_then(nodedb_types::NdbDateTime::parse);
            let dt2 = args
                .get(1)
                .and_then(|v| v.as_str())
                .and_then(nodedb_types::NdbDateTime::parse);
            match (dt1, dt2) {
                (Some(a), Some(b)) => to_json_number(a.duration_since(&b).as_secs_f64()),
                _ => serde_json::Value::Null,
            }
        }
        "duration" | "to_duration" => args
            .first()
            .and_then(|v| v.as_str())
            .and_then(nodedb_types::NdbDuration::parse)
            .map_or(serde_json::Value::Null, |d| {
                serde_json::Value::String(d.to_human())
            }),
        "decimal" | "to_decimal" => args.first().map_or(serde_json::Value::Null, |v| {
            let s = json_to_display_string(v);
            match s.parse::<rust_decimal::Decimal>() {
                Ok(d) => serde_json::Value::String(d.to_string()),
                Err(_) => serde_json::Value::Null,
            }
        }),
        _ => return None,
    };
    Some(v)
}
