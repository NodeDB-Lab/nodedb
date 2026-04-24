//! MessagePack + JSON ingest formats for timeseries.
//!
//! Both paths normalize their payload into ILP text and delegate to
//! `execute_ilp_ingest`. Kept separate from `ingest.rs` to keep each
//! file under the 500-line limit.

use sonic_rs::{JsonContainerTrait, JsonValueTrait};

use super::msgpack_decode::{self, MsgpackValue};
use crate::bridge::envelope::{ErrorCode, Response};
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::task::ExecutionTask;

impl CoreLoop {
    /// Payload is a msgpack array of maps (same schema as JSON ingest but in msgpack).
    /// Converts each row to an ILP line and delegates to the ILP ingest path.
    pub(super) fn execute_msgpack_ingest(
        &mut self,
        task: &ExecutionTask,
        tid: crate::types::TenantId,
        collection: &str,
        payload: &[u8],
        wal_lsn: Option<u64>,
        now_ms: i64,
    ) -> Response {
        let measurement = collection
            .split_once(':')
            .map(|(_, name)| name)
            .unwrap_or(collection);

        if !measurement
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '-')
        {
            return self.response_error(
                task,
                ErrorCode::Internal {
                    detail: format!(
                        "invalid measurement name '{measurement}': only [a-zA-Z0-9_-] allowed"
                    ),
                },
            );
        }

        let rows = match msgpack_decode::decode_msgpack_rows(payload) {
            Ok(r) => r,
            Err(e) => {
                return self.response_error(
                    task,
                    ErrorCode::Internal {
                        detail: format!("msgpack decode error: {e}"),
                    },
                );
            }
        };

        if rows.is_empty() {
            return self.response_error(
                task,
                ErrorCode::Internal {
                    detail: "empty msgpack rows array".into(),
                },
            );
        }

        let mut ilp_buf = String::new();
        for row in &rows {
            let mut fields = Vec::new();
            let mut timestamp_ns: Option<i64> = None;

            for (key, val) in row {
                let lower = key.to_lowercase();
                if lower == "ts" || lower == "timestamp" || lower == "time" {
                    match val {
                        MsgpackValue::Str(s) => {
                            timestamp_ns = parse_ts_string_to_nanos(s);
                        }
                        MsgpackValue::Int(n) => {
                            timestamp_ns = Some(*n * 1_000_000);
                        }
                        MsgpackValue::Float(f) => {
                            timestamp_ns = Some(*f as i64 * 1_000_000);
                        }
                        _ => {}
                    }
                    continue;
                }

                match val {
                    MsgpackValue::Float(f) => fields.push(format!("{key}={f}")),
                    MsgpackValue::Int(n) => fields.push(format!("{key}={n}i")),
                    MsgpackValue::Str(s) => {
                        fields.push(format!("{key}=\"{}\"", s.replace('\"', "\\\"")));
                    }
                    MsgpackValue::Bool(b) => fields.push(format!("{key}={b}")),
                    _ => {}
                }
            }

            if fields.is_empty() {
                continue;
            }

            ilp_buf.push_str(measurement);
            ilp_buf.push(' ');
            ilp_buf.push_str(&fields.join(","));
            if let Some(ts) = timestamp_ns {
                ilp_buf.push(' ');
                ilp_buf.push_str(&ts.to_string());
            }
            ilp_buf.push('\n');
        }

        if ilp_buf.is_empty() {
            return self.response_error(
                task,
                ErrorCode::Internal {
                    detail: "no valid rows in msgpack payload".into(),
                },
            );
        }

        self.execute_ilp_ingest(task, tid, collection, ilp_buf.as_bytes(), wal_lsn, now_ms)
    }

    /// Payload is a JSON array like: `[{"id":"e1","ts":"2024-01-01T00:00:00Z","value":42.0}]`.
    /// Converts each row to an ILP line and delegates to the ILP ingest path.
    pub(super) fn execute_json_ingest(
        &mut self,
        task: &ExecutionTask,
        tid: crate::types::TenantId,
        collection: &str,
        payload: &[u8],
        wal_lsn: Option<u64>,
        now_ms: i64,
    ) -> Response {
        let rows: sonic_rs::Array = match sonic_rs::from_slice(payload) {
            Ok(r) => r,
            Err(e) => {
                return self.response_error(
                    task,
                    ErrorCode::Internal {
                        detail: format!("JSON parse error: {e}"),
                    },
                );
            }
        };

        if rows.is_empty() {
            return self.response_error(
                task,
                ErrorCode::Internal {
                    detail: "empty JSON rows array".into(),
                },
            );
        }

        let measurement = collection
            .split_once(':')
            .map(|(_, name)| name)
            .unwrap_or(collection);

        if !measurement
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '-')
        {
            return self.response_error(
                task,
                ErrorCode::Internal {
                    detail: format!(
                        "invalid measurement name '{measurement}': only [a-zA-Z0-9_-] allowed"
                    ),
                },
            );
        }

        let mut ilp_buf = String::new();
        for row_val in rows.iter() {
            let obj = match row_val.as_object() {
                Some(o) => o,
                None => continue,
            };

            let mut fields = Vec::new();
            let mut timestamp_ns: Option<i64> = None;

            for (key, val) in obj.iter() {
                let lower = key.to_lowercase();
                if lower == "ts" || lower == "timestamp" || lower == "time" {
                    if let Some(s) = val.as_str() {
                        timestamp_ns = parse_ts_string_to_nanos(s);
                    } else if let Some(n) = val.as_i64() {
                        timestamp_ns = Some(n * 1_000_000);
                    } else if let Some(f) = val.as_f64() {
                        timestamp_ns = Some(f as i64 * 1_000_000);
                    }
                    continue;
                }

                if let Some(f) = val.as_f64() {
                    fields.push(format!("{key}={f}"));
                } else if let Some(n) = val.as_i64() {
                    fields.push(format!("{key}={n}i"));
                } else if let Some(s) = val.as_str() {
                    fields.push(format!("{key}=\"{}\"", s.replace('\"', "\\\"")));
                } else if let Some(b) = val.as_bool() {
                    fields.push(format!("{key}={b}"));
                }
            }

            if fields.is_empty() {
                continue;
            }

            ilp_buf.push_str(measurement);
            ilp_buf.push(' ');
            ilp_buf.push_str(&fields.join(","));
            if let Some(ts) = timestamp_ns {
                ilp_buf.push(' ');
                ilp_buf.push_str(&ts.to_string());
            }
            ilp_buf.push('\n');
        }

        if ilp_buf.is_empty() {
            return self.response_error(
                task,
                ErrorCode::Internal {
                    detail: "no valid rows in JSON payload".into(),
                },
            );
        }

        self.execute_ilp_ingest(task, tid, collection, ilp_buf.as_bytes(), wal_lsn, now_ms)
    }
}

/// Parse a datetime string to nanoseconds since Unix epoch.
///
/// Accepts RFC3339 / ISO8601 with timezone (e.g., "2024-01-01T00:00:00Z"),
/// and common datetime formats without timezone (treated as UTC).
/// Returns nanoseconds since Unix epoch, or `None` if the string cannot be parsed.
fn parse_ts_string_to_nanos(s: &str) -> Option<i64> {
    use chrono::{DateTime, NaiveDateTime, TimeZone, Utc};

    if let Ok(dt) = DateTime::parse_from_rfc3339(s) {
        return dt.timestamp_nanos_opt();
    }

    let formats = [
        "%Y-%m-%d %H:%M:%S%.f",
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%dT%H:%M:%S%.f",
        "%Y-%m-%dT%H:%M:%S",
    ];
    for fmt in &formats {
        if let Ok(ndt) = NaiveDateTime::parse_from_str(s, fmt) {
            return Utc.from_utc_datetime(&ndt).timestamp_nanos_opt();
        }
    }

    None
}
