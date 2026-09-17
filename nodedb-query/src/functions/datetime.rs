// SPDX-License-Identifier: Apache-2.0

//! DateTime and duration scalar functions.
//!
//! Instant-typed arguments (`Value::DateTime`, `Value::NaiveDateTime`, or a
//! parseable ISO 8601 string) are accepted through [`instant_arg`]. A
//! function that returns an instant preserves the kind of its input:
//! `Utc` in, `DateTime` out; `Naive` in, `NaiveDateTime` out.

use crate::value_ops::{to_value_number, value_to_display_string};
use nodedb_types::{InstantKind, NdbDateTime, Value};

/// Resolve an argument to a typed instant.
///
/// `Value::DateTime`/`Value::NaiveDateTime` pass through their own kind. A
/// `Value::String` that parses via [`NdbDateTime::parse`] resolves to
/// `InstantKind::Utc` when it ends in `Z`/`z` (the only zone marker
/// `NdbDateTime::parse` recognizes) and `InstantKind::Naive` otherwise.
/// Anything else, or a string that fails to parse, is `None`.
fn instant_arg(v: &Value) -> Option<(InstantKind, NdbDateTime)> {
    if let Some(pair) = v.as_instant() {
        return Some(pair);
    }
    let s = v.as_str()?;
    let dt = NdbDateTime::parse(s)?;
    let trimmed = s.trim();
    let kind = if trimmed.ends_with('Z') || trimmed.ends_with('z') {
        InstantKind::Utc
    } else {
        InstantKind::Naive
    };
    Some((kind, dt))
}

pub(super) fn try_eval(name: &str, args: &[Value]) -> Option<Value> {
    let v = match name {
        "now" | "current_timestamp" => {
            let dt = NdbDateTime::now();
            Value::DateTime(dt)
        }
        "datetime" | "to_datetime" => args.first().map_or(Value::Null, |v| match v {
            Value::Integer(micros) => InstantKind::Utc.from_micros(*micros),
            Value::Float(f) => InstantKind::Utc.from_micros(*f as i64),
            _ => instant_arg(v).map_or(Value::Null, |(kind, dt)| kind.value(dt)),
        }),
        "unix_secs" | "epoch_secs" => args
            .first()
            .and_then(instant_arg)
            .map_or(Value::Null, |(_, dt)| Value::Integer(dt.unix_secs())),
        "unix_millis" | "epoch_millis" => args
            .first()
            .and_then(instant_arg)
            .map_or(Value::Null, |(_, dt)| Value::Integer(dt.unix_millis())),
        "extract" | "date_part" => {
            let part = args.first().and_then(|v| v.as_str()).unwrap_or("");
            let dt = args.get(1).and_then(instant_arg);
            match dt {
                Some((_, dt)) => {
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
                        _ => return Some(Value::Null),
                    };
                    Value::Integer(val)
                }
                None => Value::Null,
            }
        }
        "date_trunc" | "datetrunc" => {
            let part = args.first().and_then(|v| v.as_str()).unwrap_or("");
            let dt = args.get(1).and_then(instant_arg);
            match dt {
                Some((kind, dt)) => {
                    let c = dt.components();
                    let truncated = match part.to_lowercase().as_str() {
                        "year" => NdbDateTime::parse(&format!("{:04}-01-01T00:00:00Z", c.year)),
                        "month" => NdbDateTime::parse(&format!(
                            "{:04}-{:02}-01T00:00:00Z",
                            c.year, c.month
                        )),
                        "day" => NdbDateTime::parse(&format!(
                            "{:04}-{:02}-{:02}T00:00:00Z",
                            c.year, c.month, c.day
                        )),
                        "hour" => NdbDateTime::parse(&format!(
                            "{:04}-{:02}-{:02}T{:02}:00:00Z",
                            c.year, c.month, c.day, c.hour
                        )),
                        "minute" => NdbDateTime::parse(&format!(
                            "{:04}-{:02}-{:02}T{:02}:{:02}:00Z",
                            c.year, c.month, c.day, c.hour, c.minute
                        )),
                        "second" => NdbDateTime::parse(&format!(
                            "{:04}-{:02}-{:02}T{:02}:{:02}:{:02}Z",
                            c.year, c.month, c.day, c.hour, c.minute, c.second
                        )),
                        _ => None,
                    };
                    truncated.map_or(Value::Null, |t| kind.value(t))
                }
                None => Value::Null,
            }
        }
        "date_add" | "datetime_add" => {
            let dt = args.first().and_then(instant_arg);
            let dur = args
                .get(1)
                .and_then(|v| v.as_str())
                .and_then(nodedb_types::NdbDuration::parse);
            match (dt, dur) {
                (Some((kind, dt)), Some(dur)) => dt
                    .add_duration(dur)
                    .map(|r| kind.value(r))
                    .unwrap_or(Value::Null),
                _ => Value::Null,
            }
        }
        "date_sub" | "datetime_sub" => {
            let dt = args.first().and_then(instant_arg);
            let dur = args
                .get(1)
                .and_then(|v| v.as_str())
                .and_then(nodedb_types::NdbDuration::parse);
            match (dt, dur) {
                (Some((kind, dt)), Some(dur)) => dt
                    .sub_duration(dur)
                    .map(|r| kind.value(r))
                    .unwrap_or(Value::Null),
                _ => Value::Null,
            }
        }
        "date_diff" | "datediff" => {
            let dt1 = args.first().and_then(instant_arg).map(|(_, dt)| dt);
            let dt2 = args.get(1).and_then(instant_arg).map(|(_, dt)| dt);
            match (dt1, dt2) {
                (Some(a), Some(b)) => a
                    .duration_since(&b)
                    .map(|d| to_value_number(d.as_secs_f64()))
                    .unwrap_or(Value::Null),
                _ => Value::Null,
            }
        }
        "duration" | "to_duration" => args
            .first()
            .and_then(|v| v.as_str())
            .and_then(nodedb_types::NdbDuration::parse)
            .map_or(Value::Null, |d| Value::String(d.to_human())),
        "decimal" | "to_decimal" => args.first().map_or(Value::Null, |v| {
            let s = value_to_display_string(v);
            match s.parse::<rust_decimal::Decimal>() {
                Ok(d) => Value::String(d.to_string()),
                Err(_) => Value::Null,
            }
        }),
        "time_bucket" => eval_time_bucket(args),
        _ => return None,
    };
    Some(v)
}

/// `time_bucket(interval, timestamp)` — truncate a timestamp to the start
/// of the given interval bucket.
///
/// Accepts two argument orders (both common in SQL):
/// - `time_bucket('1 hour', timestamp_col)` — interval first
/// - `time_bucket(timestamp_col, '1 hour')` — timestamp first
///
/// The interval is a string like `'1h'`, `'5m'`, `'1 hour'`, `'30 seconds'`,
/// or an integer number of seconds.
///
/// The timestamp is either:
/// - a typed instant (`Value::DateTime`, `Value::NaiveDateTime`, or a
///   parseable instant string) — bucketed on epoch microseconds, floored
///   toward negative infinity, and returned as the same instant kind
/// - an `Value::Integer`/`Value::Float` epoch-millisecond value — bucketed
///   on epoch milliseconds and returned as `Value::Integer` milliseconds,
///   truncated toward zero
fn eval_time_bucket(args: &[Value]) -> Value {
    if args.len() < 2 {
        return Value::Null;
    }

    let instant_and_interval = instant_arg(&args[0])
        .map(|pair| (pair, &args[1]))
        .or_else(|| instant_arg(&args[1]).map(|pair| (pair, &args[0])));

    if let Some(((kind, dt), interval)) = instant_and_interval {
        let interval_us = interval_arg_ms(interval).and_then(|ms| ms.checked_mul(1000));
        return match interval_us {
            Some(i) if i > 0 => {
                let bucket = dt.micros.div_euclid(i) * i;
                kind.value(NdbDateTime::from_micros(bucket))
            }
            _ => Value::Null,
        };
    }

    // Detect which arg is the interval and which is the timestamp.
    let (interval_ms, timestamp_ms) = match (&args[0], &args[1]) {
        // time_bucket('1 hour', timestamp)
        (Value::String(_), ts_val) => (interval_arg_ms(&args[0]), value_to_timestamp_ms(ts_val)),
        // time_bucket(timestamp, '1 hour')
        (ts_val, Value::String(_)) => (interval_arg_ms(&args[1]), value_to_timestamp_ms(ts_val)),
        // time_bucket(3600, timestamp) — interval as integer seconds
        (Value::Integer(_), ts_val) => (interval_arg_ms(&args[0]), value_to_timestamp_ms(ts_val)),
        _ => return Value::Null,
    };

    match (interval_ms, timestamp_ms) {
        (Some(i), Some(ts)) if i > 0 => Value::Integer((ts / i) * i),
        _ => Value::Null,
    }
}

/// The bucket interval in milliseconds: an interval string such as
/// `'1 hour'`, or an integer count of seconds.
fn interval_arg_ms(v: &Value) -> Option<i64> {
    match v {
        Value::String(s) => parse_interval_to_ms(s),
        Value::Integer(secs) => secs.checked_mul(1000),
        _ => None,
    }
}

fn value_to_timestamp_ms(v: &Value) -> Option<i64> {
    match v {
        Value::Integer(n) => Some(*n),
        Value::Float(f) => Some(*f as i64),
        _ => None,
    }
}

/// Parse an interval string like "1h", "5m", "1 hour", "30 seconds" to ms.
///
/// Delegates to the canonical `nodedb_types::kv_parsing::parse_interval_to_ms`.
fn parse_interval_to_ms(s: &str) -> Option<i64> {
    nodedb_types::kv_parsing::parse_interval_to_ms(s)
        .ok()
        .map(|ms| ms as i64)
        .filter(|&ms| ms > 0)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn naive(s: &str) -> Value {
        Value::NaiveDateTime(NdbDateTime::parse(s).expect("valid test timestamp"))
    }

    fn utc(s: &str) -> Value {
        Value::DateTime(NdbDateTime::parse(s).expect("valid test timestamp"))
    }

    #[test]
    fn date_part_year_accepts_naive_and_utc() {
        let naive_year = try_eval(
            "date_part",
            &[Value::String("year".into()), naive("2020-03-05T10:00:00")],
        );
        assert_eq!(naive_year, Some(Value::Integer(2020)));

        let utc_year = try_eval(
            "date_part",
            &[Value::String("year".into()), utc("2020-03-05T10:00:00Z")],
        );
        assert_eq!(utc_year, Some(Value::Integer(2020)));
    }

    #[test]
    fn date_trunc_preserves_utc_kind() {
        let truncated = try_eval(
            "date_trunc",
            &[Value::String("hour".into()), utc("2020-03-05T10:42:17Z")],
        );
        assert_eq!(
            truncated,
            Some(Value::DateTime(
                NdbDateTime::parse("2020-03-05T10:00:00Z").expect("valid test timestamp")
            ))
        );
    }

    #[test]
    fn time_bucket_naive_instant_floors_to_hour() {
        let bucketed = try_eval(
            "time_bucket",
            &[Value::String("1 hour".into()), naive("2020-03-05T10:00:00")],
        );
        assert_eq!(bucketed, Some(naive("2020-03-05T10:00:00")));

        let bucketed = try_eval(
            "time_bucket",
            &[Value::String("1 hour".into()), naive("2020-03-05T10:59:59")],
        );
        assert_eq!(bucketed, Some(naive("2020-03-05T10:00:00")));
    }

    #[test]
    fn time_bucket_negative_micros_floors_toward_negative_infinity() {
        let before_epoch = Value::NaiveDateTime(NdbDateTime::from_micros(-1_800_000_000));
        let bucketed = try_eval(
            "time_bucket",
            &[Value::String("1 hour".into()), before_epoch],
        );
        assert_eq!(
            bucketed,
            Some(Value::NaiveDateTime(NdbDateTime::from_micros(
                -3_600_000_000
            )))
        );
    }

    #[test]
    fn time_bucket_integer_ms_unchanged() {
        let bucketed = try_eval(
            "time_bucket",
            &[Value::String("1 hour".into()), Value::Integer(3_661_000)],
        );
        assert_eq!(bucketed, Some(Value::Integer(3_600_000)));
    }

    #[test]
    fn datetime_of_naive_instant_keeps_kind() {
        let result = try_eval("datetime", &[naive("2020-03-05T10:00:00")]);
        assert_eq!(result, Some(naive("2020-03-05T10:00:00")));
    }

    #[test]
    fn extract_on_integer_returns_null() {
        let result = try_eval(
            "extract",
            &[Value::String("year".into()), Value::Integer(1_583_400_000)],
        );
        assert_eq!(result, Some(Value::Null));
    }

    #[test]
    fn time_bucket_integer_seconds_interval_accepts_an_instant() {
        let bucketed = try_eval(
            "time_bucket",
            &[Value::Integer(3600), naive("2020-03-05T10:59:59")],
        );
        assert_eq!(bucketed, Some(naive("2020-03-05T10:00:00")));
    }

    #[test]
    fn date_diff_accepts_typed_instants() {
        let diff = try_eval(
            "date_diff",
            &[naive("2020-03-05T10:00:00"), naive("2020-03-05T09:00:00")],
        );
        assert_eq!(diff, Some(Value::Integer(3600)));
    }
}
