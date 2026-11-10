// SPDX-License-Identifier: BUSL-1.1

//! Retention and legal hold enforcement: reject DELETE operations that
//! violate data retention policies or active legal holds.

use crate::bridge::envelope::ErrorCode;
use nodedb_physical::physical_plan::EnforcementOptions;

/// Check whether a DELETE is allowed given retention and legal hold policies.
///
/// Legal hold takes absolute precedence — if any hold is active, DELETE is rejected
/// regardless of retention period.
///
/// For retention, the row's creation timestamp (`created_at` field) is compared
/// against the current time. If the row is younger than the retention period,
/// DELETE is rejected.
pub fn check_delete_allowed(
    collection: &str,
    opts: &EnforcementOptions,
    row_created_at_secs: Option<u64>,
) -> Result<(), ErrorCode> {
    // Legal hold: absolute block on DELETE.
    if opts.has_legal_hold {
        return Err(ErrorCode::LegalHoldActive {
            collection: collection.to_string(),
        });
    }

    // Retention period: reject if row is younger than the retention duration.
    if let Some(ref retention) = opts.retention {
        if let Some(created_at) = row_created_at_secs {
            if !retention_expired(created_at, retention) {
                return Err(ErrorCode::RetentionViolation {
                    collection: collection.to_string(),
                });
            }
        } else {
            // No creation timestamp available — reject defensively.
            // Rows without a known creation time cannot prove they've
            // exceeded the retention period.
            return Err(ErrorCode::RetentionViolation {
                collection: collection.to_string(),
            });
        }
    }

    Ok(())
}

pub use nodedb_physical::physical_plan::document::{RetentionDuration, RetentionUnit};

/// Parse a human-readable retention period string.
///
/// Supports: `"7 years"`, `"90 days"`, `"6 months"`, `"1 year"`,
/// `"365d"`, `"52w"`, `"8760h"`.
pub fn parse_retention_period(s: &str) -> crate::Result<RetentionDuration> {
    let s = s.trim().to_lowercase();
    let (num_str, unit_str) = split_number_unit(&s);
    let count: u32 = num_str.parse().map_err(|_| crate::Error::BadRequest {
        detail: format!("invalid retention period number: '{num_str}'"),
    })?;

    let unit = match unit_str {
        "s" | "sec" | "secs" | "second" | "seconds" => RetentionUnit::Seconds,
        "m" | "min" | "mins" | "minute" | "minutes" => RetentionUnit::Minutes,
        "h" | "hr" | "hrs" | "hour" | "hours" => RetentionUnit::Hours,
        "d" | "day" | "days" => RetentionUnit::Days,
        "w" | "wk" | "wks" | "week" | "weeks" => RetentionUnit::Weeks,
        "mo" | "month" | "months" => RetentionUnit::Months,
        "y" | "yr" | "yrs" | "year" | "years" => RetentionUnit::Years,
        "" => {
            return Err(crate::Error::BadRequest {
                detail: "retention period requires a unit (e.g. '7 years', '90 days')".to_string(),
            });
        }
        other => {
            return Err(crate::Error::BadRequest {
                detail: format!("unknown retention period unit: '{other}'"),
            });
        }
    };

    Ok(RetentionDuration { count, unit })
}

/// Check whether `created_at` (seconds since epoch) has exceeded the retention
/// duration relative to the current time, using calendar-accurate arithmetic.
///
/// For months/years, uses actual calendar offsets (not fixed 30/365 day approximations).
pub fn retention_expired(created_at_secs: u64, duration: &RetentionDuration) -> bool {
    let now = nodedb_types::NdbDateTime::now();
    let Ok(created) = nodedb_types::NdbDateTime::from_secs(created_at_secs as i64) else {
        return false;
    };

    let now_c = now.components();
    let created_c = created.components();

    match duration.unit {
        RetentionUnit::Seconds => {
            let elapsed = (now.micros - created.micros) / 1_000_000;
            elapsed >= duration.count as i64
        }
        RetentionUnit::Minutes => {
            let elapsed = (now.micros - created.micros) / (60 * 1_000_000);
            elapsed >= duration.count as i64
        }
        RetentionUnit::Hours => {
            let elapsed = (now.micros - created.micros) / (3600 * 1_000_000);
            elapsed >= duration.count as i64
        }
        RetentionUnit::Days => {
            let elapsed = (now.micros - created.micros) / (86400 * 1_000_000);
            elapsed >= duration.count as i64
        }
        RetentionUnit::Weeks => {
            let elapsed = (now.micros - created.micros) / (7 * 86400 * 1_000_000);
            elapsed >= duration.count as i64
        }
        RetentionUnit::Months => {
            // Calendar-accurate: 7 months from March 15 = October 15.
            let month_diff =
                (now_c.year - created_c.year) * 12 + (now_c.month as i32 - created_c.month as i32);
            // Adjust if the day hasn't been reached yet this month.
            let adjusted = if now_c.day < created_c.day {
                month_diff - 1
            } else {
                month_diff
            };
            adjusted >= duration.count as i32
        }
        RetentionUnit::Years => {
            // Calendar-accurate: 7 years from 2020-03-15 = 2027-03-15.
            let year_diff = now_c.year - created_c.year;
            let adjusted = if (now_c.month, now_c.day) < (created_c.month, created_c.day) {
                year_diff - 1
            } else {
                year_diff
            };
            adjusted >= duration.count as i32
        }
    }
}

/// Split "7 years" into ("7", "years") or "365d" into ("365", "d").
fn split_number_unit(s: &str) -> (&str, &str) {
    let idx = s.find(|c: char| !c.is_ascii_digit()).unwrap_or(s.len());
    let num = s[..idx].trim();
    let unit = s[idx..].trim();
    (num, unit)
}

/// Extract the `created_at` timestamp (seconds since epoch) from a document.
///
/// Tries: `created_at` (integer seconds or millis), `_created_at`,
/// `timestamp` fields. Returns `None` if no timestamp field is found.
pub fn extract_created_at_secs(doc_bytes: &[u8]) -> Option<u64> {
    let val = crate::data::executor::doc_format::decode_document_value(doc_bytes).ok()?;
    created_at_secs_of(&val)
}

/// [`extract_created_at_secs`] over an already-decoded row.
pub fn created_at_secs_of(val: &nodedb_types::Value) -> Option<u64> {
    let obj = val.as_object()?;

    for field in &["created_at", "_created_at", "timestamp"] {
        if let Some(v) = obj.get(*field) {
            if let nodedb_types::Value::Decimal(decimal) = v
                && let Some(n) = nodedb_types::Value::decimal_as_wide_u64(decimal)
            {
                return Some(n / 1000);
            }
            if let nodedb_types::Value::DateTime(timestamp)
            | nodedb_types::Value::NaiveDateTime(timestamp) = v
            {
                return Some((timestamp.micros / 1_000_000) as u64);
            }
            if let Some(n) = v.as_i64() {
                let n = n as u64;
                return Some(if n > 1_000_000_000_000 { n / 1000 } else { n });
            }
            // ISO 8601 string → parse to seconds.
            if let Some(s) = v.as_str()
                && let Some(dt) = nodedb_types::NdbDateTime::parse(s)
            {
                return Some((dt.micros / 1_000_000) as u64);
            }
        }
    }

    None
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tagged_timestamps_keep_retention_age() {
        let fields = std::collections::HashMap::from([(
            "created_at".into(),
            nodedb_types::Value::Integer(1_700_000_000),
        )]);
        let bytes = zerompk::to_msgpack_vec(&nodedb_types::Value::Object(fields)).unwrap();
        assert_eq!(extract_created_at_secs(&bytes), Some(1_700_000_000));
    }

    fn dur(count: u32, unit: RetentionUnit) -> RetentionDuration {
        RetentionDuration { count, unit }
    }

    fn opts(retention: Option<RetentionDuration>, has_hold: bool) -> EnforcementOptions {
        EnforcementOptions {
            retention,
            has_legal_hold: has_hold,
            ..Default::default()
        }
    }

    #[test]
    fn no_restrictions_allows_delete() {
        assert!(check_delete_allowed("coll", &opts(None, false), None).is_ok());
    }

    #[test]
    fn legal_hold_blocks_delete() {
        assert!(check_delete_allowed("coll", &opts(None, true), None).is_err());
    }

    #[test]
    fn legal_hold_overrides_expired_retention() {
        // Retention expired (row is old) but hold is active → still rejected.
        let r = dur(1, RetentionUnit::Hours);
        assert!(check_delete_allowed("coll", &opts(Some(r), true), Some(0)).is_err());
    }

    #[test]
    fn retention_blocks_young_row() {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs();
        // Row created 10 seconds ago, retention is 1 hour.
        let r = dur(1, RetentionUnit::Hours);
        assert!(check_delete_allowed("coll", &opts(Some(r), false), Some(now - 10)).is_err());
    }

    #[test]
    fn retention_allows_old_row() {
        // Row created long ago (epoch 0), retention is 1 hour.
        let r = dur(1, RetentionUnit::Hours);
        assert!(check_delete_allowed("coll", &opts(Some(r), false), Some(0)).is_ok());
    }

    #[test]
    fn retention_no_timestamp_rejects() {
        // Retention set but no created_at → reject defensively.
        let r = dur(7, RetentionUnit::Years);
        assert!(check_delete_allowed("coll", &opts(Some(r), false), None).is_err());
    }

    #[test]
    fn parse_retention_years() {
        let d = parse_retention_period("7 years").unwrap();
        assert_eq!(d.count, 7);
        assert_eq!(d.unit, RetentionUnit::Years);
    }

    #[test]
    fn parse_retention_days() {
        let d = parse_retention_period("90 days").unwrap();
        assert_eq!(d.count, 90);
        assert_eq!(d.unit, RetentionUnit::Days);
    }

    #[test]
    fn parse_retention_compact() {
        let d = parse_retention_period("365d").unwrap();
        assert_eq!(d.count, 365);
        assert_eq!(d.unit, RetentionUnit::Days);
    }

    #[test]
    fn parse_retention_invalid() {
        assert!(parse_retention_period("many").is_err());
        assert!(parse_retention_period("7 fortnights").is_err());
    }

    #[test]
    fn calendar_accurate_year_retention() {
        // Row created exactly 7 years ago should be expired.
        let now = nodedb_types::NdbDateTime::now();
        let c = now.components();
        // Build a timestamp 7 years ago, same month/day.
        let seven_years_ago = nodedb_types::NdbDateTime::from_secs(0).expect("0 secs in range"); // approximation for test
        // Instead: use a row from epoch 0 with 1-year retention — definitely expired.
        let r = dur(1, RetentionUnit::Years);
        assert!(retention_expired(0, &r)); // epoch 0 is > 1 year ago

        // Row created 6 months ago with 1-year retention — NOT expired.
        let six_months_ago_secs = (now.micros / 1_000_000 - 180 * 86400) as u64;
        assert!(!retention_expired(six_months_ago_secs, &r));

        // Suppress unused variable warnings.
        let _ = (c, seven_years_ago);
    }

    #[test]
    fn calendar_accurate_month_retention() {
        let now = nodedb_types::NdbDateTime::now();
        // Row created 2 days ago, retention = 1 month → NOT expired.
        let two_days_ago = (now.micros / 1_000_000 - 2 * 86400) as u64;
        let r = dur(1, RetentionUnit::Months);
        assert!(!retention_expired(two_days_ago, &r));

        // Row from epoch 0, retention = 1 month → expired (decades ago).
        assert!(retention_expired(0, &r));
    }

    #[test]
    fn extract_created_at_json_secs() {
        let doc = serde_json::json!({"created_at": 1700000000, "name": "test"});
        let bytes = serde_json::to_vec(&doc).unwrap();
        assert_eq!(extract_created_at_secs(&bytes), Some(1700000000));
    }

    #[test]
    fn extract_created_at_millis() {
        let doc = serde_json::json!({"created_at": 1700000000000u64});
        let bytes = nodedb_types::json_to_msgpack(&doc).unwrap();
        assert_eq!(extract_created_at_secs(&bytes), Some(1700000000));
    }

    #[test]
    fn extract_created_at_missing() {
        let doc = serde_json::json!({"name": "test"});
        let bytes = serde_json::to_vec(&doc).unwrap();
        assert_eq!(extract_created_at_secs(&bytes), None);
    }
}
