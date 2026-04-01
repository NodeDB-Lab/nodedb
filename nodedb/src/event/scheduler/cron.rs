//! Minimal cron expression parser.
//!
//! Supports standard 5-field cron: `minute hour day_of_month month day_of_week`
//!
//! Field syntax:
//! - `*`       — all values
//! - `5`       — specific value
//! - `1-5`     — range (inclusive)
//! - `*/15`    — step (every 15)
//! - `1,3,5`   — list
//! - `1-5/2`   — range with step

use std::collections::BTreeSet;

/// A parsed cron expression.
#[derive(Debug, Clone)]
pub struct CronExpr {
    pub minutes: BTreeSet<u8>,       // 0-59
    pub hours: BTreeSet<u8>,         // 0-23
    pub days_of_month: BTreeSet<u8>, // 1-31
    pub months: BTreeSet<u8>,        // 1-12
    pub days_of_week: BTreeSet<u8>,  // 0-6 (0 = Sunday)
}

impl CronExpr {
    /// Parse a 5-field cron expression.
    pub fn parse(expr: &str) -> Result<Self, String> {
        let fields: Vec<&str> = expr.split_whitespace().collect();
        if fields.len() != 5 {
            return Err(format!(
                "expected 5 fields (minute hour dom month dow), got {}",
                fields.len()
            ));
        }

        Ok(Self {
            minutes: parse_field(fields[0], 0, 59)?,
            hours: parse_field(fields[1], 0, 23)?,
            days_of_month: parse_field(fields[2], 1, 31)?,
            months: parse_field(fields[3], 1, 12)?,
            days_of_week: parse_field(fields[4], 0, 6)?,
        })
    }

    /// Check if the given wall-clock time matches this cron expression.
    pub fn matches(&self, minute: u8, hour: u8, day: u8, month: u8, weekday: u8) -> bool {
        self.minutes.contains(&minute)
            && self.hours.contains(&hour)
            && self.days_of_month.contains(&day)
            && self.months.contains(&month)
            && self.days_of_week.contains(&weekday)
    }

    /// Check if the cron fires at the given Unix epoch seconds.
    pub fn matches_epoch(&self, epoch_secs: u64) -> bool {
        let (minute, hour, day, month, weekday) = epoch_to_fields(epoch_secs);
        self.matches(minute, hour, day, month, weekday)
    }

    /// Find the next fire time after `after_epoch_secs`.
    ///
    /// Scans forward minute-by-minute up to 366 days. Returns `None` if no
    /// match found (should only happen for impossible cron expressions).
    pub fn next_fire_after(&self, after_epoch_secs: u64) -> Option<u64> {
        // Start at the next minute boundary.
        let start = (after_epoch_secs / 60 + 1) * 60;
        // Scan up to 366 days (527040 minutes).
        let max_minutes = 366 * 24 * 60;
        for i in 0..max_minutes {
            let candidate = start + i * 60;
            if self.matches_epoch(candidate) {
                return Some(candidate);
            }
        }
        None
    }
}

/// Parse one cron field (e.g., "*/5", "1-3", "1,2,3", "*").
fn parse_field(field: &str, min: u8, max: u8) -> Result<BTreeSet<u8>, String> {
    let mut result = BTreeSet::new();

    for part in field.split(',') {
        let part = part.trim();
        if part == "*" {
            for v in min..=max {
                result.insert(v);
            }
        } else if let Some(step_str) = part.strip_prefix("*/") {
            let step: u8 = step_str
                .parse()
                .map_err(|_| format!("invalid step: '{step_str}'"))?;
            if step == 0 {
                return Err("step cannot be zero".into());
            }
            let mut v = min;
            while v <= max {
                result.insert(v);
                v = v.saturating_add(step);
            }
        } else if part.contains('/') {
            // range/step: "1-10/2"
            let (range_part, step_str) = part
                .split_once('/')
                .ok_or_else(|| format!("invalid field: '{part}'"))?;
            let step: u8 = step_str
                .parse()
                .map_err(|_| format!("invalid step: '{step_str}'"))?;
            if step == 0 {
                return Err("step cannot be zero".into());
            }
            let (lo, hi) = parse_range(range_part, min, max)?;
            let mut v = lo;
            while v <= hi {
                result.insert(v);
                v = v.saturating_add(step);
            }
        } else if part.contains('-') {
            let (lo, hi) = parse_range(part, min, max)?;
            for v in lo..=hi {
                result.insert(v);
            }
        } else {
            let v: u8 = part
                .parse()
                .map_err(|_| format!("invalid value: '{part}'"))?;
            if v < min || v > max {
                return Err(format!("value {v} out of range {min}-{max}"));
            }
            result.insert(v);
        }
    }

    if result.is_empty() {
        return Err(format!("field '{field}' produced no values"));
    }
    Ok(result)
}

fn parse_range(s: &str, min: u8, max: u8) -> Result<(u8, u8), String> {
    let (lo_str, hi_str) = s
        .split_once('-')
        .ok_or_else(|| format!("invalid range: '{s}'"))?;
    let lo: u8 = lo_str
        .parse()
        .map_err(|_| format!("invalid range start: '{lo_str}'"))?;
    let hi: u8 = hi_str
        .parse()
        .map_err(|_| format!("invalid range end: '{hi_str}'"))?;
    if lo < min || hi > max || lo > hi {
        return Err(format!("range {lo}-{hi} out of bounds {min}-{max}"));
    }
    Ok((lo, hi))
}

/// Convert Unix epoch seconds to (minute, hour, day, month, weekday).
/// weekday: 0=Sunday, 1=Monday, ..., 6=Saturday.
fn epoch_to_fields(epoch_secs: u64) -> (u8, u8, u8, u8, u8) {
    // Use std::time to avoid pulling in chrono.
    // Days since Unix epoch (1970-01-01, a Thursday = weekday 4).
    let total_secs = epoch_secs as i64;
    let day_secs = total_secs.rem_euclid(86_400);
    let minute = ((day_secs / 60) % 60) as u8;
    let hour = ((day_secs / 3600) % 24) as u8;

    // Date calculation (Rata Die algorithm).
    let mut days = total_secs / 86_400;
    let weekday = ((days + 4) % 7) as u8; // 1970-01-01 was Thursday (4)

    // Civil date from day count (algorithm from Howard Hinnant).
    days += 719_468;
    let era = (if days >= 0 { days } else { days - 146_096 }) / 146_097;
    let doe = (days - era * 146_097) as u32; // day of era
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146_096) / 365;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let day = (doy - (153 * mp + 2) / 5 + 1) as u8;
    let month = if mp < 10 { mp + 3 } else { mp - 9 } as u8;

    (minute, hour, day, month, weekday)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_wildcard() {
        let e = CronExpr::parse("* * * * *").unwrap();
        assert_eq!(e.minutes.len(), 60);
        assert_eq!(e.hours.len(), 24);
    }

    #[test]
    fn parse_specific() {
        let e = CronExpr::parse("0 0 * * *").unwrap();
        assert_eq!(e.minutes, BTreeSet::from([0]));
        assert_eq!(e.hours, BTreeSet::from([0]));
    }

    #[test]
    fn parse_step() {
        let e = CronExpr::parse("*/15 * * * *").unwrap();
        assert_eq!(e.minutes, BTreeSet::from([0, 15, 30, 45]));
    }

    #[test]
    fn parse_range() {
        let e = CronExpr::parse("* 9-17 * * *").unwrap();
        assert_eq!(e.hours.len(), 9); // 9,10,11,12,13,14,15,16,17
        assert!(e.hours.contains(&9));
        assert!(e.hours.contains(&17));
    }

    #[test]
    fn parse_list() {
        let e = CronExpr::parse("0,30 * * * *").unwrap();
        assert_eq!(e.minutes, BTreeSet::from([0, 30]));
    }

    #[test]
    fn parse_range_with_step() {
        let e = CronExpr::parse("1-10/3 * * * *").unwrap();
        assert_eq!(e.minutes, BTreeSet::from([1, 4, 7, 10]));
    }

    #[test]
    fn parse_error_wrong_field_count() {
        assert!(CronExpr::parse("* * *").is_err());
    }

    #[test]
    fn matches_midnight_daily() {
        let e = CronExpr::parse("0 0 * * *").unwrap();
        assert!(e.matches(0, 0, 15, 3, 2));
        assert!(!e.matches(1, 0, 15, 3, 2));
        assert!(!e.matches(0, 1, 15, 3, 2));
    }

    #[test]
    fn epoch_to_fields_known_date() {
        // 2024-01-01 00:00:00 UTC = 1704067200
        let (min, hour, day, month, weekday) = epoch_to_fields(1_704_067_200);
        assert_eq!(min, 0);
        assert_eq!(hour, 0);
        assert_eq!(day, 1);
        assert_eq!(month, 1);
        assert_eq!(weekday, 1); // Monday
    }

    #[test]
    fn matches_epoch_works() {
        let e = CronExpr::parse("0 0 1 1 *").unwrap(); // Jan 1 at midnight
        assert!(e.matches_epoch(1_704_067_200)); // 2024-01-01 00:00 UTC
    }
}
