//! First-class DateTime and Duration types.
//!
//! `NdbDateTime` stores microseconds since Unix epoch (1970-01-01T00:00:00Z).
//! `NdbDuration` stores microseconds as a signed i64.
//!
//! Both serialize as strings (ISO 8601 for DateTime, human-readable for Duration)
//! for JSON compatibility. Internal representation is i64 for efficient comparison
//! and arithmetic.

use serde::{Deserialize, Serialize};

/// Microseconds-precision UTC timestamp.
///
/// Stores microseconds since Unix epoch as i64. Supports dates from
/// ~292,000 years BCE to ~292,000 years CE.
///
/// String format: ISO 8601 `"2024-03-15T10:30:00.000000Z"`.
#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Hash,
    Serialize,
    Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
pub struct NdbDateTime {
    /// Microseconds since Unix epoch (1970-01-01T00:00:00Z).
    pub micros: i64,
}

impl NdbDateTime {
    /// Create from microseconds since epoch.
    pub fn from_micros(micros: i64) -> Self {
        Self { micros }
    }

    /// Create from milliseconds since epoch.
    pub fn from_millis(millis: i64) -> Self {
        Self {
            micros: millis * 1000,
        }
    }

    /// Create from seconds since epoch.
    pub fn from_secs(secs: i64) -> Self {
        Self {
            micros: secs * 1_000_000,
        }
    }

    /// Current UTC time.
    pub fn now() -> Self {
        let dur = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default();
        Self {
            micros: dur.as_micros() as i64,
        }
    }

    /// Extract year, month, day, hour, minute, second components.
    pub fn components(&self) -> DateTimeComponents {
        let total_secs = self.micros / 1_000_000;
        let micros_rem = (self.micros % 1_000_000).unsigned_abs();

        // Civil date from Unix timestamp (algorithm from Howard Hinnant).
        let mut days = total_secs.div_euclid(86400) as i32;
        let day_secs = total_secs.rem_euclid(86400) as u32;

        days += 719_468; // shift epoch from 1970-01-01 to 0000-03-01
        let era = if days >= 0 { days } else { days - 146_096 } / 146_097;
        let doe = (days - era * 146_097) as u32;
        let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146_096) / 365;
        let y = yoe as i32 + era * 400;
        let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
        let mp = (5 * doy + 2) / 153;
        let d = doy - (153 * mp + 2) / 5 + 1;
        let m = if mp < 10 { mp + 3 } else { mp - 9 };
        let year = if m <= 2 { y + 1 } else { y };

        DateTimeComponents {
            year,
            month: m as u8,
            day: d as u8,
            hour: (day_secs / 3600) as u8,
            minute: ((day_secs % 3600) / 60) as u8,
            second: (day_secs % 60) as u8,
            microsecond: micros_rem as u32,
        }
    }

    /// Format as ISO 8601 string: `"2024-03-15T10:30:00.000000Z"`.
    pub fn to_iso8601(&self) -> String {
        let c = self.components();
        format!(
            "{:04}-{:02}-{:02}T{:02}:{:02}:{:02}.{:06}Z",
            c.year, c.month, c.day, c.hour, c.minute, c.second, c.microsecond
        )
    }

    /// Parse from ISO 8601 string (basic subset).
    ///
    /// Supports: `"2024-03-15T10:30:00Z"`, `"2024-03-15T10:30:00.123456Z"`,
    /// `"2024-03-15"` (midnight UTC).
    pub fn parse(s: &str) -> Option<Self> {
        let s = s.trim().trim_end_matches('Z').trim_end_matches('z');

        if s.len() == 10 {
            // Date only: "2024-03-15" → midnight UTC.
            let parts: Vec<&str> = s.split('-').collect();
            if parts.len() != 3 {
                return None;
            }
            let year: i32 = parts[0].parse().ok()?;
            let month: u32 = parts[1].parse().ok()?;
            let day: u32 = parts[2].parse().ok()?;
            return Some(Self::from_civil(year, month, day, 0, 0, 0, 0));
        }

        // Full: "2024-03-15T10:30:00" or "2024-03-15T10:30:00.123456"
        let (date_part, time_part) = s.split_once('T').or_else(|| s.split_once(' '))?;
        let date_parts: Vec<&str> = date_part.split('-').collect();
        if date_parts.len() != 3 {
            return None;
        }
        let year: i32 = date_parts[0].parse().ok()?;
        let month: u32 = date_parts[1].parse().ok()?;
        let day: u32 = date_parts[2].parse().ok()?;

        let (time_main, frac) = if let Some((t, f)) = time_part.split_once('.') {
            (t, f)
        } else {
            (time_part, "0")
        };
        let time_parts: Vec<&str> = time_main.split(':').collect();
        if time_parts.len() < 2 {
            return None;
        }
        let hour: u32 = time_parts[0].parse().ok()?;
        let minute: u32 = time_parts[1].parse().ok()?;
        let second: u32 = time_parts.get(2).and_then(|s| s.parse().ok()).unwrap_or(0);

        // Parse fractional seconds (up to microseconds).
        let frac_padded = format!("{frac:0<6}");
        let micros: u32 = frac_padded[..6].parse().unwrap_or(0);

        Some(Self::from_civil(
            year, month, day, hour, minute, second, micros,
        ))
    }

    /// Build from civil date components.
    fn from_civil(
        year: i32,
        month: u32,
        day: u32,
        hour: u32,
        minute: u32,
        second: u32,
        micros: u32,
    ) -> Self {
        // Inverse of the Hinnant algorithm.
        let y = if month <= 2 { year - 1 } else { year };
        let m = if month <= 2 { month + 9 } else { month - 3 };
        let era = if y >= 0 { y } else { y - 399 } / 400;
        let yoe = (y - era * 400) as u32;
        let doy = (153 * m + 2) / 5 + day - 1;
        let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
        let days = era as i64 * 146_097 + doe as i64 - 719_468;
        let total_secs = days * 86400 + hour as i64 * 3600 + minute as i64 * 60 + second as i64;
        Self {
            micros: total_secs * 1_000_000 + micros as i64,
        }
    }

    /// Add a duration.
    pub fn add_duration(&self, d: NdbDuration) -> Self {
        Self {
            micros: self.micros + d.micros,
        }
    }

    /// Subtract a duration.
    pub fn sub_duration(&self, d: NdbDuration) -> Self {
        Self {
            micros: self.micros - d.micros,
        }
    }

    /// Duration between two timestamps.
    pub fn duration_since(&self, other: &NdbDateTime) -> NdbDuration {
        NdbDuration {
            micros: self.micros - other.micros,
        }
    }

    /// Unix epoch seconds.
    pub fn unix_secs(&self) -> i64 {
        self.micros / 1_000_000
    }

    /// Unix epoch milliseconds.
    pub fn unix_millis(&self) -> i64 {
        self.micros / 1_000
    }
}

impl std::fmt::Display for NdbDateTime {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.to_iso8601())
    }
}

/// Components of a civil date-time.
#[derive(Debug, Clone, Copy)]
pub struct DateTimeComponents {
    pub year: i32,
    pub month: u8,
    pub day: u8,
    pub hour: u8,
    pub minute: u8,
    pub second: u8,
    pub microsecond: u32,
}

/// Microseconds-precision duration (signed).
///
/// String format: human-readable `"1h30m15s"` or `"500ms"`.
#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Hash,
    Serialize,
    Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
pub struct NdbDuration {
    /// Microseconds (signed: negative = past).
    pub micros: i64,
}

impl NdbDuration {
    pub fn from_micros(micros: i64) -> Self {
        Self { micros }
    }

    pub fn from_millis(millis: i64) -> Self {
        Self {
            micros: millis * 1_000,
        }
    }

    pub fn from_secs(secs: i64) -> Self {
        Self {
            micros: secs * 1_000_000,
        }
    }

    pub fn from_minutes(mins: i64) -> Self {
        Self {
            micros: mins * 60 * 1_000_000,
        }
    }

    pub fn from_hours(hours: i64) -> Self {
        Self {
            micros: hours * 3600 * 1_000_000,
        }
    }

    pub fn from_days(days: i64) -> Self {
        Self {
            micros: days * 86400 * 1_000_000,
        }
    }

    pub fn as_secs_f64(&self) -> f64 {
        self.micros as f64 / 1_000_000.0
    }

    pub fn as_millis(&self) -> i64 {
        self.micros / 1_000
    }

    /// Format as human-readable string.
    pub fn to_human(&self) -> String {
        let abs = self.micros.unsigned_abs();
        let sign = if self.micros < 0 { "-" } else { "" };

        if abs < 1_000 {
            return format!("{sign}{abs}us");
        }
        if abs < 1_000_000 {
            return format!("{sign}{}ms", abs / 1_000);
        }

        let total_secs = abs / 1_000_000;
        let hours = total_secs / 3600;
        let mins = (total_secs % 3600) / 60;
        let secs = total_secs % 60;

        if hours > 0 {
            if mins > 0 || secs > 0 {
                format!("{sign}{hours}h{mins}m{secs}s")
            } else {
                format!("{sign}{hours}h")
            }
        } else if mins > 0 {
            if secs > 0 {
                format!("{sign}{mins}m{secs}s")
            } else {
                format!("{sign}{mins}m")
            }
        } else {
            format!("{sign}{secs}s")
        }
    }

    /// Parse from human-readable string: "1h30m", "500ms", "30s", "2d".
    pub fn parse(s: &str) -> Option<Self> {
        let s = s.trim();
        if s.is_empty() {
            return None;
        }

        let (neg, s) = if let Some(rest) = s.strip_prefix('-') {
            (true, rest)
        } else {
            (false, s)
        };

        // Simple suffix parsing.
        if let Some(n) = s.strip_suffix("us") {
            let v: i64 = n.trim().parse().ok()?;
            return Some(Self::from_micros(if neg { -v } else { v }));
        }
        if let Some(n) = s.strip_suffix("ms") {
            let v: i64 = n.trim().parse().ok()?;
            return Some(Self::from_millis(if neg { -v } else { v }));
        }
        if let Some(n) = s.strip_suffix('d') {
            let v: i64 = n.trim().parse().ok()?;
            return Some(Self::from_days(if neg { -v } else { v }));
        }

        // Compound: "1h30m15s"
        let mut total_micros: i64 = 0;
        let mut num_buf = String::new();
        for c in s.chars() {
            if c.is_ascii_digit() {
                num_buf.push(c);
            } else {
                let n: i64 = num_buf.parse().ok()?;
                num_buf.clear();
                match c {
                    'h' => total_micros += n * 3_600_000_000,
                    'm' => total_micros += n * 60_000_000,
                    's' => total_micros += n * 1_000_000,
                    _ => return None,
                }
            }
        }
        // Trailing number without suffix = seconds.
        if !num_buf.is_empty() {
            let n: i64 = num_buf.parse().ok()?;
            total_micros += n * 1_000_000;
        }

        if total_micros == 0 {
            return None;
        }

        Some(Self::from_micros(if neg {
            -total_micros
        } else {
            total_micros
        }))
    }
}

impl std::fmt::Display for NdbDuration {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.to_human())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn datetime_now_roundtrip() {
        let dt = NdbDateTime::now();
        let iso = dt.to_iso8601();
        let parsed = NdbDateTime::parse(&iso).unwrap();
        // Allow 1 microsecond rounding difference.
        assert!(
            (dt.micros - parsed.micros).abs() <= 1,
            "dt={}, parsed={}",
            dt.micros,
            parsed.micros
        );
    }

    #[test]
    fn datetime_epoch() {
        let dt = NdbDateTime::from_micros(0);
        assert_eq!(dt.to_iso8601(), "1970-01-01T00:00:00.000000Z");
    }

    #[test]
    fn datetime_known_date() {
        let dt = NdbDateTime::parse("2024-03-15T10:30:00Z").unwrap();
        let c = dt.components();
        assert_eq!(c.year, 2024);
        assert_eq!(c.month, 3);
        assert_eq!(c.day, 15);
        assert_eq!(c.hour, 10);
        assert_eq!(c.minute, 30);
        assert_eq!(c.second, 0);
    }

    #[test]
    fn datetime_fractional_seconds() {
        let dt = NdbDateTime::parse("2024-01-01T00:00:00.123456Z").unwrap();
        let c = dt.components();
        assert_eq!(c.microsecond, 123456);
    }

    #[test]
    fn datetime_date_only() {
        let dt = NdbDateTime::parse("2024-03-15").unwrap();
        let c = dt.components();
        assert_eq!(c.year, 2024);
        assert_eq!(c.month, 3);
        assert_eq!(c.day, 15);
        assert_eq!(c.hour, 0);
    }

    #[test]
    fn datetime_arithmetic() {
        let dt = NdbDateTime::parse("2024-01-01T00:00:00Z").unwrap();
        let later = dt.add_duration(NdbDuration::from_hours(24));
        let c = later.components();
        assert_eq!(c.day, 2);
    }

    #[test]
    fn datetime_ordering() {
        let a = NdbDateTime::parse("2024-01-01T00:00:00Z").unwrap();
        let b = NdbDateTime::parse("2024-01-02T00:00:00Z").unwrap();
        assert!(a < b);
    }

    #[test]
    fn duration_human_format() {
        assert_eq!(NdbDuration::from_secs(90).to_human(), "1m30s");
        assert_eq!(NdbDuration::from_hours(2).to_human(), "2h");
        assert_eq!(NdbDuration::from_millis(500).to_human(), "500ms");
        assert_eq!(NdbDuration::from_micros(42).to_human(), "42us");
        assert_eq!(NdbDuration::from_secs(3661).to_human(), "1h1m1s");
    }

    #[test]
    fn duration_parse() {
        assert_eq!(NdbDuration::parse("30s").unwrap().micros, 30_000_000);
        assert_eq!(NdbDuration::parse("1h30m").unwrap().micros, 5_400_000_000);
        assert_eq!(NdbDuration::parse("500ms").unwrap().micros, 500_000);
        assert_eq!(NdbDuration::parse("2d").unwrap().micros, 172_800_000_000);
        assert_eq!(NdbDuration::parse("-5s").unwrap().micros, -5_000_000);
    }

    #[test]
    fn duration_roundtrip() {
        let d = NdbDuration::from_secs(3661);
        let s = d.to_human();
        let parsed = NdbDuration::parse(&s).unwrap();
        assert_eq!(d.micros, parsed.micros);
    }

    #[test]
    fn unix_accessors() {
        let dt = NdbDateTime::from_secs(1_700_000_000);
        assert_eq!(dt.unix_secs(), 1_700_000_000);
        assert_eq!(dt.unix_millis(), 1_700_000_000_000);
    }
}
