// SPDX-License-Identifier: Apache-2.0

//! Microseconds-precision UTC timestamp type.

use serde::{Deserialize, Serialize};

use super::duration::NdbDuration;
use super::error::NdbDateTimeError;

/// Microseconds-precision UTC timestamp.
///
/// Stores microseconds since Unix epoch as i64. Supports dates from
/// ~292,000 years BCE to ~292,000 years CE.
///
/// String format: ISO 8601 `"2024-03-15T10:30:00.000000Z"`.
///
/// `#[non_exhaustive]` — a timezone offset field may be added when
/// named-timezone support is introduced.
#[non_exhaustive]
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
    ///
    /// Returns `Err` if `millis * 1_000` overflows `i64`.
    pub fn from_millis(millis: i64) -> Result<Self, NdbDateTimeError> {
        let micros = millis
            .checked_mul(1_000)
            .ok_or(NdbDateTimeError::Overflow {
                input: millis,
                unit: "millis",
            })?;
        Ok(Self { micros })
    }

    /// Create from seconds since epoch.
    ///
    /// Returns `Err` if `secs * 1_000_000` overflows `i64`.
    pub fn from_secs(secs: i64) -> Result<Self, NdbDateTimeError> {
        let micros = secs
            .checked_mul(1_000_000)
            .ok_or(NdbDateTimeError::Overflow {
                input: secs,
                unit: "secs",
            })?;
        Ok(Self { micros })
    }

    /// Current UTC time.
    ///
    /// Converts `SystemTime` microseconds (`u128`) to `i64`. Saturates at
    /// `i64::MAX` (year ~292,277 CE) rather than wrapping — clocks that far
    /// in the future simply report the maximum representable timestamp.
    pub fn now() -> Self {
        let dur = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_else(|_| {
                use std::sync::atomic::{AtomicBool, Ordering};
                static LOGGED: AtomicBool = AtomicBool::new(false);
                if !LOGGED.swap(true, Ordering::Relaxed) {
                    tracing::error!(
                        module = module_path!(),
                        "system clock is before UNIX_EPOCH; using 0 (epoch) \
                         — check NTP/RTC configuration"
                    );
                }
                std::time::Duration::ZERO
            });
        Self {
            micros: i64::try_from(dur.as_micros()).unwrap_or(i64::MAX),
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

    /// Parse from ISO 8601 text.
    ///
    /// Accepts `"2024-03-15T10:30:00Z"`, `"2024-03-15 10:30:00"`,
    /// `"2024-03-15T10:30:00.123456Z"`, `"2024-03-15"` (midnight UTC), and
    /// a trailing UTC offset in place of `Z` — `+05:30`, `-0800`, `+02` —
    /// which shifts the result to UTC. Seconds are optional. Every component
    /// must parse whole: trailing characters after the seconds, the fraction
    /// or the offset are refused rather than dropped, so an unrecognised
    /// spelling is `None`, never a silently different instant.
    pub fn parse(s: &str) -> Option<Self> {
        let (body, offset_secs) = split_utc_offset(s.trim())?;

        if body.len() == 10 {
            // Date only: "2024-03-15" → midnight UTC.
            let (year, month, day) = parse_civil_date(body)?;
            return Self::from_civil(year, month, day, 0, 0, 0, 0)?.shift_secs(-offset_secs);
        }

        // Full: "2024-03-15T10:30:00" or "2024-03-15T10:30:00.123456"
        let (date_part, time_part) = body.split_once('T').or_else(|| body.split_once(' '))?;
        let (year, month, day) = parse_civil_date(date_part)?;

        let (time_main, frac) = match time_part.split_once('.') {
            Some((t, f)) => (t, Some(f)),
            None => (time_part, None),
        };
        let time_parts: Vec<&str> = time_main.split(':').collect();
        if time_parts.len() < 2 || time_parts.len() > 3 {
            return None;
        }
        let hour: u32 = time_parts[0].parse().ok()?;
        let minute: u32 = time_parts[1].parse().ok()?;
        let second: u32 = match time_parts.get(2) {
            Some(text) => text.parse().ok()?,
            None => 0,
        };

        // Fractional seconds: one to nine digits, read to microseconds.
        let micros: u32 = match frac {
            Some(digits) => {
                if digits.is_empty()
                    || digits.len() > 9
                    || !digits.bytes().all(|b| b.is_ascii_digit())
                {
                    return None;
                }
                let padded = format!("{digits:0<6}");
                padded[..6].parse().ok()?
            }
            None => 0,
        };

        Self::from_civil(year, month, day, hour, minute, second, micros)?.shift_secs(-offset_secs)
    }

    /// Shift by whole seconds, or `None` on overflow.
    fn shift_secs(self, secs: i64) -> Option<Self> {
        let micros = self.micros.checked_add(secs.checked_mul(1_000_000)?)?;
        Some(Self { micros })
    }

    /// Build from civil date components.
    ///
    /// Returns `None` if any intermediate multiplication overflows `i64`.
    fn from_civil(
        year: i32,
        month: u32,
        day: u32,
        hour: u32,
        minute: u32,
        second: u32,
        micros: u32,
    ) -> Option<Self> {
        // Inverse of the Hinnant algorithm.
        let y = if month <= 2 { year - 1 } else { year };
        let m = if month <= 2 { month + 9 } else { month - 3 };
        let era = if y >= 0 { y } else { y - 399 } / 400;
        let yoe = (y - era * 400) as u32;
        let doy = (153 * m + 2) / 5 + day - 1;
        let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
        let days = (era as i64)
            .checked_mul(146_097)?
            .checked_add(doe as i64)?
            .checked_sub(719_468)?;
        let total_secs = days
            .checked_mul(86400)?
            .checked_add(hour as i64 * 3600)?
            .checked_add(minute as i64 * 60)?
            .checked_add(second as i64)?;
        let result_micros = total_secs
            .checked_mul(1_000_000)?
            .checked_add(micros as i64)?;
        Some(Self {
            micros: result_micros,
        })
    }

    /// Add a duration.
    ///
    /// Returns `Err` if the result overflows `i64`.
    pub fn add_duration(&self, d: NdbDuration) -> Result<Self, NdbDateTimeError> {
        let micros = self
            .micros
            .checked_add(d.micros)
            .ok_or(NdbDateTimeError::AddOverflow)?;
        Ok(Self { micros })
    }

    /// Subtract a duration.
    ///
    /// Returns `Err` if the result overflows `i64`.
    pub fn sub_duration(&self, d: NdbDuration) -> Result<Self, NdbDateTimeError> {
        let micros = self
            .micros
            .checked_sub(d.micros)
            .ok_or(NdbDateTimeError::SubOverflow)?;
        Ok(Self { micros })
    }

    /// Duration between two timestamps (self - other).
    ///
    /// Returns `Err` if the result overflows `i64`.
    pub fn duration_since(&self, other: &NdbDateTime) -> Result<NdbDuration, NdbDateTimeError> {
        let micros = self
            .micros
            .checked_sub(other.micros)
            .ok_or(NdbDateTimeError::SubOverflow)?;
        Ok(NdbDuration { micros })
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

/// Split a trailing `Z` or `±HH[:MM]` / `±HHMM` UTC offset off `s`, returning
/// the remaining text and the offset in seconds east of UTC (`0` when there
/// is none). A `-` inside the date part is never read as an offset: only a
/// sign after the last `T` / ` ` time separator counts.
fn split_utc_offset(s: &str) -> Option<(&str, i64)> {
    if let Some(body) = s.strip_suffix('Z').or_else(|| s.strip_suffix('z')) {
        return Some((body, 0));
    }
    // The offset can only follow the time part, or a bare `YYYY-MM-DD`.
    let time_start = match s.rfind(['T', ' ']) {
        Some(i) => i + 1,
        None => {
            let at = s.len().min(10);
            if !s.is_char_boundary(at) {
                return None;
            }
            at
        }
    };
    let Some(sign_at) = s[time_start..].rfind(['+', '-']).map(|i| time_start + i) else {
        return Some((s, 0));
    };
    let sign: i64 = if s.as_bytes()[sign_at] == b'-' { -1 } else { 1 };
    let rest = &s[sign_at + 1..];
    let (hh, mm) = match (rest.len(), rest.as_bytes().get(2)) {
        (2, None) => (rest, "0"),
        (4, Some(b)) if b.is_ascii_digit() => (&rest[..2], &rest[2..]),
        (5, Some(&b':')) => (&rest[..2], &rest[3..]),
        _ => return None,
    };
    if !hh.bytes().all(|b| b.is_ascii_digit()) || !mm.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    let hours: i64 = hh.parse().ok()?;
    let minutes: i64 = mm.parse().ok()?;
    if hours > 23 || minutes > 59 {
        return None;
    }
    Some((&s[..sign_at], sign * (hours * 3600 + minutes * 60)))
}

/// Parse `YYYY-MM-DD` into its three components.
fn parse_civil_date(s: &str) -> Option<(i32, u32, u32)> {
    let parts: Vec<&str> = s.split('-').collect();
    if parts.len() != 3 {
        return None;
    }
    Some((
        parts[0].parse().ok()?,
        parts[1].parse().ok()?,
        parts[2].parse().ok()?,
    ))
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

    /// A trailing offset shifts the instant to UTC; a space separator and
    /// missing seconds are accepted.
    #[test]
    fn datetime_parse_applies_utc_offset() {
        let utc = NdbDateTime::parse("2024-06-15T06:30:00Z").unwrap();
        assert_eq!(
            NdbDateTime::parse("2024-06-15 12:00:00+05:30").unwrap(),
            utc
        );
        assert_eq!(NdbDateTime::parse("2024-06-15T12:00:00+0530").unwrap(), utc);
        assert_eq!(
            NdbDateTime::parse("2024-06-15T04:30:00-02:00").unwrap(),
            utc
        );
        assert_eq!(NdbDateTime::parse("2024-06-15T04:30-02").unwrap(), utc);
        assert_eq!(
            NdbDateTime::parse("2024-06-15T06:30:00.250+00:00").unwrap(),
            NdbDateTime::from_micros(utc.micros + 250_000)
        );
        assert_eq!(
            NdbDateTime::parse("2024-06-16+05:30").unwrap(),
            NdbDateTime::parse("2024-06-15T18:30:00Z").unwrap()
        );
    }

    /// Text that is not a whole timestamp is refused, never read as a
    /// different instant.
    #[test]
    fn datetime_parse_refuses_partial_spellings() {
        for text in [
            "yesterday",
            "1583402400000000",
            "2024-06-15T12:00:00+05:30:00",
            "2024-06-15T12:00:xx",
            "2024-06-15T12:00:00.abc",
            "2024-06-15T12:00:00.",
            "2024-06-15T12:00:00.1234567890",
            "2024-06-15T12",
            "2024-06-15T12:00:00+25:00",
        ] {
            assert!(
                NdbDateTime::parse(text).is_none(),
                "{text:?} must not parse"
            );
        }
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
        let later = dt
            .add_duration(NdbDuration::from_hours(24).expect("24 hours in range"))
            .expect("add_duration in range");
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
    fn unix_accessors() {
        let dt = NdbDateTime::from_secs(1_700_000_000).expect("known unix timestamp in range");
        assert_eq!(dt.unix_secs(), 1_700_000_000);
        assert_eq!(dt.unix_millis(), 1_700_000_000_000);
    }

    #[test]
    fn datetime_from_millis_overflow() {
        assert!(NdbDateTime::from_millis(i64::MAX).is_err());
        assert_eq!(
            NdbDateTime::from_millis(i64::MAX),
            Err(NdbDateTimeError::Overflow {
                input: i64::MAX,
                unit: "millis"
            })
        );
    }

    #[test]
    fn datetime_from_secs_overflow() {
        assert!(NdbDateTime::from_secs(i64::MAX).is_err());
        assert_eq!(
            NdbDateTime::from_secs(i64::MAX),
            Err(NdbDateTimeError::Overflow {
                input: i64::MAX,
                unit: "secs"
            })
        );
    }

    #[test]
    fn add_duration_overflow() {
        let dt = NdbDateTime::from_micros(i64::MAX);
        let one_us = NdbDuration::from_micros(1);
        assert_eq!(dt.add_duration(one_us), Err(NdbDateTimeError::AddOverflow));
    }

    #[test]
    fn sub_duration_overflow() {
        let dt = NdbDateTime::from_micros(i64::MIN);
        let one_us = NdbDuration::from_micros(1);
        assert_eq!(dt.sub_duration(one_us), Err(NdbDateTimeError::SubOverflow));
    }

    #[test]
    fn duration_since_overflow() {
        let a = NdbDateTime::from_micros(i64::MIN);
        let b = NdbDateTime::from_micros(i64::MAX);
        // i64::MIN - i64::MAX overflows
        assert_eq!(a.duration_since(&b), Err(NdbDateTimeError::SubOverflow));
    }

    #[test]
    fn now_returns_positive() {
        // Sanity: current time is after epoch.
        let dt = NdbDateTime::now();
        assert!(dt.micros > 0, "now() returned non-positive: {}", dt.micros);
    }
}
