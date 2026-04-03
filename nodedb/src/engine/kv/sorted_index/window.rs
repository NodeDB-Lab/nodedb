//! Time-windowed leaderboard filtering.
//!
//! A sorted index can optionally have a `WindowConfig` that restricts which
//! entries are visible based on a timestamp column. Window types:
//!
//! - `Daily`: resets at UTC midnight
//! - `Weekly`: resets at UTC Monday 00:00
//! - `Monthly`: resets at UTC 1st of month 00:00
//! - `Custom { start_ms, end_ms }`: fixed time range
//!
//! The sorted index stores ALL entries (no data deletion). Window filtering
//! is applied at query time by comparing the entry's timestamp against the
//! current window boundary.

/// Window type for a time-windowed sorted index.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WindowType {
    /// No windowing — all entries are always visible.
    None,
    /// Resets daily at UTC midnight.
    Daily,
    /// Resets weekly at UTC Monday 00:00.
    Weekly,
    /// Resets monthly at UTC 1st of month 00:00.
    Monthly,
    /// Fixed time range [start_ms, end_ms).
    Custom { start_ms: u64, end_ms: u64 },
}

/// Configuration for a time-windowed sorted index.
#[derive(Debug, Clone)]
pub struct WindowConfig {
    /// The window type.
    pub window_type: WindowType,
    /// Name of the timestamp column used for windowing.
    pub timestamp_column: String,
}

impl WindowConfig {
    pub fn none() -> Self {
        Self {
            window_type: WindowType::None,
            timestamp_column: String::new(),
        }
    }

    pub fn daily(timestamp_column: impl Into<String>) -> Self {
        Self {
            window_type: WindowType::Daily,
            timestamp_column: timestamp_column.into(),
        }
    }

    pub fn weekly(timestamp_column: impl Into<String>) -> Self {
        Self {
            window_type: WindowType::Weekly,
            timestamp_column: timestamp_column.into(),
        }
    }

    pub fn monthly(timestamp_column: impl Into<String>) -> Self {
        Self {
            window_type: WindowType::Monthly,
            timestamp_column: timestamp_column.into(),
        }
    }

    pub fn custom(timestamp_column: impl Into<String>, start_ms: u64, end_ms: u64) -> Self {
        Self {
            window_type: WindowType::Custom { start_ms, end_ms },
            timestamp_column: timestamp_column.into(),
        }
    }

    /// Returns `true` if this window has no filtering (all entries visible).
    pub fn is_unwindowed(&self) -> bool {
        self.window_type == WindowType::None
    }

    /// Compute the start of the current window (inclusive) given `now_ms`.
    ///
    /// Returns `None` for `WindowType::None` (no filtering).
    /// Returns `Some(start_ms)` for all other types — entries with timestamp
    /// >= start_ms are in the current window.
    pub fn window_start(&self, now_ms: u64) -> Option<u64> {
        match &self.window_type {
            WindowType::None => None,
            WindowType::Daily => Some(start_of_day_utc(now_ms)),
            WindowType::Weekly => Some(start_of_week_utc(now_ms)),
            WindowType::Monthly => Some(start_of_month_utc(now_ms)),
            WindowType::Custom { start_ms, end_ms } => {
                if now_ms >= *start_ms && now_ms < *end_ms {
                    Some(*start_ms)
                } else {
                    // Outside the custom window — no entries visible.
                    Some(u64::MAX)
                }
            }
        }
    }

    /// Compute the window start for a specific target timestamp (for historical queries).
    pub fn window_start_at(&self, target_ms: u64) -> Option<u64> {
        self.window_start(target_ms)
    }
}

// ── Time boundary helpers ──────────────────────────────────────────────

const MS_PER_SECOND: u64 = 1_000;
const MS_PER_DAY: u64 = 86_400 * MS_PER_SECOND;

/// Start of the UTC day containing `now_ms`.
fn start_of_day_utc(now_ms: u64) -> u64 {
    (now_ms / MS_PER_DAY) * MS_PER_DAY
}

/// Start of the UTC week (Monday 00:00) containing `now_ms`.
fn start_of_week_utc(now_ms: u64) -> u64 {
    // Unix epoch (1970-01-01) was a Thursday. Monday is day 4 of that week.
    // Days since epoch.
    let days_since_epoch = now_ms / MS_PER_DAY;
    // Day of week: 0=Thursday, 1=Friday, ..., 3=Sunday, 4=Monday, ...
    // Shift so 0=Monday: (days + 3) % 7.
    let day_of_week = (days_since_epoch + 3) % 7; // 0=Monday, 6=Sunday
    let monday = days_since_epoch - day_of_week;
    monday * MS_PER_DAY
}

/// Start of the UTC month containing `now_ms`.
///
/// Uses a simple calendar computation. Handles leap years.
fn start_of_month_utc(now_ms: u64) -> u64 {
    let secs = now_ms / MS_PER_SECOND;
    // Use a simple approach: compute year/month from seconds since epoch.
    let (year, month, _day) = seconds_to_ymd(secs);
    ymd_to_ms(year, month, 1)
}

/// Convert seconds since epoch to (year, month, day).
fn seconds_to_ymd(secs: u64) -> (i32, u32, u32) {
    // Civil calendar from days. Algorithm from Howard Hinnant.
    let days = (secs / 86400) as i64;
    let z = days + 719468;
    let era = (if z >= 0 { z } else { z - 146096 }) / 146097;
    let doe = (z - era * 146097) as u64; // day of era [0, 146096]
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365;
    let y = yoe as i64 + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let y = if m <= 2 { y + 1 } else { y };
    (y as i32, m as u32, d as u32)
}

/// Convert (year, month, day) to milliseconds since epoch.
fn ymd_to_ms(year: i32, month: u32, day: u32) -> u64 {
    let y = if month <= 2 {
        year as i64 - 1
    } else {
        year as i64
    };
    let m = if month <= 2 {
        month as i64 + 9
    } else {
        month as i64 - 3
    };
    let era = (if y >= 0 { y } else { y - 399 }) / 400;
    let yoe = (y - era * 400) as u64;
    let doy = (153 * m as u64 + 2) / 5 + day as u64 - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    let days = era * 146097 + doe as i64 - 719468;
    (days as u64) * MS_PER_DAY
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn unwindowed_always_visible() {
        let config = WindowConfig::none();
        assert!(config.is_unwindowed());
        assert!(config.window_start(1000).is_none());
    }

    #[test]
    fn daily_window_start() {
        // 2024-05-02 15:30:00 UTC = 1714664200000 ms
        let now_ms = 1714664200000u64;
        let config = WindowConfig::daily("updated_at");
        let start = config.window_start(now_ms).unwrap();
        // Should be 2024-05-02 00:00:00 UTC = 1714608000000 ms
        assert_eq!(start, start_of_day_utc(now_ms));
        assert!(start <= now_ms);
        assert!(now_ms - start < MS_PER_DAY);
    }

    #[test]
    fn weekly_window_start() {
        // 2024-05-02 (Thursday) 15:30:00 UTC
        let now_ms = 1714664200000u64;
        let config = WindowConfig::weekly("updated_at");
        let start = config.window_start(now_ms).unwrap();
        // Monday of that week: 2024-04-29 00:00:00 UTC
        assert!(start <= now_ms);
        assert!(now_ms - start < 7 * MS_PER_DAY);
        // Verify it's a Monday.
        let start_days = start / MS_PER_DAY;
        let dow = (start_days + 3) % 7; // 0=Monday
        assert_eq!(dow, 0, "window start should be Monday");
    }

    #[test]
    fn monthly_window_start() {
        // 2024-05-15 12:00:00 UTC
        let now_ms = 1715774400000u64;
        let config = WindowConfig::monthly("updated_at");
        let start = config.window_start(now_ms).unwrap();
        // Should be 2024-05-01 00:00:00 UTC
        let (y, m, d) = seconds_to_ymd(start / MS_PER_SECOND);
        assert_eq!(y, 2024);
        assert_eq!(m, 5);
        assert_eq!(d, 1);
    }

    #[test]
    fn custom_window() {
        let start = 1000u64;
        let end = 2000u64;
        let config = WindowConfig::custom("ts", start, end);

        // Inside window.
        assert_eq!(config.window_start(1500), Some(1000));

        // Outside window.
        assert_eq!(config.window_start(2500), Some(u64::MAX));
    }

    #[test]
    fn ymd_roundtrip() {
        // 2024-01-01 00:00:00 UTC
        let ms = ymd_to_ms(2024, 1, 1);
        let (y, m, d) = seconds_to_ymd(ms / MS_PER_SECOND);
        assert_eq!((y, m, d), (2024, 1, 1));

        // 1970-01-01 (epoch)
        let ms = ymd_to_ms(1970, 1, 1);
        assert_eq!(ms, 0);

        // 2000-02-29 (leap year)
        let ms = ymd_to_ms(2000, 2, 29);
        let (y, m, d) = seconds_to_ymd(ms / MS_PER_SECOND);
        assert_eq!((y, m, d), (2000, 2, 29));
    }

    #[test]
    fn start_of_day_boundary() {
        // Exactly midnight.
        let midnight = 1714608000000u64; // Some midnight
        assert_eq!(start_of_day_utc(midnight), midnight);
        // One ms before midnight.
        assert_eq!(start_of_day_utc(midnight - 1), midnight - MS_PER_DAY);
    }
}
