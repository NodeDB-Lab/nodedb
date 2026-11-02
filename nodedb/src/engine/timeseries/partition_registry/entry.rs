// SPDX-License-Identifier: BUSL-1.1

//! Partition entry + directory-name formatting.

use nodedb_types::timeseries::PartitionMeta;

/// A partition entry in the registry.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct PartitionEntry {
    pub meta: PartitionMeta,
    /// Directory name for this partition.
    pub dir_name: String,
}

/// The registry handle of a partition: the `min_ts` it was filed under,
/// then its directory name. Directory names are unique in a collection, so
/// two partitions that share a `min_ts` (late or duplicate-timestamp ingest,
/// or an ended partition a restore installs beside a live one) never share
/// a key, and one never replaces the other. Keys order by time first, so a
/// walk over the registry visits partitions in time order.
#[derive(
    Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, serde::Serialize, serde::Deserialize,
)]
pub struct PartitionKey {
    pub min_ts: i64,
    pub dir_name: String,
}

impl PartitionKey {
    /// The key `entry` files under when it joins a registry.
    pub fn of(entry: &PartitionEntry) -> Self {
        Self {
            min_ts: entry.meta.min_ts,
            dir_name: entry.dir_name.clone(),
        }
    }
}

/// Format a partition directory name from start/end timestamps.
///
/// Uses `YYYYMMDD-HHmmss` format (no colons, cross-platform safe).
pub fn format_partition_dir(start_ms: i64, end_ms: i64) -> String {
    format!(
        "ts-{}_{}",
        format_compact_ts(start_ms),
        format_compact_ts(end_ms)
    )
}

/// Format a timestamp as `YYYYMMDD-HHmmss`.
fn format_compact_ts(ms: i64) -> String {
    if ms == i64::MAX {
        return "unbounded".to_string();
    }
    // Simple epoch-to-date conversion (no external dep).
    let secs = ms / 1000;
    let (year, month, day, hour, min, sec) = epoch_to_datetime(secs);
    format!("{year:04}{month:02}{day:02}-{hour:02}{min:02}{sec:02}")
}

/// Convert epoch seconds to (year, month, day, hour, minute, second).
/// Civil calendar via Hinnant's algorithm.
fn epoch_to_datetime(epoch_secs: i64) -> (i32, u32, u32, u32, u32, u32) {
    let secs_in_day = 86400i64;
    let mut days = epoch_secs / secs_in_day;
    let time_of_day = (epoch_secs % secs_in_day + secs_in_day) % secs_in_day;

    let hour = (time_of_day / 3600) as u32;
    let min = ((time_of_day % 3600) / 60) as u32;
    let sec = (time_of_day % 60) as u32;

    // Shift epoch from 1970-01-01 to 0000-03-01.
    days += 719468;
    let era = if days >= 0 { days } else { days - 146096 } / 146097;
    let doe = (days - era * 146097) as u32;
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365;
    let y = yoe as i64 + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let y = if m <= 2 { y + 1 } else { y };

    (y as i32, m, d, hour, min, sec)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn format_partition_dir_test() {
        let dir = format_partition_dir(1_704_067_200_000, 1_704_153_600_000);
        // 2024-01-01 00:00:00 to 2024-01-02 00:00:00
        assert_eq!(dir, "ts-20240101-000000_20240102-000000");
    }
}
