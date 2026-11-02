// SPDX-License-Identifier: Apache-2.0

//! Partition types: metadata, state, interval, and flushed data.

use std::collections::HashMap;

use serde::{Deserialize, Serialize};
use zerompk::{FromMessagePack, ToMessagePack};

use super::ingest::TimeRange;

/// Error parsing a partition interval string (e.g., "1h", "3d").
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum IntervalParseError {
    #[error("invalid interval format: '{input}' — expected format like '1h', '3d', '1w'")]
    InvalidFormat { input: String },

    #[error("invalid number in interval: '{input}'")]
    InvalidNumber { input: String },

    #[error("partition interval must be > 0")]
    ZeroInterval,

    #[error("unknown unit '{unit}': expected s, m, h, d, w, M, y")]
    UnknownUnit { unit: String },

    #[error("unsupported calendar interval '{input}': {hint}")]
    UnsupportedCalendar { input: String, hint: &'static str },
}

/// Lifecycle state of a partition in the partition manifest.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, ToMessagePack, FromMessagePack,
)]
#[non_exhaustive]
pub enum PartitionState {
    /// Actively receiving writes.
    Active,
    /// Immutable, compactable, archivable.
    Sealed,
    /// Being merged into a larger partition (transient).
    Merging,
    /// Result of a merge operation.
    Merged,
    /// Marked for deletion (sources of a completed merge).
    Deleted,
    /// Uploaded to S3/cold storage.
    Archived,
}

/// Metadata for a single time partition.
///
/// Stored in the partition manifest (redb). Shared between Origin and Lite.
#[derive(Debug, Clone, Serialize, Deserialize, ToMessagePack, FromMessagePack)]
pub struct PartitionMeta {
    /// Inclusive lower bound of timestamps in this partition.
    pub min_ts: i64,
    /// Inclusive upper bound of timestamps in this partition.
    pub max_ts: i64,
    /// Number of rows.
    pub row_count: u64,
    /// On-disk size in bytes (all column files combined).
    pub size_bytes: u64,
    /// Schema version — incremented on column add/drop/rename.
    pub schema_version: u32,
    /// Current lifecycle state.
    pub state: PartitionState,
    /// The partition interval duration in milliseconds that produced this partition.
    pub interval_ms: u64,
    /// WAL LSN at last successful flush to this partition.
    pub last_flushed_wal_lsn: u64,
    /// Per-column statistics (codec, min/max/sum/count/cardinality).
    #[serde(default, skip_serializing_if = "HashMap::is_empty")]
    pub column_stats: HashMap<String, nodedb_codec::ColumnStatistics>,
    /// The newest `_ts_system` system-time ordinal across rows in this
    /// partition, 0 when no row carries one. Retention on bitemporal
    /// collections uses its millisecond instead of `max_ts`, so late-arriving
    /// backfill survives an event-time-based TTL.
    #[serde(default)]
    pub max_system_ts: i64,
    /// The system-time ordinal a `TRUNCATE` ended every row of this partition
    /// at. `None` while the rows are current. An ended partition stays as
    /// history: a read before the end still sees its rows.
    #[serde(default)]
    pub ended_at: Option<i64>,
}

impl PartitionMeta {
    /// Whether this partition's time range overlaps a query range.
    pub fn overlaps(&self, range: &TimeRange) -> bool {
        self.min_ts <= range.end_ms && range.start_ms <= self.max_ts
    }

    /// Whether a read at system-time ordinal `at` sees rows of this
    /// partition past any `TRUNCATE` end. `None` reads the current rows,
    /// which an ended partition no longer holds.
    pub fn open_at(&self, at: Option<i64>) -> bool {
        match (self.ended_at, at) {
            (None, _) => true,
            (Some(_), None) => false,
            (Some(end), Some(at)) => end > at,
        }
    }

    /// Whether this partition is queryable (Active, Sealed, or Merged).
    pub fn is_queryable(&self) -> bool {
        matches!(
            self.state,
            PartitionState::Active | PartitionState::Sealed | PartitionState::Merged
        )
    }
}

/// Partition interval — how wide each time partition is.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[non_exhaustive]
pub enum PartitionInterval {
    /// Duration in milliseconds (e.g., 3600000 for 1h, 86400000 for 1d).
    Duration(u64),
    /// Calendar month (variable length).
    Month,
    /// Calendar year (variable length).
    Year,
    /// Single partition, split only when size threshold is reached.
    Unbounded,
    /// Engine chooses adaptively based on ingest rate and partition row count.
    Auto,
}

impl PartitionInterval {
    /// Parse a duration string like "1h", "3d", "1w", "1M", "1y", "AUTO", "UNBOUNDED".
    pub fn parse(s: &str) -> Result<Self, IntervalParseError> {
        let s = s.trim();
        match s.to_uppercase().as_str() {
            "AUTO" => return Ok(Self::Auto),
            "UNBOUNDED" | "NONE" => return Ok(Self::Unbounded),
            _ => {}
        }

        if s.ends_with('M') && s.len() > 1 && s[..s.len() - 1].chars().all(|c| c.is_ascii_digit()) {
            let n: u64 = s[..s.len() - 1]
                .parse()
                .map_err(|_| IntervalParseError::InvalidNumber { input: s.into() })?;
            if n != 1 {
                return Err(IntervalParseError::UnsupportedCalendar {
                    input: s.into(),
                    hint: "only '1M' (one calendar month) is supported",
                });
            }
            return Ok(Self::Month);
        }

        if s.ends_with('y') && s.len() > 1 && s[..s.len() - 1].chars().all(|c| c.is_ascii_digit()) {
            let n: u64 = s[..s.len() - 1]
                .parse()
                .map_err(|_| IntervalParseError::InvalidNumber { input: s.into() })?;
            if n != 1 {
                return Err(IntervalParseError::UnsupportedCalendar {
                    input: s.into(),
                    hint: "only '1y' (one calendar year) is supported",
                });
            }
            return Ok(Self::Year);
        }

        let (num_str, unit) = if s.len() > 1 && s.as_bytes()[s.len() - 1].is_ascii_alphabetic() {
            (&s[..s.len() - 1], &s[s.len() - 1..])
        } else {
            return Err(IntervalParseError::InvalidFormat { input: s.into() });
        };

        let n: u64 = num_str
            .parse()
            .map_err(|_| IntervalParseError::InvalidNumber { input: s.into() })?;
        if n == 0 {
            return Err(IntervalParseError::ZeroInterval);
        }

        let ms = match unit {
            "s" => n * 1_000,
            "m" => n * 60_000,
            "h" => n * 3_600_000,
            "d" => n * 86_400_000,
            "w" => n * 604_800_000,
            _ => {
                return Err(IntervalParseError::UnknownUnit { unit: unit.into() });
            }
        };

        Ok(Self::Duration(ms))
    }

    /// Duration in milliseconds, if fixed-duration.
    pub fn as_millis(&self) -> Option<u64> {
        match self {
            Self::Duration(ms) => Some(*ms),
            _ => None,
        }
    }
}

impl std::fmt::Display for PartitionInterval {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Duration(ms) => {
                if *ms % 604_800_000 == 0 {
                    write!(f, "{}w", ms / 604_800_000)
                } else if *ms % 86_400_000 == 0 {
                    write!(f, "{}d", ms / 86_400_000)
                } else if *ms % 3_600_000 == 0 {
                    write!(f, "{}h", ms / 3_600_000)
                } else if *ms % 60_000 == 0 {
                    write!(f, "{}m", ms / 60_000)
                } else {
                    write!(f, "{}s", ms / 1_000)
                }
            }
            Self::Month => write!(f, "1M"),
            Self::Year => write!(f, "1y"),
            Self::Unbounded => write!(f, "UNBOUNDED"),
            Self::Auto => write!(f, "AUTO"),
        }
    }
}

/// Segment file reference for the L1/L2 index.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SegmentRef {
    pub path: String,
    pub min_ts: i64,
    pub max_ts: i64,
    pub kind: SegmentKind,
    pub size_bytes: u64,
    pub created_at_ms: i64,
}

/// Whether a segment contains metrics or logs.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[non_exhaustive]
pub enum SegmentKind {
    Metric,
    Log,
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::*;

    #[test]
    fn partition_interval_parse() {
        assert_eq!(
            PartitionInterval::parse("1h").unwrap(),
            PartitionInterval::Duration(3_600_000)
        );
        assert_eq!(
            PartitionInterval::parse("3d").unwrap(),
            PartitionInterval::Duration(3 * 86_400_000)
        );
        assert_eq!(
            PartitionInterval::parse("2w").unwrap(),
            PartitionInterval::Duration(2 * 604_800_000)
        );
        assert_eq!(
            PartitionInterval::parse("1M").unwrap(),
            PartitionInterval::Month
        );
        assert_eq!(
            PartitionInterval::parse("1y").unwrap(),
            PartitionInterval::Year
        );
        assert_eq!(
            PartitionInterval::parse("AUTO").unwrap(),
            PartitionInterval::Auto
        );
        assert_eq!(
            PartitionInterval::parse("UNBOUNDED").unwrap(),
            PartitionInterval::Unbounded
        );
        assert!(matches!(
            PartitionInterval::parse("0h"),
            Err(IntervalParseError::ZeroInterval)
        ));
        assert!(matches!(
            PartitionInterval::parse("2M"),
            Err(IntervalParseError::UnsupportedCalendar { .. })
        ));
    }

    #[test]
    fn partition_interval_display_roundtrip() {
        let cases = ["1h", "3d", "2w", "1M", "1y", "AUTO", "UNBOUNDED"];
        for s in cases {
            let parsed = PartitionInterval::parse(s).unwrap();
            let displayed = parsed.to_string();
            let reparsed = PartitionInterval::parse(&displayed).unwrap();
            assert_eq!(parsed, reparsed, "roundtrip failed for {s}");
        }
    }

    #[test]
    fn partition_meta_queryable() {
        let meta = PartitionMeta {
            min_ts: 1000,
            max_ts: 2000,
            row_count: 500,
            size_bytes: 1024,
            schema_version: 1,
            state: PartitionState::Sealed,
            interval_ms: 86_400_000,
            last_flushed_wal_lsn: 42,
            column_stats: HashMap::new(),
            max_system_ts: 0,
            ended_at: None,
        };
        assert!(meta.is_queryable());
        assert!(meta.overlaps(&TimeRange::new(1500, 2500)));
        assert!(!meta.overlaps(&TimeRange::new(3000, 4000)));
    }

    #[test]
    fn partition_meta_not_queryable_when_deleted() {
        let meta = PartitionMeta {
            min_ts: 0,
            max_ts: 0,
            row_count: 0,
            size_bytes: 0,
            schema_version: 1,
            state: PartitionState::Deleted,
            interval_ms: 0,
            last_flushed_wal_lsn: 0,
            column_stats: HashMap::new(),
            max_system_ts: 0,
            ended_at: None,
        };
        assert!(!meta.is_queryable());
    }

    #[test]
    fn an_ended_partition_is_open_only_before_its_end() {
        let mut meta = PartitionMeta {
            min_ts: 0,
            max_ts: 0,
            row_count: 1,
            size_bytes: 0,
            schema_version: 1,
            state: PartitionState::Sealed,
            interval_ms: 0,
            last_flushed_wal_lsn: 0,
            column_stats: HashMap::new(),
            max_system_ts: 5,
            ended_at: None,
        };
        assert!(meta.open_at(None) && meta.open_at(Some(1)));
        meta.ended_at = Some(10);
        assert!(
            !meta.open_at(None),
            "a current read skips an ended partition"
        );
        assert!(meta.open_at(Some(9)));
        assert!(!meta.open_at(Some(10)));
    }
}
