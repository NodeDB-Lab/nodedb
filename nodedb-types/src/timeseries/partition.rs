//! Partition types: metadata, state, interval, and flushed data.

use std::collections::HashMap;

use serde::{Deserialize, Serialize};

use super::ingest::{LogEntry, TimeRange};

/// Error parsing a partition interval string (e.g., "1h", "3d").
#[derive(Debug, thiserror::Error)]
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
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
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
#[derive(Debug, Clone, Serialize, Deserialize)]
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
    /// Maximum `_ts_system` value across rows in this partition
    /// (bitemporal only; 0 for non-bitemporal partitions). Retention on
    /// bitemporal collections uses this instead of `max_ts` so that
    /// late-arriving backfill survives an event-time-based TTL.
    #[serde(default)]
    pub max_system_ts: i64,
}

impl PartitionMeta {
    /// Whether this partition's time range overlaps a query range.
    pub fn overlaps(&self, range: &TimeRange) -> bool {
        self.min_ts <= range.end_ms && range.start_ms <= self.max_ts
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

/// Data from a single series after memtable drain.
#[derive(Debug)]
pub struct FlushedSeries {
    pub series_id: super::series::SeriesId,
    pub kind: FlushedKind,
    pub min_ts: i64,
    pub max_ts: i64,
}

/// Type-specific flushed data.
#[derive(Debug)]
pub enum FlushedKind {
    Metric {
        gorilla_block: Vec<u8>,
        sample_count: u64,
    },
    Log {
        entries: Vec<LogEntry>,
        total_bytes: usize,
    },
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
pub enum SegmentKind {
    Metric,
    Log,
}
