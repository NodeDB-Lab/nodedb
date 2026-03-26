//! Shared timeseries types for multi-model database engines.
//!
//! Used by both `nodedb` (server) and `nodedb-lite` (embedded) for
//! timeseries ingest, storage, and query. Edge devices record sensor
//! telemetry, event logs, and metrics using these types.

use std::collections::HashMap;
use std::hash::{DefaultHasher, Hash, Hasher};

use serde::{Deserialize, Serialize};

// ---------------------------------------------------------------------------
// Error types
// ---------------------------------------------------------------------------

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

/// Error validating a `TieredPartitionConfig`.
#[derive(Debug, thiserror::Error)]
#[error("config validation: {field} — {reason}")]
pub struct ConfigValidationError {
    pub field: String,
    pub reason: String,
}

// ---------------------------------------------------------------------------
// SeriesId — identity
// ---------------------------------------------------------------------------

/// Unique identifier for a timeseries (hash of metric name + sorted tag set).
pub type SeriesId = u64;

/// The canonical key for a series — used for collision detection in the
/// series catalog. Two `SeriesKey`s that hash to the same `SeriesId` are
/// a collision; the catalog rehashes with a salt.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct SeriesKey {
    pub metric: String,
    pub tags: Vec<(String, String)>,
}

impl SeriesKey {
    pub fn new(metric: impl Into<String>, mut tags: Vec<(String, String)>) -> Self {
        tags.sort();
        Self {
            metric: metric.into(),
            tags,
        }
    }

    /// Compute the SeriesId for this key with an optional collision salt.
    pub fn to_series_id(&self, salt: u64) -> SeriesId {
        let mut hasher = DefaultHasher::new();
        salt.hash(&mut hasher);
        self.metric.hash(&mut hasher);
        for (k, v) in &self.tags {
            k.hash(&mut hasher);
            v.hash(&mut hasher);
        }
        hasher.finish()
    }
}

/// Persistent catalog that maps SeriesId → SeriesKey with collision detection.
///
/// On insert, if the SeriesId already maps to a *different* SeriesKey, the
/// catalog rehashes with incrementing salts until it finds a free slot.
/// This is one lookup per new series (not per row).
#[derive(Debug, Default, Serialize, Deserialize)]
pub struct SeriesCatalog {
    /// SeriesId → (SeriesKey, salt used to produce this ID).
    entries: HashMap<SeriesId, (SeriesKey, u64)>,
}

impl SeriesCatalog {
    pub fn new() -> Self {
        Self::default()
    }

    /// Resolve a SeriesKey to its SeriesId, registering it if new.
    ///
    /// Returns the SeriesId (potentially rehashed if the natural hash collided).
    pub fn resolve(&mut self, key: &SeriesKey) -> SeriesId {
        // Try salt = 0 first (the common case).
        let mut salt = 0u64;
        loop {
            let id = key.to_series_id(salt);
            match self.entries.get(&id) {
                None => {
                    // Free slot — register.
                    self.entries.insert(id, (key.clone(), salt));
                    return id;
                }
                Some((existing_key, _)) if existing_key == key => {
                    // Already registered with this exact key.
                    return id;
                }
                Some(_) => {
                    // Collision with a different key — try next salt.
                    salt += 1;
                }
            }
        }
    }

    /// Look up a SeriesId to get its canonical key.
    pub fn get(&self, id: SeriesId) -> Option<&SeriesKey> {
        self.entries.get(&id).map(|(k, _)| k)
    }

    /// Number of registered series.
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }
}

// ---------------------------------------------------------------------------
// LiteId — edge instance identity
// ---------------------------------------------------------------------------

/// Persistent identity of a NodeDB-Lite database instance.
///
/// Generated as a CUID2 on first `open()`, stored in redb metadata.
/// Scope = one database file. Not a device ID, user ID, or app ID.
pub type LiteId = String;

// ---------------------------------------------------------------------------
// Battery state (Lite-B mobile)
// ---------------------------------------------------------------------------

/// Battery state reported by the host application for battery-aware flushing.
///
/// The Lite engine uses this to defer disk I/O when battery is low.
/// The application provides this via a callback — NodeDB doesn't read
/// battery state directly (platform-specific API).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub enum BatteryState {
    /// Battery level is sufficient (>50%) or device is on AC power.
    Normal,
    /// Battery is low (<20%) and not charging. Defer non-critical I/O.
    Low,
    /// Device is currently charging. Safe to flush.
    Charging,
    /// Battery state unknown (desktop, non-mobile). Treat as Normal.
    #[default]
    Unknown,
}

impl BatteryState {
    /// Whether flushing should be deferred in battery-aware mode.
    pub fn should_defer_flush(&self) -> bool {
        matches!(self, Self::Low)
    }
}

// ---------------------------------------------------------------------------
// Ingest types
// ---------------------------------------------------------------------------

/// A single metric sample (timestamp + scalar value).
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub struct MetricSample {
    pub timestamp_ms: i64,
    pub value: f64,
}

/// A single log entry (timestamp + arbitrary bytes).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LogEntry {
    pub timestamp_ms: i64,
    pub data: Vec<u8>,
}

/// Result of an ingest operation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IngestResult {
    /// Write accepted, memtable healthy.
    Ok,
    /// Write accepted, but memtable should be flushed (memory pressure).
    FlushNeeded,
    /// Write rejected — memory budget exhausted and cannot evict further.
    /// Caller should apply backpressure.
    Rejected,
}

impl IngestResult {
    pub fn is_flush_needed(&self) -> bool {
        matches!(self, Self::FlushNeeded)
    }

    pub fn is_rejected(&self) -> bool {
        matches!(self, Self::Rejected)
    }
}

// ---------------------------------------------------------------------------
// Time range
// ---------------------------------------------------------------------------

/// Time range for queries (inclusive on both ends).
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub struct TimeRange {
    pub start_ms: i64,
    pub end_ms: i64,
}

impl TimeRange {
    pub fn new(start_ms: i64, end_ms: i64) -> Self {
        Self { start_ms, end_ms }
    }

    pub fn contains(&self, ts: i64) -> bool {
        ts >= self.start_ms && ts <= self.end_ms
    }

    /// Whether two ranges overlap.
    pub fn overlaps(&self, other: &TimeRange) -> bool {
        self.start_ms <= other.end_ms && other.start_ms <= self.end_ms
    }
}

// ---------------------------------------------------------------------------
// Symbol dictionary
// ---------------------------------------------------------------------------

/// Bidirectional symbol dictionary for tag value interning.
///
/// Tag columns store 4-byte u32 IDs instead of full strings. Shared by
/// Origin columnar segments, Lite native segments, and WASM segments.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct SymbolDictionary {
    /// String → symbol ID.
    forward: HashMap<String, u32>,
    /// Symbol ID → string (index = id).
    reverse: Vec<String>,
}

impl SymbolDictionary {
    pub fn new() -> Self {
        Self::default()
    }

    /// Resolve a string to its symbol ID, inserting if new.
    ///
    /// Returns `None` if the dictionary has reached `max_cardinality`.
    pub fn resolve(&mut self, value: &str, max_cardinality: u32) -> Option<u32> {
        if let Some(&id) = self.forward.get(value) {
            return Some(id);
        }
        if self.reverse.len() as u32 >= max_cardinality {
            return None; // Cardinality breaker tripped.
        }
        let id = self.reverse.len() as u32;
        self.forward.insert(value.to_string(), id);
        self.reverse.push(value.to_string());
        Some(id)
    }

    /// Look up a string by symbol ID.
    pub fn get(&self, id: u32) -> Option<&str> {
        self.reverse.get(id as usize).map(|s| s.as_str())
    }

    /// Look up a symbol ID by string.
    pub fn get_id(&self, value: &str) -> Option<u32> {
        self.forward.get(value).copied()
    }

    /// Number of symbols.
    pub fn len(&self) -> usize {
        self.reverse.len()
    }

    pub fn is_empty(&self) -> bool {
        self.reverse.is_empty()
    }

    /// Merge another dictionary into this one.
    ///
    /// Returns a remap table: `old_id → new_id` for the source dictionary.
    pub fn merge(&mut self, other: &SymbolDictionary, max_cardinality: u32) -> Vec<u32> {
        let mut remap = Vec::with_capacity(other.reverse.len());
        for symbol in &other.reverse {
            match self.resolve(symbol, max_cardinality) {
                Some(new_id) => remap.push(new_id),
                None => {
                    // Cardinality exceeded during merge — map to sentinel.
                    remap.push(u32::MAX);
                }
            }
        }
        remap
    }
}

// ---------------------------------------------------------------------------
// Sync delta
// ---------------------------------------------------------------------------

/// Wire format for Lite→Origin timeseries delta exchange.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TimeseriesDelta {
    /// Source Lite instance ID (CUID2).
    pub source_id: LiteId,
    /// Series identifier (metric + tags hash).
    pub series_id: SeriesId,
    /// Canonical series key for the source (without `__source` tag — Origin adds that).
    pub series_key: SeriesKey,
    /// Minimum timestamp in this block.
    pub min_ts: i64,
    /// Maximum timestamp in this block.
    pub max_ts: i64,
    /// Gorilla-encoded compressed samples.
    pub encoded_block: Vec<u8>,
    /// Number of samples in the block.
    pub sample_count: u64,
}

// ---------------------------------------------------------------------------
// Partition types
// ---------------------------------------------------------------------------

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
    /// Different partitions may have different widths after a config change.
    pub interval_ms: u64,
    /// WAL LSN at last successful flush to this partition.
    /// Used for crash recovery — replay WAL records with LSN > this value.
    pub last_flushed_wal_lsn: u64,
    /// Per-column statistics (codec, min/max/sum/count/cardinality).
    /// Keyed by column name. Populated at flush time. Empty for legacy partitions.
    #[serde(default, skip_serializing_if = "HashMap::is_empty")]
    pub column_stats: HashMap<String, nodedb_codec::ColumnStatistics>,
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

// ---------------------------------------------------------------------------
// Partition interval
// ---------------------------------------------------------------------------

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

        // Duration: Nh, Nd, Nw, Ns, Nm (minutes)
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

    /// Duration in milliseconds, if fixed-duration. Returns None for Month/Year/Unbounded/Auto.
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

// ---------------------------------------------------------------------------
// Tiered partition config
// ---------------------------------------------------------------------------

/// Full lifecycle configuration for a timeseries collection.
///
/// All fields have sensible defaults. Platform-aware: Origin and Lite
/// use different defaults for memory budgets and retention.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TieredPartitionConfig {
    // -- Tier 0: Hot (RAM memtable) --
    /// Flush interval in milliseconds.
    pub memtable_flush_interval_ms: u64,
    /// Maximum memtable memory in bytes.
    pub memtable_max_memory_bytes: u64,

    // -- Tier 1: Warm (NVMe, columnar partitions) --
    /// Partition interval (how wide each partition is).
    pub partition_by: PartitionInterval,
    /// Merge sealed partitions older than this (milliseconds).
    pub merge_after_ms: u64,
    /// How many consecutive partitions to merge at a time.
    pub merge_count: u32,

    // -- Tier 2: Cold (S3 Parquet) --
    /// Archive partitions older than this (milliseconds). 0 = never.
    pub archive_after_ms: u64,
    /// Compression codec for archived partitions.
    pub archive_compression: ArchiveCompression,

    // -- Retention --
    /// Drop partitions older than this (milliseconds). 0 = infinite.
    pub retention_period_ms: u64,

    // -- Timestamp --
    /// Explicit timestamp column name. Empty = auto-detect.
    pub timestamp_column: String,

    // -- Tags --
    /// Maximum cardinality per tag column before the circuit breaker trips.
    pub max_tag_cardinality: u32,

    // -- WAL --
    /// Whether writes are WAL'd for crash safety.
    pub wal_enabled: bool,

    // -- CDC --
    /// Whether timeseries writes publish to ChangeStream.
    pub cdc_enabled: bool,

    // -- Sync (Lite only) --
    /// Pre-sync downsampling resolution in milliseconds.
    /// 0 = send raw samples. >0 = average samples within each window before
    /// writing to memtable. E.g., 10000 = 10-second resolution.
    pub sync_resolution_ms: u64,
    /// Sync batch interval in milliseconds.
    pub sync_interval_ms: u64,
    /// Whether to retain unsynced data past the retention period.
    pub retain_until_synced: bool,

    // -- Battery (Lite-B mobile only) --
    /// Whether to defer flushes on low battery. Default: false.
    /// When true and battery < 20%, memtable limit is doubled to defer I/O.
    /// Flush resumes when charging or battery > 50%.
    #[serde(default)]
    pub battery_aware: bool,

    // -- Burst ingestion (Lite-C) --
    /// Maximum rows to accumulate before flushing in bulk import mode.
    /// 0 = disabled (use normal memtable limits). When burst ingestion is
    /// detected (rate > 10x normal for > 30s), this overrides memtable_max_memory_bytes.
    #[serde(default)]
    pub bulk_import_threshold_rows: u64,
}

/// Compression codec for archived (cold) partitions.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub enum ArchiveCompression {
    #[default]
    Zstd,
    Lz4,
    Snappy,
}

impl TieredPartitionConfig {
    /// Default configuration for Origin (cloud server).
    pub fn origin_defaults() -> Self {
        Self {
            memtable_flush_interval_ms: 10_000,          // 10s
            memtable_max_memory_bytes: 64 * 1024 * 1024, // 64MB
            partition_by: PartitionInterval::Auto,
            merge_after_ms: 30 * 86_400_000, // 30d
            merge_count: 10,
            archive_after_ms: 0,    // never
            retention_period_ms: 0, // infinite
            archive_compression: ArchiveCompression::Zstd,
            timestamp_column: String::new(), // auto-detect
            max_tag_cardinality: 100_000,
            wal_enabled: true,
            cdc_enabled: false,
            sync_resolution_ms: 0,
            sync_interval_ms: 0,
            retain_until_synced: false,
            battery_aware: false,
            bulk_import_threshold_rows: 0,
        }
    }

    /// Default configuration for Lite (edge device).
    pub fn lite_defaults() -> Self {
        Self {
            memtable_flush_interval_ms: 30_000,         // 30s
            memtable_max_memory_bytes: 4 * 1024 * 1024, // 4MB
            partition_by: PartitionInterval::Auto,
            merge_after_ms: 7 * 86_400_000, // 7d
            merge_count: 4,
            archive_after_ms: 0,                 // N/A on Lite
            retention_period_ms: 7 * 86_400_000, // 7d
            archive_compression: ArchiveCompression::Zstd,
            timestamp_column: String::new(),
            max_tag_cardinality: 10_000, // tighter on edge
            wal_enabled: true,
            cdc_enabled: false,
            sync_resolution_ms: 0,    // raw by default
            sync_interval_ms: 30_000, // 30s
            retain_until_synced: false,
            battery_aware: false,                  // opt-in on mobile
            bulk_import_threshold_rows: 1_000_000, // 1M rows for Lite bulk import
        }
    }

    /// Validate configuration consistency.
    pub fn validate(&self) -> Result<(), ConfigValidationError> {
        if self.merge_count < 2 {
            return Err(ConfigValidationError {
                field: "merge_count".into(),
                reason: "must be >= 2".into(),
            });
        }
        if self.retention_period_ms > 0
            && self.archive_after_ms > 0
            && self.retention_period_ms < self.archive_after_ms
        {
            return Err(ConfigValidationError {
                field: "retention_period".into(),
                reason: "must be >= archive_after".into(),
            });
        }
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// WAL payload types
// ---------------------------------------------------------------------------

/// WAL record payload for a timeseries metric batch.
///
/// One WAL record per ingest batch. On crash recovery, replay all records
/// with LSN > `last_flushed_wal_lsn` per partition.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TimeseriesWalBatch {
    /// Collection name.
    pub collection: String,
    /// Batch of metric samples: (series_id, timestamp_ms, value).
    pub samples: Vec<(SeriesId, i64, f64)>,
}

/// WAL record payload for a timeseries log batch.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LogWalBatch {
    /// Collection name.
    pub collection: String,
    /// Batch of log entries: (series_id, timestamp_ms, data).
    pub entries: Vec<(SeriesId, i64, Vec<u8>)>,
}

// ---------------------------------------------------------------------------
// Flushed data (existing types, preserved)
// ---------------------------------------------------------------------------

/// Data from a single series after memtable drain.
#[derive(Debug)]
pub struct FlushedSeries {
    pub series_id: SeriesId,
    pub kind: FlushedKind,
    pub min_ts: i64,
    pub max_ts: i64,
}

/// Type-specific flushed data.
#[derive(Debug)]
pub enum FlushedKind {
    Metric {
        /// Gorilla-compressed block.
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
    /// On-disk size in bytes.
    pub size_bytes: u64,
    /// Timestamp when segment was created (for retention).
    pub created_at_ms: i64,
}

/// Whether a segment contains metrics or logs.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum SegmentKind {
    Metric,
    Log,
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn series_key_sorted_tags() {
        let k1 = SeriesKey::new(
            "cpu",
            vec![("host".into(), "a".into()), ("dc".into(), "us".into())],
        );
        let k2 = SeriesKey::new(
            "cpu",
            vec![("dc".into(), "us".into()), ("host".into(), "a".into())],
        );
        assert_eq!(k1, k2);
        assert_eq!(k1.to_series_id(0), k2.to_series_id(0));
    }

    #[test]
    fn series_catalog_resolve() {
        let mut catalog = SeriesCatalog::new();
        let k = SeriesKey::new("cpu", vec![("host".into(), "prod-1".into())]);
        let id1 = catalog.resolve(&k);
        let id2 = catalog.resolve(&k);
        assert_eq!(id1, id2);
        assert_eq!(catalog.len(), 1);
    }

    #[test]
    fn series_catalog_different_keys() {
        let mut catalog = SeriesCatalog::new();
        let k1 = SeriesKey::new("cpu", vec![("host".into(), "a".into())]);
        let k2 = SeriesKey::new("mem", vec![("host".into(), "a".into())]);
        let id1 = catalog.resolve(&k1);
        let id2 = catalog.resolve(&k2);
        assert_ne!(id1, id2);
        assert_eq!(catalog.len(), 2);
    }

    #[test]
    fn symbol_dictionary_basic() {
        let mut dict = SymbolDictionary::new();
        assert_eq!(dict.resolve("us-east-1", 100_000), Some(0));
        assert_eq!(dict.resolve("us-west-2", 100_000), Some(1));
        assert_eq!(dict.resolve("us-east-1", 100_000), Some(0)); // existing
        assert_eq!(dict.len(), 2);
        assert_eq!(dict.get(0), Some("us-east-1"));
        assert_eq!(dict.get(1), Some("us-west-2"));
        assert_eq!(dict.get_id("us-east-1"), Some(0));
    }

    #[test]
    fn symbol_dictionary_cardinality_breaker() {
        let mut dict = SymbolDictionary::new();
        for i in 0..100 {
            assert!(dict.resolve(&format!("val-{i}"), 100).is_some());
        }
        // 101st should be rejected.
        assert!(dict.resolve("one-too-many", 100).is_none());
        assert_eq!(dict.len(), 100);
    }

    #[test]
    fn symbol_dictionary_merge() {
        let mut d1 = SymbolDictionary::new();
        d1.resolve("a", 1000);
        d1.resolve("b", 1000);

        let mut d2 = SymbolDictionary::new();
        d2.resolve("b", 1000); // overlap
        d2.resolve("c", 1000);

        let remap = d1.merge(&d2, 1000);
        assert_eq!(d1.len(), 3); // a, b, c
        assert_eq!(remap[0], d1.get_id("b").unwrap()); // d2's "b" (id 0) → d1's "b" (id 1)
        assert_eq!(remap[1], d1.get_id("c").unwrap()); // d2's "c" (id 1) → d1's "c" (id 2)
    }

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
        };
        assert!(!meta.is_queryable());
    }

    #[test]
    fn tiered_config_validation() {
        let mut cfg = TieredPartitionConfig::origin_defaults();
        assert!(cfg.validate().is_ok());

        cfg.merge_count = 1;
        let err = cfg.validate().unwrap_err();
        assert_eq!(err.field, "merge_count");

        cfg.merge_count = 10;
        cfg.retention_period_ms = 1000;
        cfg.archive_after_ms = 2000;
        let err = cfg.validate().unwrap_err();
        assert_eq!(err.field, "retention_period");
    }

    #[test]
    fn time_range_overlap() {
        let r1 = TimeRange::new(100, 200);
        let r2 = TimeRange::new(150, 250);
        let r3 = TimeRange::new(300, 400);
        assert!(r1.overlaps(&r2));
        assert!(!r1.overlaps(&r3));
    }

    #[test]
    fn timeseries_delta_serialization() {
        let delta = TimeseriesDelta {
            source_id: "clxyz1234test".into(),
            series_id: 12345,
            series_key: SeriesKey::new("cpu", vec![("host".into(), "prod".into())]),
            min_ts: 1000,
            max_ts: 2000,
            encoded_block: vec![1, 2, 3, 4],
            sample_count: 100,
        };
        let json = serde_json::to_string(&delta).unwrap();
        let back: TimeseriesDelta = serde_json::from_str(&json).unwrap();
        assert_eq!(back.source_id, "clxyz1234test");
        assert_eq!(back.series_id, 12345);
        assert_eq!(back.sample_count, 100);
    }
}
