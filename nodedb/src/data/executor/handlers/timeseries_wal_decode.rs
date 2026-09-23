// SPDX-License-Identifier: BUSL-1.1

//! Record-level arguments for replaying `TimeseriesBatch` WAL records.

/// Record-level fields for replaying a single columnar WAL batch.
pub(super) struct ColumnarReplayArgs<'a> {
    pub collection: &'a str,
    pub payload: &'a [u8],
    pub record_lsn: u64,
    pub provenance: Option<nodedb_types::sync::wire::SyncProvenance>,
    /// Per-row surrogates index-aligned with `payload` rows. An empty `Vec`
    /// falls back to fresh surrogate allocation for legacy records.
    pub surrogates: Vec<nodedb_types::Surrogate>,
    /// The insert's encoded conflict policy (`crate::wal::ColumnarConflictPolicy`).
    pub conflict_policy: Vec<u8>,
}

/// Record-level fields for replaying one timeseries WAL batch.
pub(super) struct TimeseriesReplayArgs<'a> {
    pub collection: &'a str,
    pub payload: &'a [u8],
    pub record_lsn: u64,
    pub provenance: Option<nodedb_types::sync::wire::SyncProvenance>,
    pub format: Option<&'a str>,
    /// The timestamp every untimed row takes, when the record carries one.
    pub default_timestamp_ms: Option<i64>,
}
