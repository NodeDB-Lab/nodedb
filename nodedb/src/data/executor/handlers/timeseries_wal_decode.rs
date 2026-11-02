// SPDX-License-Identifier: BUSL-1.1

//! Record-level arguments for replaying `TimeseriesBatch` WAL records.

/// Record-level fields for replaying a single columnar WAL batch.
pub(super) struct ColumnarReplayArgs<'a> {
    pub collection: &'a str,
    pub payload: &'a [u8],
    pub record_lsn: u64,
    pub provenance: Option<nodedb_types::sync::wire::SyncProvenance>,
    /// Per-row surrogates index-aligned with `payload` rows. An empty `Vec`
    /// replays the rows unbound, as the live insert that carried none did.
    pub surrogates: Vec<nodedb_types::Surrogate>,
    /// The insert's encoded conflict policy (`crate::wal::ColumnarConflictPolicy`).
    pub conflict_policy: Vec<u8>,
    /// System-time ordinal the live insert stamped its first row with.
    pub system_from: Option<i64>,
    /// The stamps of each row of a RESTORE re-issue, index-aligned with the
    /// payload rows. Empty for every other insert.
    pub restored_versions: Vec<nodedb_types::columnar::RestoredVersion>,
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
    /// The system-time ordinal the record carries
    /// (`RecordHeader::system_ordinal`): the one its live apply stamped.
    pub system_ordinal: Option<i64>,
}
