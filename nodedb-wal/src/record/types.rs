//! Record type discriminants.
//!
//! Types 0-255 are reserved for NodeDB core.
//! Types 256+ are available for NodeDB-specific records.
//!
//! Bit 15 (0x8000) marks a record as **required** — unknown required records
//! cause a replay failure. Unknown records without bit 15 are safely skipped.

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u16)]
pub enum RecordType {
    /// No-op / padding record (skipped during replay).
    Noop = 0,

    /// Generic key-value write.
    Put = 1 | 0x8000,

    /// Generic key deletion.
    Delete = 2 | 0x8000,

    /// Vector engine: insert/update embedding.
    VectorPut = 10 | 0x8000,

    /// Vector engine: soft-delete a vector by internal ID.
    VectorDelete = 11 | 0x8000,

    /// Vector engine: set HNSW index parameters for a collection.
    VectorParams = 12 | 0x8000,

    /// CRDT engine: delta application.
    CrdtDelta = 20 | 0x8000,

    /// Timeseries engine: metric sample batch.
    TimeseriesBatch = 30,

    /// Timeseries engine: log entry batch.
    LogBatch = 31,

    /// Array engine: insert/update one or more cells in an array.
    /// Payload: zerompk-encoded `ArrayPutPayload`.
    ArrayPut = 40 | 0x8000,

    /// Array engine: delete one or more cells in an array.
    /// Payload: zerompk-encoded `ArrayDeletePayload`.
    ArrayDelete = 41 | 0x8000,

    /// Array engine: a memtable was flushed to a new on-disk segment.
    /// Replay treats this as a watermark — memtable mutations whose LSN
    /// is <= the flush record's LSN are already durable on the segment
    /// and must not be re-applied to the live memtable.
    /// Payload: zerompk-encoded `ArrayFlushPayload`.
    ArrayFlush = 42 | 0x8000,

    /// Atomic transaction: wraps multiple sub-records into a single WAL
    /// group. On replay, either all sub-records apply or none.
    /// Payload: MessagePack-encoded `Vec<(record_type: u16, payload: Vec<u8>)>`.
    Transaction = 50 | 0x8000,

    /// Checkpoint marker — indicates a consistent snapshot point.
    Checkpoint = 100 | 0x8000,

    /// Collection hard-delete tombstone.
    CollectionTombstoned = 101 | 0x8000,

    /// LSN ↔ wall-clock anchor for bitemporal `system_from_ms` interpolation.
    /// Emitted periodically by the WAL writer. Payload: `LsnMsAnchorPayload`
    /// (fixed 16 bytes, little-endian: `[lsn: u64, wall_ms: i64]`).
    ///
    /// Not required: a replay that skips these records produces a slightly
    /// coarser interpolation table but does not corrupt state.
    LsnMsAnchor = 102,

    /// Bitemporal version purge — drops one or more *superseded* row
    /// versions (those with finite `_ts_valid_until`) once
    /// `audit_retain_ms` has elapsed. Distinct from `Delete`, which
    /// removes the current live row; replay must not conflate them
    /// because a `TemporalPurge` must never delete live state.
    ///
    /// Required: a replay that skipped this record would leave purged
    /// versions resurrected and diverge from the leader's state.
    TemporalPurge = 103 | 0x8000,
}

impl RecordType {
    /// Whether this record type is required (must be understood for correct replay).
    pub fn is_required(raw: u16) -> bool {
        raw & 0x8000 != 0
    }

    /// Convert a raw u16 to a known RecordType, or None if unknown.
    pub fn from_raw(raw: u16) -> Option<Self> {
        match raw {
            0 => Some(Self::Noop),
            x if x == 1 | 0x8000 => Some(Self::Put),
            x if x == 2 | 0x8000 => Some(Self::Delete),
            x if x == 10 | 0x8000 => Some(Self::VectorPut),
            x if x == 11 | 0x8000 => Some(Self::VectorDelete),
            x if x == 12 | 0x8000 => Some(Self::VectorParams),
            x if x == 20 | 0x8000 => Some(Self::CrdtDelta),
            x if x == 50 | 0x8000 => Some(Self::Transaction),
            30 => Some(Self::TimeseriesBatch),
            31 => Some(Self::LogBatch),
            x if x == 40 | 0x8000 => Some(Self::ArrayPut),
            x if x == 41 | 0x8000 => Some(Self::ArrayDelete),
            x if x == 42 | 0x8000 => Some(Self::ArrayFlush),
            x if x == 100 | 0x8000 => Some(Self::Checkpoint),
            x if x == 101 | 0x8000 => Some(Self::CollectionTombstoned),
            102 => Some(Self::LsnMsAnchor),
            x if x == 103 | 0x8000 => Some(Self::TemporalPurge),
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn record_type_required_flag() {
        assert!(RecordType::is_required(RecordType::Put as u16));
        assert!(RecordType::is_required(RecordType::Delete as u16));
        assert!(RecordType::is_required(RecordType::Checkpoint as u16));
        assert!(!RecordType::is_required(RecordType::Noop as u16));
        assert!(!RecordType::is_required(RecordType::TimeseriesBatch as u16));
        assert!(!RecordType::is_required(RecordType::LogBatch as u16));
        assert!(!RecordType::is_required(RecordType::LsnMsAnchor as u16));
        assert!(RecordType::is_required(RecordType::TemporalPurge as u16));
    }

    #[test]
    fn from_raw_roundtrip() {
        for ty in [
            RecordType::Noop,
            RecordType::Put,
            RecordType::Delete,
            RecordType::VectorPut,
            RecordType::VectorDelete,
            RecordType::VectorParams,
            RecordType::CrdtDelta,
            RecordType::TimeseriesBatch,
            RecordType::LogBatch,
            RecordType::ArrayPut,
            RecordType::ArrayDelete,
            RecordType::ArrayFlush,
            RecordType::Transaction,
            RecordType::Checkpoint,
            RecordType::CollectionTombstoned,
            RecordType::LsnMsAnchor,
            RecordType::TemporalPurge,
        ] {
            assert_eq!(RecordType::from_raw(ty as u16), Some(ty));
        }
        assert_eq!(RecordType::from_raw(0xFFFE), None);
    }
}
