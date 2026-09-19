// SPDX-License-Identifier: Apache-2.0

//! Record type discriminants.
//!
//! Types 0-255 are reserved for NodeDB core.
//! Types 256+ are available for NodeDB-specific records.
//!
//! Bit 15 (0x0000_8000) marks a record as **required** — unknown required
//! records cause a replay failure. Unknown records without bit 15 are safely
//! skipped.
//!
//! The repr is u32 to match the widened `record_type` field in `RecordHeader`.

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u32)]
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

    /// Vector engine: drop one vector index (`DROP INDEX` on a vector index).
    ///
    /// Required: skipping it on replay resurrects an index the user dropped,
    /// because the `VectorParams` record that created it is still in the log.
    VectorIndexDrop = 18 | 0x8000,

    /// Vector engine: direct upsert into a vector-primary collection.
    ///
    /// Distinct from `VectorPut`: a vector-primary insert bypasses the
    /// document store and carries its own payload sidecar, quantization,
    /// storage dtype, and payload-index registration. Replaying it as a plain
    /// `VectorPut` would restore the HNSW node but drop the payload body and
    /// bitmap indexes, so it needs its own record and replay arm.
    ///
    /// Required: skipping on replay loses an acknowledged vector-primary write.
    VectorDirectUpsert = 13 | 0x8000,

    /// Vector engine: delete rows of a vector-primary collection by surrogate
    /// or by sidecar predicate.
    ///
    /// Required: skipping on replay resurrects deleted vector-primary rows.
    VectorDirectDelete = 19 | 0x8000,

    /// Vector engine: update rows of a vector-primary collection — a new
    /// vector, a payload patch, or both — by surrogate or by sidecar predicate.
    ///
    /// Required: skipping on replay loses an acknowledged vector-primary write.
    VectorDirectUpdate = 23 | 0x8000,

    /// Vector engine: apply the row mutations a governed vector-primary
    /// write resolved to — deletes, rewrites, and whole-row upserts, each
    /// naming its surrogate and carrying its full stored image.
    ///
    /// Required: skipping on replay loses an acknowledged vector-primary write.
    VectorResolvedDirectWrite = 24 | 0x8000,

    /// Vector engine: remove every row of a vector-primary collection
    /// (`TRUNCATE`).
    ///
    /// Required: skipping on replay resurrects every truncated row, because
    /// the `VectorDirectUpsert` records that created them are still in the log.
    VectorDirectTruncate = 25 | 0x8000,

    /// Vector engine: insert (upsert) a sparse vector into the inverted index.
    ///
    /// Targets the `SparseInvertedIndex` (keyed by document id), a separate
    /// in-memory structure from the HNSW graph, so it needs its own record.
    ///
    /// Required: skipping on replay loses an acknowledged sparse write.
    SparseVectorPut = 14 | 0x8000,

    /// Vector engine: delete a document from the sparse inverted index.
    ///
    /// Required: skipping on replay resurrects a deleted sparse document.
    SparseVectorDelete = 15 | 0x8000,

    /// Vector engine: insert all vectors of a multi-vector (ColBERT-style)
    /// document, bound to one shared document surrogate.
    ///
    /// Distinct from `VectorPut`: N vectors share one surrogate and are tracked
    /// in `multi_doc_map` for bulk deletion. Replaying as N `VectorPut`s would
    /// not reconstruct that grouping, so it needs its own record and replay arm.
    ///
    /// Required: skipping on replay loses an acknowledged multi-vector write.
    MultiVectorPut = 16 | 0x8000,

    /// Vector engine: delete all vectors of a multi-vector document.
    ///
    /// Required: skipping on replay resurrects a deleted multi-vector document.
    MultiVectorDelete = 17 | 0x8000,

    /// CRDT engine: delta application.
    CrdtDelta = 20 | 0x8000,

    /// CRDT engine: block-list intent op (`ListInsert` / `ListDelete` /
    /// `ListMove`) on a nested `LoroMovableList`.
    ///
    /// These ops mutate Data-Plane-only Loro state and the Control Plane has
    /// no `LoroDoc` to compute a delta from, so the record carries the
    /// **intent** (collection, document, list path, operation kind, and
    /// position(s)) rather than a Loro delta; replay re-executes the exact
    /// same live handler that ran on first application.
    ///
    /// Deliberately NOT `CrdtDelta`: that record type's replay contract is
    /// idempotent, commutative Loro import with no LSN gate. List ops are
    /// position-based and re-applying one is not idempotent, so mixing them
    /// into `CrdtDelta`'s replay path would violate that contract.
    ///
    /// Required: skipping this record on replay silently drops an
    /// acknowledged list mutation, diverging list order/content from what
    /// was acknowledged to the client.
    CrdtListOp = 21 | 0x8000,

    /// CRDT engine: document-row intent op (`DocUpsert` / `DocDelete`) —
    /// field-carrying insert-or-replace / partial-update / delete of a
    /// top-level `LoroMap` row for SQL DML on a `crdt='true'` collection.
    ///
    /// Like `CrdtListOp`, the Data Plane builds the Loro mutation server-side
    /// and the Control Plane has no `LoroDoc` to compute a delta from, so the
    /// record carries the **intent** (collection, document, surrogate, fields,
    /// partial flag) rather than a Loro delta; replay re-executes the exact
    /// same live handler that ran on first application.
    ///
    /// Required: skipping this record on replay silently drops an
    /// acknowledged document write, diverging row content from what was
    /// acknowledged to the client.
    CrdtDocOp = 22 | 0x8000,

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
    ///
    /// The sub-record tag is written as a `u16` and hard-codes the generic
    /// `Put` type for every sub-op, so replay cannot tell which engine each
    /// sub-op targeted. This record is a durability placeholder only — it is
    /// never replayed into any engine. `TransactionRedo` supersedes it for
    /// replayable transaction groups.
    Transaction = 50 | 0x8000,

    /// Redo-log transaction group: an ordered set of engine-native sub-records
    /// committed as one durable unit and replayable into their engines.
    ///
    /// Unlike `Transaction`, each sub-record preserves its own engine
    /// `record_type` as a `u32` tag (matching this header's `record_type`
    /// width) and carries the exact payload that engine's per-op WAL record
    /// uses, so replay reconstitutes a `WalRecord` per sub-op and feeds it to
    /// that engine's existing replay path — no tag loss, no re-encoding.
    ///
    /// May also carry a Calvin stamp so a cross-shard transaction's durable
    /// record doubles as its sequencer applied-marker.
    /// Payload: zerompk-encoded `RedoRecord` (see the `wal::redo` module).
    ///
    /// Required: skipping this record on replay would drop a committed
    /// transaction's writes, diverging from the leader's state.
    TransactionRedo = 58 | 0x8000,

    /// Surrogate allocator: high-watermark flush record.
    ///
    /// Emitted periodically by `SurrogateRegistry::flush` (every N=1024
    /// allocations or T=200ms, whichever first) to make the surrogate
    /// allocator's hwm crash-recoverable. Replay advances the in-memory
    /// allocator past `hi` so post-restart allocations never collide
    /// with pre-restart ones.
    ///
    /// Payload: zerompk-encoded `SurrogateAllocPayload { hi: u32 }`
    /// (4-byte little-endian u32 wrapped in msgpack).
    ///
    /// Required: a replay that skipped this record could re-issue
    /// surrogates that already point at live engine state, corrupting
    /// every per-engine index keyed on Surrogate.
    SurrogateAlloc = 51 | 0x8000,

    /// Surrogate ↔ PK binding record.
    ///
    /// Emitted by `SurrogateAssigner::assign` immediately after the
    /// catalog two-table txn that writes `_system.surrogate_pk{,_rev}`,
    /// so a crash between the catalog write and the next hwm checkpoint
    /// still recovers the binding on replay (idempotent re-apply).
    ///
    /// Payload: zerompk-encoded `SurrogateBindPayload {
    /// surrogate: u32, collection: String, pk_bytes: Vec<u8> }`.
    ///
    /// Required: skipping a bind on replay would leave the catalog
    /// behind the registry hwm, so a subsequent insert with the same
    /// user PK would allocate a fresh surrogate and break identity.
    SurrogateBind = 52 | 0x8000,

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

    /// Calvin scheduler: marks a sequenced transaction as applied on this
    /// vshard.
    ///
    /// Written by the Calvin executor after a `MetaOp::CalvinExecute` batch
    /// commits successfully. The scheduler's rebuild path scans the WAL for
    /// these records to determine `last_applied_epoch` on restart.
    ///
    /// Payload: zerompk-encoded `CalvinAppliedPayload { epoch: u64,
    /// position: u32, vshard_id: u32 }`.
    ///
    /// Required: a replay that skipped this record would leave the scheduler
    /// believing the transaction was not applied and re-dispatch it after
    /// restart, causing double-application.
    CalvinApplied = 110 | 0x8000,

    /// Sync idempotency watermark — advances the durable per-stream
    /// high-watermark for a given producer so the receiver can safely
    /// deduplicate re-delivered ingest frames on reconnect.
    ///
    /// Payload: fixed 32-byte little-endian `SyncSeqAdvancePayload`
    /// (`producer_id: u64, epoch: u64, stream_id: u64, seq: u64`).
    ///
    /// Required: a replay that skipped this record would leave the HWM
    /// behind the tail, causing the receiver to re-apply already-committed
    /// frames as duplicates after a restart.
    SyncSeqAdvance = 53 | 0x8000,

    /// FTS engine: index a document into the inverted BM25 index.
    /// Payload: length-prefixed `FtsIndexPayload` (see `fts_spatial.rs`).
    ///
    /// Required: skipping this record on replay would leave the FTS index
    /// behind the storage engine, breaking full-text queries.
    FtsIndex = 54 | 0x8000,

    /// FTS engine: remove a document from the inverted BM25 index.
    /// Payload: length-prefixed `FtsDeletePayload` (see `fts_spatial.rs`).
    ///
    /// Required: skipping on replay would leave stale postings in the index.
    FtsDelete = 55 | 0x8000,

    /// Spatial engine: insert or update a geometry entry in the R-tree.
    /// Payload: length-prefixed `SpatialPutPayload` (see `fts_spatial.rs`).
    ///
    /// Required: skipping on replay would leave the R-tree missing entries.
    SpatialPut = 56 | 0x8000,

    /// Spatial engine: remove a geometry entry from the R-tree.
    /// Payload: length-prefixed `SpatialDeletePayload` (see `fts_spatial.rs`).
    ///
    /// Required: skipping on replay would leave stale entries in the R-tree.
    SpatialDelete = 57 | 0x8000,

    /// Graph engine: set one or more node labels on the bitset-based label
    /// index (up to 64 distinct labels per partition).
    ///
    /// A dedicated type rather than riding `Put` with a boolean set/remove
    /// flag or a trial-decoded arity: the payload is `(node_id, labels)`, an
    /// arity that happens not to collide with any current `Put` tuple, but
    /// aliasing a durability record on coincidental arity is exactly the
    /// silent-corruption risk `wal_replay_redo_graph.rs` documents for the
    /// edge `Put`/`Delete` disambiguation. Set/Remove get their own
    /// discriminators instead — the same shape as the `VectorPut`/
    /// `VectorDelete` and `SpatialPut`/`SpatialDelete` pairs above.
    ///
    /// Required: the node-label bitset (`CsrIndex::node_label_bits`) has no
    /// other durable backing — unlike edges, which are rebuilt from the
    /// `EdgeStore` (redb) at startup, labels exist only in memory until this
    /// record is replayed.
    GraphNodeLabelSet = 59 | 0x8000,

    /// Graph engine: remove one or more node labels. See `GraphNodeLabelSet`.
    GraphNodeLabelRemove = 60 | 0x8000,

    /// Names an already-appended write record that must NEVER be replayed.
    /// Payload: `WriteAbortedPayload` (fixed 8 bytes, little-endian
    /// `aborted_lsn`).
    ///
    /// The forward record is appended before the executing engine decides
    /// whether to accept the write, so a refusal arrives with the record
    /// already in the log. Without this marker, replay re-applies a write the
    /// server told the client it refused — a row that a policy, constraint, or
    /// type guard rejected reappears after a restart.
    ///
    /// Required: silently skipping this record IS the resurrection bug it
    /// exists to prevent, so an older binary pointed at a WAL containing one
    /// must fail to start loudly rather than readmit refused data.
    WriteAborted = 61 | 0x8000,
}

impl RecordType {
    /// Whether this record type is required (must be understood for correct replay).
    pub fn is_required(raw: u32) -> bool {
        raw & 0x8000 != 0
    }

    /// Convert a raw u32 to a known RecordType, or None if unknown.
    pub fn from_raw(raw: u32) -> Option<Self> {
        match raw {
            0 => Some(Self::Noop),
            x if x == 1 | 0x8000 => Some(Self::Put),
            x if x == 2 | 0x8000 => Some(Self::Delete),
            x if x == 10 | 0x8000 => Some(Self::VectorPut),
            x if x == 11 | 0x8000 => Some(Self::VectorDelete),
            x if x == 12 | 0x8000 => Some(Self::VectorParams),
            x if x == 13 | 0x8000 => Some(Self::VectorDirectUpsert),
            x if x == 19 | 0x8000 => Some(Self::VectorDirectDelete),
            x if x == 23 | 0x8000 => Some(Self::VectorDirectUpdate),
            x if x == 24 | 0x8000 => Some(Self::VectorResolvedDirectWrite),
            x if x == 25 | 0x8000 => Some(Self::VectorDirectTruncate),
            x if x == 14 | 0x8000 => Some(Self::SparseVectorPut),
            x if x == 15 | 0x8000 => Some(Self::SparseVectorDelete),
            x if x == 16 | 0x8000 => Some(Self::MultiVectorPut),
            x if x == 17 | 0x8000 => Some(Self::MultiVectorDelete),
            x if x == 18 | 0x8000 => Some(Self::VectorIndexDrop),
            x if x == 20 | 0x8000 => Some(Self::CrdtDelta),
            x if x == 21 | 0x8000 => Some(Self::CrdtListOp),
            x if x == 22 | 0x8000 => Some(Self::CrdtDocOp),
            x if x == 50 | 0x8000 => Some(Self::Transaction),
            x if x == 58 | 0x8000 => Some(Self::TransactionRedo),
            x if x == 51 | 0x8000 => Some(Self::SurrogateAlloc),
            x if x == 52 | 0x8000 => Some(Self::SurrogateBind),
            30 => Some(Self::TimeseriesBatch),
            31 => Some(Self::LogBatch),
            x if x == 40 | 0x8000 => Some(Self::ArrayPut),
            x if x == 41 | 0x8000 => Some(Self::ArrayDelete),
            x if x == 42 | 0x8000 => Some(Self::ArrayFlush),
            x if x == 100 | 0x8000 => Some(Self::Checkpoint),
            x if x == 101 | 0x8000 => Some(Self::CollectionTombstoned),
            102 => Some(Self::LsnMsAnchor),
            x if x == 103 | 0x8000 => Some(Self::TemporalPurge),
            x if x == 110 | 0x8000 => Some(Self::CalvinApplied),
            x if x == 53 | 0x8000 => Some(Self::SyncSeqAdvance),
            x if x == 54 | 0x8000 => Some(Self::FtsIndex),
            x if x == 55 | 0x8000 => Some(Self::FtsDelete),
            x if x == 56 | 0x8000 => Some(Self::SpatialPut),
            x if x == 57 | 0x8000 => Some(Self::SpatialDelete),
            x if x == 59 | 0x8000 => Some(Self::GraphNodeLabelSet),
            x if x == 60 | 0x8000 => Some(Self::GraphNodeLabelRemove),
            x if x == 61 | 0x8000 => Some(Self::WriteAborted),
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn record_type_required_flag() {
        assert!(RecordType::is_required(RecordType::Put as u32));
        assert!(RecordType::is_required(RecordType::Delete as u32));
        assert!(RecordType::is_required(RecordType::Checkpoint as u32));
        assert!(!RecordType::is_required(RecordType::Noop as u32));
        assert!(!RecordType::is_required(RecordType::TimeseriesBatch as u32));
        assert!(!RecordType::is_required(RecordType::LogBatch as u32));
        assert!(!RecordType::is_required(RecordType::LsnMsAnchor as u32));
        assert!(RecordType::is_required(RecordType::TemporalPurge as u32));
        assert!(RecordType::is_required(RecordType::SyncSeqAdvance as u32));
        assert!(RecordType::is_required(RecordType::FtsIndex as u32));
        assert!(RecordType::is_required(RecordType::FtsDelete as u32));
        assert!(RecordType::is_required(RecordType::SpatialPut as u32));
        assert!(RecordType::is_required(RecordType::SpatialDelete as u32));
        assert!(RecordType::is_required(RecordType::TransactionRedo as u32));
        assert!(RecordType::is_required(
            RecordType::GraphNodeLabelSet as u32
        ));
        assert!(RecordType::is_required(
            RecordType::GraphNodeLabelRemove as u32
        ));
        assert!(RecordType::is_required(RecordType::CrdtListOp as u32));
        assert!(RecordType::is_required(RecordType::CrdtDocOp as u32));
        // Without the flag an older reader skips the abort marker and replays
        // the refused write it names — the exact bug it exists to prevent.
        assert!(RecordType::is_required(RecordType::WriteAborted as u32));
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
            RecordType::VectorDirectUpsert,
            RecordType::VectorDirectDelete,
            RecordType::VectorDirectUpdate,
            RecordType::VectorResolvedDirectWrite,
            RecordType::VectorDirectTruncate,
            RecordType::SparseVectorPut,
            RecordType::SparseVectorDelete,
            RecordType::MultiVectorPut,
            RecordType::MultiVectorDelete,
            RecordType::VectorIndexDrop,
            RecordType::CrdtDelta,
            RecordType::CrdtListOp,
            RecordType::CrdtDocOp,
            RecordType::TimeseriesBatch,
            RecordType::LogBatch,
            RecordType::ArrayPut,
            RecordType::ArrayDelete,
            RecordType::ArrayFlush,
            RecordType::Transaction,
            RecordType::TransactionRedo,
            RecordType::SurrogateAlloc,
            RecordType::SurrogateBind,
            RecordType::Checkpoint,
            RecordType::CollectionTombstoned,
            RecordType::LsnMsAnchor,
            RecordType::TemporalPurge,
            RecordType::CalvinApplied,
            RecordType::SyncSeqAdvance,
            RecordType::FtsIndex,
            RecordType::FtsDelete,
            RecordType::SpatialPut,
            RecordType::SpatialDelete,
            RecordType::GraphNodeLabelSet,
            RecordType::GraphNodeLabelRemove,
            RecordType::WriteAborted,
        ] {
            assert_eq!(RecordType::from_raw(ty as u32), Some(ty));
        }
        assert_eq!(RecordType::from_raw(0xFFFE), None);
    }
}
