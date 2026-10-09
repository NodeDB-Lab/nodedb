// SPDX-License-Identifier: BUSL-1.1

//! Which WAL record types carry row writes the Event Plane rebuilds events
//! for.

use nodedb_wal::record::RecordType;

/// A record type that carries row writes.
#[derive(Debug, Clone, Copy)]
pub(super) enum RowRecord {
    Put,
    Delete,
    Redo,
    /// A `WriteGroup` record: the rows one write stored.
    Group,
    LabelSet,
    LabelRemove,
    Timeseries,
}

/// The row-write kind of a record type. `None` for a type that carries no
/// row write the forward path emits an event for.
pub(super) fn row_kind(record_type: RecordType) -> Option<RowRecord> {
    match record_type {
        RecordType::Put => Some(RowRecord::Put),
        RecordType::Delete => Some(RowRecord::Delete),
        // A Calvin cross-shard or single-shard commit is durable as a
        // `TransactionRedo` whose sub-ops carry each engine's own per-op
        // payload. Decompose it into the same WriteEvents the forward path
        // emitted, so the effect (triggers/CDC) is not lost on replay. Every
        // emitted event's `lsn` is this redo record's WAL LSN — the Event-Plane
        // watermark keys on it to dedup against the forward-path event.
        RecordType::TransactionRedo => Some(RowRecord::Redo),
        // A write group's records carry the rows one write stored, in the
        // shapes the raw records use. Every event of the write names the
        // group's origin (see `wal_replay_group`).
        RecordType::WriteGroup => Some(RowRecord::Group),
        // Graph node-label mutations carry no natural collection (they are
        // tenant-wide), so they surface on the nameable `__graph_node_labels__`
        // CDC stream. The forward-path emit (Data Plane `SetNodeLabels` /
        // `RemoveNodeLabels`) produces the same `(collection, row_id, op, value)`
        // shape, so replayed events dedup against forward events on LSN.
        RecordType::GraphNodeLabelSet => Some(RowRecord::LabelSet),
        RecordType::GraphNodeLabelRemove => Some(RowRecord::LabelRemove),
        // A timeseries ingest record carries the rows it resolved to, and,
        // when a consumer read the collection at resolve, each row's image.
        // The forward install emitted one Insert per image; replay rebuilds
        // the same events. A columnar insert rides the same record type and
        // rebuilds none: its forward path emits no WriteEvent.
        RecordType::TimeseriesBatch => Some(RowRecord::Timeseries),
        // The records below carry NO forward-path Data-Plane WriteEvent, so
        // there is nothing for replay to reconstruct. `record_to_events`
        // reconstructs exactly the forward WriteEvent stream the Data Plane
        // emits (Document / KV / Graph — see
        // `data::executor::core_loop::event_emit`), keyed on LSN so replayed
        // events dedup against forward ones. Emitting a WriteEvent for a record
        // the forward path never emitted fires triggers / audit /
        // CRDT-sync / CDC on WAL replay and snapshot-catchup but NOT on the live
        // write — a recovery-divergence bug. Each group states why it has no
        // forward WriteEvent.
        //
        // Vector / CRDT records replay through their own Data-Plane paths and
        // never rode the WriteEvent stream. Infra records (Checkpoint,
        // Surrogate*, tombstone, anchors, sync HWM, Noop, …) are not row writes.
        RecordType::VectorPut
        | RecordType::VectorDelete
        | RecordType::VectorParams
        | RecordType::VectorIndexDrop
        | RecordType::VectorDirectUpsert
        | RecordType::VectorDirectDelete
        | RecordType::VectorDirectUpdate
        | RecordType::VectorDirectTruncate
        | RecordType::VectorResolvedDirectWrite
        | RecordType::MultiVectorPut
        | RecordType::MultiVectorDelete
        | RecordType::CrdtDelta
        // CrdtListOp: position-based list-op intent, replayed by
        // `data::executor::wal_replay::crdt_list`, not the Event Plane's
        // WriteEvent stream.
        | RecordType::CrdtListOp
        // CrdtDocOp: document-row intent, replayed by
        // `data::executor::wal_replay::crdt_doc`, not the Event Plane's
        // WriteEvent stream.
        | RecordType::CrdtDocOp
        | RecordType::LogBatch
        | RecordType::Transaction
        | RecordType::SurrogateAlloc
        | RecordType::SurrogateBind
        | RecordType::Checkpoint
        | RecordType::CollectionTombstoned
        | RecordType::TimeAnchor
        | RecordType::TemporalPurge
        // SyncSeqAdvance: emitted by the sync layer; replay HWM reconstruction
        // is wired in the idempotency replay pass, not the Event Plane.
        | RecordType::SyncSeqAdvance
        | RecordType::Noop
        // Columnar-family truncate: whole-collection clear with no per-row
        // identity; its forward CDC rides the Control-Plane change stream
        // (`extract_write_metadata`, keyed `(collection, "*", Delete)`).
        | RecordType::ColumnarTruncate
        | RecordType::TimeseriesTruncate
        // Array: `ArrayPut` / `ArrayDelete` cells decode per-cell, but the forward
        // path emits no Data-Plane WriteEvent — array CDC rides the Control-Plane
        // change stream (`extract_write_metadata`, keyed `(array_name, "*", op)`).
        // `ArrayFlush` only reorganizes on-disk tiles (no logical cell).
        | RecordType::ArrayPut
        | RecordType::ArrayDelete
        | RecordType::ArrayFlush
        // FTS / Spatial / Sparse-vector: secondary index overlays over a
        // Document/Columnar row that already publishes its own change event on the
        // forward path. The index write emits no separate WriteEvent — one here
        // double-publishes the underlying row (and the spatial geometry blob is
        // zerompk-tagged, not a standard-msgpack pass-through value anyway).
        | RecordType::FtsIndex
        | RecordType::FtsDelete
        | RecordType::SpatialPut
        | RecordType::SpatialDelete
        | RecordType::SparseVectorPut
        | RecordType::SparseVectorDelete
        // WriteAborted names a refused write; the record it names has already
        // been dropped from this stream by the replay-source filter (see
        // `WalManager::replay_from`). The marker itself is not a row write.
        | RecordType::WriteAborted
        // ProposalApplied marks a Raft proposal as applied; it writes no row.
        | RecordType::ProposalApplied
        // ChangePosition names a replicated log position; it writes no row.
        | RecordType::ChangePosition
        | RecordType::RestorePoint
        // A node cascade an older build journalled emitted no event on its
        // live delete, so replay rebuilds none.
        | RecordType::GraphNodeCascade
        // A TRUNCATE's edge cut writes no edge version, and its install
        // emits no per-edge event, so replay rebuilds none.
        | RecordType::GraphEdgeCut
        // A snapshot install marker writes no row.
        | RecordType::SnapshotInstalled
        // A redo chunk holds bytes of a redo stream. The stream's final
        // record carries the rows and their events.
        | RecordType::RedoChunk => None,
    }
}
