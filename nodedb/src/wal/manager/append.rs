// SPDX-License-Identifier: BUSL-1.1

use nodedb_wal::record::RecordType;

use super::core::WalManager;
use crate::types::{DatabaseId, Lsn, TenantId, VShardId};

impl WalManager {
    /// Internal: append a record of the given type to the WAL.
    fn append_record(
        &self,
        record_type: RecordType,
        tenant_id: TenantId,
        vshard_id: VShardId,
        database_id: DatabaseId,
        payload: &[u8],
    ) -> crate::Result<Lsn> {
        let mut wal = self.wal.lock().unwrap_or_else(|p| p.into_inner());
        let lsn = wal
            .append(
                record_type as u32,
                tenant_id.as_u64(),
                vshard_id.as_u32(),
                database_id.as_u64(),
                payload,
            )
            .map_err(crate::Error::Wal)?;
        Ok(Lsn::new(lsn))
    }

    pub fn append_put(
        &self,
        tid: TenantId,
        vs: VShardId,
        db: DatabaseId,
        p: &[u8],
    ) -> crate::Result<Lsn> {
        self.append_record(RecordType::Put, tid, vs, db, p)
    }

    pub fn append_delete(
        &self,
        tid: TenantId,
        vs: VShardId,
        db: DatabaseId,
        p: &[u8],
    ) -> crate::Result<Lsn> {
        self.append_record(RecordType::Delete, tid, vs, db, p)
    }

    pub fn append_vector_put(
        &self,
        tid: TenantId,
        vs: VShardId,
        db: DatabaseId,
        p: &[u8],
    ) -> crate::Result<Lsn> {
        self.append_record(RecordType::VectorPut, tid, vs, db, p)
    }

    pub fn append_vector_delete(
        &self,
        tid: TenantId,
        vs: VShardId,
        db: DatabaseId,
        p: &[u8],
    ) -> crate::Result<Lsn> {
        self.append_record(RecordType::VectorDelete, tid, vs, db, p)
    }

    pub fn append_vector_params(
        &self,
        tid: TenantId,
        vs: VShardId,
        db: DatabaseId,
        p: &[u8],
    ) -> crate::Result<Lsn> {
        self.append_record(RecordType::VectorParams, tid, vs, db, p)
    }

    /// Append a `VectorDirectUpsert` record for a vector-primary insert.
    /// Payload is produced by `encode_vector_direct_upsert_payload`.
    pub fn append_vector_direct_upsert(
        &self,
        tid: TenantId,
        vs: VShardId,
        db: DatabaseId,
        p: &[u8],
    ) -> crate::Result<Lsn> {
        self.append_record(RecordType::VectorDirectUpsert, tid, vs, db, p)
    }

    /// Append a `SparseVectorPut` record. Payload is produced by
    /// `encode_sparse_vector_put_payload`.
    pub fn append_sparse_vector_put(
        &self,
        tid: TenantId,
        vs: VShardId,
        db: DatabaseId,
        p: &[u8],
    ) -> crate::Result<Lsn> {
        self.append_record(RecordType::SparseVectorPut, tid, vs, db, p)
    }

    /// Append a `SparseVectorDelete` record. Payload is produced by
    /// `encode_sparse_vector_delete_payload`.
    pub fn append_sparse_vector_delete(
        &self,
        tid: TenantId,
        vs: VShardId,
        db: DatabaseId,
        p: &[u8],
    ) -> crate::Result<Lsn> {
        self.append_record(RecordType::SparseVectorDelete, tid, vs, db, p)
    }

    /// Append a `MultiVectorPut` record. Payload is produced by
    /// `encode_multi_vector_put_payload`.
    pub fn append_multi_vector_put(
        &self,
        tid: TenantId,
        vs: VShardId,
        db: DatabaseId,
        p: &[u8],
    ) -> crate::Result<Lsn> {
        self.append_record(RecordType::MultiVectorPut, tid, vs, db, p)
    }

    /// Append a `MultiVectorDelete` record. Payload is produced by
    /// `encode_multi_vector_delete_payload`.
    pub fn append_multi_vector_delete(
        &self,
        tid: TenantId,
        vs: VShardId,
        db: DatabaseId,
        p: &[u8],
    ) -> crate::Result<Lsn> {
        self.append_record(RecordType::MultiVectorDelete, tid, vs, db, p)
    }

    pub fn append_transaction(
        &self,
        tid: TenantId,
        vs: VShardId,
        db: DatabaseId,
        p: &[u8],
    ) -> crate::Result<Lsn> {
        self.append_record(RecordType::Transaction, tid, vs, db, p)
    }

    /// Append a `TransactionRedo` record wrapping an ordered set of
    /// engine-native sub-records as one durable, replayable unit.
    ///
    /// The record is serialized here and appended atomically; the returned LSN
    /// is the write's WAL position, which the caller uses to write-ahead the
    /// transaction before installing its effects.
    pub fn append_transaction_redo(
        &self,
        tid: TenantId,
        vs: VShardId,
        db: DatabaseId,
        record: &crate::wal::RedoRecord,
    ) -> crate::Result<Lsn> {
        let payload = record.to_bytes()?;
        self.append_record(RecordType::TransactionRedo, tid, vs, db, &payload)
    }

    pub fn append_crdt_delta(
        &self,
        tid: TenantId,
        vs: VShardId,
        db: DatabaseId,
        delta: &[u8],
    ) -> crate::Result<Lsn> {
        self.append_record(RecordType::CrdtDelta, tid, vs, db, delta)
    }

    /// Append a `CrdtListOp` record. Payload is a zerompk-encoded
    /// `CrdtListOpWalRecord` carrying the list-mutation intent (see that
    /// type's doc comment for why intent, not a Loro delta, is logged).
    pub fn append_crdt_list_op(
        &self,
        tid: TenantId,
        vs: VShardId,
        db: DatabaseId,
        p: &[u8],
    ) -> crate::Result<Lsn> {
        self.append_record(RecordType::CrdtListOp, tid, vs, db, p)
    }

    /// Append a `CrdtDocOp` record. Payload is a zerompk-encoded
    /// `CrdtDocOpWalRecord` carrying the document-row mutation intent (see that
    /// type's doc comment for why intent, not a Loro delta, is logged).
    pub fn append_crdt_doc_op(
        &self,
        tid: TenantId,
        vs: VShardId,
        db: DatabaseId,
        p: &[u8],
    ) -> crate::Result<Lsn> {
        self.append_record(RecordType::CrdtDocOp, tid, vs, db, p)
    }

    /// Append a checkpoint marker. Serializes the LSN before writing.
    pub fn append_checkpoint(
        &self,
        tid: TenantId,
        vs: VShardId,
        db: DatabaseId,
        checkpoint_lsn: u64,
    ) -> crate::Result<Lsn> {
        let payload =
            zerompk::to_msgpack_vec(&checkpoint_lsn).map_err(|e| crate::Error::Serialization {
                format: "msgpack".into(),
                detail: format!("checkpoint: {e}"),
            })?;
        self.append_record(RecordType::Checkpoint, tid, vs, db, &payload)
    }

    pub fn append_timeseries_batch(
        &self,
        tid: TenantId,
        vs: VShardId,
        db: DatabaseId,
        p: &[u8],
    ) -> crate::Result<Lsn> {
        self.append_record(RecordType::TimeseriesBatch, tid, vs, db, p)
    }

    pub fn append_log_batch(
        &self,
        tid: TenantId,
        vs: VShardId,
        db: DatabaseId,
        p: &[u8],
    ) -> crate::Result<Lsn> {
        self.append_record(RecordType::LogBatch, tid, vs, db, p)
    }

    pub fn append_array_put(
        &self,
        tid: TenantId,
        vs: VShardId,
        db: DatabaseId,
        p: &[u8],
    ) -> crate::Result<Lsn> {
        self.append_record(RecordType::ArrayPut, tid, vs, db, p)
    }

    pub fn append_array_delete(
        &self,
        tid: TenantId,
        vs: VShardId,
        db: DatabaseId,
        p: &[u8],
    ) -> crate::Result<Lsn> {
        self.append_record(RecordType::ArrayDelete, tid, vs, db, p)
    }

    /// Append a `CollectionTombstoned` record. Any subsequent replay
    /// that extracts this record will filter prior writes for
    /// `(tid, collection)` whose LSN is less than `purge_lsn`.
    ///
    /// `vshard_id` of `0` is conventional — tombstones are tenant-level
    /// metadata, not sharded user data. Replay filters on
    /// `(tenant_id, collection)` pair alone.
    /// Append a `TemporalPurge` audit record. Emitted by the
    /// Control Plane's bitemporal-retention scheduler after a successful
    /// dispatch of `MetaOp::TemporalPurge*` to the Data Plane, providing
    /// a durable audit trail distinct from regular `Delete` records.
    ///
    /// `vshard_id` is `0` — bitemporal audit-purge is collection-scoped
    /// metadata, not sharded user data.
    pub fn append_temporal_purge(
        &self,
        tid: TenantId,
        engine: nodedb_wal::TemporalPurgeEngine,
        collection: &str,
        cutoff_system_ms: i64,
        purged_count: u64,
    ) -> crate::Result<Lsn> {
        let payload = nodedb_wal::TemporalPurgePayload::new(
            engine,
            collection,
            cutoff_system_ms,
            purged_count,
        )
        .to_bytes()
        .map_err(crate::Error::Wal)?;
        self.append_record(
            RecordType::TemporalPurge,
            tid,
            VShardId::new(0),
            DatabaseId::DEFAULT,
            &payload,
        )
    }

    /// Append a `SurrogateAlloc` high-watermark record. Emitted by
    /// `SurrogateRegistry::flush` (every 1024 allocations or 200 ms,
    /// whichever first) so the global surrogate counter is
    /// crash-recoverable independent of the redb `_system.surrogate_hwm`
    /// row. `vshard_id` is `0` — the surrogate hwm is a node-global
    /// allocator counter, not sharded user data.
    pub fn append_surrogate_alloc(&self, hi: u32) -> crate::Result<Lsn> {
        let payload = nodedb_wal::record::SurrogateAllocPayload::new(hi).to_bytes();
        self.append_record(
            RecordType::SurrogateAlloc,
            TenantId::new(0),
            VShardId::new(0),
            DatabaseId::DEFAULT,
            &payload,
        )
    }

    /// Append a `SurrogateBind` record. Emitted by
    /// `SurrogateAssigner::assign` immediately after the catalog
    /// two-table txn that writes `_system.surrogate_pk{,_rev}`, so the
    /// binding survives a crash before the next hwm checkpoint. The
    /// record is durably persisted before `assign` returns.
    /// `vshard_id` is `0` — surrogate bindings are node-global metadata.
    pub fn append_surrogate_bind(
        &self,
        database_id: DatabaseId,
        tenant_id: TenantId,
        surrogate: u32,
        collection: &str,
        pk_bytes: &[u8],
    ) -> crate::Result<Lsn> {
        let payload =
            nodedb_wal::record::SurrogateBindPayload::new(surrogate, collection, pk_bytes.to_vec())
                .to_bytes()
                .map_err(crate::Error::Wal)?;
        self.append_record(
            RecordType::SurrogateBind,
            tenant_id,
            VShardId::new(0),
            database_id,
            &payload,
        )
    }

    /// Append a `CalvinApplied` record after a Calvin executor successfully
    /// commits a `MetaOp::CalvinExecute` batch.
    ///
    /// The scheduler's restart path scans these records to compute
    /// `last_applied_epoch` for a given vshard without re-reading the full
    /// Raft sequencer log. `vshard_id` is stored in the payload (not in the
    /// WAL record header's `vshard_id` field) so it can be decoded during
    /// a scan of all records regardless of which vshard they were written on.
    pub fn append_calvin_applied(
        &self,
        vshard_id: crate::types::VShardId,
        epoch: u64,
        position: u32,
    ) -> crate::Result<crate::types::Lsn> {
        let payload =
            nodedb_wal::CalvinAppliedPayload::new(epoch, position, vshard_id.as_u32()).to_bytes();
        self.append_record(
            RecordType::CalvinApplied,
            TenantId::new(0),
            vshard_id,
            DatabaseId::DEFAULT,
            &payload,
        )
    }

    /// Append a `SyncSeqAdvance` watermark record. Emitted by the Data Plane sync
    /// handler after durably applying an ingest message, to make the per-stream
    /// high-watermark crash-recoverable.
    ///
    /// `vshard_id` is `0` — the sync HWM is a node-global idempotency state,
    /// not sharded user data.
    pub fn append_sync_seq_advance(
        &self,
        producer_id: u64,
        epoch: u64,
        stream_id: u64,
        seq: u64,
    ) -> crate::Result<Lsn> {
        let payload =
            nodedb_wal::record::SyncSeqAdvancePayload::new(producer_id, epoch, stream_id, seq)
                .to_bytes();
        self.append_record(
            RecordType::SyncSeqAdvance,
            TenantId::new(0),
            VShardId::new(0),
            DatabaseId::DEFAULT,
            &payload,
        )
    }

    /// Append an `FtsIndex` record. Payload is a length-prefixed `FtsIndexPayload`
    /// produced by `nodedb_wal::record::FtsIndexPayload::to_bytes()`.
    pub fn append_fts_index(
        &self,
        tid: TenantId,
        vs: VShardId,
        db: DatabaseId,
        p: &[u8],
    ) -> crate::Result<Lsn> {
        self.append_record(RecordType::FtsIndex, tid, vs, db, p)
    }

    /// Append an `FtsDelete` record. Payload is a length-prefixed `FtsDeletePayload`
    /// produced by `nodedb_wal::record::FtsDeletePayload::to_bytes()`.
    pub fn append_fts_delete(
        &self,
        tid: TenantId,
        vs: VShardId,
        db: DatabaseId,
        p: &[u8],
    ) -> crate::Result<Lsn> {
        self.append_record(RecordType::FtsDelete, tid, vs, db, p)
    }

    /// Append a `SpatialPut` record. Payload is a length-prefixed `SpatialPutPayload`
    /// produced by `nodedb_wal::record::SpatialPutPayload::to_bytes()`.
    pub fn append_spatial_put(
        &self,
        tid: TenantId,
        vs: VShardId,
        db: DatabaseId,
        p: &[u8],
    ) -> crate::Result<Lsn> {
        self.append_record(RecordType::SpatialPut, tid, vs, db, p)
    }

    /// Append a `SpatialDelete` record. Payload is a length-prefixed `SpatialDeletePayload`
    /// produced by `nodedb_wal::record::SpatialDeletePayload::to_bytes()`.
    pub fn append_spatial_delete(
        &self,
        tid: TenantId,
        vs: VShardId,
        db: DatabaseId,
        p: &[u8],
    ) -> crate::Result<Lsn> {
        self.append_record(RecordType::SpatialDelete, tid, vs, db, p)
    }

    /// Append a `GraphNodeLabelSet` record. Payload is produced by
    /// `encode_graph_node_label_payload`.
    pub fn append_graph_node_label_set(
        &self,
        tid: TenantId,
        vs: VShardId,
        db: DatabaseId,
        p: &[u8],
    ) -> crate::Result<Lsn> {
        self.append_record(RecordType::GraphNodeLabelSet, tid, vs, db, p)
    }

    /// Append a `GraphNodeLabelRemove` record. Payload is produced by
    /// `encode_graph_node_label_payload`.
    pub fn append_graph_node_label_remove(
        &self,
        tid: TenantId,
        vs: VShardId,
        db: DatabaseId,
        p: &[u8],
    ) -> crate::Result<Lsn> {
        self.append_record(RecordType::GraphNodeLabelRemove, tid, vs, db, p)
    }

    pub fn append_collection_tombstone(
        &self,
        tid: TenantId,
        database_id: DatabaseId,
        collection: &str,
        purge_lsn: u64,
    ) -> crate::Result<Lsn> {
        let payload = nodedb_wal::CollectionTombstonePayload::new(collection, purge_lsn)
            .to_bytes()
            .map_err(crate::Error::Wal)?;
        self.append_record(
            RecordType::CollectionTombstoned,
            tid,
            VShardId::new(0),
            database_id,
            &payload,
        )
    }
}
