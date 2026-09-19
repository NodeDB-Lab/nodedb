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

    /// Append a `VectorIndexDrop` record. Payload is the
    /// `(collection, field_name)` tuple the drop targets.
    pub fn append_vector_index_drop(
        &self,
        tid: TenantId,
        vs: VShardId,
        db: DatabaseId,
        p: &[u8],
    ) -> crate::Result<Lsn> {
        self.append_record(RecordType::VectorIndexDrop, tid, vs, db, p)
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

    /// Append a `VectorDirectDelete` record for a vector-primary delete.
    /// Payload is produced by `encode_vector_direct_delete_payload`.
    pub fn append_vector_direct_delete(
        &self,
        tid: TenantId,
        vs: VShardId,
        db: DatabaseId,
        p: &[u8],
    ) -> crate::Result<Lsn> {
        self.append_record(RecordType::VectorDirectDelete, tid, vs, db, p)
    }

    /// Append a `VectorDirectUpdate` record for a vector-primary update.
    /// Payload is produced by `encode_vector_direct_update_payload`.
    pub fn append_vector_direct_update(
        &self,
        tid: TenantId,
        vs: VShardId,
        db: DatabaseId,
        p: &[u8],
    ) -> crate::Result<Lsn> {
        self.append_record(RecordType::VectorDirectUpdate, tid, vs, db, p)
    }

    /// Append a `VectorResolvedDirectWrite` record for a resolved
    /// vector-primary write. Payload is produced by
    /// `encode_vector_resolved_direct_write_payload`.
    pub fn append_vector_resolved_direct_write(
        &self,
        tid: TenantId,
        vs: VShardId,
        db: DatabaseId,
        p: &[u8],
    ) -> crate::Result<Lsn> {
        self.append_record(RecordType::VectorResolvedDirectWrite, tid, vs, db, p)
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

    /// Append a `WriteAborted` record naming `aborted_lsn`, the LSN of a
    /// forward write record the executing engine refused after it was already
    /// appended.
    ///
    /// The returned LSN must be made durable before the refusal is
    /// acknowledged: the forward record may already have been fsynced by a
    /// concurrent writer's group commit, so an abort that is still only
    /// buffered leaves the refused write recoverable.
    pub fn append_write_aborted(
        &self,
        tid: TenantId,
        vs: VShardId,
        db: DatabaseId,
        aborted_lsn: Lsn,
    ) -> crate::Result<Lsn> {
        let payload = nodedb_wal::WriteAbortedPayload::new(aborted_lsn.as_u64()).to_bytes();
        self.append_record(RecordType::WriteAborted, tid, vs, db, &payload)
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

#[cfg(test)]
mod tests {
    use super::*;
    use nodedb_wal::record::{FtsIndexPayload, SyncSeqAdvancePayload};

    #[test]
    fn sync_seq_advance_roundtrip() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("wal_dir");

        let wal = WalManager::open_for_testing(&path).unwrap();

        let lsn = wal
            .append_sync_seq_advance(0xCAFE_BABE_DEAD_BEEF, 7, 42, 1_000_000)
            .unwrap();
        assert_eq!(lsn, Lsn::new(1));

        wal.sync().unwrap();

        let records = wal.replay().unwrap();
        assert_eq!(records.len(), 1);
        assert_eq!(
            records[0].header.record_type,
            RecordType::SyncSeqAdvance as u32
        );
        let payload = SyncSeqAdvancePayload::from_bytes(&records[0].payload).unwrap();
        assert_eq!(payload.producer_id, 0xCAFE_BABE_DEAD_BEEF);
        assert_eq!(payload.epoch, 7);
        assert_eq!(payload.stream_id, 42);
        assert_eq!(payload.seq, 1_000_000);
    }

    #[test]
    fn append_and_replay() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("wal_dir");

        let wal = WalManager::open_for_testing(&path).unwrap();

        let t = TenantId::new(1);
        let v = VShardId::new(0);
        let db = DatabaseId::DEFAULT;

        let lsn1 = wal.append_put(t, v, db, b"key1=value1").unwrap();
        let lsn2 = wal.append_put(t, v, db, b"key2=value2").unwrap();
        let lsn3 = wal.append_delete(t, v, db, b"key1").unwrap();

        assert_eq!(lsn1, Lsn::new(1));
        assert_eq!(lsn2, Lsn::new(2));
        assert_eq!(lsn3, Lsn::new(3));

        wal.sync().unwrap();

        let records = wal.replay().unwrap();
        assert_eq!(records.len(), 3);
        assert_eq!(records[0].payload, b"key1=value1");
        assert_eq!(records[2].payload, b"key1");
    }

    #[test]
    fn append_transaction_redo_returns_monotonic_lsn() {
        use crate::wal::{RedoRecord, RedoSubRecord};

        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("wal_dir");

        let wal = WalManager::open_for_testing(&path).unwrap();

        let t = TenantId::new(3);
        let v = VShardId::new(1);
        let db = DatabaseId::DEFAULT;

        let record = RedoRecord {
            version: 1,
            ops: vec![RedoSubRecord {
                record_type: RecordType::Put as u32,
                payload: vec![1, 2, 3],
            }],
            calvin_stamp: None,
        };

        let lsn1 = wal.append_transaction_redo(t, v, db, &record).unwrap();
        let lsn2 = wal.append_transaction_redo(t, v, db, &record).unwrap();
        let lsn3 = wal.append_transaction_redo(t, v, db, &record).unwrap();

        assert_eq!(lsn1, Lsn::new(1));
        assert_eq!(lsn2, Lsn::new(2));
        assert_eq!(lsn3, Lsn::new(3));
        assert!(lsn2 > lsn1);
        assert!(lsn3 > lsn2);

        wal.sync().unwrap();

        let records = wal.replay().unwrap();
        assert_eq!(records.len(), 3);
        assert_eq!(
            records[0].header.record_type,
            RecordType::TransactionRedo as u32
        );
        let decoded = RedoRecord::from_bytes(&records[0].payload).unwrap();
        assert_eq!(decoded, record);
    }

    #[test]
    fn crdt_delta_roundtrip() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("wal_dir");

        let wal = WalManager::open_for_testing(&path).unwrap();

        let t = TenantId::new(5);
        let v = VShardId::new(42);
        let db = DatabaseId::DEFAULT;

        let lsn = wal
            .append_crdt_delta(t, v, db, b"loro-delta-bytes")
            .unwrap();
        assert_eq!(lsn, Lsn::new(1));

        wal.sync().unwrap();

        let records = wal.replay().unwrap();
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].header.record_type, RecordType::CrdtDelta as u32);
        assert_eq!(records[0].header.tenant_id, 5);
        assert_eq!(records[0].header.vshard_id, 42);
        assert_eq!(records[0].payload, b"loro-delta-bytes");
    }

    #[test]
    fn fts_index_append_roundtrip() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("wal_fts");

        let wal = WalManager::open_for_testing(&path).unwrap();

        let payload = FtsIndexPayload::new(
            nodedb_types::sync::wire::SyncProvenance {
                producer_id: 0xDEAD_BEEF_CAFE_1234,
                epoch: 5,
                stream_id: 99,
                seq: 42,
            },
            "articles",
            "doc-abc",
            "hello world nodedb fts",
        );
        let bytes = payload.to_bytes().unwrap();

        let t = TenantId::new(2);
        let v = VShardId::new(7);
        let db = DatabaseId::DEFAULT;

        let lsn = wal.append_fts_index(t, v, db, &bytes).unwrap();
        assert_eq!(lsn, Lsn::new(1));

        wal.sync().unwrap();

        let records = wal.replay().unwrap();
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].header.record_type, RecordType::FtsIndex as u32);
        assert_eq!(records[0].header.tenant_id, 2);
        assert_eq!(records[0].header.vshard_id, 7);

        let decoded = FtsIndexPayload::from_bytes(&records[0].payload).unwrap();
        assert_eq!(decoded.provenance.producer_id, 0xDEAD_BEEF_CAFE_1234);
        assert_eq!(decoded.provenance.epoch, 5);
        assert_eq!(decoded.provenance.stream_id, 99);
        assert_eq!(decoded.provenance.seq, 42);
        assert_eq!(decoded.collection, "articles");
        assert_eq!(decoded.doc_id, "doc-abc");
        assert_eq!(decoded.text, "hello world nodedb fts");
    }
}
