use nodedb_wal::record::RecordType;

use super::core::WalManager;
use crate::types::{Lsn, TenantId, VShardId};

impl WalManager {
    /// Internal: append a record of the given type to the WAL.
    fn append_record(
        &self,
        record_type: RecordType,
        tenant_id: TenantId,
        vshard_id: VShardId,
        payload: &[u8],
    ) -> crate::Result<Lsn> {
        let mut wal = self.wal.lock().unwrap_or_else(|p| p.into_inner());
        let lsn = wal
            .append(
                record_type as u16,
                tenant_id.as_u32(),
                vshard_id.as_u16(),
                payload,
            )
            .map_err(crate::Error::Wal)?;
        Ok(Lsn::new(lsn))
    }

    pub fn append_put(&self, tid: TenantId, vs: VShardId, p: &[u8]) -> crate::Result<Lsn> {
        self.append_record(RecordType::Put, tid, vs, p)
    }

    pub fn append_delete(&self, tid: TenantId, vs: VShardId, p: &[u8]) -> crate::Result<Lsn> {
        self.append_record(RecordType::Delete, tid, vs, p)
    }

    pub fn append_vector_put(&self, tid: TenantId, vs: VShardId, p: &[u8]) -> crate::Result<Lsn> {
        self.append_record(RecordType::VectorPut, tid, vs, p)
    }

    pub fn append_vector_delete(
        &self,
        tid: TenantId,
        vs: VShardId,
        p: &[u8],
    ) -> crate::Result<Lsn> {
        self.append_record(RecordType::VectorDelete, tid, vs, p)
    }

    pub fn append_vector_params(
        &self,
        tid: TenantId,
        vs: VShardId,
        p: &[u8],
    ) -> crate::Result<Lsn> {
        self.append_record(RecordType::VectorParams, tid, vs, p)
    }

    pub fn append_transaction(&self, tid: TenantId, vs: VShardId, p: &[u8]) -> crate::Result<Lsn> {
        self.append_record(RecordType::Transaction, tid, vs, p)
    }

    pub fn append_crdt_delta(
        &self,
        tid: TenantId,
        vs: VShardId,
        delta: &[u8],
    ) -> crate::Result<Lsn> {
        self.append_record(RecordType::CrdtDelta, tid, vs, delta)
    }

    /// Append a checkpoint marker. Serializes the LSN before writing.
    pub fn append_checkpoint(
        &self,
        tid: TenantId,
        vs: VShardId,
        checkpoint_lsn: u64,
    ) -> crate::Result<Lsn> {
        let payload =
            zerompk::to_msgpack_vec(&checkpoint_lsn).map_err(|e| crate::Error::Serialization {
                format: "msgpack".into(),
                detail: format!("checkpoint: {e}"),
            })?;
        self.append_record(RecordType::Checkpoint, tid, vs, &payload)
    }

    pub fn append_timeseries_batch(
        &self,
        tid: TenantId,
        vs: VShardId,
        p: &[u8],
    ) -> crate::Result<Lsn> {
        self.append_record(RecordType::TimeseriesBatch, tid, vs, p)
    }

    pub fn append_log_batch(&self, tid: TenantId, vs: VShardId, p: &[u8]) -> crate::Result<Lsn> {
        self.append_record(RecordType::LogBatch, tid, vs, p)
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
        self.append_record(RecordType::TemporalPurge, tid, VShardId::new(0), &payload)
    }

    pub fn append_collection_tombstone(
        &self,
        tid: TenantId,
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
            &payload,
        )
    }
}
