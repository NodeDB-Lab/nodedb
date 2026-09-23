// SPDX-License-Identifier: BUSL-1.1

//! WAL appends for node-global metadata: temporal-purge audit, surrogate
//! allocation/binding, Calvin epoch tracking, applied Raft proposals, and
//! sync watermarks.

use nodedb_wal::record::RecordType;

use super::core::WalManager;
use crate::types::{DatabaseId, Lsn, TenantId, VShardId};

impl WalManager {
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

    /// Append a payload-free `ProposalApplied` marker for the replicated
    /// proposal whose apply is in scope (see [`WalManager::with_apply_key`]).
    /// An apply that writes no record of its own appends it in place of its
    /// forward record, so the proposal's key still reaches the WAL.
    ///
    /// Returns `None` and appends nothing outside a proposal's apply.
    pub fn append_proposal_applied(
        &self,
        tenant_id: TenantId,
        vshard_id: VShardId,
        database_id: DatabaseId,
    ) -> crate::Result<Option<Lsn>> {
        if super::append::current_apply_key() == 0 {
            return Ok(None);
        }
        self.append_record(
            RecordType::ProposalApplied,
            tenant_id,
            vshard_id,
            database_id,
            &[],
        )
        .map(Some)
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
}
