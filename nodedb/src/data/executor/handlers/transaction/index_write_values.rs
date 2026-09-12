// SPDX-License-Identifier: BUSL-1.1

//! Per-index write-VALUE recording for transaction batches — the
//! distributed-Calvin staging carrier and the fast-path/staged recorders.
//! Sibling of `write_version.rs` (per-key/collection versions).

use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::task::ExecutionTask;
use crate::types::TenantId;

use super::undo::UndoEntry;

/// Hard upper bound on the number of `(epoch, position, vshard)` buckets of
/// distributed-Calvin-flush index tuples staged and awaiting their post-apply
/// `RecordCalvinWriteVersions` drain. Overflow evicts the lowest-keyed bucket.
const MAX_CALVIN_FLUSH_INDEX_TUPLES: usize = 16_384;

/// Staged index-value tuples for distributed-Calvin flushes, keyed by the batch
/// identity `(epoch, position, vshard)`. Each bucket holds one flushed batch's
/// `(collection, (field, value) pairs)`, awaiting the post-apply drain.
pub(in crate::data::executor) type StagedCalvinIndexTuples =
    std::collections::HashMap<(u64, u32, u32), Vec<(String, Vec<(String, String)>)>>;

/// Extract the `(collection, (field, value) tuples)` a document `UndoEntry`
/// touched, combining every index dimension the write mutated. Returns `None`
/// for non-document entries (whose per-key identity is engine-internal). Shared
/// by the immediate-record and distributed-Calvin-flush staging paths so both
/// derive the identical tuple set.
fn entry_index_tuples(entry: &UndoEntry) -> Option<(String, Vec<(String, String)>)> {
    match entry {
        UndoEntry::PutDocument {
            collection,
            secondary_index_added,
            secondary_index_removed,
            bitemporal_index_tuples,
            ..
        } => {
            let mut tuples = Vec::with_capacity(
                secondary_index_added.len()
                    + secondary_index_removed.len()
                    + bitemporal_index_tuples.len(),
            );
            tuples.extend_from_slice(secondary_index_added);
            tuples.extend_from_slice(secondary_index_removed);
            tuples.extend_from_slice(bitemporal_index_tuples);
            Some((collection.clone(), tuples))
        }
        UndoEntry::DeleteDocument {
            collection,
            secondary_index_tuples,
            bitemporal_index_tuples,
            ..
        } => {
            let mut tuples =
                Vec::with_capacity(secondary_index_tuples.len() + bitemporal_index_tuples.len());
            tuples.extend_from_slice(secondary_index_tuples);
            tuples.extend_from_slice(bitemporal_index_tuples);
            Some((collection.clone(), tuples))
        }
        UndoEntry::InsertVector { .. }
        | UndoEntry::DeleteVector { .. }
        | UndoEntry::SpatialInsert { .. }
        | UndoEntry::SpatialDelete { .. }
        | UndoEntry::PutEdge { .. }
        | UndoEntry::DeleteEdge { .. }
        | UndoEntry::KvPut { .. }
        | UndoEntry::KvDelete { .. }
        | UndoEntry::KvBatchPut { .. }
        | UndoEntry::KvTransfer { .. }
        | UndoEntry::KvTransferItem { .. }
        | UndoEntry::KvTtl { .. }
        | UndoEntry::SortedIndexDdl { .. }
        | UndoEntry::MarkNodeDeleted { .. }
        | UndoEntry::ColumnarInsert { .. }
        | UndoEntry::ColumnarUpdate { .. }
        | UndoEntry::ColumnarDelete { .. }
        | UndoEntry::TimeseriesIngest(_)
        | UndoEntry::StatsRestore { .. } => None,
    }
}

impl CoreLoop {
    /// Record the touched secondary-index VALUES of every document write in a
    /// committed transaction batch into the per-index write-value substrate.
    ///
    /// Fast path — the task carries the batch's committed WAL LSN: record each
    /// write's tuples immediately at that LSN. Distributed-Calvin-flush path —
    /// the apply carries no WAL LSN (the committed LSN is only known post-apply)
    /// but a `calvin_flush_key` is scoped: STAGE the tuples under that key for
    /// the later `RecordCalvinWriteVersions` drain, in undo-log order. Otherwise
    /// (no LSN, no flush key) no-op — the version is never advanced with a wrong
    /// value. The undo log is the carrier: it already holds each write's
    /// `(field, value)` tuples plus its collection, in deterministic plan order.
    pub(in crate::data::executor) fn record_batch_index_write_values(
        &mut self,
        task: &ExecutionTask,
        tid: u64,
        undo_log: &[UndoEntry],
    ) {
        let db = task.request.database_id;
        let tenant = TenantId::new(tid);
        match (task.wal_lsn(), self.calvin_flush_key) {
            (Some(lsn), _) => {
                for entry in undo_log {
                    if let Some((collection, tuples)) = entry_index_tuples(entry) {
                        self.note_index_write_values(db, tenant, &collection, &tuples, lsn);
                    }
                }
            }
            (None, Some(key)) => {
                let mut staged: Vec<(String, Vec<(String, String)>)> = Vec::new();
                for entry in undo_log {
                    if let Some(pair) = entry_index_tuples(entry) {
                        staged.push(pair);
                    }
                }
                if staged.is_empty() {
                    return;
                }
                self.calvin_flush_index_tuples
                    .entry(key)
                    .or_default()
                    .extend(staged);
                self.evict_calvin_flush_index_overflow();
            }
            (None, None) => {}
        }
    }

    /// Bound the staged distributed-Calvin-flush index tuples: while the number
    /// of `(epoch, position, vshard)` buckets exceeds
    /// `MAX_CALVIN_FLUSH_INDEX_TUPLES`, evict the lowest-keyed bucket. The key
    /// order is a total order, so eviction is deterministic across replicas.
    fn evict_calvin_flush_index_overflow(&mut self) {
        while self.calvin_flush_index_tuples.len() > MAX_CALVIN_FLUSH_INDEX_TUPLES {
            let Some(victim) = self.calvin_flush_index_tuples.keys().min().copied() else {
                break;
            };
            self.calvin_flush_index_tuples.remove(&victim);
        }
    }

    /// Record the index-value tuples a distributed Calvin flush staged for
    /// `(epoch, position, vshard)` at the replicated applied `lsn`, then drop the
    /// staged entry. No-op if nothing was staged (single-shard fast path).
    pub(in crate::data::executor) fn record_staged_calvin_index_values(
        &mut self,
        db: crate::types::DatabaseId,
        tenant: TenantId,
        epoch: u64,
        position: u32,
        vshard: u32,
        lsn: crate::types::Lsn,
    ) {
        if let Some(entries) = self
            .calvin_flush_index_tuples
            .remove(&(epoch, position, vshard))
        {
            for (collection, tuples) in entries {
                self.note_index_write_values(db, tenant, &collection, &tuples, lsn);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::time::{Duration, Instant};

    use nodedb_types::Surrogate;

    use super::*;
    use crate::bridge::envelope::{Admission, ExemptReason, Priority, Request};
    use crate::data::executor::core_loop::tests::make_core_with_dir;
    use crate::types::{DatabaseId, Lsn, RequestId, TraceId, VShardId};

    /// A minimal `ExecutionTask` homing to vShard 0, tenant 1, database DEFAULT,
    /// carrying no WAL LSN — the shape a distributed Calvin flush apply dispatches
    /// under, so `record_batch_index_write_values` takes the staging branch.
    fn make_task() -> ExecutionTask {
        let plan = crate::bridge::envelope::PhysicalPlan::Meta(
            nodedb_physical::physical_plan::meta::MetaOp::Compact,
        );
        let request = Request {
            request_id: RequestId::new(1),
            tenant_id: TenantId::new(1),
            database_id: DatabaseId::DEFAULT,
            vshard_id: VShardId::new(0),
            plan,
            deadline: Instant::now() + Duration::from_secs(5),
            priority: Priority::Normal,
            trace_id: TraceId::ZERO,
            consistency: crate::types::ReadConsistency::Strong,
            idempotency_key: None,
            event_source: crate::event::EventSource::User,
            user_roles: Vec::new(),
            user_id: None,
            statement_digest: None,
            txn_id: None,
            wal_lsn: None,
            resolved_now_ms: None,
            admission: Admission::Exempt(ExemptReason::Read),
        };
        ExecutionTask::new(request)
    }

    fn put_entry(collection: &str, field: &str, value: &str) -> UndoEntry {
        UndoEntry::PutDocument {
            collection: collection.to_string(),
            document_id: nodedb_types::StorageKey::for_surrogate(Surrogate::new(1)),
            identity: nodedb_types::StorageKey::for_surrogate(Surrogate::new(1)).to_identity(),
            old_value: None,
            bitemporal_sys_from_ms: None,
            bitemporal_index_tuples: Vec::new(),
            secondary_index_added: vec![(field.to_string(), value.to_string())],
            secondary_index_removed: Vec::new(),
            chain_hash_prior: None,
        }
    }

    fn delete_entry(collection: &str, field: &str, value: &str) -> UndoEntry {
        UndoEntry::DeleteDocument {
            collection: collection.to_string(),
            document_id: nodedb_types::StorageKey::for_surrogate(Surrogate::new(2)),
            identity: nodedb_types::StorageKey::for_surrogate(Surrogate::new(2)).to_identity(),
            old_value: Vec::new(),
            bitemporal_sys_from_ms: None,
            bitemporal_index_tuples: Vec::new(),
            secondary_index_tuples: vec![(field.to_string(), value.to_string())],
            chain_hash_prior: None,
        }
    }

    /// A distributed Calvin flush stages its index tuples under the flush key
    /// (the apply carries `wal_lsn: None`) instead of recording them; the
    /// post-apply drain records the staged put- AND delete-tuples at the
    /// replicated applied LSN and empties the staging map.
    #[test]
    fn calvin_flush_stage_then_drain_records_index_values() {
        let dir = tempfile::tempdir().unwrap();
        let (mut core, _tx, _rx) = make_core_with_dir(dir.path());

        let task = make_task();
        let tenant = TenantId::new(1);
        let db = DatabaseId::DEFAULT;
        let key = (7u64, 3u32, 0u32);

        // Flush scope active: the batch's index tuples must STAGE, not record.
        core.calvin_flush_key = Some(key);
        let undo_log = vec![
            put_entry("orders", "email", "a@b.c"),
            delete_entry("orders", "status", "gone"),
        ];
        core.record_batch_index_write_values(&task, tenant.as_u64(), &undo_log);

        // Nothing recorded yet (the applied LSN is not known at flush time).
        assert_eq!(
            core.write_index
                .index_values
                .value_lsn(db, tenant, "orders", "email", "a@b.c"),
            None,
            "flush staging must not record into the substrate"
        );
        assert!(
            core.calvin_flush_index_tuples.contains_key(&key),
            "the flush's index tuples must be staged under the flush key"
        );

        // Post-apply drain records both tuples at the replicated applied LSN.
        core.record_staged_calvin_index_values(db, tenant, key.0, key.1, key.2, Lsn::new(42));

        assert_eq!(
            core.write_index
                .index_values
                .value_lsn(db, tenant, "orders", "email", "a@b.c"),
            Some(Lsn::new(42)),
            "the drain must record the staged PUT tuple at the applied LSN"
        );
        assert_eq!(
            core.write_index
                .index_values
                .value_lsn(db, tenant, "orders", "status", "gone"),
            Some(Lsn::new(42)),
            "the drain must record the staged DELETE tuple at the applied LSN"
        );
        assert!(
            core.calvin_flush_index_tuples.is_empty(),
            "the drain must empty the staging map (cleanup)"
        );
    }

    /// Draining a `(epoch, position, vshard)` nothing was staged under is an
    /// idempotent no-op — the single-shard fast path never stages.
    #[test]
    fn drain_without_staging_is_noop() {
        let dir = tempfile::tempdir().unwrap();
        let (mut core, _tx, _rx) = make_core_with_dir(dir.path());
        let tenant = TenantId::new(1);
        let db = DatabaseId::DEFAULT;

        core.record_staged_calvin_index_values(db, tenant, 1, 0, 0, Lsn::new(9));

        assert!(core.calvin_flush_index_tuples.is_empty());
        assert_eq!(
            core.write_index
                .index_values
                .value_lsn(db, tenant, "orders", "email", "a@b.c"),
            None,
        );
    }
}
