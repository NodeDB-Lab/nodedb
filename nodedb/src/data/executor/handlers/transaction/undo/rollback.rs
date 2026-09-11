// SPDX-License-Identifier: BUSL-1.1

//! Rollback driver for the undo log.

use super::UndoEntry;
use crate::data::executor::core_loop::CoreLoop;

impl CoreLoop {
    /// Roll back completed writes in reverse order.
    ///
    /// Returns `Ok(())` if all undo entries were applied successfully.
    ///
    /// Returns `Err((entry_index, detail))` on the first undo failure —
    /// the entry index is the original forward-order position of the failed
    /// entry (before reversal). On failure the caller **must** return a
    /// `RollbackFailed` error to the client; the shard state is unknown
    /// and requires a restart to restore consistency via WAL replay.
    pub(in crate::data::executor::handlers) fn rollback_undo_log(
        &mut self,
        did: u64,
        tid: u64,
        undo_log: Vec<UndoEntry>,
    ) -> Result<(), (usize, String)> {
        self.rollback_undo_log_inner(did, tid, None, undo_log)
    }

    pub(in crate::data::executor::handlers) fn rollback_undo_log_at(
        &mut self,
        did: u64,
        tid: u64,
        vshard_id: crate::types::VShardId,
        undo_log: Vec<UndoEntry>,
    ) -> Result<(), (usize, String)> {
        self.rollback_undo_log_inner(did, tid, Some(vshard_id), undo_log)
    }

    fn rollback_undo_log_inner(
        &mut self,
        did: u64,
        tid: u64,
        vshard_id: Option<crate::types::VShardId>,
        undo_log: Vec<UndoEntry>,
    ) -> Result<(), (usize, String)> {
        let total = undo_log.len();
        for (rev_idx, entry) in undo_log.into_iter().rev().enumerate() {
            // Convert reversed index back to original forward-order index for
            // diagnostics (makes it easier to correlate with the sub-plan that
            // produced this undo entry).
            let original_idx = total.saturating_sub(1 + rev_idx);
            self.apply_undo_entry(did, tid, vshard_id, original_idx, entry)?;
        }
        Ok(())
    }

    /// Apply a single undo entry. Returns `Err((entry_index, detail))` if the
    /// undo cannot be applied — this is a fatal condition: the shard's in-memory
    /// state is now partially rolled back and must not serve writes.
    fn apply_undo_entry(
        &mut self,
        did: u64,
        tid: u64,
        vshard_id: Option<crate::types::VShardId>,
        entry_index: usize,
        entry: UndoEntry,
    ) -> Result<(), (usize, String)> {
        match entry {
            UndoEntry::PutDocument { .. } | UndoEntry::DeleteDocument { .. } => {
                self.apply_undo_document(did, tid, entry_index, entry)
            }
            UndoEntry::InsertVector { .. } | UndoEntry::DeleteVector { .. } => {
                self.apply_undo_vector(tid, entry_index, entry)
            }
            UndoEntry::SpatialInsert { .. } | UndoEntry::SpatialDelete { .. } => {
                self.apply_undo_spatial(entry_index, entry)
            }
            UndoEntry::PutEdge { ref src_id, .. } | UndoEntry::DeleteEdge { ref src_id, .. } => {
                let account_stats = vshard_id.is_none_or(|vshard_id| {
                    vshard_id == crate::types::VShardId::from_key(src_id.as_bytes())
                });
                self.apply_undo_edge_with_stats(did, tid, entry_index, entry, account_stats)
            }
            UndoEntry::KvPut { .. }
            | UndoEntry::KvDelete { .. }
            | UndoEntry::KvBatchPut { .. }
            | UndoEntry::KvTransfer { .. }
            | UndoEntry::KvTransferItem { .. }
            | UndoEntry::KvTtl { .. }
            | UndoEntry::SortedIndexDdl { .. } => self.apply_undo_kv(did, tid, entry_index, entry),
            UndoEntry::ColumnarInsert { .. }
            | UndoEntry::ColumnarUpdate { .. }
            | UndoEntry::ColumnarDelete { .. } => self.apply_undo_columnar(entry_index, entry),
            UndoEntry::TimeseriesIngest(_) => self.apply_undo_timeseries(entry_index, entry),
            UndoEntry::StatsRestore { .. } => self.apply_undo_stats(entry_index, entry),
            UndoEntry::MarkNodeDeleted { .. } => self.apply_undo_mark_node(entry_index, entry),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::time::{Duration, Instant};

    use super::*;
    use crate::bridge::envelope::{PhysicalPlan, Priority, Request};
    use crate::data::executor::core_loop::tests::make_core_with_dir;
    use crate::data::executor::handlers::point::apply_delete::PointDeleteParams;
    use crate::data::executor::handlers::point::apply_put::PointPutParams;
    use crate::data::executor::handlers::transaction::sub_plan_doc::{TxPointDelete, TxPointPut};
    use crate::data::executor::task::ExecutionTask;
    use crate::engine::document::store::CollectionConfig;
    use crate::engine::graph::csr::Direction;
    use crate::engine::graph::edge_store::EdgeRef;
    use crate::engine::sparse::btree_versioned::{VersionedIndexEntry, VersionedPut};
    use crate::types::{DatabaseId, ReadConsistency, RequestId, TenantId, TraceId, VShardId};
    use nodedb_physical::physical_plan::DocumentOp;
    use nodedb_types::Surrogate;

    const DB: u64 = 0;
    const TID: u64 = 1;

    fn seed_version(core: &CoreLoop, doc: &str, t: i64, body: &[u8]) {
        core.sparse
            .versioned_put(VersionedPut {
                database_id: DB,
                tenant: TID,
                coll: "c",
                doc_id: doc,
                sys_from_ms: t,
                valid_from_ms: 0,
                valid_until_ms: i64::MAX,
                body,
            })
            .unwrap();
    }

    fn seed_index(core: &CoreLoop, doc: &str, t: i64) {
        core.sparse
            .versioned_index_put(VersionedIndexEntry {
                database_id: DB,
                tenant: TID,
                coll: "c",
                field: "status",
                value: "active",
                doc_id: doc,
                sys_from_ms: t,
            })
            .unwrap();
    }

    fn index_lookup(core: &CoreLoop) -> Vec<String> {
        core.sparse
            .versioned_index_lookup_as_of(DB, TID, "c", "status", "active", None)
            .unwrap()
    }

    /// Scenario 4 (unit level): a rolled-back transaction that does a
    /// bitemporal PUT followed by a bitemporal DELETE (tombstone) must, via
    /// `rollback_undo_log` — the same reverse-order driver `execute_transaction_batch`
    /// uses on abort — restore `core.sparse.versioned_get_current` to its
    /// pre-transaction state (nothing) with the version rows and index entries
    /// physically gone, not merely hidden.
    #[test]
    fn rollback_undo_log_restores_pre_txn_state_for_bitemporal_put_then_delete() {
        let dir = tempfile::tempdir().unwrap();
        let (mut core, _tx, _rx) = make_core_with_dir(dir.path());

        // Pre-txn state: nothing exists for "d1".
        assert!(
            core.sparse
                .versioned_get_current(DB, TID, "c", "d1")
                .unwrap()
                .is_none()
        );

        // Forward tx: PUT at t=1000, then DELETE (tombstone) at t=2000.
        seed_version(&core, "d1", 1_000, b"v1");
        seed_index(&core, "d1", 1_000);
        core.sparse
            .versioned_tombstone(DB, TID, "c", "d1", 2_000)
            .unwrap();
        core.sparse
            .versioned_index_tombstone(VersionedIndexEntry {
                database_id: DB,
                tenant: TID,
                coll: "c",
                field: "status",
                value: "active",
                doc_id: "d1",
                sys_from_ms: 2_000,
            })
            .unwrap();

        // Sanity: the forward tx did delete the row (as observed mid-tx).
        assert!(
            core.sparse
                .versioned_get_current(DB, TID, "c", "d1")
                .unwrap()
                .is_none()
        );

        let undo_log = vec![
            UndoEntry::PutDocument {
                collection: "c".into(),
                document_id: "d1".into(),
                surrogate: nodedb_types::Surrogate::ZERO,
                old_value: None,
                bitemporal_sys_from_ms: Some(1_000),
                bitemporal_index_tuples: vec![("status".into(), "active".into())],
                secondary_index_added: Vec::new(),
                secondary_index_removed: Vec::new(),
                chain_hash_prior: None,
            },
            UndoEntry::DeleteDocument {
                collection: "c".into(),
                document_id: "d1".into(),
                surrogate: nodedb_types::Surrogate::ZERO,
                old_value: b"v1".to_vec(),
                bitemporal_sys_from_ms: Some(2_000),
                bitemporal_index_tuples: vec![("status".into(), "active".into())],
                secondary_index_tuples: Vec::new(),
                chain_hash_prior: None,
            },
        ];

        // Abort: roll back in reverse order, exactly as `execute_transaction_batch`
        // does when a sub-plan fails.
        core.rollback_undo_log(DB, TID, undo_log)
            .expect("rollback must succeed");

        // Pre-txn state restored: no current version, no index entry.
        assert!(
            core.sparse
                .versioned_get_current(DB, TID, "c", "d1")
                .unwrap()
                .is_none(),
            "aborted bitemporal put+delete must leave no current version behind"
        );
        assert!(
            index_lookup(&core).is_empty(),
            "aborted bitemporal put+delete must leave no index entry behind"
        );
    }

    // ── Transaction index-parity tests ──────────────────────────────────────
    //
    // A tx write behaves identically to an autocommit write (COMMIT parity),
    // and a rolled-back tx write returns every index to its pre-transaction
    // state (ROLLBACK parity), across a collection carrying a secondary index
    // + spatial R-tree + HNSW vector index + column stats + graph edges.
    //
    // SCOPE: FTS/inverted-index postings are included — a rolled-back DELETE
    // re-indexes the restored body (recomputed deterministically), and a
    // rolled-back PUT removes the postings it wrote. Both directions are
    // asserted via `fts_searchable`.

    const COLL: &str = "c";
    const PK: &str = "doc1";
    type Core = CoreLoop;

    /// Register the collection config (secondary index on `status`) and the
    /// schemaless vector params (field `emb`) so `apply_point_put` exercises every
    /// side-effect path.
    fn register(core: &mut Core) {
        core.doc_configs.insert(
            (
                nodedb_types::DatabaseId::new(DB),
                TenantId::new(TID),
                COLL.to_string(),
            ),
            CollectionConfig::new(COLL).with_index("status"),
        );
        core.vector_params.insert(
            (
                nodedb_types::DatabaseId::new(DB),
                TenantId::new(TID),
                format!("{COLL}:emb"),
            ),
            crate::engine::vector::hnsw::HnswParams::default(),
        );
    }

    /// A document with an indexed scalar (`status`), a GeoJSON geometry (`geom`),
    /// and a vector field (`emb`) — one value that fans out to all four index
    /// families.
    fn doc_bytes() -> Vec<u8> {
        use nodedb_types::Value;
        let mut geom = std::collections::HashMap::new();
        geom.insert("type".to_string(), Value::String("Point".into()));
        geom.insert(
            "coordinates".to_string(),
            Value::Array(vec![Value::Float(1.0), Value::Float(2.0)]),
        );
        let mut obj = std::collections::HashMap::new();
        obj.insert("status".to_string(), Value::String("active".into()));
        obj.insert("geom".to_string(), Value::Object(geom));
        obj.insert(
            "emb".to_string(),
            Value::Array(vec![
                Value::Float(1.0),
                Value::Float(2.0),
                Value::Float(3.0),
            ]),
        );
        zerompk::to_msgpack_vec(&Value::Object(obj)).unwrap()
    }

    fn row_key() -> String {
        crate::engine::document::store::surrogate_to_doc_id(Surrogate::new(1))
    }

    fn parity_spatial_key() -> (nodedb_types::DatabaseId, TenantId, String, String) {
        (
            nodedb_types::DatabaseId::new(DB),
            TenantId::new(TID),
            COLL.to_string(),
            "geom".to_string(),
        )
    }

    fn vector_key() -> (nodedb_types::DatabaseId, TenantId, String) {
        CoreLoop::vector_index_key(DB, TID, COLL, "emb")
    }

    // ── State probes (used to compare parity across paths) ──────────────────

    fn secondary_index_docs(core: &Core) -> Vec<String> {
        let mut v: Vec<String> = core
            .sparse
            .scan_index_values(DB, TID, COLL, "status", 100)
            .unwrap()
            .into_iter()
            .map(|(doc_id, _value)| doc_id.to_string())
            .collect();
        v.sort();
        v
    }

    fn stats_row_count(core: &Core) -> Option<u64> {
        core.stats_store
            .get(DB, TID, COLL, "status")
            .unwrap()
            .map(|s| s.row_count)
    }

    fn spatial_entry_present(core: &Core) -> bool {
        let entry_id = crate::util::fnv1a_hash(row_key().as_bytes());
        core.spatial_indexes
            .get(&parity_spatial_key())
            .map(|rt| rt.entries().into_iter().any(|e| e.id == entry_id))
            .unwrap_or(false)
    }

    /// Whether the document's text (`status: "active"`) is findable in the
    /// full-text inverted index. This is the direct probe for the
    /// restored-but-unsearchable gap: a rolled-back DELETE must make it true again.
    fn fts_searchable(core: &Core) -> bool {
        !core
            .inverted
            .search(
                DB,
                TenantId::new(TID),
                COLL,
                nodedb_fts::FtsSearchParams {
                    query: "active",
                    top_k: 10,
                    fuzzy_enabled: false,
                    mode: nodedb_fts::posting::QueryMode::And,
                    prefilter: None,
                },
            )
            .unwrap()
            .is_empty()
    }

    /// Whether the collection's single vector is searchable (not soft-deleted).
    fn vector_searchable(core: &Core) -> bool {
        core.vector_collections
            .get(&vector_key())
            .map(|coll| !coll.search(&[1.0, 2.0, 3.0], 1, 16).is_empty())
            .unwrap_or(false)
    }

    fn edge_present(core: &mut Core) -> bool {
        let tenant = nodedb_types::TenantId::new(TID);
        let store = core
            .edge_store
            .get_edge(DB, tenant, COLL, PK, "KNOWS", "bob")
            .unwrap()
            .is_some();
        let csr = !core
            .csr_partition_mut(DB, TID)
            .neighbors(PK, None, Direction::Out)
            .is_empty();
        store && csr
    }

    // ── Write drivers ─────────────────────────────────────────────────────────

    /// Autocommit PUT via `apply_point_put` inside a self-owned redb txn (mirrors
    /// `execute_point_put`).
    fn autocommit_put(core: &mut Core) {
        let value = doc_bytes();
        let txn = core.sparse.begin_write().unwrap();
        core.apply_point_put(
            &txn,
            PointPutParams {
                resolved_targets: &[],
                database_id: DB,
                tid: TID,
                collection: COLL,
                document_id: &row_key(),
                surrogate: Surrogate::new(1),
                value: &value,
                index_text: true,
                user_roles: &[],
                enforce: true,
                wal_lsn: None,
            },
        )
        .unwrap();
        txn.commit().unwrap();
    }

    /// Autocommit DELETE via `apply_point_delete` inside a self-owned redb txn
    /// (mirrors `execute_point_delete`).
    fn autocommit_delete(core: &mut Core) {
        let txn = core.sparse.begin_write().unwrap();
        core.apply_point_delete(
            &txn,
            PointDeleteParams {
                resolved_targets: &[],
                database_id: DB,
                tid: TID,
                collection: COLL,
                document_id: PK,
                surrogate: Surrogate::new(1),
                user_roles: &[],
                enforce: true,
            },
        )
        .unwrap();
        txn.commit().unwrap();
    }

    /// A throwaway `ExecutionTask` (DEFAULT database id, inert `PointGet` plan) —
    /// the only fields the tx doc helpers read are `database_id` and `request_id`.
    fn dummy_task() -> ExecutionTask {
        ExecutionTask::new(Request {
            request_id: RequestId::new(1),
            tenant_id: TenantId::new(TID),
            database_id: DatabaseId::DEFAULT,
            vshard_id: VShardId::new(0),
            plan: PhysicalPlan::Document(DocumentOp::PointGet {
                collection: nodedb_types::QualifiedCollection::new(DatabaseId::DEFAULT, COLL),
                document_id: PK.into(),
                surrogate: Surrogate::ZERO,
                pk_bytes: Vec::new(),
                rls_filters: Vec::new(),
                system_time: nodedb_types::SystemTimeScope::Current,
                valid_at_ms: None,
            }),
            // no-determinism: test-only deadline is not written to Calvin state.
            deadline: Instant::now() + Duration::from_secs(30),
            priority: Priority::Normal,
            trace_id: TraceId::ZERO,
            consistency: ReadConsistency::Strong,
            idempotency_key: None,
            event_source: crate::event::EventSource::User,
            user_roles: Vec::new(),
            user_id: None,
            statement_digest: None,
            txn_id: None,
            wal_lsn: None,
            resolved_now_ms: None,
            admission: crate::bridge::envelope::Admission::Exempt(
                crate::bridge::envelope::ExemptReason::Read,
            ),
        })
    }

    fn seed_edge(core: &mut Core) {
        let tenant = nodedb_types::TenantId::new(TID);
        let ord = core.hlc.next_ordinal();
        core.edge_store
            .put_edge_versioned(
                EdgeRef::new(
                    nodedb_types::DatabaseId::new(DB),
                    tenant,
                    COLL,
                    PK,
                    "KNOWS",
                    "bob",
                ),
                b"p1",
                ord,
                nodedb_types::ordinal_to_ms(ord),
                i64::MAX,
            )
            .unwrap();
        core.csr_partition_mut(DB, TID)
            .add_edge(PK, "KNOWS", "bob")
            .unwrap();
    }

    // ── PUT parity ────────────────────────────────────────────────────────────

    #[test]
    fn tx_put_commit_matches_autocommit_across_all_indexes() {
        let dir_a = tempfile::tempdir().unwrap();
        let (mut a, _ta, _ra) = make_core_with_dir(dir_a.path());
        register(&mut a);
        autocommit_put(&mut a);

        let dir_b = tempfile::tempdir().unwrap();
        let (mut b, _tb, _rb) = make_core_with_dir(dir_b.path());
        register(&mut b);
        let task = dummy_task();
        let mut undo_log = Vec::new();
        let value = doc_bytes();
        b.tx_point_put(
            TxPointPut {
                task: &task,
                tid: TID,
                collection: COLL,
                document_id: PK,
                surrogate: Surrogate::new(1),
                value: &value,
                user_roles: &[],
                insert_if_absent: None,
                resolved_sum_targets: &[],
                deferred_sum_targets: &[],
            },
            &mut undo_log,
        )
        .unwrap();

        // Identical index state across the autocommit and committed-tx paths.
        assert_eq!(secondary_index_docs(&a), secondary_index_docs(&b));
        assert_eq!(secondary_index_docs(&b), vec![row_key()]);
        assert_eq!(stats_row_count(&a), stats_row_count(&b));
        assert_eq!(stats_row_count(&b), Some(1));
        assert_eq!(spatial_entry_present(&a), spatial_entry_present(&b));
        assert!(spatial_entry_present(&b));
        assert_eq!(vector_searchable(&a), vector_searchable(&b));
        assert!(vector_searchable(&b));
        assert_eq!(fts_searchable(&a), fts_searchable(&b));
        assert!(fts_searchable(&b), "text must be searchable after tx PUT");
    }

    #[test]
    fn tx_put_rollback_restores_pre_tx_state_across_all_indexes() {
        let dir = tempfile::tempdir().unwrap();
        let (mut core, _t, _r) = make_core_with_dir(dir.path());
        register(&mut core);

        // Pre-tx: everything empty.
        assert!(secondary_index_docs(&core).is_empty());
        assert_eq!(stats_row_count(&core), None);
        assert!(!spatial_entry_present(&core));
        assert!(!vector_searchable(&core));
        assert!(!fts_searchable(&core));

        let task = dummy_task();
        let mut undo_log = Vec::new();
        let value = doc_bytes();
        core.tx_point_put(
            TxPointPut {
                task: &task,
                tid: TID,
                collection: COLL,
                document_id: PK,
                surrogate: Surrogate::new(1),
                value: &value,
                user_roles: &[],
                insert_if_absent: None,
                resolved_sum_targets: &[],
                deferred_sum_targets: &[],
            },
            &mut undo_log,
        )
        .unwrap();
        // Mid-tx: side-effects landed.
        assert_eq!(secondary_index_docs(&core), vec![row_key()]);
        assert!(spatial_entry_present(&core));
        assert!(vector_searchable(&core));
        assert!(fts_searchable(&core));

        core.rollback_undo_log(DB, TID, undo_log)
            .expect("rollback must succeed");

        // Post-rollback: back to the pre-tx state on every index.
        assert!(
            secondary_index_docs(&core).is_empty(),
            "secondary index must be empty after put-rollback"
        );
        assert_eq!(
            stats_row_count(&core),
            None,
            "column stats must be removed after put-rollback"
        );
        assert!(
            !spatial_entry_present(&core),
            "spatial entry must be gone after put-rollback"
        );
        assert!(
            !vector_searchable(&core),
            "vector must be soft-deleted (unsearchable) after put-rollback"
        );
        assert!(
            !fts_searchable(&core),
            "text must be unsearchable after put-rollback (postings removed)"
        );
    }

    // ── DELETE parity ─────────────────────────────────────────────────────────

    #[test]
    fn tx_delete_commit_matches_autocommit_across_all_indexes() {
        let dir_a = tempfile::tempdir().unwrap();
        let (mut a, _ta, _ra) = make_core_with_dir(dir_a.path());
        register(&mut a);
        autocommit_put(&mut a);
        seed_edge(&mut a);
        autocommit_delete(&mut a);

        let dir_b = tempfile::tempdir().unwrap();
        let (mut b, _tb, _rb) = make_core_with_dir(dir_b.path());
        register(&mut b);
        autocommit_put(&mut b);
        seed_edge(&mut b);
        let task = dummy_task();
        let mut undo_log = Vec::new();
        b.tx_point_delete(
            TxPointDelete {
                task: &task,
                tid: TID,
                collection: COLL,
                document_id: PK,
                surrogate: Surrogate::new(1),
                user_roles: &[],
                resolved_sum_targets: &[],
            },
            &mut undo_log,
        )
        .unwrap();

        // Both paths wiped every index identically.
        assert_eq!(secondary_index_docs(&a), secondary_index_docs(&b));
        assert!(secondary_index_docs(&b).is_empty());
        assert_eq!(spatial_entry_present(&a), spatial_entry_present(&b));
        assert!(!spatial_entry_present(&b));
        assert_eq!(vector_searchable(&a), vector_searchable(&b));
        assert!(!vector_searchable(&b));
        assert_eq!(fts_searchable(&a), fts_searchable(&b));
        assert!(
            !fts_searchable(&b),
            "text must be unsearchable after delete"
        );
        assert_eq!(edge_present(&mut a), edge_present(&mut b));
        assert!(!edge_present(&mut b));
        assert_eq!(
            a.is_node_deleted(DB, TID, PK),
            b.is_node_deleted(DB, TID, PK)
        );
        assert!(b.is_node_deleted(DB, TID, PK));
    }

    #[test]
    fn tx_delete_rollback_restores_pre_tx_state_across_all_indexes() {
        let dir = tempfile::tempdir().unwrap();
        let (mut core, _t, _r) = make_core_with_dir(dir.path());
        register(&mut core);
        autocommit_put(&mut core);
        seed_edge(&mut core);

        // Pre-tx snapshot: every index populated, node not yet deleted.
        assert_eq!(secondary_index_docs(&core), vec![row_key()]);
        assert!(spatial_entry_present(&core));
        assert!(vector_searchable(&core));
        assert!(fts_searchable(&core));
        assert!(edge_present(&mut core));
        assert!(!core.is_node_deleted(DB, TID, PK));

        let task = dummy_task();
        let mut undo_log = Vec::new();
        core.tx_point_delete(
            TxPointDelete {
                task: &task,
                tid: TID,
                collection: COLL,
                document_id: PK,
                surrogate: Surrogate::new(1),
                user_roles: &[],
                resolved_sum_targets: &[],
            },
            &mut undo_log,
        )
        .unwrap();
        // Mid-tx: the delete cascaded.
        assert!(!spatial_entry_present(&core));
        assert!(!vector_searchable(&core));
        assert!(!fts_searchable(&core));
        assert!(core.is_node_deleted(DB, TID, PK));

        core.rollback_undo_log(DB, TID, undo_log)
            .expect("rollback must succeed");

        // Post-rollback: every NON-FTS index restored to its pre-tx state.
        assert_eq!(
            secondary_index_docs(&core),
            vec![row_key()],
            "secondary index must be restored after delete-rollback"
        );
        assert!(
            spatial_entry_present(&core),
            "spatial entry must be restored after delete-rollback"
        );
        assert!(
            vector_searchable(&core),
            "vector must be un-soft-deleted (searchable) after delete-rollback"
        );
        assert!(
            edge_present(&mut core),
            "graph edge must be restored in both stores after delete-rollback"
        );
        assert!(
            !core.is_node_deleted(DB, TID, PK),
            "deleted-node tombstone must be un-marked after delete-rollback"
        );
        assert!(
            fts_searchable(&core),
            "FTS postings must be restored (doc searchable again) after delete-rollback"
        );
    }
}
