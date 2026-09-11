// SPDX-License-Identifier: BUSL-1.1

//! Deleted-node-tracker undo entry application logic.
//!
//! The PointDelete cascade records a deleted document's node id in the
//! in-memory `deleted_nodes` set so a subsequent `EdgePut` to that node is
//! rejected as dangling. This tracker is IN-MEMORY, so an aborted redb write
//! transaction does NOT reverse it — a rolled-back tx DELETE must explicitly
//! un-mark the node (mirroring the vector/spatial/stats undo paths, which
//! reverse in-memory side-effects an aborted redb txn leaves behind).
//!
//! The forward capture only pushes a `MarkNodeDeleted` entry when the mark
//! newly inserted the node, so this un-mark never resurrects a tombstone a
//! prior committed op created.
//!
//! Returns `Err((entry_index, detail))` on fatal failure so the caller can
//! escalate to a typed `RollbackFailed` response.

use crate::data::executor::core_loop::CoreLoop;

use super::UndoEntry;

impl CoreLoop {
    pub(super) fn apply_undo_mark_node(
        &mut self,
        _entry_index: usize,
        entry: UndoEntry,
    ) -> Result<(), (usize, String)> {
        match entry {
            UndoEntry::MarkNodeDeleted {
                database_id,
                tid,
                node_id,
            } => {
                self.unmark_node_deleted(database_id, tid, &node_id);
                Ok(())
            }
            _ => unreachable!("apply_undo_mark_node called with non-mark-node entry"),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::time::{Duration, Instant};

    use super::*;
    use crate::bridge::envelope::{PhysicalPlan, Priority, Request};
    use crate::data::executor::core_loop::tests::{make_core_with_dir, make_default_task};
    use crate::data::executor::handlers::point::apply_put::PointPutParams;
    use crate::data::executor::handlers::transaction::sub_plan_doc::TxPointDelete;
    use crate::data::executor::task::ExecutionTask;
    use crate::engine::document::store::CollectionConfig;
    use crate::types::{DatabaseId, ReadConsistency, RequestId, TenantId, TraceId, VShardId};
    use nodedb_physical::physical_plan::DocumentOp;
    use nodedb_types::Surrogate;

    const DB: u64 = 0;
    const TID: u64 = 1;
    const COLL: &str = "c";
    const PK: &str = "doc1";

    /// Register the collection config (secondary index on `status`) and the
    /// schemaless vector params (field `emb`), matching the parity fixture in
    /// `rollback.rs` — this file only needs it to seed a real document before
    /// exercising the node-tombstone undo path.
    fn register(core: &mut CoreLoop) {
        core.doc_configs.insert(
            (
                nodedb_types::DatabaseId::new(DB),
                TenantId::new(TID),
                COLL.to_string(),
            ),
            CollectionConfig::new(COLL).with_index("status"),
        );
    }

    fn doc_bytes() -> Vec<u8> {
        use nodedb_types::Value;
        let mut obj = std::collections::HashMap::new();
        obj.insert("status".to_string(), Value::String("active".into()));
        zerompk::to_msgpack_vec(&Value::Object(obj)).unwrap()
    }

    fn row_key() -> String {
        crate::engine::document::store::surrogate_to_doc_id(Surrogate::new(1))
    }

    /// Autocommit PUT via `apply_point_put` inside a self-owned redb txn (mirrors
    /// `execute_point_put`).
    fn autocommit_put(core: &mut CoreLoop) {
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

    #[test]
    fn mark_node_returns_true_only_on_first_insert() {
        let dir = tempfile::tempdir().unwrap();
        let (mut core, _t, _r) = make_core_with_dir(dir.path());
        assert!(
            core.mark_node_deleted(DB, TID, PK),
            "first mark newly inserts"
        );
        assert!(
            !core.mark_node_deleted(DB, TID, PK),
            "second mark is a no-op (already present)"
        );
        core.unmark_node_deleted(DB, TID, PK);
        assert!(!core.is_node_deleted(DB, TID, PK));
    }

    /// A tx DELETE of a document whose node a PRIOR committed op already tombstoned
    /// must NOT un-mark that node on rollback — the pre-existing tombstone survives.
    #[test]
    fn tx_delete_rollback_preserves_pre_existing_node_tombstone() {
        let dir = tempfile::tempdir().unwrap();
        let (mut core, _t, _r) = make_core_with_dir(dir.path());
        register(&mut core);
        autocommit_put(&mut core);

        // A prior committed op already marked this node deleted.
        assert!(core.mark_node_deleted(DB, TID, PK));
        assert!(core.is_node_deleted(DB, TID, PK));

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
        // The delete's mark was a no-op (already marked) → no MarkNodeDeleted undo
        // was captured, so rollback must leave the tombstone intact.
        assert!(
            !undo_log
                .iter()
                .any(|e| matches!(e, UndoEntry::MarkNodeDeleted { .. })),
            "no MarkNodeDeleted undo when the node was already marked"
        );

        core.rollback_undo_log(DB, TID, undo_log)
            .expect("rollback must succeed");

        assert!(
            core.is_node_deleted(DB, TID, PK),
            "pre-existing node tombstone must survive rollback"
        );
    }

    #[test]
    fn tx_edge_put_to_deleted_node_records_no_phantom_undo() {
        use crate::bridge::envelope::Status;

        let dir = tempfile::tempdir().unwrap();
        let (mut core, _tx, _rx) = make_core_with_dir(dir.path());
        let tenant = TenantId::new(TID);

        // The destination node is soft-deleted, so the edge insert is rejected by
        // `execute_edge_put`'s dangling-endpoint validation BEFORE any store write.
        core.mark_node_deleted(DB, TID, "bob");

        let task = make_default_task();
        let mut undo_log: Vec<UndoEntry> = Vec::new();
        let resp = core.execute_edge_put_with_undo(
            &task,
            crate::data::executor::handlers::graph::EdgePutParams {
                tid: TID,
                collection: "c",
                src_id: "alice",
                label: "KNOWS",
                dst_id: "bob",
                properties: b"p1",
                src_surrogate: nodedb_types::Surrogate::ZERO,
                dst_surrogate: nodedb_types::Surrogate::ZERO,
            },
            Some(&mut undo_log),
        );

        assert_eq!(
            resp.status,
            Status::Error,
            "an edge insert to a deleted node must be rejected"
        );
        assert!(
            undo_log.is_empty(),
            "a rejected insert must record NO compensation entry; a phantom PutEdge \
             undo would soft-delete a never-written edge on rollback, corrupting \
             bitemporal history"
        );
        assert!(
            core.edge_store
                .get_edge(DB, tenant, "c", "alice", "KNOWS", "bob")
                .unwrap()
                .is_none(),
            "the rejected insert must not have written any edge version"
        );
    }
}
