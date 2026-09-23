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
//! The node-label undo puts each label back, then withdraws the label names
//! and the node the op interned, so the CSR holds what it held before.
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

    /// The undo of a node-label op on `node_id` setting or removing `labels`,
    /// captured before the op runs.
    pub(in crate::data::executor) fn capture_node_labels_undo(
        &self,
        database_id: u64,
        tid: u64,
        node_id: &str,
        labels: &[String],
    ) -> UndoEntry {
        let partition = self.csr_partition(database_id, tid);
        let local = partition.and_then(|p| p.node_id_raw(node_id));
        let prior = labels
            .iter()
            .map(|label| {
                let carried = match (partition, local) {
                    (Some(p), Some(id)) => p.node_has_label(id, label),
                    _ => false,
                };
                (label.clone(), carried)
            })
            .collect();
        let mut interned_labels: Vec<String> = Vec::new();
        for label in labels {
            let known = partition.is_some_and(|p| p.has_node_label_name(label));
            if !known && !interned_labels.contains(label) {
                interned_labels.push(label.clone());
            }
        }
        UndoEntry::NodeLabels {
            database_id,
            tid,
            node_id: node_id.to_string(),
            prior,
            interned_labels,
            created_node: local.is_none(),
        }
    }

    /// Put every label a node-label op touched back to its prior state, then
    /// withdraw the label names and the node the op interned.
    pub(super) fn apply_undo_node_labels(
        &mut self,
        entry_index: usize,
        undo: NodeLabelsUndo,
    ) -> Result<(), (usize, String)> {
        let NodeLabelsUndo {
            database_id,
            tid,
            node_id,
            prior,
            interned_labels,
            created_node,
        } = undo;
        let partition = self.csr_partition_mut(database_id, tid);
        for (label, carried) in prior {
            if carried {
                partition.add_node_label(&node_id, &label).map_err(|e| {
                    (
                        entry_index,
                        format!("restoring label '{label}' on node '{node_id}': {e}"),
                    )
                })?;
            } else {
                partition.remove_node_label(&node_id, &label);
            }
        }
        for label in interned_labels.iter().rev() {
            partition
                .withdraw_newest_node_label(label)
                .map_err(|e| (entry_index, format!("withdrawing label '{label}': {e}")))?;
        }
        if created_node {
            partition
                .withdraw_newest_node(&node_id)
                .map_err(|e| (entry_index, format!("withdrawing node '{node_id}': {e}")))?;
        }
        Ok(())
    }
}

/// The fields of an `UndoEntry::NodeLabels`.
pub(super) struct NodeLabelsUndo {
    pub database_id: u64,
    pub tid: u64,
    pub node_id: String,
    pub prior: Vec<(String, bool)>,
    pub interned_labels: Vec<String>,
    pub created_node: bool,
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
                storage_key: crate::engine::document::store::StorageKey::for_surrogate(
                    Surrogate::new(1),
                ),
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
            "a rejected insert must record no undo entry: it wrote no edge version \
             for a rollback to remove"
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
