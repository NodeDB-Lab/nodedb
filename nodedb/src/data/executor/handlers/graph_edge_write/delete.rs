// SPDX-License-Identifier: BUSL-1.1

//! `EdgeDelete`: single-edge tombstone, with optional transactional undo.

use tracing::debug;

use crate::bridge::envelope::{ErrorCode, Response};
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::task::ExecutionTask;
use crate::types::TenantId;

use super::shared::{EdgeDeleteParams, owns_logical_edge_stats};

impl CoreLoop {
    pub(in crate::data::executor) fn execute_edge_delete(
        &mut self,
        task: &ExecutionTask,
        params: EdgeDeleteParams<'_>,
    ) -> Response {
        self.execute_edge_delete_with_undo(task, params, None)
    }

    /// Edge delete with optional transactional compensation.
    ///
    /// The `UndoEntry::DeleteEdge` is recorded only when a live pre-image
    /// existed *and* the tombstone was durably written — never speculatively
    /// before the write. A phantom entry would otherwise re-insert an edge that
    /// was never deleted when the surrounding transaction rolls back.
    ///
    /// The RLS write policy is decided against that same pre-image and BEFORE
    /// the tombstone: the row a policy governs is the edge that exists now, and
    /// a delete the policy rejects must leave the edge in place.
    ///
    /// The bitemporal edge store always appends a tombstone version on
    /// success, whether or not a live edge existed — so the affected count is
    /// never assumed and is always read from the pre-image lookup: 1 when a
    /// live edge existed to remove, 0 when the edge was already absent.
    pub(in crate::data::executor) fn execute_edge_delete_with_undo(
        &mut self,
        task: &ExecutionTask,
        params: EdgeDeleteParams<'_>,
        undo: Option<&mut Vec<crate::data::executor::handlers::transaction::undo::UndoEntry>>,
    ) -> Response {
        let EdgeDeleteParams {
            tid,
            collection,
            src_id,
            label,
            dst_id,
            rls_write_check,
        } = params;
        debug!(core = self.core_id, tid, %collection, %src_id, %label, %dst_id, "edge delete");
        let database_id = task.request.database_id.as_u64();

        // The pre-image is always read now: the RLS write gate needs it for
        // any non-admit-all policy, the undo log needs it for compensation,
        // and the response needs it to report a truthful affected count.
        let old_properties = self
            .edge_store
            .get_edge(
                database_id,
                TenantId::new(tid),
                collection,
                src_id,
                label,
                dst_id,
            )
            .ok()
            .flatten();
        let existed = old_properties.is_some();

        if let Err(error) = crate::data::executor::handlers::rls_write_gate::admit_edge_properties(
            rls_write_check,
            old_properties.as_deref(),
            tid,
            collection,
        ) {
            return self.response_error(task, error);
        }

        let ord = self
            .active_graph_system_from
            .unwrap_or_else(|| self.hlc.next_ordinal());
        use crate::engine::graph::edge_store::EdgeRef;
        match self.edge_store.soft_delete_edge_with_stats(
            EdgeRef::new(
                task.request.database_id,
                TenantId::new(tid),
                collection,
                src_id,
                label,
                dst_id,
            ),
            ord,
            owns_logical_edge_stats(task, src_id),
        ) {
            Ok(_) => {
                // Tombstone is durable; record the compensation for a rollback.
                if let (Some(undo), Some(props)) = (undo, old_properties) {
                    undo.push(
                        crate::data::executor::handlers::transaction::undo::UndoEntry::DeleteEdge {
                            collection: collection.to_string(),
                            src_id: src_id.to_string(),
                            label: label.to_string(),
                            dst_id: dst_id.to_string(),
                            old_properties: props,
                        },
                    );
                }
                let partition = self.csr_partition_mut(database_id, tid);
                partition.remove_edge_in_collection(src_id, label, dst_id, collection);
                self.checkpoint_coordinator.mark_dirty("sparse", 1);
                self.note_edge_write_lsn(task, tid, collection, src_id, label, dst_id);
                // CDC: emit after `note_edge_write_lsn` so the event LSN matches
                // this edge's WAL LSN (the WAL-replay reconstruction key).
                self.emit_graph_edge_event(
                    task,
                    crate::data::executor::core_loop::event_emit::GraphEdgeEvent {
                        collection,
                        src_id,
                        label,
                        dst_id,
                        op: crate::event::WriteOp::Delete,
                        properties: None,
                    },
                );
                self.response_affected(task, u64::from(existed))
            }
            Err(e) => self.response_error(
                task,
                ErrorCode::Internal {
                    detail: e.to_string(),
                },
            ),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bridge::envelope::Status;
    use crate::event::WriteOp;
    use crate::event::bus::create_event_bus_with_capacity;
    use nodedb_types::{RlsWriteCheck, Surrogate};

    use super::super::shared::EdgePutParams;
    use super::super::shared::test_support::{affected_count, make_core, make_task_with_lsn};

    /// Compiled filters equivalent to a `FOR WRITE` policy on `owner`.
    fn owner_write_check(owner: &str) -> Vec<u8> {
        let filter = crate::bridge::scan_filter::ScanFilter {
            field: "owner".into(),
            op: crate::bridge::scan_filter::FilterOp::Eq,
            value: nodedb_types::Value::String(owner.into()),
            clauses: Vec::new(),
            expr: None,
        };
        zerompk::to_msgpack_vec(&vec![filter]).expect("encode policy filter")
    }

    /// The delete is decided against the edge's STORED property object before
    /// the tombstone is written, so a rejected delete leaves the edge in place.
    #[test]
    fn edge_delete_rejected_by_the_write_policy_leaves_the_edge() {
        let mut h = make_core();
        let put_task = make_task_with_lsn(90);
        assert_eq!(
            h.core
                .execute_edge_put(
                    &put_task,
                    EdgePutParams {
                        tid: 1,
                        collection: "knows",
                        src_id: "a",
                        label: "KNOWS",
                        dst_id: "b",
                        properties: br#"{"owner":"alice"}"#,
                        src_surrogate: Surrogate::new(1),
                        dst_surrogate: Surrogate::new(2),
                    },
                )
                .status,
            Status::Ok
        );

        let check = RlsWriteCheck::Predicate(owner_write_check("mallory"));
        let del_task = make_task_with_lsn(91);
        let resp = h.core.execute_edge_delete(
            &del_task,
            EdgeDeleteParams {
                tid: 1,
                collection: "knows",
                src_id: "a",
                label: "KNOWS",
                dst_id: "b",
                rls_write_check: &check,
            },
        );
        assert_eq!(resp.status, Status::Error);
        assert!(
            h.core
                .edge_store
                .get_edge(
                    crate::types::DatabaseId::DEFAULT.as_u64(),
                    TenantId::new(1),
                    "knows",
                    "a",
                    "KNOWS",
                    "b",
                )
                .expect("read edge back")
                .is_some(),
            "a refused delete must leave the edge present"
        );
    }

    /// …and a policy the stored properties satisfy lets the delete through, and
    /// reports the one edge it removed.
    #[test]
    fn edge_delete_admitted_by_the_write_policy_applies() {
        let mut h = make_core();
        let put_task = make_task_with_lsn(92);
        assert_eq!(
            h.core
                .execute_edge_put(
                    &put_task,
                    EdgePutParams {
                        tid: 1,
                        collection: "knows",
                        src_id: "a",
                        label: "KNOWS",
                        dst_id: "b",
                        properties: br#"{"owner":"alice"}"#,
                        src_surrogate: Surrogate::new(1),
                        dst_surrogate: Surrogate::new(2),
                    },
                )
                .status,
            Status::Ok
        );

        let check = RlsWriteCheck::Predicate(owner_write_check("alice"));
        let del_task = make_task_with_lsn(93);
        let resp = h.core.execute_edge_delete(
            &del_task,
            EdgeDeleteParams {
                tid: 1,
                collection: "knows",
                src_id: "a",
                label: "KNOWS",
                dst_id: "b",
                rls_write_check: &check,
            },
        );
        assert_eq!(resp.status, Status::Ok);
        assert_eq!(affected_count(&resp), 1, "the live edge was removed");
    }

    /// Deleting an edge that was never written affects nothing.
    #[test]
    fn edge_delete_of_an_absent_edge_reports_zero_affected() {
        let mut h = make_core();
        let no_policy = RlsWriteCheck::NoPolicyApplies;
        let del_task = make_task_with_lsn(94);
        let resp = h.core.execute_edge_delete(
            &del_task,
            EdgeDeleteParams {
                tid: 1,
                collection: "knows",
                src_id: "ghost-a",
                label: "KNOWS",
                dst_id: "ghost-b",
                rls_write_check: &no_policy,
            },
        );
        assert_eq!(resp.status, Status::Ok);
        assert_eq!(
            affected_count(&resp),
            0,
            "there was never an edge to remove"
        );
    }

    #[test]
    fn edge_delete_emits_cdc_delete_on_its_collection() {
        let (mut producers, mut consumers) = create_event_bus_with_capacity(1, 64);
        let mut h = make_core();
        h.core
            .set_event_producer(producers.pop().expect("producer"));

        // Seed the edge so the delete has something to remove.
        let put_task = make_task_with_lsn(80);
        assert_eq!(
            h.core
                .execute_edge_put(
                    &put_task,
                    EdgePutParams {
                        tid: 1,
                        collection: "knows",
                        src_id: "a",
                        label: "KNOWS",
                        dst_id: "b",
                        properties: b"",
                        src_surrogate: Surrogate::new(1),
                        dst_surrogate: Surrogate::new(2),
                    },
                )
                .status,
            Status::Ok
        );
        let _ = consumers[0].try_recv(); // drain the put event

        let del_task = make_task_with_lsn(81);
        // This test exercises CDC emission on delete, not the RLS write gate,
        // so it carries no policy — mirrors the old empty-slice "admits
        // everything" convention.
        let no_policy = RlsWriteCheck::NoPolicyApplies;
        let resp = h.core.execute_edge_delete(
            &del_task,
            EdgeDeleteParams {
                tid: 1,
                collection: "knows",
                src_id: "a",
                label: "KNOWS",
                dst_id: "b",
                rls_write_check: &no_policy,
            },
        );
        assert_eq!(resp.status, Status::Ok);
        assert_eq!(affected_count(&resp), 1);

        let event = consumers[0]
            .try_recv()
            .expect("edge delete must emit a CDC WriteEvent");
        assert_eq!(event.collection.as_ref(), "knows");
        assert_eq!(
            event.row_id.as_str(),
            crate::event::graph_cdc::edge_row_id("a", "KNOWS", "b").as_str()
        );
        assert_eq!(event.op, WriteOp::Delete);
    }
}
