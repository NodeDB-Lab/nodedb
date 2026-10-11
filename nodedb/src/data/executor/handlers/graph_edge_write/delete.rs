// SPDX-License-Identifier: BUSL-1.1

//! `EdgeDelete`: single-edge tombstone, with optional transactional undo.

use tracing::debug;

use crate::bridge::envelope::{EdgeImage, ErrorCode, Response, WriteSetEntry};
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::task::ExecutionTask;
use crate::types::TenantId;

use crate::data::executor::handlers::transaction::undo::UndoEntry;
use crate::data::executor::handlers::transaction::undo::edge_write::EdgeTarget;

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
    /// The `UndoEntry::EdgeWrite` is recorded once the tombstone and its CSR
    /// change both stand, never before: it names the tombstone version, and a
    /// rollback removes exactly that version.
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
        undo: Option<&mut Vec<UndoEntry>>,
    ) -> Response {
        let EdgeDeleteParams {
            tid,
            collection,
            src_id,
            label,
            dst_id,
            src_surrogate,
            dst_surrogate,
            rls_write_check,
        } = params;
        debug!(core = self.core_id, tid, %collection, %src_id, %label, %dst_id, "edge delete");
        let database_id = task.request.database_id.as_u64();
        // Both endpoints carry the surrogate their coordinator bound.
        for surrogate in [src_surrogate, dst_surrogate] {
            if let Some(refusal) =
                crate::data::executor::handlers::unbound_surrogate::refuse_unbound(
                    "graph", collection, surrogate,
                )
            {
                return self.response_error(task, refusal);
            }
        }

        // The pre-image is always read: the RLS write gate needs it for any
        // non-admit-all policy, and the response needs it to report a
        // truthful affected count.
        // A pre-image read error refuses the delete: read as absent, it would
        // admit the delete without the policy and report nothing removed.
        let old_properties = match self.edge_store.get_edge(
            database_id,
            TenantId::new(tid),
            collection,
            src_id,
            label,
            dst_id,
        ) {
            Ok(properties) => properties,
            Err(e) => return self.response_error(task, ErrorCode::from(e)),
        };
        let existed = old_properties.is_some();

        if let Err(error) = crate::data::executor::handlers::rls_write_gate::admit_edge_properties(
            rls_write_check,
            old_properties.as_deref(),
            tid,
            collection,
        ) {
            return self.response_error(task, error);
        }

        let stamp = match self.graph_write_stamp() {
            Ok(stamp) => stamp,
            Err(e) => return self.response_error(task, e),
        };
        let ord = stamp.system_from;
        let target = EdgeTarget {
            database_id,
            tid,
            collection,
            src_id,
            label,
            dst_id,
        };
        // The CSR state a reversal puts back: a transaction rollback, or the
        // reversal of a tombstone the CSR refuses below.
        let csr_prior = self.capture_edge_csr(&target);
        use crate::engine::graph::edge_store::EdgeRef;
        match self.edge_store.soft_delete_edge_recorded(
            EdgeRef::new(
                task.request.database_id,
                TenantId::new(tid),
                collection,
                src_id,
                label,
                dst_id,
            ),
            stamp,
            owns_logical_edge_stats(task, src_id),
        ) {
            Ok(tombstone) => {
                let current = tombstone.current.clone();
                let edge_undo = target.undo(tombstone, csr_prior);
                // The CSR follows what the edge resolves to: a tombstone a
                // TRUNCATE hides, or one below a newer version, leaves the
                // edge as it was. A CSR refusal takes the tombstone back out,
                // so the edge store and the CSR never disagree.
                if let Err(e) = self.mirror_edge_csr(
                    database_id,
                    tid,
                    (src_id, label, dst_id),
                    collection,
                    current.as_deref(),
                ) {
                    let code = self.reverse_edge_write(edge_undo, e);
                    return self.response_error(task, code);
                }
                // The tombstone is written whether or not the edge was live,
                // so the undo that removes it is recorded either way.
                if let Some(undo) = undo {
                    undo.push(UndoEntry::EdgeWrite(Box::new(edge_undo)));
                }
                self.checkpoint_coordinator.mark_dirty("sparse", 1);
                self.note_edge_write(task, tid, collection, src_id, label, dst_id);
                // CDC: emit after `note_edge_write` so the event LSN matches
                // this edge's WAL LSN (the WAL-replay reconstruction key).
                self.emit_graph_edge_event(
                    task,
                    crate::data::executor::core_loop::event_emit::GraphEdgeEvent {
                        collection,
                        src_id,
                        label,
                        dst_id,
                        src_surrogate,
                        dst_surrogate,
                        op: crate::event::WriteOp::Delete,
                        properties: None,
                    },
                );
                let mut response = self.response_affected(task, u64::from(existed));
                // The tombstone's ordinal was decided here, so the tombstone is
                // journalled after apply at exactly that ordinal.
                response.write_set = vec![WriteSetEntry::edge(EdgeImage::Delete(
                    crate::wal::EdgeDeleteRedo {
                        collection: collection.to_string(),
                        src_id: src_id.to_string(),
                        label: label.to_string(),
                        dst_id: dst_id.to_string(),
                        src_surrogate: src_surrogate.as_u32(),
                        dst_surrogate: dst_surrogate.as_u32(),
                        system_from: Some(ord),
                        applied: (stamp.applied != ord).then_some(stamp.applied),
                    },
                ))];
                response
            }
            Err(e) => self.response_error(task, ErrorCode::from(e)),
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

    /// The stored property map `{"owner": owner}`, as plain MessagePack.
    fn owner_properties(owner: &str) -> Vec<u8> {
        nodedb_types::json_msgpack::json_to_msgpack(&serde_json::json!({ "owner": owner }))
            .expect("encode properties")
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
                        properties: &owner_properties("alice"),
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
                src_surrogate: Surrogate::new(1),
                dst_surrogate: Surrogate::new(2),
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
                        properties: &owner_properties("alice"),
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
                src_surrogate: Surrogate::new(1),
                dst_surrogate: Surrogate::new(2),
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
                src_surrogate: Surrogate::new(3),
                dst_surrogate: Surrogate::new(4),
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
                src_surrogate: Surrogate::new(1),
                dst_surrogate: Surrogate::new(2),
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
        let crate::event::types::RowId::Edge(edge) = &event.row_id else {
            panic!("an edge delete names an edge row");
        };
        assert_eq!(edge.src_surrogate(), Surrogate::new(1));
        assert_eq!(edge.dst_surrogate(), Surrogate::new(2));
    }
}
