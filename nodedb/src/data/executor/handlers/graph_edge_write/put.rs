// SPDX-License-Identifier: BUSL-1.1

//! `EdgePut`: single-edge upsert, with optional transactional undo.

use tracing::debug;

use crate::bridge::envelope::{EdgeImage, ErrorCode, Response, WriteSetEntry};
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::task::ExecutionTask;
use crate::types::TenantId;

use crate::data::executor::handlers::transaction::undo::UndoEntry;
use crate::data::executor::handlers::transaction::undo::edge_write::EdgeTarget;

use super::shared::{EdgePutParams, owns_logical_edge_stats};

impl CoreLoop {
    pub(in crate::data::executor) fn execute_edge_put(
        &mut self,
        task: &ExecutionTask,
        params: EdgePutParams<'_>,
    ) -> Response {
        self.execute_edge_put_with_undo(task, params, None)
    }

    /// Edge upsert with optional transactional compensation.
    ///
    /// When `undo` is `Some`, the `UndoEntry::EdgeWrite` is recorded once the
    /// edge-store version and its CSR edge both stand. It names the version
    /// the put added, so a rollback removes exactly that version.
    ///
    /// A CSR refusal after the version is stored takes the version back out,
    /// so the edge store never holds an edge the CSR misses.
    ///
    /// A put writes a new edge-store version and makes the CSR edge live with
    /// the weight in `properties`, so a successful put always reports exactly
    /// one edge affected.
    pub(in crate::data::executor) fn execute_edge_put_with_undo(
        &mut self,
        task: &ExecutionTask,
        params: EdgePutParams<'_>,
        undo: Option<&mut Vec<UndoEntry>>,
    ) -> Response {
        let EdgePutParams {
            tid,
            collection,
            src_id,
            label,
            dst_id,
            properties,
            src_surrogate,
            dst_surrogate,
        } = params;
        debug!(core = self.core_id, tid, %collection, %src_id, %label, %dst_id, "edge put");
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

        // A decided ordinal marks a committed version that replay or a
        // committed-redo install writes again. The dangling rule admits new
        // writes only: replay runs the document arm before the graph arm, so
        // the tracker already names nodes a later record deletes.
        let committed_version = self.apply_scope.graph_system_from.is_some();
        if !committed_version && self.is_node_deleted(database_id, tid, collection, src_id) {
            return self.response_error(
                task,
                ErrorCode::RejectedDanglingEdge {
                    missing_node: src_id.to_string(),
                },
            );
        }
        if !committed_version && self.is_node_deleted(database_id, tid, collection, dst_id) {
            return self.response_error(
                task,
                ErrorCode::RejectedDanglingEdge {
                    missing_node: dst_id.to_string(),
                },
            );
        }

        let stamp = match self.graph_write_stamp() {
            Ok(stamp) => stamp,
            Err(e) => return self.response_error(task, e),
        };
        let ord = stamp.system_from;
        // Under a Calvin batch, `epoch_system_ms` is the deterministic epoch
        // timestamp. Outside Calvin it is None and the valid time is the
        // ordinal's wall time.
        let valid_from_ms = match self.epoch_system_ms {
            Some(ms) => ms,
            None => nodedb_types::ordinal_to_ms(ord),
        };
        let target = EdgeTarget {
            database_id,
            tid,
            collection,
            src_id,
            label,
            dst_id,
        };
        // The CSR state a reversal puts back: a transaction rollback, or the
        // reversal of a version the CSR refuses below.
        let csr_prior = self.capture_edge_csr(&target);
        use crate::engine::graph::edge_store::EdgeRef;
        match self.edge_store.put_edge_version_recorded(
            EdgeRef::new(
                task.request.database_id,
                TenantId::new(tid),
                collection,
                src_id,
                label,
                dst_id,
            )
            .with_surrogates(src_surrogate, dst_surrogate),
            properties,
            stamp,
            valid_from_ms,
            i64::MAX,
            owns_logical_edge_stats(task, src_id),
        ) {
            Ok(version) => {
                let current = version.current.clone();
                let edge_undo = target.undo(version, csr_prior);
                // The CSR follows what the edge resolves to: a version a
                // TRUNCATE hides, or one below a newer version, leaves the
                // edge as it was.
                let csr_result = self.mirror_edge_csr(
                    database_id,
                    tid,
                    (src_id, label, dst_id),
                    collection,
                    current.as_deref(),
                );
                match csr_result {
                    Ok(()) => {
                        // The version and its CSR edge both stand, so a
                        // transaction rollback reverses them together.
                        if let Some(undo) = undo {
                            undo.push(UndoEntry::EdgeWrite(Box::new(edge_undo)));
                        }
                        let partition = self.csr_partition_mut(database_id, tid);
                        // Populate the per-node surrogates so future bitmap-gated
                        // traversals can check membership without a separate lookup.
                        partition.set_node_surrogate(src_id, src_surrogate);
                        partition.set_node_surrogate(dst_id, dst_surrogate);
                        self.checkpoint_coordinator.mark_dirty("sparse", 1);
                        self.note_edge_write(task, tid, collection, src_id, label, dst_id);
                        // CDC: emit after `note_edge_write` so the core
                        // watermark (the event's LSN) already reflects this
                        // edge's WAL LSN, matching the WAL-replay reconstruction.
                        self.emit_graph_edge_event(
                            task,
                            crate::data::executor::core_loop::event_emit::GraphEdgeEvent {
                                collection,
                                src_id,
                                label,
                                dst_id,
                                src_surrogate,
                                dst_surrogate,
                                op: crate::event::WriteOp::Insert,
                                properties: Some(properties),
                            },
                        );
                        let mut response = self.response_affected(task, 1);
                        // The version's ordinal was decided here, so the version
                        // is journalled after apply at exactly that ordinal.
                        response.write_set = vec![WriteSetEntry::edge(EdgeImage::Put(
                            crate::wal::EdgePutRedo {
                                collection: collection.to_string(),
                                src_id: src_id.to_string(),
                                label: label.to_string(),
                                dst_id: dst_id.to_string(),
                                properties: properties.to_vec(),
                                src_surrogate: src_surrogate.as_u32(),
                                dst_surrogate: dst_surrogate.as_u32(),
                                system_from: Some(ord),
                                applied: (stamp.applied != ord).then_some(stamp.applied),
                            },
                        ))];
                        response
                    }
                    // The CSR never misses a stored edge: the version the CSR
                    // refused is taken back out of the edge store.
                    Err(e) => {
                        let code = self.reverse_edge_write(edge_undo, e);
                        self.response_error(task, code)
                    }
                }
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
    use crate::types::Lsn;
    use nodedb_types::Surrogate;

    use super::super::shared::test_support::{affected_count, make_core, make_task_with_lsn};

    #[test]
    fn edge_put_emits_cdc_insert_on_its_collection() {
        let (mut producers, mut consumers) = create_event_bus_with_capacity(1, 64);
        let mut h = make_core();
        h.core
            .set_event_producer(producers.pop().expect("producer"));

        let task = make_task_with_lsn(77);
        let resp = h.core.execute_edge_put(
            &task,
            EdgePutParams {
                tid: 1,
                collection: "knows",
                src_id: "a",
                label: "KNOWS",
                dst_id: "b",
                properties: b"w=1",
                src_surrogate: Surrogate::new(1),
                dst_surrogate: Surrogate::new(2),
            },
        );
        assert_eq!(resp.status, Status::Ok);
        assert_eq!(affected_count(&resp), 1, "a put always affects one edge");

        let event = consumers[0]
            .try_recv()
            .expect("edge put must emit a CDC WriteEvent");
        assert_eq!(event.collection.as_ref(), "knows");
        assert_eq!(
            event.row_id.as_str(),
            crate::event::graph_cdc::edge_row_id("a", "KNOWS", "b").as_str()
        );
        assert_eq!(event.op, WriteOp::Insert);
        assert_eq!(
            event.lsn,
            Lsn::new(77),
            "event LSN matches the edge's WAL LSN"
        );
        assert_eq!(event.new_value.as_deref(), Some(b"w=1".as_slice()));
    }

    /// A CSR refusal after the edge store took the version takes the version
    /// back out, and the answer keeps the CSR error's typed code.
    #[test]
    fn a_reversed_edge_write_leaves_no_version_and_keeps_the_typed_code() {
        use crate::data::executor::handlers::transaction::undo::edge_write::EdgeTarget;
        use crate::engine::graph::edge_store::{EdgeRef, VersionStamp};

        let mut h = make_core();
        let task = make_task_with_lsn(79);
        let database_id = task.request.database_id;
        let target = EdgeTarget {
            database_id: database_id.as_u64(),
            tid: 1,
            collection: "knows",
            src_id: "a",
            label: "KNOWS",
            dst_id: "b",
        };
        let csr_prior = h.core.capture_edge_csr(&target);
        let version = h
            .core
            .edge_store
            .put_edge_version_recorded(
                EdgeRef::new(database_id, TenantId::new(1), "knows", "a", "KNOWS", "b")
                    .with_surrogates(Surrogate::new(1), Surrogate::new(2)),
                b"w=1",
                VersionStamp::at(100),
                100,
                i64::MAX,
                true,
            )
            .expect("put edge version");

        let code = h.core.reverse_edge_write(
            target.undo(version, csr_prior),
            nodedb_graph::GraphError::RebuildInProgress,
        );

        assert!(
            matches!(code, ErrorCode::ObjectNotInPrerequisiteState { .. }),
            "{code:?}"
        );
        let stored = h
            .core
            .edge_store
            .get_edge(
                database_id.as_u64(),
                TenantId::new(1),
                "knows",
                "a",
                "KNOWS",
                "b",
            )
            .expect("edge lookup");
        assert!(stored.is_none(), "the reversed version is gone");
    }

    /// An endpoint under `Surrogate::ZERO` names no node: the put is refused
    /// and no edge version is written.
    #[test]
    fn an_edge_put_with_an_unbound_endpoint_is_refused_and_writes_nothing() {
        let mut h = make_core();
        let task = make_task_with_lsn(78);
        let resp = h.core.execute_edge_put(
            &task,
            EdgePutParams {
                tid: 1,
                collection: "knows",
                src_id: "a",
                label: "KNOWS",
                dst_id: "b",
                properties: b"w=1",
                src_surrogate: Surrogate::new(1),
                dst_surrogate: Surrogate::ZERO,
            },
        );
        assert!(matches!(
            resp.error_code.as_deref(),
            Some(ErrorCode::RejectedPrevalidation { .. })
        ));
        let stored = h
            .core
            .edge_store
            .get_edge(
                task.request.database_id.as_u64(),
                TenantId::new(1),
                "knows",
                "a",
                "KNOWS",
                "b",
            )
            .expect("edge lookup");
        assert!(stored.is_none(), "a refused put writes no edge");
    }
}
