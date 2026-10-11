// SPDX-License-Identifier: BUSL-1.1

//! `EdgePutBatch`: batched edge insert in a single SPSC round-trip.

use tracing::debug;

use crate::bridge::envelope::{EdgeImage, ErrorCode, Response, WriteSetEntry};
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::handlers::partial_refusal::refusal_after_rows;
use crate::data::executor::handlers::transaction::undo::edge_write::EdgeTarget;
use crate::data::executor::task::ExecutionTask;
use crate::types::TenantId;

use super::shared::owns_logical_edge_stats;

impl CoreLoop {
    /// Apply a batched edge insert in a single SPSC round-trip.
    ///
    /// Each edge unconditionally writes a new edge-store version and CSR
    /// entry (same no-op-free semantics as [`CoreLoop::execute_edge_put`]),
    /// so a successful batch always reports exactly `edges.len()` affected.
    pub(in crate::data::executor) fn execute_edge_put_batch(
        &mut self,
        task: &ExecutionTask,
        tid: u64,
        edges: &[nodedb_physical::physical_plan::BatchEdge],
    ) -> Response {
        debug!(core = self.core_id, count = edges.len(), "edge put batch");
        let database_id = task.request.database_id.as_u64();
        // Every endpoint carries the surrogate its coordinator bound. One
        // unbound endpoint refuses the batch before any edge is written.
        if let Some(refusal) = edges.iter().find_map(|edge| {
            [edge.src_surrogate, edge.dst_surrogate]
                .into_iter()
                .find_map(|surrogate| {
                    crate::data::executor::handlers::unbound_surrogate::refuse_unbound(
                        "graph",
                        edge.collection.as_str(),
                        surrogate,
                    )
                })
        }) {
            return self.response_error(task, refusal);
        }
        // Every endpoint is checked before any edge is written, so a dangling
        // refusal applies nothing. A decided ordinal marks committed versions
        // written again, which the dangling rule never refuses.
        if self.apply_scope.graph_system_from.is_none()
            && let Some(missing_node) = edges.iter().find_map(|edge| {
                [&edge.src_id, &edge.dst_id]
                    .into_iter()
                    .find(|node| {
                        self.is_node_deleted(database_id, tid, edge.collection.as_str(), node)
                    })
                    .cloned()
            })
        {
            return self.response_error(task, ErrorCode::RejectedDanglingEdge { missing_node });
        }
        // Each edge's version, at the ordinal decided here, journalled after
        // apply. An edge that failed after earlier ones landed refuses the
        // batch with the landed versions, which stay.
        let mut write_set: Vec<WriteSetEntry> = Vec::with_capacity(edges.len());
        for edge in edges {
            let target = EdgeTarget {
                database_id,
                tid,
                collection: edge.collection.as_str(),
                src_id: &edge.src_id,
                label: &edge.label,
                dst_id: &edge.dst_id,
            };
            let csr_prior = self.capture_edge_csr(&target);
            let stamp = match self.graph_write_stamp() {
                Ok(stamp) => stamp,
                Err(e) => {
                    let code = refusal_after_rows(write_set.len() as u64, e);
                    return self.refusal_with_landed_rows(task, code, write_set);
                }
            };
            let ord = stamp.system_from;
            let valid_from_ms = nodedb_types::ordinal_to_ms(ord);
            use crate::engine::graph::edge_store::EdgeRef;
            match self.edge_store.put_edge_version_recorded(
                EdgeRef::new(
                    task.request.database_id,
                    TenantId::new(tid),
                    edge.collection.as_str(),
                    &edge.src_id,
                    &edge.label,
                    &edge.dst_id,
                )
                .with_surrogates(edge.src_surrogate, edge.dst_surrogate),
                &[],
                stamp,
                valid_from_ms,
                i64::MAX,
                owns_logical_edge_stats(task, &edge.src_id),
            ) {
                Ok(version) => {
                    let current = version.current.clone();
                    // The CSR follows what the edge resolves to: a version a
                    // TRUNCATE hides leaves the edge as it was. A CSR refusal
                    // takes this version back out; the edges before it stand
                    // and are journalled.
                    if let Err(e) = self.mirror_edge_csr(
                        database_id,
                        tid,
                        (&edge.src_id, &edge.label, &edge.dst_id),
                        edge.collection.as_str(),
                        current.as_deref(),
                    ) {
                        let reversed = self.reverse_edge_write(target.undo(version, csr_prior), e);
                        let code = refusal_after_rows(write_set.len() as u64, reversed);
                        return self.refusal_with_landed_rows(task, code, write_set);
                    }
                    write_set.push(WriteSetEntry::edge(EdgeImage::Put(
                        crate::wal::EdgePutRedo {
                            collection: edge.collection.to_string(),
                            src_id: edge.src_id.clone(),
                            label: edge.label.clone(),
                            dst_id: edge.dst_id.clone(),
                            properties: Vec::new(),
                            src_surrogate: edge.src_surrogate.as_u32(),
                            dst_surrogate: edge.dst_surrogate.as_u32(),
                            system_from: Some(ord),
                            applied: (stamp.applied != ord).then_some(stamp.applied),
                        },
                    )));
                    let partition = self.csr_partition_mut(database_id, tid);
                    partition.set_node_surrogate(&edge.src_id, edge.src_surrogate);
                    partition.set_node_surrogate(&edge.dst_id, edge.dst_surrogate);
                }
                Err(e) => {
                    let code = refusal_after_rows(write_set.len() as u64, e);
                    return self.refusal_with_landed_rows(task, code, write_set);
                }
            }
        }
        if !edges.is_empty() {
            self.checkpoint_coordinator
                .mark_dirty("sparse", edges.len());
        }
        for edge in edges {
            self.note_edge_write(
                task,
                tid,
                edge.collection.as_str(),
                &edge.src_id,
                &edge.label,
                &edge.dst_id,
            );
            // CDC: batch edges are applied with empty properties (see
            // `execute_edge_put_batch`'s hardcoded `&[]`), so `new_value` is an
            // empty payload — a faithful pre-image of what was applied.
            self.emit_graph_edge_event(
                task,
                crate::data::executor::core_loop::event_emit::GraphEdgeEvent {
                    collection: edge.collection.as_str(),
                    src_id: &edge.src_id,
                    label: &edge.label,
                    dst_id: &edge.dst_id,
                    src_surrogate: edge.src_surrogate,
                    dst_surrogate: edge.dst_surrogate,
                    op: crate::event::WriteOp::Insert,
                    properties: Some(&[]),
                },
            );
        }
        let mut response = self.response_affected(task, edges.len() as u64);
        response.write_set = write_set;
        response
    }
}

#[cfg(test)]
mod tests {
    use crate::bridge::envelope::{ErrorCode, Status};
    use crate::types::TenantId;
    use nodedb_physical::physical_plan::BatchEdge;
    use nodedb_types::{DatabaseId, QualifiedCollection, Surrogate};

    use super::super::shared::test_support::{make_core, make_task_with_lsn};

    fn edge(src: &str, dst: &str) -> BatchEdge {
        BatchEdge {
            collection: QualifiedCollection::new(DatabaseId::DEFAULT, "knows"),
            src_id: src.to_string(),
            label: "KNOWS".to_string(),
            dst_id: dst.to_string(),
            src_surrogate: Surrogate::new(1),
            dst_surrogate: Surrogate::new(2),
        }
    }

    /// A node deleted from another collection is not dangling here: a row
    /// delete scopes the tracker to its own collection.
    #[test]
    fn a_node_deleted_in_another_collection_is_not_dangling() {
        let mut h = make_core();
        h.core
            .mark_node_deleted(DatabaseId::DEFAULT.as_u64(), 1, "users", "gone");
        let task = make_task_with_lsn(9);
        let resp = h
            .core
            .execute_edge_put_batch(&task, 1, &[edge("c", "gone")]);
        assert_eq!(resp.status, Status::Ok, "{resp:?}");
    }

    /// The funnel cancels the batch's record on a dangling refusal, so the
    /// refusal must leave no edge of the batch behind.
    #[test]
    fn a_dangling_edge_late_in_the_batch_writes_no_edge() {
        let mut h = make_core();
        h.core
            .mark_node_deleted(DatabaseId::DEFAULT.as_u64(), 1, "knows", "gone");
        let task = make_task_with_lsn(9);
        let edges = vec![edge("a", "b"), edge("c", "gone")];

        let resp = h.core.execute_edge_put_batch(&task, 1, &edges);

        assert_eq!(resp.status, Status::Error);
        assert!(matches!(
            resp.error_code.as_deref(),
            Some(ErrorCode::RejectedDanglingEdge { missing_node }) if missing_node == "gone"
        ));
        let first = h
            .core
            .edge_store
            .get_edge(
                DatabaseId::DEFAULT.as_u64(),
                TenantId::new(1),
                "knows",
                "a",
                "KNOWS",
                "b",
            )
            .expect("read edge");
        assert!(
            first.is_none(),
            "the edge before the dangling one is not written"
        );
    }
}
