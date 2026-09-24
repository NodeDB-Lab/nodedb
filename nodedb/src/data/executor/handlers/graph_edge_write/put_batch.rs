// SPDX-License-Identifier: BUSL-1.1

//! `EdgePutBatch`: batched edge insert in a single SPSC round-trip.

use tracing::debug;

use crate::bridge::envelope::{ErrorCode, Response};
use crate::data::executor::core_loop::CoreLoop;
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
        // Every endpoint is checked before any edge is written, so a dangling
        // refusal applies nothing.
        if let Some(missing_node) = edges.iter().find_map(|edge| {
            [&edge.src_id, &edge.dst_id]
                .into_iter()
                .find(|node| self.is_node_deleted(database_id, tid, node))
                .cloned()
        }) {
            return self.response_error(task, ErrorCode::RejectedDanglingEdge { missing_node });
        }
        for (idx, edge) in edges.iter().enumerate() {
            let ord = self
                .active_graph_system_from
                .unwrap_or_else(|| self.hlc.next_ordinal());
            let valid_from_ms = nodedb_types::ordinal_to_ms(ord);
            use crate::engine::graph::edge_store::EdgeRef;
            match self.edge_store.put_edge_versioned_with_stats(
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
                ord,
                valid_from_ms,
                i64::MAX,
                owns_logical_edge_stats(task, &edge.src_id),
            ) {
                Ok(()) => {
                    let partition = self.csr_partition_mut(database_id, tid);
                    if let Err(e) = partition.add_edge_in_collection(
                        &edge.src_id,
                        &edge.label,
                        &edge.dst_id,
                        edge.collection.as_str(),
                    ) {
                        return self.response_error(
                            task,
                            ErrorCode::Internal {
                                detail: format!("edge {idx} (label interning): {e}"),
                            },
                        );
                    }
                    partition.set_node_surrogate(&edge.src_id, edge.src_surrogate);
                    partition.set_node_surrogate(&edge.dst_id, edge.dst_surrogate);
                }
                Err(e) => {
                    return self.response_error(
                        task,
                        ErrorCode::Internal {
                            detail: format!("edge {idx}: {e}"),
                        },
                    );
                }
            }
        }
        if !edges.is_empty() {
            self.checkpoint_coordinator
                .mark_dirty("sparse", edges.len());
        }
        for edge in edges {
            self.note_edge_write_lsn(
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
                    op: crate::event::WriteOp::Insert,
                    properties: Some(&[]),
                },
            );
        }
        self.response_affected(task, edges.len() as u64)
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

    /// The funnel cancels the batch's record on a dangling refusal, so the
    /// refusal must leave no edge of the batch behind.
    #[test]
    fn a_dangling_edge_late_in_the_batch_writes_no_edge() {
        let mut h = make_core();
        h.core
            .mark_node_deleted(DatabaseId::DEFAULT.as_u64(), 1, "gone");
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
