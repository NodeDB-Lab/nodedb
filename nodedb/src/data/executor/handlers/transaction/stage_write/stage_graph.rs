// SPDX-License-Identifier: BUSL-1.1

//! Statement-time staging for the six stageable graph writes: `EdgePut`,
//! `EdgeDelete`, `EdgePutBatch`, `EdgeDeleteBatch`, `SetNodeLabels`,
//! `RemoveNodeLabels`. These stage into a dedicated [`GraphTxnOverlay`]
//! rather than the shared surrogate-keyed `TxnOverlay`, purely as in-memory
//! read-your-own-writes plumbing for Neighbors/Hop — commit durable replay
//! still runs the buffered `GraphOp` plan through the real handlers.

use nodedb_physical::physical_plan::{BatchEdge, GraphOp};

use crate::bridge::envelope::Response;
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::handlers::transaction::overlay::{
    GraphCollKey, GraphTxnOverlay, MAX_TXN_OVERLAY_BYTES,
};
use crate::data::executor::task::ExecutionTask;
use crate::types::{TenantId, TxnId};

/// Sentinel collection key node-label deltas are staged under, since
/// `SetNodeLabels`/`RemoveNodeLabels` operate tenant-wide with no collection
/// argument. The leading NUL makes it un-nameable by any CDC subscriber; the
/// nameable twin is [`crate::event::graph_cdc::GRAPH_LABEL_STREAM`].
pub(in crate::data::executor) const GRAPH_LABEL_COLL_KEY: &str = "\0__graph_node_labels__";

impl CoreLoop {
    /// Route a stageable `GraphOp` to its staging handler. `op` must be one
    /// of the six ops `is_stageable_write` accepts — every other `GraphOp`
    /// is unreachable here.
    pub(in crate::data::executor) fn execute_stage_graph(
        &mut self,
        task: &ExecutionTask,
        tid: u64,
        txn_id: TxnId,
        op: &GraphOp,
    ) -> Response {
        match op {
            GraphOp::EdgePut {
                collection,
                src_id,
                label,
                dst_id,
                properties,
                ..
            } => {
                if let Err(e) = self.stage_graph_capped(properties.len()) {
                    return self.response_error(task, e);
                }
                let coll_key = graph_coll_key(task, tid, collection.as_str());
                self.graph_txn_overlay_mut(txn_id).stage_edge_put(
                    coll_key,
                    src_id,
                    label,
                    dst_id,
                    properties.clone(),
                );
                self.stage_count_response(task, 1)
            }

            GraphOp::EdgeDelete {
                collection,
                src_id,
                label,
                dst_id,
                ..
            } => {
                let coll_key = graph_coll_key(task, tid, collection.as_str());
                let overlay = self.graph_txn_overlay_mut(txn_id);
                let staged_presence =
                    overlay.staged_edge_presence(&coll_key, src_id, label, dst_id);
                overlay.stage_edge_delete(coll_key, src_id, label, dst_id);
                // BASE ∪ OVERLAY: this transaction's own overlay wins when it
                // has already touched the edge; otherwise fall back to the
                // durable edge store, exactly like every other stage handler's
                // real affected-count computation.
                let existed = staged_presence.unwrap_or_else(|| {
                    self.edge_store
                        .get_edge(
                            task.request.database_id.as_u64(),
                            TenantId::new(tid),
                            collection.as_str(),
                            src_id,
                            label,
                            dst_id,
                        )
                        .ok()
                        .flatten()
                        .is_some()
                });
                self.stage_count_response(task, usize::from(existed))
            }

            GraphOp::EdgePutBatch { edges } => self.stage_edge_put_batch(task, tid, txn_id, edges),
            GraphOp::EdgeDeleteBatch { edges } => {
                self.stage_edge_delete_batch(task, tid, txn_id, edges)
            }

            GraphOp::SetNodeLabels { node_id, labels } => {
                let coll_key = graph_coll_key(task, tid, GRAPH_LABEL_COLL_KEY);
                self.graph_txn_overlay_mut(txn_id)
                    .stage_node_labels_set(coll_key, node_id, labels);
                // Replay always interns the node (`ensure_node`), so a set
                // touches exactly one node — same deterministic count as the
                // autocommit handler, never `labels.len()`.
                self.stage_count_response(task, 1)
            }

            GraphOp::RemoveNodeLabels { node_id, labels } => {
                let coll_key = graph_coll_key(task, tid, GRAPH_LABEL_COLL_KEY);
                let database_id = task.request.database_id.as_u64();
                // BASE ∪ OVERLAY: the node exists if this transaction already
                // staged labels onto it (a pending `added` set means a staged
                // `SetNodeLabels` will create it at replay), or if it already
                // exists in the durable CSR partition.
                let overlay_created = self
                    .graph_txn_overlay_mut(txn_id)
                    .labels_delta(&coll_key, node_id)
                    .is_some_and(|delta| !delta.added.is_empty());
                let existed = overlay_created
                    || self
                        .csr_partition(database_id, tid)
                        .is_some_and(|p| p.contains_node(node_id));
                self.graph_txn_overlay_mut(txn_id)
                    .stage_node_labels_remove(coll_key, node_id, labels);
                self.stage_count_response(task, usize::from(existed))
            }

            GraphOp::ResolveEdgeDelete(_)
            | GraphOp::Hop { .. }
            | GraphOp::Neighbors { .. }
            | GraphOp::NeighborsMulti { .. }
            | GraphOp::Path { .. }
            | GraphOp::Subgraph { .. }
            | GraphOp::RagFusion { .. }
            | GraphOp::Algo { .. }
            | GraphOp::Match { .. }
            | GraphOp::MatchContinuation { .. }
            | GraphOp::MatchVarLenResume { .. }
            | GraphOp::BspSuperstep(_)
            | GraphOp::WccSuperstep(_)
            | GraphOp::TemporalNeighbors { .. }
            | GraphOp::TemporalAlgorithm { .. }
            | GraphOp::Stats { .. } => self.stage_not_point_write(task),
        }
    }

    fn stage_edge_put_batch(
        &mut self,
        task: &ExecutionTask,
        tid: u64,
        txn_id: TxnId,
        edges: &[BatchEdge],
    ) -> Response {
        let incoming_bytes: usize = edges
            .iter()
            .map(|e| e.src_id.len() + e.label.len() + e.dst_id.len())
            .sum();
        if let Err(e) = self.stage_graph_capped(incoming_bytes) {
            return self.response_error(task, e);
        }
        let overlay: &mut GraphTxnOverlay = self.graph_txn_overlay_mut(txn_id);
        for edge in edges {
            let coll_key = graph_coll_key(task, tid, edge.collection.as_str());
            overlay.stage_edge_put(
                coll_key,
                &edge.src_id,
                &edge.label,
                &edge.dst_id,
                Vec::new(),
            );
        }
        self.stage_count_response(task, edges.len())
    }

    fn stage_edge_delete_batch(
        &mut self,
        task: &ExecutionTask,
        tid: u64,
        txn_id: TxnId,
        edges: &[BatchEdge],
    ) -> Response {
        let database_id = task.request.database_id.as_u64();
        let mut removed: usize = 0;
        for edge in edges {
            let coll_key = graph_coll_key(task, tid, edge.collection.as_str());
            let overlay: &mut GraphTxnOverlay = self.graph_txn_overlay_mut(txn_id);
            let staged_presence =
                overlay.staged_edge_presence(&coll_key, &edge.src_id, &edge.label, &edge.dst_id);
            overlay.stage_edge_delete(coll_key, &edge.src_id, &edge.label, &edge.dst_id);
            // BASE ∪ OVERLAY, same as the single-edge `EdgeDelete` handler.
            let existed = staged_presence.unwrap_or_else(|| {
                self.edge_store
                    .get_edge(
                        database_id,
                        TenantId::new(tid),
                        edge.collection.as_str(),
                        &edge.src_id,
                        &edge.label,
                        &edge.dst_id,
                    )
                    .ok()
                    .flatten()
                    .is_some()
            });
            if existed {
                removed += 1;
            }
        }
        self.stage_count_response(task, removed)
    }

    /// Enforce the per-transaction GRAPH overlay memory cap, reusing the
    /// same budget the surrogate-keyed overlay uses (summed across every
    /// in-flight transaction's GRAPH overlay on this core).
    fn stage_graph_capped(&self, incoming_bytes: usize) -> crate::Result<()> {
        let current: usize = self
            .graph_txn_overlays
            .values()
            .map(GraphTxnOverlay::memory_size_estimate)
            .sum();
        if current.saturating_add(incoming_bytes) > MAX_TXN_OVERLAY_BYTES {
            return Err(crate::Error::TxnOverlayMemoryExceeded {
                limit: MAX_TXN_OVERLAY_BYTES,
            });
        }
        Ok(())
    }
}

fn graph_coll_key(task: &ExecutionTask, tid: u64, collection: &str) -> GraphCollKey {
    (
        task.request.database_id,
        TenantId::new(tid),
        collection.to_string(),
    )
}
