// SPDX-License-Identifier: BUSL-1.1

//! Statement-time staging for the six stageable GRAPH writes: `EdgePut`,
//! `EdgeDelete`, `EdgePutBatch`, `EdgeDeleteBatch`, `SetNodeLabels`,
//! `RemoveNodeLabels`.
//!
//! Graph is the first engine whose unit-of-mutation is not a single
//! surrogate-addressed row, so these stage into a dedicated
//! [`GraphTxnOverlay`] (`self.graph_txn_overlays`) rather than the shared
//! surrogate-keyed
//! [`crate::data::executor::handlers::transaction::overlay::TxnOverlay`]
//! every other engine uses. COMMIT durable replay is unchanged: the
//! buffered `GraphOp` plan is still replayed through the real
//! `execute_edge_put` / `execute_edge_delete` / `execute_edge_put_batch` /
//! `execute_edge_delete_batch` handlers inside the COMMIT
//! `TransactionBatch`. The `GraphTxnOverlay` entry staged here is purely
//! in-memory read-your-own-writes plumbing for Neighbors / Hop, dropped at
//! commit or rollback alongside the surrogate overlay
//! (`MetaOp::DropTxnOverlay`).
//!
//! Every other `GraphOp` (Hop, Neighbors, Path, Subgraph, Algo, Match, ...)
//! never reaches this file: `is_stageable_write` only routes the six ops
//! above here; everything else stays on the pre-existing buffer + "OK"
//! deferral via `stage_not_point_write`.

use nodedb_physical::physical_plan::{BatchEdge, GraphOp};

use crate::bridge::envelope::Response;
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::handlers::transaction::overlay::{
    GraphCollKey, GraphTxnOverlay, MAX_TXN_OVERLAY_BYTES,
};
use crate::data::executor::task::ExecutionTask;
use crate::types::{TenantId, TxnId};

/// Sentinel collection key node-label deltas are staged under.
/// `SetNodeLabels` / `RemoveNodeLabels` operate tenant-wide on the durable
/// CSR partition (no collection argument on the plan), so the overlay --
/// which keys everything per-collection like every other GRAPH op -- stores
/// label deltas under this fixed key instead of a real collection name.
const GRAPH_LABEL_COLL_KEY: &str = "\0__graph_node_labels__";

impl CoreLoop {
    /// Route a stageable `GraphOp` to its staging handler.
    ///
    /// Caller invariant: `op` must be one of the six ops `is_stageable_write`
    /// accepts. Every other `GraphOp` is unreachable here -- the Control
    /// Plane never builds a `StageWrite` for them.
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
                let coll_key = graph_coll_key(task, tid, collection);
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
                let coll_key = graph_coll_key(task, tid, collection);
                self.graph_txn_overlay_mut(txn_id)
                    .stage_edge_delete(coll_key, src_id, label, dst_id);
                self.stage_count_response(task, 1)
            }

            GraphOp::EdgePutBatch { edges } => self.stage_edge_put_batch(task, tid, txn_id, edges),
            GraphOp::EdgeDeleteBatch { edges } => {
                self.stage_edge_delete_batch(task, tid, txn_id, edges)
            }

            GraphOp::SetNodeLabels { node_id, labels } => {
                let coll_key = graph_coll_key(task, tid, GRAPH_LABEL_COLL_KEY);
                self.graph_txn_overlay_mut(txn_id)
                    .stage_node_labels_set(coll_key, node_id, labels);
                self.stage_count_response(task, labels.len())
            }

            GraphOp::RemoveNodeLabels { node_id, labels } => {
                let coll_key = graph_coll_key(task, tid, GRAPH_LABEL_COLL_KEY);
                self.graph_txn_overlay_mut(txn_id)
                    .stage_node_labels_remove(coll_key, node_id, labels);
                self.stage_count_response(task, labels.len())
            }

            GraphOp::Hop { .. }
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
            let coll_key = graph_coll_key(task, tid, &edge.collection);
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
        let overlay: &mut GraphTxnOverlay = self.graph_txn_overlay_mut(txn_id);
        for edge in edges {
            let coll_key = graph_coll_key(task, tid, &edge.collection);
            overlay.stage_edge_delete(coll_key, &edge.src_id, &edge.label, &edge.dst_id);
        }
        self.stage_count_response(task, edges.len())
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
