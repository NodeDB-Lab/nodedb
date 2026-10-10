// SPDX-License-Identifier: BUSL-1.1

//! Statement-time staging for the six stageable graph writes: `EdgePut`,
//! `EdgeDelete`, `EdgePutBatch`, `EdgeDeleteBatch`, `SetNodeLabels`,
//! `RemoveNodeLabels`. These stage into a dedicated [`GraphTxnOverlay`]
//! rather than the shared surrogate-keyed `TxnOverlay`, purely as in-memory
//! read-your-own-writes plumbing for Neighbors/Hop — commit durable replay
//! still runs the buffered `GraphOp` plan through the real handlers.
//!
//! Each staged edge and node-label delta is also tagged in the value overlay
//! (`TxnOverlay::note_row`), so a trigger body's graph write commits under
//! `Trigger` per row.

use nodedb_physical::physical_plan::{BatchEdge, GraphOp};

use crate::bridge::envelope::Response;
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::handlers::transaction::overlay::{
    GraphCollKey, GraphTxnOverlay, MAX_TXN_OVERLAY_BYTES,
};
use crate::data::executor::task::ExecutionTask;
use crate::types::{TenantId, TxnId};
use nodedb_types::RowIdentity;

/// Sentinel collection key node-label deltas are staged under, since
/// `SetNodeLabels`/`RemoveNodeLabels` operate tenant-wide with no collection
/// argument. The leading NUL makes it un-nameable by any CDC subscriber; the
/// nameable twin is [`crate::event::graph_cdc::GRAPH_LABEL_STREAM`].
pub(in crate::data::executor) const GRAPH_LABEL_COLL_KEY: &str = "\0__graph_node_labels__";

/// The refusal of an edge write with an endpoint under `Surrogate::ZERO`, as
/// the autocommit handlers refuse it. One unbound endpoint refuses a whole
/// batch before any edge stages. Node-label writes carry no surrogate.
fn unbound_edge_refusal(op: &GraphOp) -> Option<crate::bridge::envelope::ErrorCode> {
    let refuse = |collection: &str, src: nodedb_types::Surrogate, dst: nodedb_types::Surrogate| {
        [src, dst].into_iter().find_map(|surrogate| {
            crate::data::executor::handlers::unbound_surrogate::refuse_unbound(
                "graph", collection, surrogate,
            )
        })
    };
    match op {
        GraphOp::EdgePut {
            collection,
            src_surrogate,
            dst_surrogate,
            ..
        }
        | GraphOp::EdgeDelete {
            collection,
            src_surrogate,
            dst_surrogate,
            ..
        } => refuse(collection.as_str(), *src_surrogate, *dst_surrogate),
        GraphOp::EdgePutBatch { edges } | GraphOp::EdgeDeleteBatch { edges } => {
            edges.iter().find_map(|edge| {
                refuse(
                    edge.collection.as_str(),
                    edge.src_surrogate,
                    edge.dst_surrogate,
                )
            })
        }
        GraphOp::SetNodeLabels { .. }
        | GraphOp::RemoveNodeLabels { .. }
        | GraphOp::ResolveEdgeDelete(_)
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
        | GraphOp::Stats { .. }
        | GraphOp::NodeEdgeGuard { .. }
        | GraphOp::NodePresenceGuard { .. }
        | GraphOp::TruncateEdges { .. }
        | GraphOp::NodePresenceRead { .. } => None,
    }
}

impl CoreLoop {
    /// Route a staged `GraphOp` to its staging handler: an edge or label
    /// write, or a node delete's guard. Every other `GraphOp` answers that
    /// it is no point write.
    pub(in crate::data::executor) fn execute_stage_graph(
        &mut self,
        task: &ExecutionTask,
        tid: u64,
        txn_id: TxnId,
        op: &GraphOp,
    ) -> Response {
        if let Some(refusal) = unbound_edge_refusal(op) {
            return self.response_error(task, refusal);
        }
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
                self.tag_staged_edge(txn_id, &coll_key, src_id, label, dst_id);
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
                self.tag_staged_edge(txn_id, &coll_key, src_id, label, dst_id);
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
                self.tag_staged_labels(txn_id, &coll_key, node_id);
                self.graph_txn_overlay_mut(txn_id)
                    .stage_node_labels_set(coll_key, node_id, labels);
                // Replay always interns the node (`ensure_node`), so a set
                // touches exactly one node — same deterministic count as the
                // autocommit handler, never `labels.len()`.
                self.stage_count_response(task, 1)
            }

            GraphOp::RemoveNodeLabels { node_id, labels } => {
                let coll_key = graph_coll_key(task, tid, GRAPH_LABEL_COLL_KEY);
                self.tag_staged_labels(txn_id, &coll_key, node_id);
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
            | GraphOp::Stats { .. }
            | GraphOp::NodePresenceRead { .. } => self.stage_not_point_write(task),

            // A TRUNCATE's edge share stages nothing: its resolve records a
            // cut of the collection at the transaction's ordinal.
            GraphOp::TruncateEdges { .. } => self.stage_count_response(task, 0),

            // A node delete's guard stages nothing: it checks that the
            // node's live edges are the ones the planner read. A Calvin
            // transaction stages it with its node lock held, after every
            // lower writer of an edge on the node flushed. A session COMMIT
            // checks it again against the state it commits on.
            GraphOp::NodeEdgeGuard {
                collection,
                node_id,
                expected,
            } => {
                let checked =
                    self.execute_node_edge_guard(task, tid, collection.as_str(), node_id, expected);
                if checked.status == crate::bridge::envelope::Status::Ok {
                    self.stage_count_response(task, 0)
                } else {
                    checked
                }
            }

            // A CRDT document delete's presence guard stages nothing: it
            // checks that the stored documents are the ones the planner
            // read. A Calvin transaction stages it with the key of every
            // document it names and the collection key held, after every
            // lower writer of those documents flushed. A session COMMIT
            // checks it again against the state it commits on.
            GraphOp::NodePresenceGuard {
                collection,
                present,
                absent,
                ..
            } => {
                let checked =
                    self.execute_node_presence_guard(task, collection.as_str(), present, absent);
                if checked.status == crate::bridge::envelope::Status::Ok {
                    self.stage_count_response(task, 0)
                } else {
                    checked
                }
            }
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
        for edge in edges {
            let coll_key = graph_coll_key(task, tid, edge.collection.as_str());
            self.tag_staged_edge(txn_id, &coll_key, &edge.src_id, &edge.label, &edge.dst_id);
            let overlay: &mut GraphTxnOverlay = self.graph_txn_overlay_mut(txn_id);
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
            self.tag_staged_edge(txn_id, &coll_key, &edge.src_id, &edge.label, &edge.dst_id);
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

    /// Tag a staged edge for the transaction's row sources, before the
    /// graph overlay stages it. The edge's write event names it by its
    /// `(src, label, dst)` row id in its collection.
    fn tag_staged_edge(
        &mut self,
        txn_id: TxnId,
        coll_key: &GraphCollKey,
        src: &str,
        label: &str,
        dst: &str,
    ) {
        let was_staged = self
            .graph_txn_overlays
            .get(&txn_id)
            .and_then(|overlay| overlay.staged_edge_presence(coll_key, src, label, dst))
            .is_some();
        let row = RowIdentity::from_user_key(crate::event::graph_cdc::edge_row_id(src, label, dst));
        self.txn_overlay_mut(txn_id)
            .note_row(coll_key.clone(), row, was_staged);
    }

    /// Tag a staged node-label delta for the transaction's row sources,
    /// before the graph overlay stages it. The delta's write event names it
    /// by its node id on the label stream.
    fn tag_staged_labels(&mut self, txn_id: TxnId, coll_key: &GraphCollKey, node_id: &str) {
        let was_staged = self
            .graph_txn_overlays
            .get(&txn_id)
            .is_some_and(|overlay| overlay.labels_delta(coll_key, node_id).is_some());
        let (database_id, tenant_id, _) = coll_key;
        let stream_key = (
            *database_id,
            *tenant_id,
            crate::event::graph_cdc::GRAPH_LABEL_STREAM.to_string(),
        );
        self.txn_overlay_mut(txn_id).note_row(
            stream_key,
            RowIdentity::from_user_key(node_id),
            was_staged,
        );
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

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use nodedb_bridge::buffer::RingBuffer;
    use nodedb_physical::physical_plan::PhysicalPlan;
    use nodedb_types::{DatabaseId, QualifiedCollection, Surrogate};

    use super::*;
    use crate::bridge::dispatch::{BridgeRequest, BridgeResponse};
    use crate::bridge::envelope::{Admission, ExemptReason, Priority, Request, Status};
    use crate::data::executor::task::TaskState;
    use crate::event::EventSource;
    use crate::event::graph_cdc::{GRAPH_LABEL_STREAM, edge_row_id};
    use crate::types::{ReadConsistency, RequestId, TraceId, VShardId};

    fn make_core() -> (CoreLoop, tempfile::TempDir) {
        let dir = tempfile::tempdir().expect("tempdir");
        let (_request_tx, request_rx) = RingBuffer::channel::<BridgeRequest>(8);
        let (response_tx, _response_rx) = RingBuffer::channel::<BridgeResponse>(8);
        let core = CoreLoop::open(
            0,
            request_rx,
            response_tx,
            dir.path(),
            Arc::new(nodedb_types::OrdinalClock::new()),
            crate::data::executor::core_loop::test_governor(),
        )
        .expect("open CoreLoop");
        (core, dir)
    }

    fn stage_task(plan: &PhysicalPlan, source: EventSource, txn_id: TxnId) -> ExecutionTask {
        ExecutionTask {
            request: Request {
                request_id: RequestId::new(1),
                tenant_id: TenantId::new(1),
                database_id: DatabaseId::DEFAULT,
                vshard_id: VShardId::new(0),
                plan: plan.clone(),
                deadline: std::time::Instant::now() + std::time::Duration::from_secs(1),
                priority: Priority::Normal,
                trace_id: TraceId::ZERO,
                consistency: ReadConsistency::Strong,
                idempotency_key: None,
                event_source: source,
                user_roles: Vec::new(),
                user_id: None,
                statement_digest: None,
                txn_id: Some(txn_id),
                wal_lsn: None,
                resolved_now_ms: None,
                commit_hlc: None,
                entry_version: None,
                admission: Admission::Exempt(ExemptReason::AlreadyOrdered),
            },
            state: TaskState::Running,
            wal_lsn: None,
            resolved_now_ms: None,
        }
    }

    fn stage(core: &mut CoreLoop, plan: PhysicalPlan, source: EventSource, txn_id: TxnId) {
        let task = stage_task(&plan, source, txn_id);
        let response = core.execute_stage_write(&task, 1, &plan);
        assert_eq!(response.status, Status::Ok, "{:?}", response.error_code);
    }

    fn edge_put(dst: &str) -> PhysicalPlan {
        PhysicalPlan::Graph(GraphOp::EdgePut {
            collection: QualifiedCollection::new(DatabaseId::DEFAULT, "social"),
            src_id: "a".into(),
            label: "KNOWS".into(),
            dst_id: dst.into(),
            properties: Vec::new(),
            src_surrogate: Surrogate::new(1),
            dst_surrogate: Surrogate::new(2),
        })
    }

    fn body_rows(core: &CoreLoop, txn_id: TxnId) -> Vec<(String, String)> {
        let mut rows = core
            .txn_overlays
            .get(&txn_id)
            .map(|overlay| {
                overlay
                    .body_writes(DatabaseId::DEFAULT, TenantId::new(1))
                    .rows
            })
            .unwrap_or_default();
        rows.sort();
        rows
    }

    /// A trigger body's edge commits as a `Trigger` row beside the client's
    /// edge in the same collection, and a body delete of the client's edge
    /// keeps it the client's.
    #[test]
    fn a_body_edge_is_tagged_beside_the_clients_edge() {
        let (mut core, _dir) = make_core();
        let txn = TxnId::new(7);
        stage(&mut core, edge_put("b"), EventSource::User, txn);
        stage(&mut core, edge_put("c"), EventSource::Trigger, txn);
        stage(
            &mut core,
            PhysicalPlan::Graph(GraphOp::EdgeDelete {
                collection: QualifiedCollection::new(DatabaseId::DEFAULT, "social"),
                src_id: "a".into(),
                label: "KNOWS".into(),
                dst_id: "b".into(),
                src_surrogate: Surrogate::new(1),
                dst_surrogate: Surrogate::new(2),
                rls_write_check: nodedb_types::RlsWriteCheck::NoPolicyApplies,
            }),
            EventSource::Trigger,
            txn,
        );
        assert_eq!(
            body_rows(&core, txn),
            vec![("social".to_string(), edge_row_id("a", "KNOWS", "c"))]
        );
    }

    /// A trigger body's node-label delta commits as a `Trigger` row on the
    /// label stream beside the client's delta.
    #[test]
    fn a_body_node_label_is_tagged_beside_the_clients_label() {
        let (mut core, _dir) = make_core();
        let txn = TxnId::new(8);
        let set = |node: &str| {
            PhysicalPlan::Graph(GraphOp::SetNodeLabels {
                node_id: node.into(),
                labels: vec!["Person".into()],
            })
        };
        stage(&mut core, set("n1"), EventSource::User, txn);
        stage(&mut core, set("n2"), EventSource::Trigger, txn);
        assert_eq!(
            body_rows(&core, txn),
            vec![(GRAPH_LABEL_STREAM.to_string(), "n2".to_string())]
        );
    }
}
