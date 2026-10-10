// SPDX-License-Identifier: BUSL-1.1

//! `EdgeDeleteBatch`: batched edge tombstone in a single SPSC round-trip.

use tracing::debug;

use crate::bridge::envelope::{EdgeImage, Response, WriteSetEntry};
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::handlers::partial_refusal::refusal_after_rows;
use crate::data::executor::handlers::transaction::undo::edge_write::EdgeTarget;
use crate::data::executor::task::ExecutionTask;
use crate::types::TenantId;

use super::shared::owns_logical_edge_stats;

impl CoreLoop {
    /// Apply a batched edge delete in a single SPSC round-trip.
    ///
    /// The bitemporal edge store always appends a tombstone version on
    /// success, whether or not a live edge existed, so each edge's pre-image
    /// is read before its tombstone is written and the affected count is the
    /// number that were actually live — never the batch length.
    pub(in crate::data::executor) fn execute_edge_delete_batch(
        &mut self,
        task: &ExecutionTask,
        tid: u64,
        edges: &[nodedb_physical::physical_plan::BatchEdge],
    ) -> Response {
        debug!(
            core = self.core_id,
            count = edges.len(),
            "edge delete batch"
        );
        // Every endpoint carries the surrogate its coordinator bound. One
        // unbound endpoint refuses the batch before any tombstone is written.
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
        let database_id = task.request.database_id.as_u64();
        let mut removed: u64 = 0;
        // Each edge's tombstone, at the ordinal decided here, journalled after
        // apply. A tombstone that failed after earlier ones landed refuses the
        // batch with the landed tombstones, which stay.
        let mut write_set: Vec<WriteSetEntry> = Vec::with_capacity(edges.len());
        for edge in edges {
            // A pre-image read error refuses the batch: read as absent, the
            // count would leave out an edge the tombstone removes.
            let existed = match self.edge_store.get_edge(
                database_id,
                TenantId::new(tid),
                edge.collection.as_str(),
                &edge.src_id,
                &edge.label,
                &edge.dst_id,
            ) {
                Ok(properties) => properties.is_some(),
                Err(e) => {
                    let code = refusal_after_rows(write_set.len() as u64, e);
                    return self.refusal_with_landed_rows(task, code, write_set);
                }
            };
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
            use crate::engine::graph::edge_store::EdgeRef;
            // A tombstone that fails to persist fails the statement, same as
            // the single-edge delete: a count that ignored it would report an
            // edge removed that is still live.
            let tombstone = match self.edge_store.soft_delete_edge_recorded(
                EdgeRef::new(
                    task.request.database_id,
                    TenantId::new(tid),
                    edge.collection.as_str(),
                    &edge.src_id,
                    &edge.label,
                    &edge.dst_id,
                ),
                stamp,
                owns_logical_edge_stats(task, &edge.src_id),
            ) {
                Ok(tombstone) => tombstone,
                Err(e) => {
                    let code = refusal_after_rows(write_set.len() as u64, e);
                    return self.refusal_with_landed_rows(task, code, write_set);
                }
            };
            let current = tombstone.current.clone();
            // The CSR follows what the edge resolves to after the tombstone.
            // A CSR refusal takes this tombstone back out; the edges before it
            // stand and are journalled.
            if let Err(e) = self.mirror_edge_csr(
                database_id,
                tid,
                (&edge.src_id, &edge.label, &edge.dst_id),
                edge.collection.as_str(),
                current.as_deref(),
            ) {
                let reversed = self.reverse_edge_write(target.undo(tombstone, csr_prior), e);
                let code = refusal_after_rows(write_set.len() as u64, reversed);
                return self.refusal_with_landed_rows(task, code, write_set);
            }
            if existed {
                removed += 1;
            }
            write_set.push(WriteSetEntry::edge(EdgeImage::Delete(
                crate::wal::EdgeDeleteRedo {
                    collection: edge.collection.to_string(),
                    src_id: edge.src_id.clone(),
                    label: edge.label.clone(),
                    dst_id: edge.dst_id.clone(),
                    src_surrogate: edge.src_surrogate.as_u32(),
                    dst_surrogate: edge.dst_surrogate.as_u32(),
                    system_from: Some(ord),
                    applied: (stamp.applied != ord).then_some(stamp.applied),
                },
            )));
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
            // CDC: one Delete event per edge on the edge's own collection.
            self.emit_graph_edge_event(
                task,
                crate::data::executor::core_loop::event_emit::GraphEdgeEvent {
                    collection: edge.collection.as_str(),
                    src_id: &edge.src_id,
                    label: &edge.label,
                    dst_id: &edge.dst_id,
                    src_surrogate: edge.src_surrogate,
                    dst_surrogate: edge.dst_surrogate,
                    op: crate::event::WriteOp::Delete,
                    properties: None,
                },
            );
        }
        let mut response = self.response_affected(task, removed);
        response.write_set = write_set;
        response
    }
}
