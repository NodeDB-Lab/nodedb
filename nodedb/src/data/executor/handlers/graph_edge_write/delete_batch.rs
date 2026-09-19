// SPDX-License-Identifier: BUSL-1.1

//! `EdgeDeleteBatch`: batched edge tombstone in a single SPSC round-trip.

use tracing::debug;

use crate::bridge::envelope::{ErrorCode, Response};
use crate::data::executor::core_loop::CoreLoop;
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
        let database_id = task.request.database_id.as_u64();
        let mut removed: u64 = 0;
        for edge in edges {
            let existed = self
                .edge_store
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
                .is_some();
            if existed {
                removed += 1;
            }
            let ord = self
                .active_graph_system_from
                .unwrap_or_else(|| self.hlc.next_ordinal());
            use crate::engine::graph::edge_store::EdgeRef;
            // A tombstone that fails to persist fails the statement, same as
            // the single-edge delete: a count that ignored it would report an
            // edge removed that is still live.
            if let Err(e) = self.edge_store.soft_delete_edge_with_stats(
                EdgeRef::new(
                    task.request.database_id,
                    TenantId::new(tid),
                    edge.collection.as_str(),
                    &edge.src_id,
                    &edge.label,
                    &edge.dst_id,
                ),
                ord,
                owns_logical_edge_stats(task, &edge.src_id),
            ) {
                return self.response_error(
                    task,
                    ErrorCode::Internal {
                        detail: e.to_string(),
                    },
                );
            }
            let partition = self.csr_partition_mut(database_id, tid);
            partition.remove_edge_in_collection(
                &edge.src_id,
                &edge.label,
                &edge.dst_id,
                edge.collection.as_str(),
            );
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
            // CDC: one Delete event per edge on the edge's own collection.
            self.emit_graph_edge_event(
                task,
                crate::data::executor::core_loop::event_emit::GraphEdgeEvent {
                    collection: edge.collection.as_str(),
                    src_id: &edge.src_id,
                    label: &edge.label,
                    dst_id: &edge.dst_id,
                    op: crate::event::WriteOp::Delete,
                    properties: None,
                },
            );
        }
        self.response_affected(task, removed)
    }
}
