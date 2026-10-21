// SPDX-License-Identifier: BUSL-1.1

//! Bitemporal 1-hop neighbors: [`GraphOp::TemporalNeighbors`].
//!
//! Reads versioned edges directly from the `EdgeStore` at the requested
//! cut, rather than a CSR.
//!
//! [`GraphOp::TemporalNeighbors`]: nodedb_physical::physical_plan::GraphOp::TemporalNeighbors

use nodedb_physical::physical_plan::GraphReadScope;
use nodedb_types::TenantId;
use tracing::debug;

use crate::bridge::envelope::{ErrorCode, Response};
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::task::ExecutionTask;
use crate::engine::graph::edge_store::Direction;

/// Parameters for [`CoreLoop::execute_graph_temporal_neighbors`]. Packed
/// so the handler signature stays within clippy's 7-arg budget — the
/// wire-level `GraphOp::TemporalNeighbors` variant carries the same
/// fields, and the dispatcher populates this struct once per call.
pub(in crate::data::executor) struct TemporalNeighborsParams<'a> {
    pub tid: u64,
    pub collection: &'a str,
    pub node_id: &'a str,
    pub edge_label: &'a Option<String>,
    pub direction: Direction,
    pub scope: &'a GraphReadScope,
}

impl CoreLoop {
    pub(in crate::data::executor) fn execute_graph_temporal_neighbors(
        &self,
        task: &ExecutionTask,
        p: TemporalNeighborsParams<'_>,
    ) -> Response {
        let TemporalNeighborsParams {
            tid,
            collection,
            node_id,
            edge_label,
            direction,
            scope,
        } = p;
        debug!(
            core = self.core_id,
            tid,
            %collection,
            %node_id,
            ?edge_label,
            ?direction,
            ?scope,
            "graph temporal neighbors"
        );
        // One row per base edge: every system-time version of an edge is no
        // single row.
        let Some(cutoff_ordinal) = scope.cutoff_ordinal() else {
            return self.response_error(
                task,
                ErrorCode::Unsupported {
                    detail: "AS OF SYSTEM TIME NULL (all-versions) is not supported on graph \
                             neighbors"
                        .into(),
                },
            );
        };
        let database_id = task.request.database_id.as_u64();
        let tenant = TenantId::new(tid);
        let as_of_params = crate::engine::graph::edge_store::NeighborsAsOfParams {
            db: database_id,
            tid: tenant,
            collection,
            node: node_id,
            label_filter: edge_label.as_deref(),
            cutoff_ordinal,
            valid_at_ms: scope.valid_at_ms,
        };
        let edges = match self.edge_store.node_edges_as_of(as_of_params, direction) {
            Ok(edges) => edges,
            Err(e) => return self.response_error(task, ErrorCode::from(e)),
        };
        // A live read inside a transaction sees that transaction's own staged
        // edge writes in this collection.
        let edges = match (scope.is_live(), task.request.txn_id) {
            (true, Some(txn_id)) => {
                self.touch_overlay(txn_id);
                super::graph::graph_txn_merge::merge_graph_txn_overlay_collection_edges(
                    self.graph_txn_overlays.get(&txn_id),
                    &(task.request.database_id, tenant, collection.to_string()),
                    node_id,
                    edge_label.as_deref().as_slice(),
                    direction,
                    edges,
                )
            }
            _ => edges,
        };

        let entries: Vec<super::super::response_codec::NeighborEntry<'_>> = edges
            .iter()
            .map(|(src, label, dst)| {
                let opposite = if src == node_id {
                    dst.as_str()
                } else {
                    src.as_str()
                };
                super::super::response_codec::NeighborEntry {
                    label: label.as_str(),
                    node: opposite,
                }
            })
            .collect();
        match super::super::response_codec::encode(&entries) {
            Ok(payload) => self.response_with_payload(task, payload),
            Err(e) => self.response_error(task, ErrorCode::from(e)),
        }
    }
}
