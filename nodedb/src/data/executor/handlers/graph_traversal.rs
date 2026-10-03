// SPDX-License-Identifier: BUSL-1.1

//! GraphPath and GraphSubgraph handlers for `CoreLoop`.

use nodedb_types::diagnostic::DiagnosticLayer;
use tracing::{debug, warn};

use crate::bridge::envelope::{ErrorCode, Response};
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::task::ExecutionTask;

/// Bundled arguments for [`CoreLoop::execute_graph_path`].
pub(in crate::data::executor) struct GraphPathParams<'a> {
    pub tid: u64,
    pub src: &'a str,
    pub dst: &'a str,
    /// Empty keeps every edge. Otherwise an edge with any listed label.
    pub edge_labels: &'a [String],
    pub max_depth: usize,
    /// The walk's visit cap ([`CoreLoop::walk_visit_cap`]).
    pub max_visited: usize,
    pub frontier_bitmap: Option<&'a nodedb_types::SurrogateBitmap>,
}

/// Bundled arguments for [`CoreLoop::execute_graph_subgraph`].
pub(in crate::data::executor) struct GraphSubgraphParams<'a> {
    pub tid: u64,
    pub start_nodes: &'a [String],
    /// Empty keeps every edge. Otherwise an edge with any listed label.
    pub edge_labels: &'a [String],
    pub depth: usize,
    /// The walk's visit cap ([`CoreLoop::walk_visit_cap`]).
    pub max_visited: usize,
}

impl CoreLoop {
    pub(in crate::data::executor) fn execute_graph_path(
        &self,
        task: &ExecutionTask,
        params: GraphPathParams<'_>,
    ) -> Response {
        let GraphPathParams {
            tid,
            src,
            dst,
            edge_labels,
            max_depth,
            max_visited,
            frontier_bitmap,
        } = params;
        let max_depth =
            max_depth.min(crate::engine::graph::traversal_options::MAX_GRAPH_TRAVERSAL_DEPTH);
        debug!(core = self.core_id, tid, %src, %dst, ?edge_labels, max_depth, "graph path");
        let database_id = task.request.database_id.as_u64();
        // Read-your-own-writes: fold this transaction's staged edges/tombstones
        // into the bidirectional search, including a path that must pass
        // through a node reachable only via a staged edge.
        // Read-your-own-writes refreshes the lease (see the overlay reaper).
        if let Some(txn_id) = task.request.txn_id {
            self.touch_overlay(txn_id);
        }
        let delta = task
            .request
            .txn_id
            .and_then(|txn_id| self.graph_txn_overlays.get(&txn_id))
            .map(|ov| {
                super::graph_txn_merge::build_graph_overlay_delta(
                    ov,
                    task.request.database_id,
                    crate::types::TenantId::new(tid),
                )
            });
        let label_filter: Vec<&str> = edge_labels.iter().map(String::as_str).collect();
        let path = match self.csr_partition(database_id, tid) {
            Some(partition) => partition.shortest_path(
                crate::engine::graph::csr::ShortestPathParams {
                    src,
                    dst,
                    label_filter: &label_filter,
                    max_depth,
                    max_visited,
                    frontier_bitmap,
                },
                delta.as_ref(),
            ),
            None => None,
        };
        match path {
            Some(path) => {
                if let Some(ref m) = self.metrics {
                    m.record_graph_traversal();
                }
                match crate::data::executor::response_codec::encode(&path) {
                    Ok(payload) => self.response_with_payload(task, payload),
                    Err(e) => {
                        warn!(core = self.core_id, layer = DiagnosticLayer::WireShape.as_str(), error = %e, "graph path serialization failed");
                        self.response_error(
                            task,
                            ErrorCode::Internal {
                                detail: e.to_string(),
                            },
                        )
                    }
                }
            }
            None => self.response_error(task, ErrorCode::NotFound),
        }
    }

    pub(in crate::data::executor) fn execute_graph_subgraph(
        &self,
        task: &ExecutionTask,
        params: GraphSubgraphParams<'_>,
    ) -> Response {
        let GraphSubgraphParams {
            tid,
            start_nodes,
            edge_labels,
            depth,
            max_visited,
        } = params;
        debug!(
            core = self.core_id,
            tid,
            ?start_nodes,
            ?edge_labels,
            depth,
            "graph subgraph"
        );
        let database_id = task.request.database_id.as_u64();
        let depth = depth.min(crate::engine::graph::traversal_options::MAX_GRAPH_TRAVERSAL_DEPTH);
        let refs: Vec<&str> = start_nodes.iter().map(String::as_str).collect();
        // A subgraph plan is the out-edge closure of its start nodes.
        let direction = crate::engine::graph::edge_store::Direction::Out;
        // Read-your-own-writes: fold this transaction's staged edges/tombstones
        // into the materialized subgraph, including through staged-only nodes.
        // Read-your-own-writes refreshes the lease (see the overlay reaper).
        if let Some(txn_id) = task.request.txn_id {
            self.touch_overlay(txn_id);
        }
        let delta = task
            .request
            .txn_id
            .and_then(|txn_id| self.graph_txn_overlays.get(&txn_id))
            .map(|ov| {
                super::graph_txn_merge::build_graph_overlay_delta(
                    ov,
                    task.request.database_id,
                    crate::types::TenantId::new(tid),
                )
            });
        let labels: Vec<&str> = edge_labels.iter().map(String::as_str).collect();
        let edges: Vec<(String, String, String)> = match self.csr_partition(database_id, tid) {
            Some(partition) => partition.subgraph(
                &refs,
                &labels,
                direction,
                depth,
                max_visited,
                delta.as_ref(),
            ),
            None => Vec::new(),
        };
        let result: Vec<_> = edges
            .iter()
            .map(
                |(s, l, d)| crate::data::executor::response_codec::SubgraphEdge {
                    src: s.as_str(),
                    label: l.as_str(),
                    dst: d.as_str(),
                },
            )
            .collect();
        if let Some(ref m) = self.metrics {
            m.record_graph_traversal();
        }
        match crate::data::executor::response_codec::encode(&result) {
            Ok(payload) => self.response_with_payload(task, payload),
            Err(e) => {
                warn!(core = self.core_id, layer = DiagnosticLayer::WireShape.as_str(), error = %e, "graph subgraph serialization failed");
                self.response_error(
                    task,
                    ErrorCode::Internal {
                        detail: e.to_string(),
                    },
                )
            }
        }
    }
}
