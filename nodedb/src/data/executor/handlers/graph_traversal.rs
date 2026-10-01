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
    pub edge_label: &'a Option<String>,
    pub max_depth: usize,
    pub frontier_bitmap: Option<&'a nodedb_types::SurrogateBitmap>,
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
            edge_label,
            max_depth,
            frontier_bitmap,
        } = params;
        let max_depth =
            max_depth.min(crate::engine::graph::traversal_options::MAX_GRAPH_TRAVERSAL_DEPTH);
        debug!(core = self.core_id, tid, %src, %dst, ?edge_label, max_depth, "graph path");
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
        let label_filter = edge_label.as_deref();
        let path = match self.csr_partition(database_id, tid) {
            Some(partition) => partition.shortest_path(
                crate::engine::graph::csr::ShortestPathParams {
                    src,
                    dst,
                    label_filter: label_filter.as_slice(),
                    max_depth,
                    max_visited: self.graph_tuning.max_visited,
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
        tid: u64,
        start_nodes: &[String],
        edge_label: &Option<String>,
        depth: usize,
    ) -> Response {
        debug!(
            core = self.core_id,
            tid,
            ?start_nodes,
            ?edge_label,
            depth,
            "graph subgraph"
        );
        let database_id = task.request.database_id.as_u64();
        let depth = depth.min(crate::engine::graph::traversal_options::MAX_GRAPH_TRAVERSAL_DEPTH);
        let refs: Vec<&str> = start_nodes.iter().map(String::as_str).collect();
        // Subgraph currently materializes the out-edge closure; `direction` is
        // threaded through so staged in-edges can surface once the DML surface
        // carries it.
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
        let edges: Vec<(String, String, String)> = match self.csr_partition(database_id, tid) {
            Some(partition) => partition.subgraph(
                &refs,
                edge_label.as_deref(),
                direction,
                depth,
                self.graph_tuning.max_visited,
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
