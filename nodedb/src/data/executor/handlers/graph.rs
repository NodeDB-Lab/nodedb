// SPDX-License-Identifier: BUSL-1.1

//! Graph operation handlers: EdgePut, EdgeDelete, GraphHop, GraphNeighbors,
//! GraphPath, GraphSubgraph. The CSR index is partitioned structurally by
//! tenant; handlers resolve the partition once via
//! `self.csr_partition(_mut)(tid)` and address node ids in their raw,
//! user-visible form throughout — no `<tid>:` prefix, no `scoped_node()`.

use nodedb_types::diagnostic::DiagnosticLayer;
use tracing::{debug, warn};

use crate::bridge::envelope::{ErrorCode, Response};

use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::task::ExecutionTask;
use crate::types::TenantId;

#[path = "graph_edge_resolve.rs"]
pub(in crate::data::executor) mod graph_edge_resolve;
#[path = "graph_edge_write/mod.rs"]
pub(in crate::data::executor) mod graph_edge_write;
#[path = "graph_traversal.rs"]
pub(in crate::data::executor) mod graph_traversal;
#[path = "graph_txn_merge.rs"]
pub(in crate::data::executor) mod graph_txn_merge;

pub(in crate::data::executor) use graph_edge_write::{EdgeDeleteParams, EdgePutParams};

use graph_txn_merge::merge_graph_txn_overlay_neighbors;

/// Bundled arguments for [`CoreLoop::execute_graph_hop`].
pub(in crate::data::executor) struct GraphHopParams<'a> {
    pub tid: u64,
    pub start_nodes: &'a [String],
    pub edge_label: &'a Option<String>,
    pub direction: crate::engine::graph::edge_store::Direction,
    pub depth: usize,
    pub frontier_bitmap: Option<&'a nodedb_types::SurrogateBitmap>,
}

/// Arguments for [`CoreLoop::execute_graph_neighbors_multi`].
pub(in crate::data::executor) struct GraphNeighborsMultiArgs<'a> {
    pub node_ids: &'a [String],
    pub edge_label: &'a Option<String>,
    pub direction: crate::engine::graph::edge_store::Direction,
    pub max_results: u32,
    /// Collection scope, or `None` for a label-only traversal.
    pub collection: Option<&'a str>,
}

impl CoreLoop {
    pub(in crate::data::executor) fn execute_graph_hop(
        &self,
        task: &ExecutionTask,
        params: GraphHopParams<'_>,
    ) -> Response {
        let GraphHopParams {
            tid,
            start_nodes,
            edge_label,
            direction,
            depth,
            frontier_bitmap,
        } = params;
        debug!(
            core = self.core_id,
            tid,
            ?start_nodes,
            ?edge_label,
            ?direction,
            depth,
            "graph hop"
        );
        let database_id = task.request.database_id.as_u64();
        let depth = depth.min(crate::engine::graph::traversal_options::MAX_GRAPH_TRAVERSAL_DEPTH);
        let refs: Vec<&str> = start_nodes.iter().map(String::as_str).collect();
        // Read-your-own-writes refreshes the lease (see the overlay reaper).
        if let Some(txn_id) = task.request.txn_id {
            self.touch_overlay(txn_id);
        }
        let overlay = task
            .request
            .txn_id
            .and_then(|txn_id| self.graph_txn_overlays.get(&txn_id));
        // Multi-hop pushes the staged delta into the traversal; single-hop
        // (depth == 1) is handled by `merge_hop_single_hop` below instead.
        let delta = if depth > 1 {
            overlay.map(|ov| {
                graph_txn_merge::build_graph_overlay_delta(
                    ov,
                    task.request.database_id,
                    TenantId::new(tid),
                )
            })
        } else {
            None
        };
        let result: Vec<String> = match self.csr_partition(database_id, tid) {
            Some(partition) => partition.traverse_bfs(
                nodedb_graph::BfsParams {
                    start_nodes: &refs,
                    label_filter: edge_label.as_deref(),
                    direction,
                    max_depth: depth,
                    max_visited: self.graph_tuning.max_visited,
                    frontier_bitmap,
                },
                delta.as_ref(),
            ),
            None => Vec::new(),
        };
        let result: Vec<String> =
            graph_txn_merge::merge_hop_single_hop(graph_txn_merge::HopMergeParams {
                overlay,
                durable_neighbors_of: |start: &str| {
                    self.csr_partition(database_id, tid)
                        .map(|p| p.neighbors(start, edge_label.as_deref(), direction))
                        .unwrap_or_default()
                },
                starts: &refs,
                depth,
                database_id: task.request.database_id,
                tenant: TenantId::new(tid),
                edge_label: edge_label.as_deref(),
                direction,
                has_bitmap: frontier_bitmap.is_some(),
                durable_result: result,
            });
        if let Some(ref m) = self.metrics {
            m.record_graph_traversal();
        }
        match super::super::response_codec::encode(&result) {
            Ok(payload) => self.response_with_payload(task, payload),
            Err(e) => {
                warn!(core = self.core_id, layer = DiagnosticLayer::WireShape.as_str(), error = %e, "graph hop serialization failed");
                self.response_error(
                    task,
                    ErrorCode::Internal {
                        detail: e.to_string(),
                    },
                )
            }
        }
    }

    pub(in crate::data::executor) fn execute_graph_neighbors(
        &self,
        task: &ExecutionTask,
        tid: u64,
        node_id: &str,
        edge_label: &Option<String>,
        direction: crate::engine::graph::edge_store::Direction,
        collection: Option<&str>,
    ) -> Response {
        debug!(core = self.core_id, tid, %node_id, ?edge_label, ?direction, "graph neighbors");
        let database_id = task.request.database_id.as_u64();
        // A named collection restricts the walk; unscoped `neighbors` would
        // silently span every collection's edges in the shared node space.
        let durable: Vec<(String, String)> = match self.csr_partition(database_id, tid) {
            Some(partition) => match collection {
                Some(collection) => partition.neighbors_in_collection(
                    node_id,
                    edge_label.as_deref(),
                    direction,
                    collection,
                ),
                None => partition.neighbors(node_id, edge_label.as_deref(), direction),
            },
            None => Vec::new(),
        };
        // Read-your-own-writes: fold staged edge writes into the durable
        // result (see `graph_txn_merge`), and refresh the overlay lease.
        if let Some(txn_id) = task.request.txn_id {
            self.touch_overlay(txn_id);
        }
        let overlay = task
            .request
            .txn_id
            .and_then(|txn_id| self.graph_txn_overlays.get(&txn_id));
        let neighbors = merge_graph_txn_overlay_neighbors(
            overlay,
            task.request.database_id,
            TenantId::new(tid),
            node_id,
            edge_label.as_deref(),
            direction,
            durable,
        );
        let result: Vec<_> = neighbors
            .iter()
            .map(
                |(label, node)| super::super::response_codec::NeighborEntry {
                    label: label.as_str(),
                    node: node.as_str(),
                },
            )
            .collect();
        if let Some(ref m) = self.metrics {
            m.record_graph_traversal();
        }
        match super::super::response_codec::encode(&result) {
            Ok(payload) => self.response_with_payload(task, payload),
            Err(e) => {
                warn!(core = self.core_id, layer = DiagnosticLayer::WireShape.as_str(), error = %e, "graph neighbors serialization failed");
                self.response_error(
                    task,
                    ErrorCode::Internal {
                        detail: e.to_string(),
                    },
                )
            }
        }
    }

    pub(in crate::data::executor) fn execute_graph_neighbors_multi(
        &self,
        task: &ExecutionTask,
        tid: u64,
        args: GraphNeighborsMultiArgs<'_>,
    ) -> Response {
        let GraphNeighborsMultiArgs {
            node_ids,
            edge_label,
            direction,
            max_results,
            collection,
        } = args;
        debug!(
            core = self.core_id,
            tid,
            count = node_ids.len(),
            ?edge_label,
            ?direction,
            max_results,
            "graph neighbors multi"
        );
        let cap: usize = if max_results == 0 {
            usize::MAX
        } else {
            max_results as usize
        };
        let database_id = task.request.database_id.as_u64();
        let mut owned: Vec<(String, String, String)> =
            Vec::with_capacity(node_ids.len().min(cap) * 4);
        let mut truncated = false;
        if let Some(partition) = self.csr_partition(database_id, tid) {
            'outer: for raw_src in node_ids {
                let neighbors = match collection {
                    Some(collection) => partition.neighbors_in_collection(
                        raw_src,
                        edge_label.as_deref(),
                        direction,
                        collection,
                    ),
                    None => partition.neighbors(raw_src, edge_label.as_deref(), direction),
                };
                for (label, node) in neighbors {
                    if owned.len() >= cap {
                        truncated = true;
                        break 'outer;
                    }
                    owned.push((raw_src.clone(), label, node));
                }
            }
        }
        let entries: Vec<super::super::response_codec::NeighborMultiEntry> = owned
            .iter()
            .map(
                |(src, label, node)| super::super::response_codec::NeighborMultiEntry {
                    src: src.as_str(),
                    label: label.as_str(),
                    node: node.as_str(),
                },
            )
            .collect();
        if let Some(ref m) = self.metrics {
            m.record_graph_traversal();
        }
        match super::super::response_codec::encode(&entries) {
            Ok(payload) => {
                if truncated {
                    self.response_partial(task, payload)
                } else {
                    self.response_with_payload(task, payload)
                }
            }
            Err(e) => {
                warn!(
                    core = self.core_id,
                    layer = DiagnosticLayer::WireShape.as_str(),
                    error = %e,
                    "graph neighbors-multi serialization failed"
                );
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
