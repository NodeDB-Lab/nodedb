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

use super::graph_edge_predicate;

/// Bundled arguments for [`CoreLoop::execute_graph_hop`].
pub(in crate::data::executor) struct GraphHopParams<'a> {
    pub tid: u64,
    pub start_nodes: &'a [String],
    /// Empty keeps every edge. Otherwise an edge with any listed label.
    pub edge_labels: &'a [String],
    pub direction: crate::engine::graph::edge_store::Direction,
    pub depth: usize,
    /// The walk's visit cap ([`CoreLoop::walk_visit_cap`]).
    pub max_visited: usize,
    pub frontier_bitmap: Option<&'a nodedb_types::SurrogateBitmap>,
}

/// Arguments for [`CoreLoop::execute_graph_neighbors_multi`].
pub(in crate::data::executor) struct GraphNeighborsMultiArgs<'a> {
    pub node_ids: &'a [String],
    /// Empty keeps every edge. Otherwise an edge with any listed label.
    pub edge_labels: &'a [String],
    pub direction: crate::engine::graph::edge_store::Direction,
    pub max_results: u32,
    /// Collection scope, or `None` for a label-only traversal.
    pub collection: Option<&'a str>,
    /// AND-ed edge-property predicate. Empty admits every edge.
    pub edge_predicate: &'a [nodedb_types::filter::MetadataFilter],
    /// Each row carries the crossed edge's property object.
    pub with_properties: bool,
}

/// One `NeighborsMulti` row: `(frontier node, label, neighbour, properties)`.
type NeighborRow = (String, String, String, Option<nodedb_types::NativeCell>);

/// The rows of one `NeighborsMulti` hop, and whether `max_results` cut it.
struct NeighborRows {
    rows: Vec<NeighborRow>,
    truncated: bool,
}

impl CoreLoop {
    /// The visit cap of a hop, subgraph or path walk: the plan's cap, bounded
    /// by this core's graph tuning. The cluster walk coordinators
    /// (`graph_dispatch::bfs`, `graph_dispatch::shortest_path`) bound it the
    /// same way, so a capped walk answers the same on one core and across a
    /// cluster.
    pub(in crate::data::executor) fn walk_visit_cap(
        &self,
        options: &crate::engine::graph::traversal_options::GraphTraversalOptions,
    ) -> usize {
        options.max_visited.min(self.graph_tuning.max_visited)
    }

    pub(in crate::data::executor) fn execute_graph_hop(
        &self,
        task: &ExecutionTask,
        params: GraphHopParams<'_>,
    ) -> Response {
        let GraphHopParams {
            tid,
            start_nodes,
            edge_labels,
            direction,
            depth,
            max_visited,
            frontier_bitmap,
        } = params;
        debug!(
            core = self.core_id,
            tid,
            ?start_nodes,
            ?edge_labels,
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
        let labels: Vec<&str> = edge_labels.iter().map(String::as_str).collect();
        let result: Vec<String> = match self.csr_partition(database_id, tid) {
            Some(partition) => partition.traverse_bfs(
                nodedb_graph::BfsParams {
                    start_nodes: &refs,
                    label_filter: &labels,
                    direction,
                    max_depth: depth,
                    max_visited,
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
                        .map(|p| p.neighbors(start, &labels, direction))
                        .unwrap_or_default()
                },
                starts: &refs,
                depth,
                database_id: task.request.database_id,
                tenant: TenantId::new(tid),
                label_filter: &labels,
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
        edge_labels: &[String],
        direction: crate::engine::graph::edge_store::Direction,
        collection: Option<&str>,
    ) -> Response {
        debug!(core = self.core_id, tid, %node_id, ?edge_labels, ?direction, "graph neighbors");
        let database_id = task.request.database_id.as_u64();
        let labels: Vec<&str> = edge_labels.iter().map(String::as_str).collect();
        // A named collection restricts the walk; unscoped `neighbors` would
        // silently span every collection's edges in the shared node space.
        let durable: Vec<(String, String)> = match self.csr_partition(database_id, tid) {
            Some(partition) => match collection {
                Some(collection) => {
                    partition.neighbors_in_collection(node_id, &labels, direction, collection)
                }
                None => partition.neighbors(node_id, &labels, direction),
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
            &labels,
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
        debug!(
            core = self.core_id,
            tid,
            count = args.node_ids.len(),
            edge_labels = ?args.edge_labels,
            direction = ?args.direction,
            max_results = args.max_results,
            predicate_terms = args.edge_predicate.len(),
            with_properties = args.with_properties,
            "graph neighbors multi"
        );
        let NeighborRows { rows, truncated } = match self.collect_neighbor_rows(task, tid, &args) {
            Ok(rows) => rows,
            Err(error) => return self.response_error(task, error),
        };
        let entries: Vec<super::super::response_codec::NeighborMultiEntry> = rows
            .iter()
            .map(|(src, label, node, properties)| {
                super::super::response_codec::NeighborMultiEntry {
                    src: src.as_str(),
                    label: label.as_str(),
                    node: node.as_str(),
                    properties: properties.as_ref(),
                }
            })
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

    /// The rows of one `NeighborsMulti` hop. A row counts against
    /// `max_results` only once the edge predicate admits it.
    ///
    /// A request with a `txn_id` merges that transaction's staged edge
    /// writes on this core: a staged tombstone drops an edge, a staged put
    /// adds one, and a staged put's property map is the map the predicate
    /// tests and the row returns.
    fn collect_neighbor_rows(
        &self,
        task: &ExecutionTask,
        tid: u64,
        args: &GraphNeighborsMultiArgs<'_>,
    ) -> crate::Result<NeighborRows> {
        let cap: usize = if args.max_results == 0 {
            usize::MAX
        } else {
            args.max_results as usize
        };
        let database_id = task.request.database_id;
        let labels: Vec<&str> = args.edge_labels.iter().map(String::as_str).collect();
        // Read-your-own-writes refreshes the lease (see the overlay reaper).
        if let Some(txn_id) = task.request.txn_id {
            self.touch_overlay(txn_id);
        }
        let overlay = task
            .request
            .txn_id
            .and_then(|txn_id| self.graph_txn_overlays.get(&txn_id));
        let mut edge_properties = graph_edge_predicate::HopEdgeProperties::open(
            &self.edge_store,
            graph_edge_predicate::HopPropertyScope {
                database: database_id,
                tenant: TenantId::new(tid),
                collection: args.collection,
                filters: args.edge_predicate,
                with_properties: args.with_properties,
            },
        )?;
        let mut out = NeighborRows {
            rows: Vec::with_capacity(args.node_ids.len().min(cap) * 4),
            truncated: false,
        };
        let partition = self.csr_partition(database_id.as_u64(), tid);
        let durable = |node: &str, direction| match (partition, args.collection) {
            (Some(partition), Some(collection)) => {
                partition.neighbors_in_collection(node, &labels, direction, collection)
            }
            (Some(partition), None) => partition.neighbors(node, &labels, direction),
            (None, _) => Vec::new(),
        };
        let oriented = edge_properties.is_some() || overlay.is_some();
        let passes = match graph_edge_predicate::hop_passes(args.direction, oriented) {
            graph_edge_predicate::HopPasses::Oriented(passes) => passes,
            // No predicate, no properties and no staged writes: each row is
            // the durable edge as the CSR returns it.
            graph_edge_predicate::HopPasses::Unoriented => {
                for raw_src in args.node_ids {
                    for (label, node) in durable(raw_src, args.direction) {
                        if out.rows.len() >= cap {
                            out.truncated = true;
                            return Ok(out);
                        }
                        out.rows.push((raw_src.clone(), label, node, None));
                    }
                }
                return Ok(out);
            }
        };
        let staged_scope = graph_txn_merge::StagedHopScope {
            database_id,
            tenant: TenantId::new(tid),
            collection: args.collection,
            edge_labels: &labels,
        };
        for raw_src in args.node_ids {
            for &pass in passes {
                let durable_rows = durable(raw_src, pass.direction());
                let neighbors: Vec<graph_txn_merge::StagedNeighbor<'_>> = match overlay {
                    Some(overlay) => graph_txn_merge::merge_staged_hop_pass(
                        overlay,
                        &staged_scope,
                        raw_src,
                        pass,
                        durable_rows,
                    ),
                    None => durable_rows
                        .into_iter()
                        .map(|(label, node)| (label, node, None))
                        .collect(),
                };
                for (label, node, staged) in neighbors {
                    let properties = match edge_properties.as_mut() {
                        None => None,
                        Some(hop) => {
                            let (src, dst) = pass.endpoints(raw_src, &node);
                            match hop.cross(src, &label, dst, staged)? {
                                graph_edge_predicate::EdgeCrossing::Rejected => continue,
                                graph_edge_predicate::EdgeCrossing::Admitted(properties) => {
                                    properties.map(nodedb_types::NativeCell)
                                }
                            }
                        }
                    };
                    if out.rows.len() >= cap {
                        out.truncated = true;
                        return Ok(out);
                    }
                    out.rows.push((raw_src.clone(), label, node, properties));
                }
            }
        }
        Ok(out)
    }
}
