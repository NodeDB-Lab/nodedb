//! Graph operation handlers: EdgePut, EdgeDelete, GraphHop, GraphNeighbors,
//! GraphPath, GraphSubgraph.
//!
//! ## Scoping at this layer
//!
//! The CSR index is partitioned structurally by tenant (see
//! `ShardedCsrIndex`). Handlers resolve the caller's partition once
//! via `self.csr_partition(_mut)(tid)` and then address node ids in
//! their raw, user-visible form — no `<tid>:` prefix, no post-hoc
//! stripping on the way out.
//!
//! `EdgeStore` now takes `(TenantId, name)` tuples and owns its
//! tenant encoding internally. Handlers pass raw user-visible names
//! throughout: to the CSR partition, to the edge store, and to the
//! `deleted_nodes` dangling-edge tracker via `mark_node_deleted` /
//! `is_node_deleted`. No `scoped_node()` wrapping at this layer.

use tracing::{debug, warn};

use crate::bridge::envelope::{ErrorCode, Response};

use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::task::ExecutionTask;
use crate::types::TenantId;

impl CoreLoop {
    #[allow(clippy::too_many_arguments)]
    pub(in crate::data::executor) fn execute_edge_put(
        &mut self,
        task: &ExecutionTask,
        tid: u32,
        collection: &str,
        src_id: &str,
        label: &str,
        dst_id: &str,
        properties: &[u8],
        src_surrogate: nodedb_types::Surrogate,
        dst_surrogate: nodedb_types::Surrogate,
    ) -> Response {
        debug!(core = self.core_id, tid, %collection, %src_id, %label, %dst_id, "edge put");

        if self.is_node_deleted(tid, src_id) {
            return self.response_error(
                task,
                ErrorCode::RejectedDanglingEdge {
                    missing_node: src_id.to_string(),
                },
            );
        }
        if self.is_node_deleted(tid, dst_id) {
            return self.response_error(
                task,
                ErrorCode::RejectedDanglingEdge {
                    missing_node: dst_id.to_string(),
                },
            );
        }

        let ord = self.hlc.next_ordinal();
        let valid_from_ms = nodedb_types::ordinal_to_ms(ord);
        use crate::engine::graph::edge_store::EdgeRef;
        match self.edge_store.put_edge_versioned(
            EdgeRef::new(TenantId::new(tid), collection, src_id, label, dst_id),
            properties,
            ord,
            valid_from_ms,
            i64::MAX,
        ) {
            Ok(()) => {
                let weight = crate::engine::graph::csr::extract_weight_from_properties(properties);
                let partition = self.csr_partition_mut(tid);
                let csr_result = if weight != 1.0 {
                    partition.add_edge_weighted(src_id, label, dst_id, weight)
                } else {
                    partition.add_edge(src_id, label, dst_id)
                };
                match csr_result {
                    Ok(()) => {
                        // Populate the per-node surrogates so future bitmap-gated
                        // traversals can check membership without a separate lookup.
                        partition.set_node_surrogate(src_id, src_surrogate);
                        partition.set_node_surrogate(dst_id, dst_surrogate);
                        self.checkpoint_coordinator.mark_dirty("sparse", 1);
                        self.response_ok(task)
                    }
                    Err(e) => self.response_error(
                        task,
                        ErrorCode::Internal {
                            detail: e.to_string(),
                        },
                    ),
                }
            }
            Err(e) => self.response_error(
                task,
                ErrorCode::Internal {
                    detail: e.to_string(),
                },
            ),
        }
    }

    /// Apply a batched edge insert in a single SPSC round-trip.
    pub(in crate::data::executor) fn execute_edge_put_batch(
        &mut self,
        task: &ExecutionTask,
        tid: u32,
        edges: &[crate::bridge::physical_plan::BatchEdge],
    ) -> Response {
        debug!(core = self.core_id, count = edges.len(), "edge put batch");
        for (idx, edge) in edges.iter().enumerate() {
            if self.is_node_deleted(tid, &edge.src_id) {
                return self.response_error(
                    task,
                    ErrorCode::RejectedDanglingEdge {
                        missing_node: edge.src_id.clone(),
                    },
                );
            }
            if self.is_node_deleted(tid, &edge.dst_id) {
                return self.response_error(
                    task,
                    ErrorCode::RejectedDanglingEdge {
                        missing_node: edge.dst_id.clone(),
                    },
                );
            }
            let ord = self.hlc.next_ordinal();
            let valid_from_ms = nodedb_types::ordinal_to_ms(ord);
            use crate::engine::graph::edge_store::EdgeRef;
            match self.edge_store.put_edge_versioned(
                EdgeRef::new(
                    TenantId::new(tid),
                    &edge.collection,
                    &edge.src_id,
                    &edge.label,
                    &edge.dst_id,
                ),
                &[],
                ord,
                valid_from_ms,
                i64::MAX,
            ) {
                Ok(()) => {
                    let partition = self.csr_partition_mut(tid);
                    if let Err(e) = partition.add_edge(&edge.src_id, &edge.label, &edge.dst_id) {
                        return self.response_error(
                            task,
                            ErrorCode::Internal {
                                detail: format!("edge {idx} (label interning): {e}"),
                            },
                        );
                    }
                    partition.set_node_surrogate(&edge.src_id, edge.src_surrogate);
                    partition.set_node_surrogate(&edge.dst_id, edge.dst_surrogate);
                }
                Err(e) => {
                    return self.response_error(
                        task,
                        ErrorCode::Internal {
                            detail: format!("edge {idx}: {e}"),
                        },
                    );
                }
            }
        }
        if !edges.is_empty() {
            self.checkpoint_coordinator
                .mark_dirty("sparse", edges.len());
        }
        self.response_ok(task)
    }

    /// Apply a batched edge delete in a single SPSC round-trip.
    pub(in crate::data::executor) fn execute_edge_delete_batch(
        &mut self,
        task: &ExecutionTask,
        tid: u32,
        edges: &[crate::bridge::physical_plan::BatchEdge],
    ) -> Response {
        debug!(
            core = self.core_id,
            count = edges.len(),
            "edge delete batch"
        );
        for edge in edges {
            let ord = self.hlc.next_ordinal();
            use crate::engine::graph::edge_store::EdgeRef;
            let _ = self.edge_store.soft_delete_edge(
                EdgeRef::new(
                    TenantId::new(tid),
                    &edge.collection,
                    &edge.src_id,
                    &edge.label,
                    &edge.dst_id,
                ),
                ord,
            );
            let partition = self.csr_partition_mut(tid);
            partition.remove_edge(&edge.src_id, &edge.label, &edge.dst_id);
        }
        if !edges.is_empty() {
            self.checkpoint_coordinator
                .mark_dirty("sparse", edges.len());
        }
        self.response_ok(task)
    }

    pub(in crate::data::executor) fn execute_edge_delete(
        &mut self,
        task: &ExecutionTask,
        tid: u32,
        collection: &str,
        src_id: &str,
        label: &str,
        dst_id: &str,
    ) -> Response {
        debug!(core = self.core_id, tid, %collection, %src_id, %label, %dst_id, "edge delete");
        let ord = self.hlc.next_ordinal();
        use crate::engine::graph::edge_store::EdgeRef;
        match self.edge_store.soft_delete_edge(
            EdgeRef::new(TenantId::new(tid), collection, src_id, label, dst_id),
            ord,
        ) {
            Ok(_) => {
                let partition = self.csr_partition_mut(tid);
                partition.remove_edge(src_id, label, dst_id);
                self.checkpoint_coordinator.mark_dirty("sparse", 1);
                self.response_ok(task)
            }
            Err(e) => self.response_error(
                task,
                ErrorCode::Internal {
                    detail: e.to_string(),
                },
            ),
        }
    }

    #[allow(clippy::too_many_arguments)]
    pub(in crate::data::executor) fn execute_graph_hop(
        &self,
        task: &ExecutionTask,
        tid: u32,
        start_nodes: &[String],
        edge_label: &Option<String>,
        direction: crate::engine::graph::edge_store::Direction,
        depth: usize,
        frontier_bitmap: Option<&nodedb_types::SurrogateBitmap>,
    ) -> Response {
        debug!(
            core = self.core_id,
            tid,
            ?start_nodes,
            ?edge_label,
            ?direction,
            depth,
            "graph hop"
        );
        let depth = depth.min(crate::engine::graph::traversal_options::MAX_GRAPH_TRAVERSAL_DEPTH);
        let refs: Vec<&str> = start_nodes.iter().map(String::as_str).collect();
        let result: Vec<String> = match self.csr_partition(tid) {
            Some(partition) => partition.traverse_bfs(
                &refs,
                edge_label.as_deref(),
                direction,
                depth,
                self.graph_tuning.max_visited,
                frontier_bitmap,
            ),
            None => Vec::new(),
        };
        if let Some(ref m) = self.metrics {
            m.record_graph_traversal();
        }
        match super::super::response_codec::encode(&result) {
            Ok(payload) => self.response_with_payload(task, payload),
            Err(e) => {
                warn!(core = self.core_id, error = %e, "graph hop serialization failed");
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
        tid: u32,
        node_id: &str,
        edge_label: &Option<String>,
        direction: crate::engine::graph::edge_store::Direction,
    ) -> Response {
        debug!(core = self.core_id, tid, %node_id, ?edge_label, ?direction, "graph neighbors");
        let neighbors: Vec<(String, String)> = match self.csr_partition(tid) {
            Some(partition) => partition.neighbors(node_id, edge_label.as_deref(), direction),
            None => Vec::new(),
        };
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
                warn!(core = self.core_id, error = %e, "graph neighbors serialization failed");
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
        tid: u32,
        node_ids: &[String],
        edge_label: &Option<String>,
        direction: crate::engine::graph::edge_store::Direction,
        max_results: u32,
    ) -> Response {
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
        let mut owned: Vec<(String, String, String)> =
            Vec::with_capacity(node_ids.len().min(cap) * 4);
        let mut truncated = false;
        if let Some(partition) = self.csr_partition(tid) {
            'outer: for raw_src in node_ids {
                let neighbors = partition.neighbors(raw_src, edge_label.as_deref(), direction);
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

    #[allow(clippy::too_many_arguments)]
    pub(in crate::data::executor) fn execute_graph_path(
        &self,
        task: &ExecutionTask,
        tid: u32,
        src: &str,
        dst: &str,
        edge_label: &Option<String>,
        max_depth: usize,
        frontier_bitmap: Option<&nodedb_types::SurrogateBitmap>,
    ) -> Response {
        let max_depth =
            max_depth.min(crate::engine::graph::traversal_options::MAX_GRAPH_TRAVERSAL_DEPTH);
        debug!(core = self.core_id, tid, %src, %dst, ?edge_label, max_depth, "graph path");
        let path = match self.csr_partition(tid) {
            Some(partition) => partition.shortest_path(
                src,
                dst,
                edge_label.as_deref(),
                max_depth,
                self.graph_tuning.max_visited,
                frontier_bitmap,
            ),
            None => None,
        };
        match path {
            Some(path) => {
                if let Some(ref m) = self.metrics {
                    m.record_graph_traversal();
                }
                match super::super::response_codec::encode(&path) {
                    Ok(payload) => self.response_with_payload(task, payload),
                    Err(e) => {
                        warn!(core = self.core_id, error = %e, "graph path serialization failed");
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
        tid: u32,
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
        let depth = depth.min(crate::engine::graph::traversal_options::MAX_GRAPH_TRAVERSAL_DEPTH);
        let refs: Vec<&str> = start_nodes.iter().map(String::as_str).collect();
        let edges: Vec<(String, String, String)> = match self.csr_partition(tid) {
            Some(partition) => partition.subgraph(
                &refs,
                edge_label.as_deref(),
                depth,
                self.graph_tuning.max_visited,
            ),
            None => Vec::new(),
        };
        let result: Vec<_> = edges
            .iter()
            .map(|(s, l, d)| super::super::response_codec::SubgraphEdge {
                src: s.as_str(),
                label: l.as_str(),
                dst: d.as_str(),
            })
            .collect();
        if let Some(ref m) = self.metrics {
            m.record_graph_traversal();
        }
        match super::super::response_codec::encode(&result) {
            Ok(payload) => self.response_with_payload(task, payload),
            Err(e) => {
                warn!(core = self.core_id, error = %e, "graph subgraph serialization failed");
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
