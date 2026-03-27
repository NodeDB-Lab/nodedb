//! Graph operation handlers: EdgePut, EdgeDelete, GraphHop, GraphNeighbors,
//! GraphPath, GraphSubgraph.

use tracing::{debug, warn};

use crate::bridge::envelope::{ErrorCode, Response};

use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::task::ExecutionTask;

impl CoreLoop {
    pub(in crate::data::executor) fn execute_edge_put(
        &mut self,
        task: &ExecutionTask,
        src_id: &str,
        label: &str,
        dst_id: &str,
        properties: &[u8],
    ) -> Response {
        debug!(core = self.core_id, %src_id, %label, %dst_id, "edge put");

        // Referential integrity: reject edges to nodes that were explicitly
        // deleted (present in the deleted_nodes set). Nodes are added to
        // deleted_nodes by PointDelete cascade. Edges between nodes that
        // simply don't exist yet are allowed — EdgePut implicitly creates
        // graph nodes in the CSR.
        if self.deleted_nodes.contains(src_id) {
            return self.response_error(
                task,
                ErrorCode::RejectedDanglingEdge {
                    missing_node: src_id.to_string(),
                },
            );
        }
        if self.deleted_nodes.contains(dst_id) {
            return self.response_error(
                task,
                ErrorCode::RejectedDanglingEdge {
                    missing_node: dst_id.to_string(),
                },
            );
        }

        match self.edge_store.put_edge(src_id, label, dst_id, properties) {
            Ok(()) => {
                let weight = crate::engine::graph::csr::extract_weight_from_properties(properties);
                if weight != 1.0 {
                    self.csr.add_edge_weighted(src_id, label, dst_id, weight);
                } else {
                    self.csr.add_edge(src_id, label, dst_id);
                }
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

    pub(in crate::data::executor) fn execute_edge_delete(
        &mut self,
        task: &ExecutionTask,
        src_id: &str,
        label: &str,
        dst_id: &str,
    ) -> Response {
        debug!(core = self.core_id, %src_id, %label, %dst_id, "edge delete");
        match self.edge_store.delete_edge(src_id, label, dst_id) {
            Ok(_) => {
                self.csr.remove_edge(src_id, label, dst_id);
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

    pub(in crate::data::executor) fn execute_graph_hop(
        &self,
        task: &ExecutionTask,
        start_nodes: &[String],
        edge_label: &Option<String>,
        direction: crate::engine::graph::edge_store::Direction,
        depth: usize,
    ) -> Response {
        debug!(
            core = self.core_id,
            ?start_nodes,
            ?edge_label,
            ?direction,
            depth,
            "graph hop"
        );
        let refs: Vec<&str> = start_nodes.iter().map(String::as_str).collect();
        let result = self
            .csr
            .traverse_bfs(&refs, edge_label.as_deref(), direction, depth);
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
        node_id: &str,
        edge_label: &Option<String>,
        direction: crate::engine::graph::edge_store::Direction,
    ) -> Response {
        debug!(core = self.core_id, %node_id, ?edge_label, ?direction, "graph neighbors");
        let neighbors = self
            .csr
            .neighbors(node_id, edge_label.as_deref(), direction);
        let result: Vec<_> = neighbors
            .iter()
            .map(
                |(label, node)| super::super::response_codec::NeighborEntry {
                    label: label.as_str(),
                    node: node.as_str(),
                },
            )
            .collect();
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

    pub(in crate::data::executor) fn execute_graph_path(
        &self,
        task: &ExecutionTask,
        src: &str,
        dst: &str,
        edge_label: &Option<String>,
        max_depth: usize,
    ) -> Response {
        debug!(core = self.core_id, %src, %dst, ?edge_label, max_depth, "graph path");
        match self
            .csr
            .shortest_path(src, dst, edge_label.as_deref(), max_depth)
        {
            Some(path) => match super::super::response_codec::encode(&path) {
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
            },
            None => self.response_error(task, ErrorCode::NotFound),
        }
    }

    pub(in crate::data::executor) fn execute_graph_subgraph(
        &self,
        task: &ExecutionTask,
        start_nodes: &[String],
        edge_label: &Option<String>,
        depth: usize,
    ) -> Response {
        debug!(
            core = self.core_id,
            ?start_nodes,
            ?edge_label,
            depth,
            "graph subgraph"
        );
        let refs: Vec<&str> = start_nodes.iter().map(String::as_str).collect();
        let edges = self.csr.subgraph(&refs, edge_label.as_deref(), depth);
        let result: Vec<_> = edges
            .iter()
            .map(|(s, l, d)| super::super::response_codec::SubgraphEdge {
                src: s.as_str(),
                label: l.as_str(),
                dst: d.as_str(),
            })
            .collect();
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
