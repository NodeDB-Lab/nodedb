//! Graph operation handlers: EdgePut, EdgeDelete, GraphHop, GraphNeighbors,
//! GraphPath, GraphSubgraph.

use tracing::{debug, warn};

use crate::bridge::envelope::{ErrorCode, Response};

use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::scoping::{scoped_node, unscoped};
use crate::data::executor::task::ExecutionTask;

impl CoreLoop {
    pub(in crate::data::executor) fn execute_edge_put(
        &mut self,
        task: &ExecutionTask,
        tid: u32,
        src_id: &str,
        label: &str,
        dst_id: &str,
        properties: &[u8],
    ) -> Response {
        let scoped_src = scoped_node(tid, src_id);
        let scoped_dst = scoped_node(tid, dst_id);
        debug!(core = self.core_id, %scoped_src, %label, %scoped_dst, "edge put");

        if self.deleted_nodes.contains(&scoped_src) {
            return self.response_error(
                task,
                ErrorCode::RejectedDanglingEdge {
                    missing_node: src_id.to_string(),
                },
            );
        }
        if self.deleted_nodes.contains(&scoped_dst) {
            return self.response_error(
                task,
                ErrorCode::RejectedDanglingEdge {
                    missing_node: dst_id.to_string(),
                },
            );
        }

        match self
            .edge_store
            .put_edge(&scoped_src, label, &scoped_dst, properties)
        {
            Ok(()) => {
                let weight = crate::engine::graph::csr::extract_weight_from_properties(properties);
                if weight != 1.0 {
                    self.csr
                        .add_edge_weighted(&scoped_src, label, &scoped_dst, weight);
                } else {
                    self.csr.add_edge(&scoped_src, label, &scoped_dst);
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
        tid: u32,
        src_id: &str,
        label: &str,
        dst_id: &str,
    ) -> Response {
        let scoped_src = scoped_node(tid, src_id);
        let scoped_dst = scoped_node(tid, dst_id);
        debug!(core = self.core_id, %scoped_src, %label, %scoped_dst, "edge delete");
        match self.edge_store.delete_edge(&scoped_src, label, &scoped_dst) {
            Ok(_) => {
                self.csr.remove_edge(&scoped_src, label, &scoped_dst);
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
        tid: u32,
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
        let depth = depth.min(crate::engine::graph::traversal_options::MAX_GRAPH_TRAVERSAL_DEPTH);
        let scoped: Vec<String> = start_nodes.iter().map(|n| scoped_node(tid, n)).collect();
        let refs: Vec<&str> = scoped.iter().map(String::as_str).collect();
        let result = self.csr.traverse_bfs(
            &refs,
            edge_label.as_deref(),
            direction,
            depth,
            self.graph_tuning.max_visited,
        );
        // Unscope node_ids in BFS result before returning to client.
        let unscoped: Vec<String> = result.iter().map(|n| unscoped(n).to_string()).collect();
        if let Some(ref m) = self.metrics {
            m.record_graph_traversal();
        }
        match super::super::response_codec::encode(&unscoped) {
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
        let scoped_id = scoped_node(tid, node_id);
        debug!(core = self.core_id, %scoped_id, ?edge_label, ?direction, "graph neighbors");
        let neighbors = self
            .csr
            .neighbors(&scoped_id, edge_label.as_deref(), direction);
        let result: Vec<_> = neighbors
            .iter()
            .map(
                |(label, node)| super::super::response_codec::NeighborEntry {
                    label: label.as_str(),
                    node: unscoped(node),
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

    pub(in crate::data::executor) fn execute_graph_path(
        &self,
        task: &ExecutionTask,
        tid: u32,
        src: &str,
        dst: &str,
        edge_label: &Option<String>,
        max_depth: usize,
    ) -> Response {
        let max_depth =
            max_depth.min(crate::engine::graph::traversal_options::MAX_GRAPH_TRAVERSAL_DEPTH);
        let scoped_src = scoped_node(tid, src);
        let scoped_dst = scoped_node(tid, dst);
        debug!(core = self.core_id, %scoped_src, %scoped_dst, ?edge_label, max_depth, "graph path");
        match self.csr.shortest_path(
            &scoped_src,
            &scoped_dst,
            edge_label.as_deref(),
            max_depth,
            self.graph_tuning.max_visited,
        ) {
            Some(path) => {
                let unscoped: Vec<String> = path.iter().map(|n| unscoped(n).to_string()).collect();
                if let Some(ref m) = self.metrics {
                    m.record_graph_traversal();
                }
                match super::super::response_codec::encode(&unscoped) {
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
            ?start_nodes,
            ?edge_label,
            depth,
            "graph subgraph"
        );
        let depth = depth.min(crate::engine::graph::traversal_options::MAX_GRAPH_TRAVERSAL_DEPTH);
        let scoped: Vec<String> = start_nodes.iter().map(|n| scoped_node(tid, n)).collect();
        let refs: Vec<&str> = scoped.iter().map(String::as_str).collect();
        let edges = self.csr.subgraph(
            &refs,
            edge_label.as_deref(),
            depth,
            self.graph_tuning.max_visited,
        );
        let result: Vec<_> = edges
            .iter()
            .map(|(s, l, d)| super::super::response_codec::SubgraphEdge {
                src: unscoped(s),
                label: l.as_str(),
                dst: unscoped(d),
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
