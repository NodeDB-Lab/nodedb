//! Graph operation dispatch.

use crate::bridge::envelope::Response;
use crate::bridge::physical_plan::GraphOp;

use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::task::ExecutionTask;

impl CoreLoop {
    pub(super) fn dispatch_graph(&mut self, task: &ExecutionTask, op: &GraphOp) -> Response {
        let tid = task.request.tenant_id.as_u32();
        match op {
            GraphOp::EdgePut {
                collection,
                src_id,
                label,
                dst_id,
                properties,
                src_surrogate,
                dst_surrogate,
            } => self.execute_edge_put(
                task,
                tid,
                collection,
                src_id,
                label,
                dst_id,
                properties,
                *src_surrogate,
                *dst_surrogate,
            ),

            GraphOp::EdgePutBatch { edges } => self.execute_edge_put_batch(task, tid, edges),

            GraphOp::EdgeDelete {
                collection,
                src_id,
                label,
                dst_id,
            } => self.execute_edge_delete(task, tid, collection, src_id, label, dst_id),

            GraphOp::EdgeDeleteBatch { edges } => self.execute_edge_delete_batch(task, tid, edges),

            GraphOp::Hop {
                start_nodes,
                edge_label,
                direction,
                depth,
                options: _,
                rls_filters: _,
            } => self.execute_graph_hop(task, tid, start_nodes, edge_label, *direction, *depth),

            GraphOp::Neighbors {
                node_id,
                edge_label,
                direction,
                rls_filters: _,
            } => self.execute_graph_neighbors(task, tid, node_id, edge_label, *direction),

            GraphOp::NeighborsMulti {
                node_ids,
                edge_label,
                direction,
                max_results,
                rls_filters: _,
            } => self.execute_graph_neighbors_multi(
                task,
                tid,
                node_ids,
                edge_label,
                *direction,
                *max_results,
            ),

            GraphOp::Path {
                src,
                dst,
                edge_label,
                max_depth,
                options: _,
                rls_filters: _,
            } => self.execute_graph_path(task, tid, src, dst, edge_label, *max_depth),

            GraphOp::Subgraph {
                start_nodes,
                edge_label,
                depth,
                options: _,
                rls_filters: _,
            } => self.execute_graph_subgraph(task, tid, start_nodes, edge_label, *depth),

            GraphOp::RagFusion {
                collection,
                query_vector,
                vector_top_k,
                edge_label,
                direction,
                expansion_depth,
                final_top_k,
                rrf_k,
                vector_field,
                options,
            } => self.execute_graph_rag_fusion(
                task,
                tid,
                collection,
                query_vector,
                *vector_top_k,
                edge_label,
                *direction,
                *expansion_depth,
                *final_top_k,
                *rrf_k,
                vector_field.as_str(),
                options.max_visited,
            ),

            GraphOp::Algo { algorithm, params } => {
                self.execute_graph_algo(task, tid, algorithm, params)
            }

            GraphOp::Match { query } => self.execute_graph_match(task, tid, query),

            GraphOp::SetNodeLabels { node_id, labels } => {
                let partition = self.csr_partition_mut(tid);
                for label in labels {
                    partition.add_node_label(node_id, label);
                }
                self.response_ok(task)
            }

            GraphOp::RemoveNodeLabels { node_id, labels } => {
                let partition = self.csr_partition_mut(tid);
                for label in labels {
                    partition.remove_node_label(node_id, label);
                }
                self.response_ok(task)
            }

            GraphOp::TemporalNeighbors {
                collection,
                node_id,
                edge_label,
                direction,
                system_as_of_ms,
                valid_at_ms,
                rls_filters: _,
            } => self.execute_graph_temporal_neighbors(
                task,
                super::super::handlers::graph_temporal::TemporalNeighborsParams {
                    tid,
                    collection,
                    node_id,
                    edge_label,
                    direction: *direction,
                    system_as_of_ms: *system_as_of_ms,
                    valid_at_ms: *valid_at_ms,
                },
            ),

            GraphOp::TemporalAlgorithm {
                algorithm,
                params,
                system_as_of_ms,
            } => self.execute_graph_temporal_algo(task, tid, algorithm, params, *system_as_of_ms),
        }
    }
}
