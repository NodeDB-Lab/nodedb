// SPDX-License-Identifier: BUSL-1.1

//! Graph operation routing: one match arm per `GraphOp`.

use crate::bridge::envelope::Response;
use nodedb_mem;
use nodedb_physical::physical_plan::GraphOp;

use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::task::ExecutionTask;

impl CoreLoop {
    pub(in crate::data::executor::dispatch) fn dispatch_graph(
        &mut self,
        task: &ExecutionTask,
        op: &GraphOp,
    ) -> Response {
        let tid = task.request.tenant_id.as_u64();
        let database_id = task.request.database_id.as_u64();
        // Pressure guard for write operations.
        let is_write = matches!(
            op,
            GraphOp::EdgePut { .. }
                | GraphOp::EdgePutBatch { .. }
                | GraphOp::EdgeDelete { .. }
                | GraphOp::EdgeDeleteBatch { .. }
                | GraphOp::TruncateEdges { .. }
        );
        if is_write && let Some(r) = self.check_engine_pressure(task, nodedb_mem::EngineId::Graph) {
            return r;
        }
        match op {
            GraphOp::EdgePut {
                collection,
                src_id,
                label,
                dst_id,
                properties,
                src_surrogate,
                dst_surrogate,
                valid_from_ms,
                valid_until_ms,
            } => self.execute_edge_put(
                task,
                crate::data::executor::handlers::graph::EdgePutParams {
                    tid,
                    collection: collection.as_str(),
                    src_id,
                    label,
                    dst_id,
                    properties,
                    src_surrogate: *src_surrogate,
                    dst_surrogate: *dst_surrogate,
                    valid_from_ms: *valid_from_ms,
                    valid_until_ms: *valid_until_ms,
                },
            ),

            GraphOp::EdgePutBatch { edges } => self.execute_edge_put_batch(task, tid, edges),

            GraphOp::EdgeDelete {
                collection,
                src_id,
                label,
                dst_id,
                src_surrogate,
                dst_surrogate,
                rls_write_check,
            } => self.execute_edge_delete(
                task,
                crate::data::executor::handlers::graph::EdgeDeleteParams {
                    tid,
                    collection: collection.as_str(),
                    src_id,
                    label,
                    dst_id,
                    src_surrogate: *src_surrogate,
                    dst_surrogate: *dst_surrogate,
                    rls_write_check,
                },
            ),

            GraphOp::ResolveEdgeDelete(inner) => self.execute_edge_delete_resolve(task, tid, inner),

            GraphOp::EdgeDeleteBatch { edges } => self.execute_edge_delete_batch(task, tid, edges),

            GraphOp::Hop {
                // A live walk's scope is enforced at the hop level: it expands
                // through `NeighborsMulti`, which carries the same collection.
                // A historical walk reads the collection's CSR at its cut.
                collection,
                start_nodes,
                edge_labels,
                direction,
                depth,
                options,
                rls_filters: _,
                frontier_bitmap,
                scope,
            } => {
                let collection = collection.as_ref().map(|c| c.as_str());
                if let Err(e) = self.prepare_csr_view(database_id, tid, collection, scope) {
                    return self.response_error(task, e);
                }
                self.execute_graph_hop(
                    task,
                    crate::data::executor::handlers::graph::GraphHopParams {
                        tid,
                        start_nodes,
                        edge_labels,
                        direction: *direction,
                        depth: *depth,
                        max_visited: self.walk_visit_cap(options),
                        frontier_bitmap: frontier_bitmap.as_ref(),
                        collection,
                        scope,
                    },
                )
            }

            GraphOp::Neighbors {
                collection,
                node_id,
                edge_labels,
                direction,
                rls_filters: _,
                scope,
            } => {
                let collection = collection.as_ref().map(|c| c.as_str());
                if let Err(e) = self.prepare_csr_view(database_id, tid, collection, scope) {
                    return self.response_error(task, e);
                }
                self.execute_graph_neighbors(
                    task,
                    tid,
                    crate::data::executor::handlers::graph::GraphNeighborsArgs {
                        node_id,
                        edge_labels,
                        direction: *direction,
                        collection,
                        scope,
                    },
                )
            }

            GraphOp::NeighborsMulti {
                collection,
                node_ids,
                edge_labels,
                direction,
                max_results,
                rls_filters: _,
                edge_predicate,
                with_properties,
                scope,
            } => {
                let collection = collection.as_ref().map(|c| c.as_str());
                if let Err(e) = self.prepare_csr_view(database_id, tid, collection, scope) {
                    return self.response_error(task, e);
                }
                self.execute_graph_neighbors_multi(
                    task,
                    tid,
                    crate::data::executor::handlers::graph::GraphNeighborsMultiArgs {
                        node_ids,
                        edge_labels,
                        direction: *direction,
                        max_results: *max_results,
                        collection,
                        edge_predicate,
                        with_properties: *with_properties,
                        scope,
                    },
                )
            }

            GraphOp::Path {
                // A live walk's scope is enforced at the hop level: it expands
                // through `NeighborsMulti`, which carries the same collection.
                // A historical walk reads the collection's CSR at its cut.
                collection,
                src,
                dst,
                edge_labels,
                max_depth,
                options,
                rls_filters: _,
                frontier_bitmap,
                scope,
            } => {
                let collection = collection.as_ref().map(|c| c.as_str());
                if let Err(e) = self.prepare_csr_view(database_id, tid, collection, scope) {
                    return self.response_error(task, e);
                }
                self.execute_graph_path(
                    task,
                    crate::data::executor::handlers::graph::graph_traversal::GraphPathParams {
                        tid,
                        src,
                        dst,
                        edge_labels,
                        max_depth: *max_depth,
                        max_visited: self.walk_visit_cap(options),
                        frontier_bitmap: frontier_bitmap.as_ref(),
                        collection,
                        scope,
                    },
                )
            }

            GraphOp::Subgraph {
                // A live walk's scope is enforced at the hop level: it expands
                // through `NeighborsMulti`, which carries the same collection.
                // A historical walk reads the collection's CSR at its cut.
                collection,
                start_nodes,
                edge_labels,
                depth,
                options,
                rls_filters: _,
                scope,
            } => {
                let collection = collection.as_ref().map(|c| c.as_str());
                if let Err(e) = self.prepare_csr_view(database_id, tid, collection, scope) {
                    return self.response_error(task, e);
                }
                self.execute_graph_subgraph(
                    task,
                    crate::data::executor::handlers::graph::graph_traversal::GraphSubgraphParams {
                        tid,
                        start_nodes,
                        edge_labels,
                        depth: *depth,
                        max_visited: self.walk_visit_cap(options),
                        collection,
                        scope,
                    },
                )
            }

            GraphOp::RagFusion { .. } => self.dispatch_rag_fusion(task, tid, op),

            GraphOp::Algo {
                algorithm,
                params,
                stage,
                scope,
            } => {
                if matches!(stage, nodedb_physical::physical_plan::AlgoStage::Local)
                    && let Err(e) = self.prepare_algo_view(task, tid, params, scope)
                {
                    return self.response_error(task, e);
                }
                self.execute_graph_algo(task, tid, algorithm, params, stage, scope)
            }

            GraphOp::Match {
                query,
                frontier_bitmap,
                cluster_mode,
                scope,
            } => self.execute_graph_match(
                task,
                tid,
                query,
                frontier_bitmap.as_ref(),
                *cluster_mode,
                scope,
            ),

            GraphOp::MatchContinuation {
                query,
                resume_triple_idx,
                partial_row,
                source_node,
                source_binding,
                scope,
            } => self.execute_graph_match_continuation(
                task,
                crate::data::executor::handlers::graph_match::GraphMatchContinuationParams {
                    tid,
                    query_bytes: query,
                    resume_triple_idx: *resume_triple_idx,
                    partial_row_bytes: partial_row,
                    source_node,
                    source_binding,
                    scope,
                },
            ),

            GraphOp::MatchVarLenResume {
                query,
                resume,
                scope,
            } => self.execute_graph_match_varlen_resume(task, tid, query, resume, scope),

            GraphOp::SetNodeLabels { node_id, labels } => {
                self.dispatch_set_node_labels(task, database_id, tid, node_id, labels)
            }

            GraphOp::RemoveNodeLabels { node_id, labels } => {
                self.dispatch_remove_node_labels(task, database_id, tid, node_id, labels)
            }

            GraphOp::TemporalNeighbors {
                collection,
                node_id,
                edge_label,
                direction,
                scope,
                rls_filters: _,
            } => self.execute_graph_temporal_neighbors(
                task,
                crate::data::executor::handlers::graph_temporal::TemporalNeighborsParams {
                    tid,
                    collection: collection.as_str(),
                    node_id,
                    edge_label,
                    direction: *direction,
                    scope,
                },
            ),

            GraphOp::BspSuperstep(plan) => {
                if let Some(cut) = plan.system_as_of
                    && let Err(e) = self.prepare_algo_view(
                        task,
                        tid,
                        &plan.params,
                        &superstep_scope(cut, plan.valid_at_ms),
                    )
                {
                    return self.response_error(task, e);
                }
                self.execute_bsp_superstep(
                    task,
                    tid,
                    crate::data::executor::handlers::graph_bsp::BspSuperstepArgs {
                        algorithm: &plan.algorithm,
                        params: &plan.params,
                        superstep: plan.superstep,
                        global_n: plan.global_n,
                        owned_vshards: &plan.owned_vshards,
                        incoming_contributions: &plan.incoming_contributions,
                        rank_seed: &plan.rank_seed,
                        global_dangling: plan.global_dangling,
                        personalization_sum: plan.personalization_sum,
                        system_as_of: plan.system_as_of,
                        valid_at_ms: plan.valid_at_ms,
                    },
                )
            }

            GraphOp::WccSuperstep(plan) => {
                if let Some(cut) = plan.system_as_of
                    && let Err(e) = self.prepare_algo_view(
                        task,
                        tid,
                        &plan.params,
                        &superstep_scope(cut, plan.valid_at_ms),
                    )
                {
                    return self.response_error(task, e);
                }
                self.execute_wcc_superstep(
                    task,
                    tid,
                    &plan.params,
                    &plan.owned_vshards,
                    plan.system_as_of,
                    plan.valid_at_ms,
                )
            }

            GraphOp::Stats { collection, as_of } => {
                self.execute_graph_stats(task, tid, collection.as_ref().map(|c| c.as_str()), *as_of)
            }

            GraphOp::NodeEdgeGuard {
                collection,
                node_id,
                expected,
            } => self.execute_node_edge_guard(task, tid, collection.as_str(), node_id, expected),

            GraphOp::NodePresenceRead {
                collection, ids, ..
            } => self.execute_node_presence_read(task, collection.as_str(), ids),

            // A presence guard checks the state a Calvin transaction commits
            // on. Outside one it has nothing to guard.
            GraphOp::NodePresenceGuard { .. } => self.response_error(
                task,
                crate::Error::Internal {
                    detail: "a CRDT delete's presence guard runs only inside a Calvin transaction"
                        .into(),
                },
            ),

            // A TRUNCATE's edge share resolves inside a Calvin transaction,
            // at the transaction's ordinal. Outside one it has no cut.
            GraphOp::TruncateEdges { .. } => self.response_error(
                task,
                crate::Error::Internal {
                    detail: "a TRUNCATE edge share runs only inside a Calvin transaction".into(),
                },
            ),
        }
    }

    /// Build or reuse the historical CSR an algorithm stage at `scope` reads
    /// through the cache. A label-filtered or live stage builds its own.
    fn prepare_algo_view(
        &mut self,
        task: &ExecutionTask,
        tid: u64,
        params: &nodedb_graph::AlgoParams,
        scope: &nodedb_physical::physical_plan::GraphReadScope,
    ) -> crate::Result<()> {
        match crate::data::executor::handlers::graph_algo_csr::algo_view_collection(
            task.request.database_id,
            params,
            scope,
        ) {
            Some(collection) => self.prepare_csr_view(
                task.request.database_id.as_u64(),
                tid,
                Some(&collection),
                scope,
            ),
            None => Ok(()),
        }
    }
}

/// The scope a BSP or WCC superstep reads: the run's pinned cut.
fn superstep_scope(
    cut: i64,
    valid_at_ms: Option<i64>,
) -> nodedb_physical::physical_plan::GraphReadScope {
    nodedb_physical::physical_plan::GraphReadScope::at_installed_cut(cut, valid_at_ms)
}
