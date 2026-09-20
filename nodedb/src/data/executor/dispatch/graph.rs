// SPDX-License-Identifier: BUSL-1.1

//! Graph operation dispatch.

use crate::bridge::envelope::Response;
use nodedb_mem;
use nodedb_physical::physical_plan::GraphOp;

use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::task::ExecutionTask;
use nodedb_types::SystemTimeScope;

/// Resolve a graph temporal op's system-time selection to a point-in-time
/// cutoff. `AllVersions` (audit log) is not yet supported on the graph
/// engine and surfaces a typed `Unsupported` error.
fn graph_system_as_of(
    system_time: &SystemTimeScope,
) -> Result<Option<i64>, crate::bridge::envelope::ErrorCode> {
    match system_time {
        SystemTimeScope::Current => Ok(None),
        SystemTimeScope::AsOf(ms) => Ok(Some(*ms)),
        SystemTimeScope::AllVersions => Err(crate::bridge::envelope::ErrorCode::Unsupported {
            detail: "AS OF SYSTEM TIME NULL (all-versions) is not yet supported on the \
                     graph engine"
                .into(),
        }),
    }
}

impl CoreLoop {
    pub(super) fn dispatch_graph(&mut self, task: &ExecutionTask, op: &GraphOp) -> Response {
        let tid = task.request.tenant_id.as_u64();
        let database_id = task.request.database_id.as_u64();
        // Pressure guard for write operations.
        let is_write = matches!(
            op,
            GraphOp::EdgePut { .. }
                | GraphOp::EdgePutBatch { .. }
                | GraphOp::EdgeDelete { .. }
                | GraphOp::EdgeDeleteBatch { .. }
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
                },
            ),

            GraphOp::EdgePutBatch { edges } => self.execute_edge_put_batch(task, tid, edges),

            GraphOp::EdgeDelete {
                collection,
                src_id,
                label,
                dst_id,
                rls_write_check,
                ..
            } => self.execute_edge_delete(
                task,
                crate::data::executor::handlers::graph::EdgeDeleteParams {
                    tid,
                    collection: collection.as_str(),
                    src_id,
                    label,
                    dst_id,
                    rls_write_check,
                },
            ),

            GraphOp::ResolveEdgeDelete(inner) => self.execute_edge_delete_resolve(task, tid, inner),

            GraphOp::EdgeDeleteBatch { edges } => self.execute_edge_delete_batch(task, tid, edges),

            GraphOp::Hop {
                // Scope is enforced for these at the hop level: they expand
                // through `NeighborsMulti`, which carries the same collection.
                collection: _,
                start_nodes,
                edge_label,
                direction,
                depth,
                options: _,
                rls_filters: _,
                frontier_bitmap,
            } => self.execute_graph_hop(
                task,
                crate::data::executor::handlers::graph::GraphHopParams {
                    tid,
                    start_nodes,
                    edge_label,
                    direction: *direction,
                    depth: *depth,
                    frontier_bitmap: frontier_bitmap.as_ref(),
                },
            ),

            GraphOp::Neighbors {
                collection,
                node_id,
                edge_label,
                direction,
                rls_filters: _,
            } => self.execute_graph_neighbors(
                task,
                tid,
                node_id,
                edge_label,
                *direction,
                collection.as_ref().map(|c| c.as_str()),
            ),

            GraphOp::NeighborsMulti {
                collection,
                node_ids,
                edge_label,
                direction,
                max_results,
                rls_filters: _,
            } => self.execute_graph_neighbors_multi(
                task,
                tid,
                super::super::handlers::graph::GraphNeighborsMultiArgs {
                    node_ids,
                    edge_label,
                    direction: *direction,
                    max_results: *max_results,
                    collection: collection.as_ref().map(|c| c.as_str()),
                },
            ),

            GraphOp::Path {
                // Scope is enforced for these at the hop level: they expand
                // through `NeighborsMulti`, which carries the same collection.
                collection: _,
                src,
                dst,
                edge_label,
                max_depth,
                options: _,
                rls_filters: _,
                frontier_bitmap,
            } => self.execute_graph_path(
                task,
                crate::data::executor::handlers::graph::graph_traversal::GraphPathParams {
                    tid,
                    src,
                    dst,
                    edge_label,
                    max_depth: *max_depth,
                    frontier_bitmap: frontier_bitmap.as_ref(),
                },
            ),

            GraphOp::Subgraph {
                // Scope is enforced for these at the hop level: they expand
                // through `NeighborsMulti`, which carries the same collection.
                collection: _,
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
                rrf_k_triple,
                vector_field,
                options,
                bm25_query,
                bm25_field,
            } => {
                if let (Some(bm25_q), Some(bm25_f), Some(triple_k)) =
                    (bm25_query.as_deref(), bm25_field.as_deref(), rrf_k_triple)
                {
                    self.execute_graph_rag_fusion_triple(
                        task,
                        crate::data::executor::handlers::graph_rag_triple::GraphRagFusionTripleParams {
                            tenant_id: tid,
                            collection: collection.as_str(),
                            query_vector,
                            vector_top_k: *vector_top_k,
                            edge_label,
                            direction: *direction,
                            expansion_depth: *expansion_depth,
                            final_top_k: *final_top_k,
                            rrf_k: *triple_k,
                            vector_field: vector_field.as_str(),
                            max_visited: options.max_visited,
                            bm25_query: bm25_q,
                            bm25_field: bm25_f,
                        },
                    )
                } else {
                    self.execute_graph_rag_fusion(
                        task,
                        crate::data::executor::handlers::graph_rag::GraphRagFusionParams {
                            tenant_id: tid,
                            collection: collection.as_str(),
                            query_vector,
                            vector_top_k: *vector_top_k,
                            edge_label,
                            direction: *direction,
                            expansion_depth: *expansion_depth,
                            final_top_k: *final_top_k,
                            rrf_k: *rrf_k,
                            vector_field: vector_field.as_str(),
                            max_visited: options.max_visited,
                        },
                    )
                }
            }

            GraphOp::Algo { algorithm, params } => {
                self.execute_graph_algo(task, tid, algorithm, params)
            }

            GraphOp::Match {
                query,
                frontier_bitmap,
                cluster_mode,
            } => {
                self.execute_graph_match(task, tid, query, frontier_bitmap.as_ref(), *cluster_mode)
            }

            GraphOp::MatchContinuation {
                query,
                resume_triple_idx,
                partial_row,
                source_node,
                source_binding,
            } => self.execute_graph_match_continuation(
                task,
                crate::data::executor::handlers::graph_match::GraphMatchContinuationParams {
                    tid,
                    query_bytes: query,
                    resume_triple_idx: *resume_triple_idx,
                    partial_row_bytes: partial_row,
                    source_node,
                    source_binding,
                },
            ),

            GraphOp::MatchVarLenResume { query, resume } => {
                self.execute_graph_match_varlen_resume(task, tid, query, resume)
            }

            GraphOp::SetNodeLabels { node_id, labels } => {
                let partition = self.csr_partition_mut(database_id, tid);
                for label in labels {
                    if let Err(e) = partition.add_node_label(node_id, label) {
                        return self.response_error(
                            task,
                            crate::bridge::envelope::ErrorCode::Internal {
                                detail: format!("set node label: {e}"),
                            },
                        );
                    }
                }
                // CDC: a node-label set surfaces as an Insert on the nameable
                // node-label stream, carrying the added labels as `new_value`.
                self.emit_graph_label_event(task, node_id, labels, crate::event::WriteOp::Insert);
                // `add_node_label` interns the node if it was new, so a
                // successful set always touches exactly one node.
                self.response_affected(task, 1)
            }

            GraphOp::RemoveNodeLabels { node_id, labels } => {
                let partition = self.csr_partition_mut(database_id, tid);
                // Checked before the removal loop: the node identity is the
                // same across every label in `labels`, and `remove_node_label`
                // itself reports nothing back about whether the node existed.
                let existed = partition.contains_node(node_id);
                for label in labels {
                    partition.remove_node_label(node_id, label);
                }
                // CDC: a node-label removal surfaces as a Delete on the nameable
                // node-label stream, carrying the removed labels as `old_value`.
                self.emit_graph_label_event(task, node_id, labels, crate::event::WriteOp::Delete);
                self.response_affected(task, u64::from(existed))
            }

            GraphOp::TemporalNeighbors {
                collection,
                node_id,
                edge_label,
                direction,
                system_time,
                valid_at_ms,
                rls_filters: _,
            } => {
                let system_as_of_ms = match graph_system_as_of(system_time) {
                    Ok(v) => v,
                    Err(resp) => return self.response_error(task, resp),
                };
                self.execute_graph_temporal_neighbors(
                    task,
                    super::super::handlers::graph_temporal::TemporalNeighborsParams {
                        tid,
                        collection: collection.as_str(),
                        node_id,
                        edge_label,
                        direction: *direction,
                        system_as_of_ms,
                        valid_at_ms: *valid_at_ms,
                    },
                )
            }

            GraphOp::TemporalAlgorithm {
                algorithm,
                params,
                system_time,
            } => {
                let system_as_of_ms = match graph_system_as_of(system_time) {
                    Ok(v) => v,
                    Err(resp) => return self.response_error(task, resp),
                };
                self.execute_graph_temporal_algo(task, tid, algorithm, params, system_as_of_ms)
            }

            GraphOp::BspSuperstep(plan) => self.execute_bsp_superstep(
                task,
                tid,
                super::super::handlers::graph_bsp::BspSuperstepArgs {
                    algorithm: &plan.algorithm,
                    params: &plan.params,
                    superstep: plan.superstep,
                    global_n: plan.global_n,
                    owned_vshards: &plan.owned_vshards,
                    incoming_contributions: &plan.incoming_contributions,
                    rank_seed: &plan.rank_seed,
                    global_dangling: plan.global_dangling,
                    personalization_sum: plan.personalization_sum,
                },
            ),

            GraphOp::WccSuperstep(plan) => {
                self.execute_wcc_superstep(task, tid, &plan.params, &plan.owned_vshards)
            }

            GraphOp::Stats { collection, as_of } => {
                self.execute_graph_stats(task, tid, collection.as_ref().map(|c| c.as_str()), *as_of)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bridge::envelope::{
        Admission, ExemptReason, PhysicalPlan, Priority, Request, Status,
    };
    use crate::event::WriteOp;
    use crate::event::bus::create_event_bus_with_capacity;
    use crate::types::{DatabaseId, Lsn, ReadConsistency, RequestId, TenantId, TraceId, VShardId};
    use nodedb_bridge::buffer::RingBuffer;
    use std::time::{Duration, Instant};

    struct CoreHarness {
        core: CoreLoop,
        _req_tx: nodedb_bridge::buffer::Producer<crate::bridge::dispatch::BridgeRequest>,
        _resp_rx: nodedb_bridge::buffer::Consumer<crate::bridge::dispatch::BridgeResponse>,
        _dir: tempfile::TempDir,
    }

    fn make_core() -> CoreHarness {
        use crate::bridge::dispatch::{BridgeRequest, BridgeResponse};
        let dir = tempfile::tempdir().expect("tempdir");
        let (req_tx, req_rx) = RingBuffer::channel::<BridgeRequest>(64);
        let (resp_tx, resp_rx) = RingBuffer::channel::<BridgeResponse>(64);
        let core = CoreLoop::open(
            0,
            req_rx,
            resp_tx,
            dir.path(),
            std::sync::Arc::new(nodedb_types::OrdinalClock::new()),
            crate::data::executor::core_loop::test_governor(),
        )
        .expect("open core");
        CoreHarness {
            core,
            _req_tx: req_tx,
            _resp_rx: resp_rx,
            _dir: dir,
        }
    }

    fn make_task_with_lsn(op: GraphOp, lsn: u64) -> ExecutionTask {
        ExecutionTask::new(Request {
            request_id: RequestId::new(1),
            tenant_id: TenantId::new(1),
            database_id: DatabaseId::DEFAULT,
            vshard_id: VShardId::new(0),
            plan: PhysicalPlan::Graph(op),
            deadline: Instant::now() + Duration::from_secs(5),
            priority: Priority::Normal,
            trace_id: TraceId::ZERO,
            consistency: ReadConsistency::Strong,
            idempotency_key: None,
            event_source: crate::event::EventSource::User,
            user_roles: Vec::new(),
            user_id: None,
            statement_digest: None,
            txn_id: None,
            wal_lsn: Some(Lsn::new(lsn)),
            resolved_now_ms: None,
            admission: Admission::Exempt(ExemptReason::Read),
        })
    }

    #[test]
    fn set_node_labels_emits_cdc_on_nameable_label_stream() {
        let (mut producers, mut consumers) = create_event_bus_with_capacity(1, 64);
        let mut h = make_core();
        h.core
            .set_event_producer(producers.pop().expect("producer"));

        let op = GraphOp::SetNodeLabels {
            node_id: "alice".to_string(),
            labels: vec!["Person".to_string()],
        };
        let task = make_task_with_lsn(op.clone(), 88);
        let resp = h.core.dispatch_graph(&task, &op);
        assert_eq!(resp.status, Status::Ok);
        assert_eq!(
            crate::control::server::shared::sql::staging_predicates::require_affected_count(
                resp.payload.as_bytes()
            )
            .expect("SetNodeLabels must report an affected count"),
            1,
            "a set always interns/touches exactly one node"
        );

        let event = consumers[0]
            .try_recv()
            .expect("SetNodeLabels must emit a CDC WriteEvent");
        assert_eq!(
            event.collection.as_ref(),
            crate::event::graph_cdc::GRAPH_LABEL_STREAM,
            "node-label CDC uses the nameable stream, not the NUL sentinel"
        );
        assert_eq!(event.row_id.as_str(), "alice");
        assert_eq!(event.op, WriteOp::Insert);
        assert_eq!(event.lsn, Lsn::new(88));
    }

    #[test]
    fn remove_node_labels_emits_cdc_delete() {
        let (mut producers, mut consumers) = create_event_bus_with_capacity(1, 64);
        let mut h = make_core();
        h.core
            .set_event_producer(producers.pop().expect("producer"));

        let op = GraphOp::RemoveNodeLabels {
            node_id: "alice".to_string(),
            labels: vec!["Person".to_string()],
        };
        let task = make_task_with_lsn(op.clone(), 89);
        let resp = h.core.dispatch_graph(&task, &op);
        assert_eq!(resp.status, Status::Ok);
        assert_eq!(
            crate::control::server::shared::sql::staging_predicates::require_affected_count(
                resp.payload.as_bytes()
            )
            .expect("RemoveNodeLabels must report an affected count"),
            0,
            "'alice' was never labeled, so there is no node to remove from"
        );

        let event = consumers[0]
            .try_recv()
            .expect("RemoveNodeLabels must emit a CDC WriteEvent");
        assert_eq!(
            event.collection.as_ref(),
            crate::event::graph_cdc::GRAPH_LABEL_STREAM
        );
        assert_eq!(event.row_id.as_str(), "alice");
        assert_eq!(event.op, WriteOp::Delete);
        assert!(event.old_value.is_some(), "removed labels ride old_value");
    }
}
