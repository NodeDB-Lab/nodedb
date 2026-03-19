//! Physical plan execution dispatch.
//!
//! Separates the large `execute()` match from `CoreLoop` to keep files
//! under the 500-line limit. All methods here are `impl CoreLoop`.

use tracing::{debug, warn};

use nodedb_crdt::constraint::ConstraintSet;

use crate::bridge::envelope::{ErrorCode, PhysicalPlan, Response};
use crate::engine::crdt::tenant_state::TenantCrdtEngine;
use crate::engine::vector::hnsw::{HnswIndex, HnswParams};
use crate::types::TenantId;

use super::core_loop::CoreLoop;
use super::task::ExecutionTask;

impl CoreLoop {
    /// Get or create a CRDT engine for the given tenant.
    pub(super) fn get_crdt_engine(&mut self, tenant_id: TenantId) -> &mut TenantCrdtEngine {
        self.crdt_engines.entry(tenant_id).or_insert_with(|| {
            debug!(core = self.core_id, %tenant_id, "creating CRDT engine for tenant");
            TenantCrdtEngine::new(tenant_id, self.core_id as u64, ConstraintSet::new())
        })
    }

    /// Execute a physical plan. Dispatches to the appropriate engine.
    pub(super) fn execute(&mut self, task: &ExecutionTask) -> Response {
        let tid = task.request.tenant_id.as_u32();
        match task.plan() {
            PhysicalPlan::PointGet {
                collection,
                document_id,
            } => {
                debug!(core = self.core_id, %collection, %document_id, "point get");
                match self.sparse.get(tid, collection, document_id) {
                    Ok(Some(data)) => self.response_with_payload(task, data),
                    Ok(None) => self.response_error(task, ErrorCode::NotFound),
                    Err(e) => {
                        warn!(core = self.core_id, error = %e, "sparse get failed");
                        self.response_error(
                            task,
                            ErrorCode::Internal {
                                detail: e.to_string(),
                            },
                        )
                    }
                }
            }

            PhysicalPlan::VectorSearch {
                collection,
                query_vector,
                top_k,
                filter_bitmap,
            } => {
                debug!(core = self.core_id, %collection, top_k, "vector search");
                let Some(index) = self.vector_indexes.get(collection) else {
                    return self.response_error(task, ErrorCode::NotFound);
                };
                if index.is_empty() {
                    return self.response_with_payload(task, b"[]".to_vec());
                }
                let ef = top_k.saturating_mul(4).max(64);
                let results = match filter_bitmap {
                    Some(bitmap_bytes) => {
                        index.search_with_bitmap_bytes(query_vector, *top_k, ef, bitmap_bytes)
                    }
                    None => index.search(query_vector, *top_k, ef),
                };
                let serializable: Vec<_> = results
                    .iter()
                    .map(|r| serde_json::json!({"id": r.id, "distance": r.distance}))
                    .collect();
                match serde_json::to_vec(&serializable) {
                    Ok(payload) => self.response_with_payload(task, payload),
                    Err(e) => {
                        warn!(core = self.core_id, error = %e, "vector search serialization failed");
                        self.response_error(
                            task,
                            ErrorCode::Internal {
                                detail: e.to_string(),
                            },
                        )
                    }
                }
            }

            PhysicalPlan::RangeScan {
                collection,
                field,
                lower,
                upper,
                limit,
            } => {
                debug!(core = self.core_id, %collection, %field, limit, "range scan");
                match self.sparse.range_scan(
                    tid,
                    collection,
                    field,
                    lower.as_deref(),
                    upper.as_deref(),
                    *limit,
                ) {
                    Ok(results) => match serde_json::to_vec(&results) {
                        Ok(payload) => self.response_with_payload(task, payload),
                        Err(e) => {
                            warn!(core = self.core_id, error = %e, "range scan serialization failed");
                            self.response_error(
                                task,
                                ErrorCode::Internal {
                                    detail: e.to_string(),
                                },
                            )
                        }
                    },
                    Err(e) => {
                        warn!(core = self.core_id, error = %e, "sparse range scan failed");
                        self.response_error(
                            task,
                            ErrorCode::Internal {
                                detail: e.to_string(),
                            },
                        )
                    }
                }
            }

            PhysicalPlan::CrdtRead {
                collection,
                document_id,
            } => {
                debug!(core = self.core_id, %collection, %document_id, "crdt read");
                let tenant_id = task.request.tenant_id;
                let engine = self.get_crdt_engine(tenant_id);
                match engine.read_snapshot(collection, document_id) {
                    Some(snapshot) => self.response_with_payload(task, snapshot),
                    None => self.response_error(task, ErrorCode::NotFound),
                }
            }

            PhysicalPlan::CrdtApply {
                collection: _,
                document_id: _,
                delta,
                peer_id: _,
            } => {
                let tenant_id = task.request.tenant_id;
                let engine = self.get_crdt_engine(tenant_id);
                match engine.apply_committed_delta(delta) {
                    Ok(()) => self.response_ok(task),
                    Err(e) => {
                        warn!(core = self.core_id, error = %e, "crdt apply failed");
                        self.response_error(
                            task,
                            ErrorCode::Internal {
                                detail: e.to_string(),
                            },
                        )
                    }
                }
            }

            PhysicalPlan::VectorInsert {
                collection,
                vector,
                dim,
            } => {
                debug!(core = self.core_id, %collection, dim, "vector insert");
                if let Some(existing) = self.vector_indexes.get(collection) {
                    if existing.dim() != *dim {
                        let existing_dim = existing.dim();
                        return self.response_error(
                            task,
                            ErrorCode::RejectedConstraint {
                                constraint: format!(
                                    "dimension mismatch: index has {existing_dim}, got {dim}"
                                ),
                            },
                        );
                    }
                }
                let core_id = self.core_id;
                let index = self
                    .vector_indexes
                    .entry(collection.clone())
                    .or_insert_with(|| {
                        debug!(core = core_id, dim, "creating HNSW index");
                        HnswIndex::with_seed(*dim, HnswParams::default(), core_id as u64 + 1)
                    });
                index.insert(vector.clone());
                self.response_ok(task)
            }

            PhysicalPlan::PointPut {
                collection,
                document_id,
                value,
            } => {
                debug!(core = self.core_id, %collection, %document_id, "point put");
                match self.sparse.put(tid, collection, document_id, value) {
                    Ok(()) => self.response_ok(task),
                    Err(e) => self.response_error(
                        task,
                        ErrorCode::Internal {
                            detail: e.to_string(),
                        },
                    ),
                }
            }

            PhysicalPlan::PointDelete {
                collection,
                document_id,
            } => {
                debug!(core = self.core_id, %collection, %document_id, "point delete");
                match self.sparse.delete(tid, collection, document_id) {
                    Ok(_) => self.response_ok(task),
                    Err(e) => self.response_error(
                        task,
                        ErrorCode::Internal {
                            detail: e.to_string(),
                        },
                    ),
                }
            }

            PhysicalPlan::EdgePut {
                src_id,
                label,
                dst_id,
                properties,
            } => self.execute_edge_put(task, src_id, label, dst_id, properties),

            PhysicalPlan::EdgeDelete {
                src_id,
                label,
                dst_id,
            } => self.execute_edge_delete(task, src_id, label, dst_id),

            PhysicalPlan::GraphHop {
                start_nodes,
                edge_label,
                direction,
                depth,
                options: _,
            } => self.execute_graph_hop(task, start_nodes, edge_label, *direction, *depth),

            PhysicalPlan::GraphNeighbors {
                node_id,
                edge_label,
                direction,
            } => self.execute_graph_neighbors(task, node_id, edge_label, *direction),

            PhysicalPlan::GraphPath {
                src,
                dst,
                edge_label,
                max_depth,
                options: _,
            } => self.execute_graph_path(task, src, dst, edge_label, *max_depth),

            PhysicalPlan::GraphSubgraph {
                start_nodes,
                edge_label,
                depth,
                options: _,
            } => self.execute_graph_subgraph(task, start_nodes, edge_label, *depth),

            PhysicalPlan::GraphRagFusion {
                collection,
                query_vector,
                vector_top_k,
                edge_label,
                direction,
                expansion_depth,
                final_top_k,
                rrf_k,
                options,
            } => self.execute_graph_rag_fusion(
                task,
                collection,
                query_vector,
                *vector_top_k,
                edge_label,
                *direction,
                *expansion_depth,
                *final_top_k,
                *rrf_k,
                options.max_visited,
            ),

            PhysicalPlan::SetCollectionPolicy {
                collection,
                policy_json,
            } => {
                debug!(core = self.core_id, %collection, "set collection policy");
                let tenant_id = task.request.tenant_id;
                let engine = self.get_crdt_engine(tenant_id);
                match engine.set_collection_policy(collection, policy_json) {
                    Ok(()) => self.response_ok(task),
                    Err(e) => {
                        warn!(core = self.core_id, error = %e, "set collection policy failed");
                        self.response_error(
                            task,
                            ErrorCode::Internal {
                                detail: e.to_string(),
                            },
                        )
                    }
                }
            }

            PhysicalPlan::WalAppend { payload } => {
                debug!(core = self.core_id, len = payload.len(), "wal append");
                self.response_ok(task)
            }

            PhysicalPlan::Cancel { target_request_id } => {
                debug!(core = self.core_id, %target_request_id, "cancel");
                if let Some(pos) = self
                    .task_queue
                    .iter()
                    .position(|t| t.request_id() == *target_request_id)
                {
                    self.task_queue.remove(pos);
                }
                self.response_ok(task)
            }
        }
    }
}
