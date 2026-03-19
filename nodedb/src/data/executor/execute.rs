//! Physical plan execution dispatch.
//!
//! Separates the large `execute()` match from `CoreLoop` to keep files
//! under the 500-line limit. All methods here are `impl CoreLoop`.

use tracing::{debug, warn};

use crate::bridge::envelope::{ErrorCode, PhysicalPlan, Response};
use crate::engine::vector::hnsw::{HnswIndex, HnswParams};

use super::core_loop::CoreLoop;
use super::scan_filter::{ScanFilter, compare_json_values};
use super::task::ExecutionTask;

impl CoreLoop {
    /// Get or create a vector index, validating dimension compatibility.
    fn get_or_create_vector_index(
        &mut self,
        tid: u32,
        collection: &str,
        dim: usize,
    ) -> Result<&mut HnswIndex, ErrorCode> {
        let index_key = CoreLoop::vector_index_key(tid, collection);
        if let Some(existing) = self.vector_indexes.get(&index_key) {
            if existing.dim() != dim {
                return Err(ErrorCode::RejectedConstraint {
                    constraint: format!(
                        "dimension mismatch: index has {}, got {dim}",
                        existing.dim()
                    ),
                });
            }
        }
        let core_id = self.core_id;
        Ok(self.vector_indexes.entry(index_key).or_insert_with(|| {
            debug!(core = core_id, dim, "creating HNSW index");
            HnswIndex::with_seed(dim, HnswParams::default(), core_id as u64 + 1)
        }))
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
                let index_key = CoreLoop::vector_index_key(tid, collection);
                let Some(index) = self.vector_indexes.get(&index_key) else {
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
                if vector.len() != *dim {
                    return self.response_error(
                        task,
                        ErrorCode::RejectedConstraint {
                            constraint: format!(
                                "vector dimension mismatch: expected {dim}, got {}",
                                vector.len()
                            ),
                        },
                    );
                }
                match self.get_or_create_vector_index(tid, collection, *dim) {
                    Ok(index) => {
                        index.insert(vector.clone());
                        self.response_ok(task)
                    }
                    Err(err) => self.response_error(task, err),
                }
            }

            PhysicalPlan::VectorBatchInsert {
                collection,
                vectors,
                dim,
            } => {
                debug!(core = self.core_id, %collection, dim, count = vectors.len(), "vector batch insert");
                match self.get_or_create_vector_index(tid, collection, *dim) {
                    Ok(index) => {
                        for vector in vectors {
                            if vector.len() != *dim {
                                return self.response_error(
                                    task,
                                    ErrorCode::RejectedConstraint {
                                        constraint: format!(
                                            "dimension mismatch in batch: expected {dim}, got {}",
                                            vector.len()
                                        ),
                                    },
                                );
                            }
                            index.insert(vector.clone());
                        }
                        let payload = serde_json::json!({"inserted": vectors.len()});
                        match serde_json::to_vec(&payload) {
                            Ok(bytes) => self.response_with_payload(task, bytes),
                            Err(e) => self.response_error(
                                task,
                                ErrorCode::Internal {
                                    detail: format!("batch insert response serialization: {e}"),
                                },
                            ),
                        }
                    }
                    Err(err) => self.response_error(task, err),
                }
            }

            PhysicalPlan::VectorDelete {
                collection,
                vector_id,
            } => {
                debug!(core = self.core_id, %collection, vector_id, "vector delete");
                let index_key = CoreLoop::vector_index_key(tid, collection);
                let Some(index) = self.vector_indexes.get_mut(&index_key) else {
                    return self.response_error(task, ErrorCode::NotFound);
                };
                if index.delete(*vector_id) {
                    self.response_ok(task)
                } else {
                    self.response_error(task, ErrorCode::NotFound)
                }
            }

            PhysicalPlan::DocumentBatchInsert {
                collection,
                documents,
            } => {
                debug!(core = self.core_id, %collection, count = documents.len(), "document batch insert");
                let refs: Vec<(&str, &[u8])> = documents
                    .iter()
                    .map(|(id, val)| (id.as_str(), val.as_slice()))
                    .collect();
                match self.sparse.batch_put(tid, collection, &refs) {
                    Ok(()) => {
                        let payload = serde_json::json!({"inserted": documents.len()});
                        match serde_json::to_vec(&payload) {
                            Ok(bytes) => self.response_with_payload(task, bytes),
                            Err(e) => self.response_error(
                                task,
                                ErrorCode::Internal {
                                    detail: format!("batch insert response serialization: {e}"),
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

            PhysicalPlan::PointUpdate {
                collection,
                document_id,
                updates,
            } => self.execute_point_update(task, tid, collection, document_id, updates),

            PhysicalPlan::DocumentScan {
                collection,
                limit,
                offset,
                sort_keys,
                filters,
            } => {
                debug!(
                    core = self.core_id,
                    %collection,
                    limit,
                    offset,
                    sort_fields = sort_keys.len(),
                    "document scan"
                );

                // Fetch extra documents to account for filtering + offset.
                let fetch_limit = (*limit + *offset).saturating_mul(2).max(1000);
                match self.sparse.scan_documents(tid, collection, fetch_limit) {
                    Ok(docs) => {
                        // Parse filters if present.
                        let filter_predicates: Vec<ScanFilter> = if filters.is_empty() {
                            Vec::new()
                        } else {
                            match serde_json::from_slice(filters) {
                                Ok(f) => f,
                                Err(e) => {
                                    warn!(core = self.core_id, error = %e, "failed to parse scan filters");
                                    return self.response_error(
                                        task,
                                        ErrorCode::Internal {
                                            detail: format!("malformed scan filters: {e}"),
                                        },
                                    );
                                }
                            }
                        };

                        // Apply filters.
                        let filtered: Vec<_> = if filter_predicates.is_empty() {
                            docs
                        } else {
                            docs.into_iter()
                                .filter(|(_, value)| {
                                    let doc: serde_json::Value = match serde_json::from_slice(value)
                                    {
                                        Ok(v) => v,
                                        Err(_) => return false,
                                    };
                                    filter_predicates.iter().all(|f| f.matches(&doc))
                                })
                                .collect()
                        };

                        // Multi-field sort: compare by each key in order,
                        // breaking ties with subsequent keys.
                        let mut sorted = filtered;
                        if !sort_keys.is_empty() {
                            sorted.sort_by(|(_, a_bytes), (_, b_bytes)| {
                                let a_doc: serde_json::Value = serde_json::from_slice(a_bytes)
                                    .unwrap_or(serde_json::Value::Null);
                                let b_doc: serde_json::Value = serde_json::from_slice(b_bytes)
                                    .unwrap_or(serde_json::Value::Null);

                                for (field, asc) in sort_keys {
                                    let a_val = a_doc.get(field.as_str());
                                    let b_val = b_doc.get(field.as_str());
                                    let cmp = compare_json_values(a_val, b_val);
                                    let ordered = if *asc { cmp } else { cmp.reverse() };
                                    if ordered != std::cmp::Ordering::Equal {
                                        return ordered;
                                    }
                                }
                                std::cmp::Ordering::Equal
                            });
                        }

                        // Apply offset + limit.
                        let result: Vec<_> = sorted
                            .into_iter()
                            .skip(*offset)
                            .take(*limit)
                            .map(|(doc_id, value)| {
                                let data: serde_json::Value = serde_json::from_slice(&value)
                                    .unwrap_or_else(|e| {
                                        tracing::warn!(error = %e, "corrupted document bytes");
                                        serde_json::Value::Null
                                    });
                                serde_json::json!({"id": doc_id, "data": data})
                            })
                            .collect();

                        match serde_json::to_vec(&result) {
                            Ok(payload) => self.response_with_payload(task, payload),
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

            PhysicalPlan::HashJoin {
                left_collection,
                right_collection,
                on,
                join_type,
                limit,
            } => self.execute_hash_join(
                task,
                tid,
                left_collection,
                right_collection,
                on,
                join_type,
                *limit,
            ),

            PhysicalPlan::Aggregate {
                collection,
                group_by,
                aggregates,
                filters,
                limit,
            } => {
                self.execute_aggregate(task, tid, collection, group_by, aggregates, filters, *limit)
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
                tid,
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
