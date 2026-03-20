//! Vector operation handlers: VectorInsert, VectorBatchInsert, VectorDelete,
//! VectorSearch, SetVectorParams.

use tracing::{debug, warn};

use crate::bridge::envelope::{ErrorCode, Response};
use crate::engine::vector::distance::DistanceMetric;
use crate::engine::vector::hnsw::{HnswIndex, HnswParams};

use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::task::ExecutionTask;

impl CoreLoop {
    /// Get or create a vector index, validating dimension compatibility.
    fn get_or_create_vector_index(
        &mut self,
        tid: u32,
        collection: &str,
        dim: usize,
        field_name: &str,
    ) -> Result<&mut HnswIndex, ErrorCode> {
        let index_key = CoreLoop::vector_index_key(tid, collection, field_name);
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
        let params = self
            .vector_params
            .get(&index_key)
            .cloned()
            .unwrap_or_default();
        Ok(self.vector_indexes.entry(index_key).or_insert_with(|| {
            debug!(core = core_id, dim, m = params.m, ef = params.ef_construction, ?params.metric, "creating HNSW index");
            HnswIndex::with_seed(dim, params, core_id as u64 + 1)
        }))
    }

    pub(in crate::data::executor) fn execute_vector_insert(
        &mut self,
        task: &ExecutionTask,
        tid: u32,
        collection: &str,
        vector: &[f32],
        dim: usize,
        field_name: &str,
    ) -> Response {
        debug!(core = self.core_id, %collection, dim, "vector insert");
        if vector.len() != dim {
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
        match self.get_or_create_vector_index(tid, collection, dim, field_name) {
            Ok(index) => {
                index.insert(vector.to_vec());
                self.response_ok(task)
            }
            Err(err) => self.response_error(task, err),
        }
    }

    /// Execute batch vector insert (always to the default/unnamed field).
    /// Named fields require separate VectorInsert requests.
    pub(in crate::data::executor) fn execute_vector_batch_insert(
        &mut self,
        task: &ExecutionTask,
        tid: u32,
        collection: &str,
        vectors: &[Vec<f32>],
        dim: usize,
    ) -> Response {
        debug!(core = self.core_id, %collection, dim, count = vectors.len(), "vector batch insert");
        match self.get_or_create_vector_index(tid, collection, dim, "") {
            Ok(index) => {
                for vector in vectors {
                    if vector.len() != dim {
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
                match super::super::response_codec::encode_count("inserted", vectors.len()) {
                    Ok(bytes) => self.response_with_payload(task, bytes),
                    Err(e) => self.response_error(task, ErrorCode::Internal { detail: e }),
                }
            }
            Err(err) => self.response_error(task, err),
        }
    }

    pub(in crate::data::executor) fn execute_vector_delete(
        &mut self,
        task: &ExecutionTask,
        tid: u32,
        collection: &str,
        vector_id: u32,
    ) -> Response {
        debug!(core = self.core_id, %collection, vector_id, "vector delete");
        let index_key = CoreLoop::vector_index_key(tid, collection, "");
        let Some(index) = self.vector_indexes.get_mut(&index_key) else {
            return self.response_error(task, ErrorCode::NotFound);
        };
        if index.delete(vector_id) {
            self.response_ok(task)
        } else {
            self.response_error(task, ErrorCode::NotFound)
        }
    }

    #[allow(clippy::too_many_arguments)]
    pub(in crate::data::executor) fn execute_vector_search(
        &self,
        task: &ExecutionTask,
        tid: u32,
        collection: &str,
        query_vector: &[f32],
        top_k: usize,
        ef_search: usize,
        filter_bitmap: Option<&std::sync::Arc<[u8]>>,
        field_name: &str,
    ) -> Response {
        debug!(core = self.core_id, %collection, top_k, ef_search, "vector search");
        let index_key = CoreLoop::vector_index_key(tid, collection, field_name);
        let Some(index) = self.vector_indexes.get(&index_key) else {
            return self.response_error(task, ErrorCode::NotFound);
        };
        if index.is_empty() {
            return self.response_with_payload(task, b"[]".to_vec());
        }
        let ef = if ef_search > 0 {
            ef_search.max(top_k)
        } else {
            top_k.saturating_mul(4).max(64)
        };
        let results = match filter_bitmap {
            Some(bitmap_bytes) => {
                index.search_with_bitmap_bytes(query_vector, top_k, ef, bitmap_bytes)
            }
            None => index.search(query_vector, top_k, ef),
        };
        let hits: Vec<_> = results
            .iter()
            .map(|r| super::super::response_codec::VectorSearchHit {
                id: r.id,
                distance: r.distance,
            })
            .collect();
        match super::super::response_codec::encode(&hits) {
            Ok(payload) => self.response_with_payload(task, payload),
            Err(e) => {
                warn!(core = self.core_id, error = %e, "vector search serialization failed");
                self.response_error(task, ErrorCode::Internal { detail: e })
            }
        }
    }

    pub(in crate::data::executor) fn execute_set_vector_params(
        &mut self,
        task: &ExecutionTask,
        tid: u32,
        collection: &str,
        m: usize,
        ef_construction: usize,
        metric: &str,
    ) -> Response {
        debug!(core = self.core_id, %collection, m, ef_construction, %metric, "set vector params");
        let index_key = CoreLoop::vector_index_key(tid, collection, "");

        if self.vector_indexes.contains_key(&index_key) {
            return self.response_error(
                task,
                ErrorCode::RejectedConstraint {
                    constraint: "cannot change HNSW params after index creation; drop and recreate the collection".into(),
                },
            );
        }

        let metric_enum = match metric {
            "l2" | "euclidean" => DistanceMetric::L2,
            "cosine" => DistanceMetric::Cosine,
            "inner_product" | "ip" | "dot" => DistanceMetric::InnerProduct,
            _ => {
                return self.response_error(
                    task,
                    ErrorCode::RejectedConstraint {
                        constraint: format!(
                            "unknown metric '{metric}'; supported: l2, cosine, inner_product"
                        ),
                    },
                );
            }
        };

        let params = HnswParams {
            m,
            m0: m * 2,
            ef_construction,
            metric: metric_enum,
        };
        self.vector_params.insert(index_key, params);
        self.response_ok(task)
    }
}
