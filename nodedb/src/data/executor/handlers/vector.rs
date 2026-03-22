//! Vector operation handlers: VectorInsert, VectorBatchInsert, VectorDelete,
//! VectorSearch, SetVectorParams.

use tracing::{debug, warn};

use crate::bridge::envelope::{ErrorCode, Response};
use crate::engine::vector::collection::VectorCollection;
use crate::engine::vector::distance::DistanceMetric;
use crate::engine::vector::hnsw::HnswParams;

use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::task::ExecutionTask;

impl CoreLoop {
    /// Get or create a vector collection, validating dimension compatibility.
    fn get_or_create_vector_index(
        &mut self,
        tid: u32,
        collection: &str,
        dim: usize,
        field_name: &str,
    ) -> Result<&mut VectorCollection, ErrorCode> {
        let index_key = CoreLoop::vector_index_key(tid, collection, field_name);
        if let Some(existing) = self.vector_collections.get(&index_key)
            && existing.dim() != dim
        {
            return Err(ErrorCode::RejectedConstraint {
                constraint: format!(
                    "dimension mismatch: index has {}, got {dim}",
                    existing.dim()
                ),
            });
        }
        let core_id = self.core_id;
        let params = self
            .vector_params
            .get(&index_key)
            .cloned()
            .unwrap_or_default();
        Ok(self.vector_collections.entry(index_key).or_insert_with(|| {
            debug!(core = core_id, dim, m = params.m, ef = params.ef_construction, ?params.metric, "creating vector collection");
            VectorCollection::new(dim, params)
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
        let index_key = CoreLoop::vector_index_key(tid, collection, field_name);

        // Check if this collection uses IVF-PQ index.
        if let Some(cfg) = self.index_configs.get(&index_key)
            && cfg.index_type == crate::engine::vector::index_config::IndexType::IvfPq
        {
            return self.ivf_insert(task, &index_key, vector, dim);
        }

        // Default: HNSW (with or without PQ).
        match self.get_or_create_vector_index(tid, collection, dim, field_name) {
            Ok(collection_ref) => {
                collection_ref.insert(vector.to_vec());
                if collection_ref.needs_seal()
                    && let Some(req) = collection_ref.seal(&index_key)
                    && let Some(tx) = &self.build_tx
                    && let Err(e) = tx.send(req)
                {
                    warn!(core = self.core_id, error = %e, "failed to send HNSW build request");
                }
                self.response_ok(task)
            }
            Err(err) => self.response_error(task, err),
        }
    }

    /// Insert into an IVF-PQ index.
    fn ivf_insert(
        &mut self,
        task: &ExecutionTask,
        index_key: &str,
        vector: &[f32],
        dim: usize,
    ) -> Response {
        let ivf = self
            .ivf_indexes
            .entry(index_key.to_string())
            .or_insert_with(|| {
                let cfg = self
                    .index_configs
                    .get(index_key)
                    .cloned()
                    .unwrap_or_default();
                let params = cfg.to_ivf_params();
                debug!(
                    core = self.core_id,
                    key = index_key,
                    "creating IVF-PQ index"
                );
                crate::engine::vector::ivf::IvfPqIndex::new(dim, params)
            });

        // IVF-PQ requires training before the first insert. If not trained,
        // buffer vectors until we have enough for training (min 256).
        if ivf.n_cells() == 0 {
            // Not yet trained — the index was just created with new().
            // We need to accumulate training data. For now, we train on
            // the first batch of vectors inserted (uses the vector itself
            // as a 1-sample train). This is a minimal bootstrap — production
            // users should call a separate TRAIN command with a sample set.
            //
            // Auto-train with the first vector as a degenerate single-sample.
            // Real training happens when the IVF index receives enough data.
            let refs: Vec<&[f32]> = vec![vector];
            ivf.train(&refs);
        }

        ivf.add(vector);
        self.response_ok(task)
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
        let index_key = CoreLoop::vector_index_key(tid, collection, "");
        match self.get_or_create_vector_index(tid, collection, dim, "") {
            Ok(collection_ref) => {
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
                    collection_ref.insert(vector.clone());
                }
                if collection_ref.needs_seal()
                    && let Some(req) = collection_ref.seal(&index_key)
                    && let Some(tx) = &self.build_tx
                    && let Err(e) = tx.send(req)
                {
                    warn!(core = self.core_id, error = %e, "failed to send HNSW build request");
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
        let Some(collection_ref) = self.vector_collections.get_mut(&index_key) else {
            return self.response_error(task, ErrorCode::NotFound);
        };
        if collection_ref.delete(vector_id) {
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

        // Check for IVF-PQ index first.
        if let Some(ivf) = self.ivf_indexes.get(&index_key) {
            if ivf.is_empty() {
                return self.response_with_payload(task, b"[]".to_vec());
            }
            let results = ivf.search(query_vector, top_k);
            let hits: Vec<_> = results
                .iter()
                .map(|r| super::super::response_codec::VectorSearchHit {
                    id: r.id,
                    distance: r.distance,
                })
                .collect();
            return match super::super::response_codec::encode(&hits) {
                Ok(payload) => self.response_with_payload(task, payload),
                Err(e) => self.response_error(task, ErrorCode::Internal { detail: e }),
            };
        }

        // Default: HNSW collection.
        let Some(collection_ref) = self.vector_collections.get(&index_key) else {
            return self.response_error(task, ErrorCode::NotFound);
        };
        if collection_ref.is_empty() {
            return self.response_with_payload(task, b"[]".to_vec());
        }
        let ef = if ef_search > 0 {
            ef_search.max(top_k)
        } else {
            top_k.saturating_mul(4).max(64)
        };
        let results = match filter_bitmap {
            Some(bitmap_bytes) => {
                collection_ref.search_with_bitmap_bytes(query_vector, top_k, ef, bitmap_bytes)
            }
            None => collection_ref.search(query_vector, top_k, ef),
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

    /// Multi-vector search: query all named vector fields in a collection,
    /// fuse results via RRF.
    #[allow(clippy::too_many_arguments)]
    pub(in crate::data::executor) fn execute_vector_multi_search(
        &self,
        task: &ExecutionTask,
        tid: u32,
        collection: &str,
        query_vector: &[f32],
        top_k: usize,
        ef_search: usize,
        filter_bitmap: Option<&std::sync::Arc<[u8]>>,
    ) -> Response {
        debug!(core = self.core_id, %collection, top_k, "vector multi-search");

        // Find all indexes matching this tenant:collection pattern.
        let prefix = format!("{tid}:{collection}:");
        let plain_key = CoreLoop::vector_index_key(tid, collection, "");

        let mut all_results: Vec<Vec<crate::engine::vector::hnsw::SearchResult>> = Vec::new();

        for (key, coll) in &self.vector_collections {
            if key == &plain_key || key.starts_with(&prefix) {
                if coll.is_empty() || coll.dim() != query_vector.len() {
                    continue;
                }
                let ef = if ef_search > 0 {
                    ef_search.max(top_k)
                } else {
                    top_k.saturating_mul(4).max(64)
                };
                let results = match filter_bitmap {
                    Some(bm) => coll.search_with_bitmap_bytes(query_vector, top_k, ef, bm),
                    None => coll.search(query_vector, top_k, ef),
                };
                all_results.push(results);
            }
        }

        if all_results.is_empty() {
            return self.response_error(task, ErrorCode::NotFound);
        }

        // Single field — return directly.
        if all_results.len() == 1 {
            // Safety: len() == 1 checked above; use expect-free path anyway.
            let Some(results) = all_results.into_iter().next() else {
                return self.response_error(task, ErrorCode::NotFound);
            };
            let hits: Vec<_> = results
                .iter()
                .map(|r| super::super::response_codec::VectorSearchHit {
                    id: r.id,
                    distance: r.distance,
                })
                .collect();
            return match super::super::response_codec::encode(&hits) {
                Ok(payload) => self.response_with_payload(task, payload),
                Err(e) => self.response_error(task, ErrorCode::Internal { detail: e }),
            };
        }

        // RRF fusion across fields.
        let rrf_k = 60.0_f64;
        let mut fused: std::collections::HashMap<u32, f64> = std::collections::HashMap::new();
        for results in &all_results {
            for (rank, r) in results.iter().enumerate() {
                *fused.entry(r.id).or_default() += 1.0 / (rrf_k + rank as f64 + 1.0);
            }
        }

        let mut ranked: Vec<(u32, f64)> = fused.into_iter().collect();
        ranked.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));
        ranked.truncate(top_k);

        let hits: Vec<_> = ranked
            .iter()
            .map(
                |&(id, score)| super::super::response_codec::VectorSearchHit {
                    id,
                    distance: score as f32,
                },
            )
            .collect();
        match super::super::response_codec::encode(&hits) {
            Ok(payload) => self.response_with_payload(task, payload),
            Err(e) => self.response_error(task, ErrorCode::Internal { detail: e }),
        }
    }

    #[allow(clippy::too_many_arguments)]
    pub(in crate::data::executor) fn execute_set_vector_params(
        &mut self,
        task: &ExecutionTask,
        tid: u32,
        collection: &str,
        m: usize,
        ef_construction: usize,
        metric: &str,
        index_type: &str,
        pq_m: usize,
        ivf_cells: usize,
        ivf_nprobe: usize,
    ) -> Response {
        debug!(core = self.core_id, %collection, m, ef_construction, %metric, %index_type, "set vector params");
        let index_key = CoreLoop::vector_index_key(tid, collection, "");

        if self.vector_collections.contains_key(&index_key) {
            return self.response_error(
                task,
                ErrorCode::RejectedConstraint {
                    constraint: "cannot change index params after creation; drop and recreate the collection".into(),
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

        let idx_type = match crate::engine::vector::index_config::IndexType::parse(index_type) {
            Some(t) => t,
            None => {
                return self.response_error(
                    task,
                    ErrorCode::RejectedConstraint {
                        constraint: format!(
                            "unknown index_type '{index_type}'; supported: hnsw, hnsw_pq, ivf_pq"
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

        let config = crate::engine::vector::index_config::IndexConfig {
            hnsw: params.clone(),
            index_type: idx_type,
            pq_m: if pq_m > 0 { pq_m } else { 8 },
            ivf_cells: if ivf_cells > 0 { ivf_cells } else { 256 },
            ivf_nprobe: if ivf_nprobe > 0 { ivf_nprobe } else { 16 },
        };

        // Store both legacy HnswParams (for backward compat) and full IndexConfig.
        self.vector_params.insert(index_key.clone(), params);
        self.index_configs.insert(index_key, config);
        self.response_ok(task)
    }
}
