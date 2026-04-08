//! Vector write handlers: VectorInsert, VectorBatchInsert, VectorDelete,
//! SetVectorParams.

use tracing::{debug, warn};

use crate::bridge::envelope::{ErrorCode, Response};
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::task::ExecutionTask;
use crate::engine::vector::collection::VectorCollection;
use crate::engine::vector::distance::DistanceMetric;
use crate::engine::vector::hnsw::HnswParams;

/// Parameters for configuring vector index settings.
pub(in crate::data::executor) struct SetVectorParamsInput<'a> {
    pub task: &'a ExecutionTask,
    pub tid: u32,
    pub collection: &'a str,
    pub m: usize,
    pub ef_construction: usize,
    pub metric: &'a str,
    pub index_type: &'a str,
    pub pq_m: usize,
    pub ivf_cells: usize,
    pub ivf_nprobe: usize,
}

/// Parameters for a vector insert operation.
pub(in crate::data::executor) struct VectorInsertParams<'a> {
    pub task: &'a ExecutionTask,
    pub tid: u32,
    pub collection: &'a str,
    pub vector: &'a [f32],
    pub dim: usize,
    pub field_name: &'a str,
    pub doc_id: Option<String>,
}

impl CoreLoop {
    /// Get or create a vector collection, validating dimension compatibility.
    pub(in crate::data::executor) fn get_or_create_vector_index(
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
        params: VectorInsertParams<'_>,
    ) -> Response {
        let VectorInsertParams {
            task,
            tid,
            collection,
            vector,
            dim,
            field_name,
            doc_id,
        } = params;
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
            return self.ivf_insert(task, &index_key, vector, dim, doc_id);
        }

        // Default: HNSW (with or without PQ).
        match self.get_or_create_vector_index(tid, collection, dim, field_name) {
            Ok(collection_ref) => {
                if let Some(did) = doc_id {
                    collection_ref.insert_with_doc_id(vector.to_vec(), did);
                } else {
                    collection_ref.insert(vector.to_vec());
                }
                if collection_ref.needs_seal()
                    && let Some(req) = collection_ref.seal(&index_key)
                    && let Some(tx) = &self.build_tx
                    && let Err(e) = tx.send(req)
                {
                    warn!(core = self.core_id, error = %e, "failed to send HNSW build request");
                }
                self.checkpoint_coordinator.mark_dirty("vector", 1);
                self.response_ok(task)
            }
            Err(err) => self.response_error(task, err),
        }
    }

    /// Insert into an IVF-PQ index, returning the assigned vector ID.
    fn ivf_insert(
        &mut self,
        task: &ExecutionTask,
        index_key: &str,
        vector: &[f32],
        dim: usize,
        doc_id: Option<String>,
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

        // IVF-PQ requires training before the first insert.
        if ivf.n_cells() == 0 {
            let refs: Vec<&[f32]> = vec![vector];
            ivf.train(&refs);
        }

        let vector_id = ivf.add(vector);

        // Register doc_id mapping using the actual IVF-assigned vector ID.
        if let Some(did) = doc_id {
            let coll = self
                .vector_collections
                .entry(index_key.to_string())
                .or_insert_with(|| VectorCollection::new(dim, Default::default()));
            coll.doc_id_map.insert(vector_id, did);
        }

        self.checkpoint_coordinator.mark_dirty("vector", 1);
        self.response_ok(task)
    }

    /// Execute batch vector insert (always to the default/unnamed field).
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
                self.checkpoint_coordinator
                    .mark_dirty("vector", vectors.len());
                match super::super::response_codec::encode_count("inserted", vectors.len()) {
                    Ok(bytes) => self.response_with_payload(task, bytes),
                    Err(e) => self.response_error(
                        task,
                        ErrorCode::Internal {
                            detail: e.to_string(),
                        },
                    ),
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
            self.checkpoint_coordinator.mark_dirty("vector", 1);
            self.response_ok(task)
        } else {
            self.response_error(task, ErrorCode::NotFound)
        }
    }

    pub(in crate::data::executor) fn execute_set_vector_params(
        &mut self,
        params: SetVectorParamsInput<'_>,
    ) -> Response {
        let SetVectorParamsInput {
            task,
            tid,
            collection,
            m,
            ef_construction,
            metric,
            index_type,
            pq_m,
            ivf_cells,
            ivf_nprobe,
        } = params;
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
            "manhattan" | "l1" => DistanceMetric::Manhattan,
            "chebyshev" | "linf" => DistanceMetric::Chebyshev,
            "hamming" => DistanceMetric::Hamming,
            "jaccard" => DistanceMetric::Jaccard,
            "pearson" => DistanceMetric::Pearson,
            _ => {
                return self.response_error(
                    task,
                    ErrorCode::RejectedConstraint {
                        constraint: format!(
                            "unknown metric '{metric}'; supported: l2, cosine, inner_product, manhattan, chebyshev, hamming, jaccard, pearson"
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

        self.vector_params.insert(index_key.clone(), params);
        self.index_configs.insert(index_key, config);
        self.response_ok(task)
    }
}
