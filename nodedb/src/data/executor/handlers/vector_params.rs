// SPDX-License-Identifier: BUSL-1.1

//! Vector index parameter configuration handler (SET VECTOR PARAMS / CREATE VECTOR INDEX).
//!
//! Extracted from `vector.rs` to keep file sizes within the 500-line limit.

use tracing::debug;

use crate::bridge::envelope::{ErrorCode, Response};
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::handlers::vector::SetVectorParamsInput;
use crate::engine::vector::distance::DistanceMetric;
use crate::engine::vector::hnsw::HnswParams;
use crate::engine::vector::index_config;

impl CoreLoop {
    pub(in crate::data::executor) fn execute_set_vector_params(
        &mut self,
        params: SetVectorParamsInput<'_>,
    ) -> Response {
        let SetVectorParamsInput {
            task,
            tid,
            collection,
            field_name,
            dim,
            m,
            ef_construction,
            metric,
            index_type,
            pq_m,
            ivf_cells,
            ivf_nprobe,
        } = params;
        debug!(core = self.core_id, %collection, field = field_name, m, ef_construction, %metric, %index_type, "set vector params");
        let database_id = task.request.database_id.as_u64();
        let index_key = CoreLoop::vector_index_key(database_id, tid, collection, field_name);

        if self.vector_collections.contains_key(&index_key) {
            return self.response_error(
                task,
                ErrorCode::Unsupported {
                    detail: "changing vector index params after the index holds vectors is not \
                             supported; drop and recreate the collection"
                        .into(),
                },
            );
        }

        // Zero / empty inputs mean "preserve existing value if present, else default".
        // This keeps ALTER SET (index_type = ...) from clobbering m / ef_construction
        // that were set at CREATE time but not re-specified in the ALTER clause.
        let existing = self.index_configs.get(&index_key).cloned();

        let resolved_metric_str: String = if metric.is_empty() {
            existing
                .as_ref()
                .map(|c| {
                    match c.hnsw.metric {
                        DistanceMetric::L2 => "l2",
                        DistanceMetric::Cosine => "cosine",
                        DistanceMetric::InnerProduct => "inner_product",
                        DistanceMetric::Manhattan => "manhattan",
                        DistanceMetric::Chebyshev => "chebyshev",
                        DistanceMetric::Hamming => "hamming",
                        DistanceMetric::Jaccard => "jaccard",
                        DistanceMetric::Pearson => "pearson",
                        _ => "cosine",
                    }
                    .to_string()
                })
                .unwrap_or_else(|| "cosine".into())
        } else {
            metric.to_string()
        };

        let metric_enum = match resolved_metric_str.as_str() {
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
                    ErrorCode::DataException {
                        detail: format!(
                            "unknown metric '{resolved_metric_str}'; supported: l2, cosine, inner_product, manhattan, chebyshev, hamming, jaccard, pearson"
                        ),
                    },
                );
            }
        };

        let idx_type = if index_type.is_empty() {
            existing
                .as_ref()
                .map(|c| c.index_type.clone())
                .unwrap_or_default()
        } else {
            match crate::engine::vector::index_config::IndexType::parse(index_type) {
                Some(t) => t,
                None => {
                    return self.response_error(
                        task,
                        ErrorCode::DataException {
                            detail: format!(
                                "unknown index_type '{index_type}'; supported: hnsw, hnsw_pq, ivf_pq"
                            ),
                        },
                    );
                }
            }
        };

        let resolved_m = if m > 0 {
            m
        } else {
            existing.as_ref().map(|c| c.hnsw.m).unwrap_or(16)
        };
        let resolved_ef = if ef_construction > 0 {
            ef_construction
        } else {
            existing
                .as_ref()
                .map(|c| c.hnsw.ef_construction)
                .unwrap_or(200)
        };
        let resolved_pq_m = if pq_m > 0 {
            pq_m
        } else {
            existing
                .as_ref()
                .map_or(index_config::DEFAULT_PQ_M, |c| c.pq_m)
        };
        let resolved_ivf_cells = if ivf_cells > 0 {
            ivf_cells
        } else {
            existing
                .as_ref()
                .map_or(index_config::DEFAULT_IVF_CELLS, |c| c.ivf_cells)
        };
        let resolved_ivf_nprobe = if ivf_nprobe > 0 {
            ivf_nprobe
        } else {
            existing
                .as_ref()
                .map_or(index_config::DEFAULT_IVF_NPROBE, |c| c.ivf_nprobe)
        };

        let params = HnswParams {
            m: resolved_m,
            m0: resolved_m * 2,
            ef_construction: resolved_ef,
            metric: metric_enum,
            dtype: nodedb_types::vector_dtype::VectorStorageDtype::F32,
        };

        // `0` means the statement did not declare a dimension (ALTER, or a
        // pre-DIM index): keep whatever was declared before rather than
        // erasing an enforced width.
        let resolved_dim = if dim > 0 {
            dim
        } else {
            existing.as_ref().map(|c| c.declared_dim).unwrap_or(0)
        };

        let config = crate::engine::vector::index_config::IndexConfig {
            hnsw: params.clone(),
            index_type: idx_type,
            pq_m: resolved_pq_m,
            ivf_cells: resolved_ivf_cells,
            ivf_nprobe: resolved_ivf_nprobe,
            declared_dim: resolved_dim,
        };

        if resolved_dim > 0
            && let Err(e) = super::vector_settle::check_ivf_dim(&config, resolved_dim)
        {
            return self.response_error(task, e);
        }
        if resolved_dim > 0 {
            self.declared_dims.insert(index_key.clone(), resolved_dim);
        }
        self.vector_params.insert(index_key.clone(), params);
        self.index_configs.insert(index_key, config);
        self.response_ok(task)
    }
}
