// SPDX-License-Identifier: Apache-2.0

//! PQ-configured constructors for `VectorCollection`.

use crate::hnsw::HnswParams;
use crate::index_config::{IndexConfig, IndexType};

use super::lifecycle::VectorCollection;

impl VectorCollection {
    /// Convenience constructor for PQ-configured collections.
    ///
    /// Equivalent to building a full `IndexConfig` with
    /// `index_type = HnswPq` and the given `pq_m`.
    pub fn with_pq_config(dim: usize, hnsw: HnswParams, pq_m: usize) -> Self {
        let config = IndexConfig {
            hnsw,
            index_type: IndexType::HnswPq,
            pq_m,
            ..IndexConfig::default()
        };
        Self::with_index_config(dim, config)
    }

    /// Convenience constructor for PQ-configured collections with a custom
    /// seal threshold.
    pub fn with_seal_threshold_and_pq_config(
        dim: usize,
        hnsw: HnswParams,
        pq_m: usize,
        seal_threshold: usize,
    ) -> Self {
        let config = IndexConfig {
            hnsw,
            index_type: IndexType::HnswPq,
            pq_m,
            ..IndexConfig::default()
        };
        Self::with_seal_threshold_and_config(dim, config, seal_threshold)
    }
}
