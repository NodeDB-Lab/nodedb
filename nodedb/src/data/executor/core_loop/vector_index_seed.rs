// SPDX-License-Identifier: BUSL-1.1

//! Boot-time seeding of the per-core vector-index configuration
//! (`vector_params` + `index_configs`) from the durable system catalog.
//!
//! A `CREATE VECTOR INDEX`'s build parameters reach the Data Plane at
//! runtime as a WAL `VectorParams` record, which is NOT crash-durable: a
//! `kill -9` before the WAL group-commit flush loses it, so on reopen the
//! core has no idea the collection carries a vector index. Left unseeded,
//! the redb HNSW rebuild ([`CoreLoop::rebuild_vector_indexes_from_store`])
//! finds no registered field and re-indexes nothing, and post-restart
//! vector search returns empty.
//!
//! [`CoreLoop::seed_vector_index_params`] closes that gap: it is called
//! with the catalog-sourced [`nodedb_types::StoredVectorIndexParams`]
//! entries (built by
//! `crate::bootstrap::data_plane::load_vector_index_param_seed`) before
//! the redb HNSW rebuild, reproducing the exact `HnswParams` + `IndexConfig`
//! mapping the live `execute_set_vector_params` CREATE path produces.

use crate::engine::vector::distance::DistanceMetric;
use crate::engine::vector::hnsw::HnswParams;
use crate::engine::vector::index_config::{IndexConfig, IndexType};

use super::state::CoreLoop;

impl CoreLoop {
    /// Insert every catalog-sourced vector-index configuration into
    /// `vector_params` + `index_configs`, keyed exactly as
    /// `execute_set_vector_params` keys a live `CREATE VECTOR INDEX`.
    /// Called once at core startup, before the durable HNSW rebuild.
    ///
    /// `CREATE VECTOR INDEX` computes its vshard with the session's
    /// `database_id`, and the stored entry now carries that id. Entries
    /// written before the field existed decode to `0`
    /// (`DatabaseId::DEFAULT`) — the historical assumption of this path.
    pub fn seed_vector_index_params(&mut self, entries: &[nodedb_types::StoredVectorIndexParams]) {
        for e in entries {
            let db = e.database_id;
            let key = CoreLoop::vector_index_key(db, e.tenant_id, &e.collection, &e.field_name);
            let (params, config) = build_index_config_from_stored(e);
            if e.dim > 0 {
                self.declared_dims.insert(key.clone(), e.dim);
            }
            self.vector_params.insert(key.clone(), params);
            self.index_configs.insert(key, config);
        }
    }
}

/// Build the `(HnswParams, IndexConfig)` pair for a stored vector-index
/// entry, replicating the string-to-enum mapping and zero-value defaults
/// of the live `execute_set_vector_params` CREATE path. There is no
/// "existing config" fallback: on a fresh boot seed the durable entry is
/// the sole source of truth. An unrecognized metric string defaults to
/// `Cosine` (the CREATE-time default); durable entries are always written
/// with a validated, lowercased metric.
fn build_index_config_from_stored(
    e: &nodedb_types::StoredVectorIndexParams,
) -> (HnswParams, IndexConfig) {
    let metric_str = if e.metric.is_empty() {
        "cosine"
    } else {
        e.metric.as_str()
    };
    let metric_enum = match metric_str {
        "l2" | "euclidean" => DistanceMetric::L2,
        "cosine" => DistanceMetric::Cosine,
        "inner_product" | "ip" | "dot" => DistanceMetric::InnerProduct,
        "manhattan" | "l1" => DistanceMetric::Manhattan,
        "chebyshev" | "linf" => DistanceMetric::Chebyshev,
        "hamming" => DistanceMetric::Hamming,
        "jaccard" => DistanceMetric::Jaccard,
        "pearson" => DistanceMetric::Pearson,
        _ => DistanceMetric::Cosine,
    };

    let idx_type = IndexType::parse(&e.index_type).unwrap_or_default();

    let resolved_m = if e.m > 0 { e.m } else { 16 };
    let resolved_ef = if e.ef_construction > 0 {
        e.ef_construction
    } else {
        200
    };
    let resolved_pq_m = if e.pq_m > 0 { e.pq_m } else { 8 };
    let resolved_ivf_cells = if e.ivf_cells > 0 { e.ivf_cells } else { 256 };
    let resolved_ivf_nprobe = if e.ivf_nprobe > 0 { e.ivf_nprobe } else { 16 };

    let params = HnswParams {
        m: resolved_m,
        m0: resolved_m * 2,
        ef_construction: resolved_ef,
        metric: metric_enum,
        dtype: nodedb_types::vector_dtype::VectorStorageDtype::F32,
    };

    let config = IndexConfig {
        hnsw: params.clone(),
        index_type: idx_type,
        pq_m: resolved_pq_m,
        ivf_cells: resolved_ivf_cells,
        ivf_nprobe: resolved_ivf_nprobe,
        declared_dim: e.dim,
    };

    (params, config)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::data::executor::core_loop::tests::make_core_with_dir;

    const TID: u64 = 1;
    const COLL: &str = "crash_vec";
    const FIELD: &str = "embedding";

    fn stored(metric: &str, m: usize, ef: usize) -> nodedb_types::StoredVectorIndexParams {
        nodedb_types::StoredVectorIndexParams {
            tenant_id: TID,
            collection: COLL.to_string(),
            field_name: FIELD.to_string(),
            dim: 4,
            metric: metric.to_string(),
            m,
            ef_construction: ef,
            index_type: String::new(),
            pq_m: 0,
            ivf_cells: 0,
            ivf_nprobe: 0,
            database_id: 0,
        }
    }

    /// Seeding a catalog entry populates both `vector_params` and
    /// `index_configs` under the `DatabaseId::DEFAULT` field-qualified key,
    /// mapping the metric string and applying zero-value defaults exactly as
    /// a live CREATE would.
    #[test]
    fn seed_populates_params_and_config() {
        let dir = tempfile::tempdir().unwrap();
        let (mut core, _req_tx, _resp_rx) = make_core_with_dir(dir.path());

        core.seed_vector_index_params(&[stored("cosine", 0, 0)]);

        let db = crate::types::DatabaseId::DEFAULT.as_u64();
        let key =
            crate::data::executor::core_loop::CoreLoop::vector_index_key(db, TID, COLL, FIELD);

        let params = core.vector_params.get(&key).expect("params seeded");
        assert_eq!(params.metric, DistanceMetric::Cosine);
        assert_eq!(params.m, 16, "zero m defaults to 16");
        assert_eq!(params.m0, 32);
        assert_eq!(params.ef_construction, 200, "zero ef defaults to 200");

        let config = core.index_configs.get(&key).expect("config seeded");
        assert_eq!(config.index_type, IndexType::Hnsw);
        assert_eq!(config.pq_m, 8);
        assert_eq!(config.ivf_cells, 256);
        assert_eq!(config.ivf_nprobe, 16);
    }

    /// Explicit non-default values pass through unchanged and the metric
    /// alias ("euclidean" -> L2) maps the same way the live handler does.
    #[test]
    fn seed_respects_explicit_values_and_metric_alias() {
        let dir = tempfile::tempdir().unwrap();
        let (mut core, _req_tx, _resp_rx) = make_core_with_dir(dir.path());

        core.seed_vector_index_params(&[stored("euclidean", 32, 400)]);

        let db = crate::types::DatabaseId::DEFAULT.as_u64();
        let key =
            crate::data::executor::core_loop::CoreLoop::vector_index_key(db, TID, COLL, FIELD);
        let params = core.vector_params.get(&key).expect("params seeded");
        assert_eq!(params.metric, DistanceMetric::L2);
        assert_eq!(params.m, 32);
        assert_eq!(params.m0, 64);
        assert_eq!(params.ef_construction, 400);
    }
}
