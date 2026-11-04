// SPDX-License-Identifier: BUSL-1.1

//! Restored vector-index configuration and the vector section's key shape.
//!
//! RESTORE re-issues each index's configuration as a durable
//! `VectorOp::SetParams`, proposed through Raft, before the index's rows
//! re-issue as Calvin transactions. Each plan names its collection by the
//! stored, database-qualified name.

use crate::bridge::envelope::PhysicalPlan;
use crate::engine::vector::index_config::{IndexConfig, IndexType};
use nodedb_physical::physical_plan::VectorOp;
use nodedb_types::vector_distance::DistanceMetric;

/// Split a vector snapshot's `coll_key` back into its `(collection,
/// field_name)` parts.
///
/// `CoreLoop::vector_index_key` builds `coll_key` as `collection` (default
/// field) or `collection:field_name` (named field); collection and field
/// names never contain `:`, so a single split reverses it exactly.
pub fn split_vector_coll_key(coll_key: &str) -> (&str, &str) {
    coll_key.split_once(':').unwrap_or((coll_key, ""))
}

/// Map a decoded `DistanceMetric` back to the string form `VectorOp::SetParams`
/// and `execute_set_vector_params` expect (mirrors the inverse mapping in
/// `execute_set_vector_params`, `handlers/vector_params.rs`).
fn metric_to_str(metric: DistanceMetric) -> &'static str {
    match metric {
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
}

/// Map a decoded `IndexType` back to the string form `VectorOp::SetParams`
/// expects (`IndexType::parse` is the inverse).
fn index_type_to_str(index_type: &IndexType) -> &'static str {
    match index_type {
        IndexType::Hnsw => "hnsw",
        IndexType::HnswPq => "hnsw_pq",
        IndexType::IvfPq => "ivf_pq",
        _ => "hnsw",
    }
}

/// Build the durable `VectorOp::SetParams` plan for one restored
/// (collection, field) HNSW index configuration.
///
/// The caller is responsible for resolving a restored `IndexConfig` snapshot
/// entry, or — when only the older `vector_params`-only section is present
/// for a (collection, field) — wrapping the decoded `HnswParams` in
/// `IndexConfig { hnsw, ..IndexConfig::default() }` before calling this.
pub fn build_vector_set_params_plan(
    collection: nodedb_types::QualifiedCollection,
    field_name: &str,
    config: &IndexConfig,
) -> PhysicalPlan {
    PhysicalPlan::Vector(VectorOp::SetParams {
        collection,
        field_name: field_name.to_string(),
        dim: config.declared_dim,
        m: config.hnsw.m,
        ef_construction: config.hnsw.ef_construction,
        metric: metric_to_str(config.hnsw.metric).to_string(),
        index_type: index_type_to_str(&config.index_type).to_string(),
        pq_m: config.pq_m,
        ivf_cells: config.ivf_cells,
        ivf_nprobe: config.ivf_nprobe,
    })
}
