//! `index_type='hnsw_pq'` must produce PQ-compressed segments.
//!
//! Spec: when a collection is configured for HNSW+PQ (advertised via the
//! SQL DDL `CREATE INDEX ... WITH (index_type='hnsw_pq', pq_m=...)`),
//! `complete_build` MUST train a `PqCodec` on the finished segment and
//! surface `VectorIndexQuantization::Pq` in `stats()`. Today, the config
//! is accepted at the DDL layer, stored, and then ignored —
//! `complete_build` unconditionally calls `build_sq8_for_index`, so
//! operators who asked for 8-16× memory reduction silently receive 4×
//! SQ8 and have no signal from `stats()` that the request was dropped.

#![cfg(feature = "collection")]

use nodedb_vector::DistanceMetric;
use nodedb_vector::collection::VectorCollection;
use nodedb_vector::hnsw::{HnswIndex, HnswParams};

fn params() -> HnswParams {
    HnswParams {
        m: 16,
        m0: 32,
        ef_construction: 100,
        metric: DistanceMetric::L2,
    }
}

/// Build a collection with 1024 vectors of dim=8 and complete one segment
/// build. The `>= 1000` vector threshold in `build_sq8_for_index` means a
/// quantizer WILL be attached — so `stats().quantization` is either `Sq8`
/// (buggy fallback) or `Pq` (spec-correct for a HnswPq-configured index).
fn make_built_collection_with_pq_config() -> VectorCollection {
    // Uses the convenience constructor `with_seal_threshold_and_pq_config` so
    // callers don't have to hand-build a full `IndexConfig` just to request PQ.
    let mut coll = VectorCollection::with_seal_threshold_and_pq_config(8, params(), 2, 1024);
    for i in 0..1024u32 {
        let mut v = vec![0.0f32; 8];
        for (d, slot) in v.iter_mut().enumerate() {
            *slot = ((i as f32) * 0.01 + (d as f32) * 0.1).sin();
        }
        coll.insert(v);
    }
    let req = coll.seal("pq").expect("seal produced request");
    let mut idx = HnswIndex::new(req.dim, req.params.clone());
    for v in &req.vectors {
        idx.insert(v.clone()).unwrap();
    }
    coll.complete_build(req.segment_id, idx);
    coll
}

#[test]
fn hnsw_pq_config_produces_pq_quantization() {
    let coll = make_built_collection_with_pq_config();
    let stats = coll.stats();
    assert_eq!(
        stats.quantization,
        nodedb_types::VectorIndexQuantization::Pq,
        "index_type='hnsw_pq' must produce PQ-compressed segments and \
         report VectorIndexQuantization::Pq; got {:?}",
        stats.quantization
    );
}

#[test]
fn hnsw_pq_config_stats_index_type_reports_hnsw_pq() {
    let coll = make_built_collection_with_pq_config();
    let stats = coll.stats();
    assert_eq!(
        stats.index_type,
        nodedb_types::VectorIndexType::HnswPq,
        "stats().index_type must reflect the configured HnswPq index"
    );
}

#[test]
fn hnsw_pq_config_survives_checkpoint_roundtrip() {
    let coll = make_built_collection_with_pq_config();
    let bytes = coll.checkpoint_to_bytes();
    let restored = VectorCollection::from_checkpoint(&bytes)
        .expect("checkpoint must deserialize for PQ-configured collection");
    let stats = restored.stats();
    assert_eq!(
        stats.quantization,
        nodedb_types::VectorIndexQuantization::Pq,
        "PQ codec must survive checkpoint roundtrip; got {:?}",
        stats.quantization
    );
}
