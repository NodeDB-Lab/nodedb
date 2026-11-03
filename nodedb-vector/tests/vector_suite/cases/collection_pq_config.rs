// SPDX-License-Identifier: Apache-2.0

//! `INDEX_TYPE hnsw_pq` makes every sealed segment carry PQ codes and report
//! `VectorIndexQuantization::Pq`. A PQ shape the codec cannot train fails
//! the build.

use nodedb_types::VectorIndexQuantization;
use nodedb_types::vector_dtype::VectorStorageDtype;
use nodedb_vector::collection::VectorCollection;
use nodedb_vector::hnsw::HnswParams;
use nodedb_vector::{DistanceMetric, VectorError, build_segment};

use crate::support::{build_now, test_memory};

fn params() -> HnswParams {
    HnswParams {
        m: 16,
        m0: 32,
        ef_construction: 100,
        metric: DistanceMetric::L2,
        ..HnswParams::default()
    }
}

/// Vector `i` of dim 8.
fn vector(i: u32) -> Vec<f32> {
    (0..8)
        .map(|d| ((i as f32) * 0.01 + (d as f32) * 0.1).sin())
        .collect()
}

/// A PQ collection (`pq_m` 2) of 1024 vectors of dim 8 under `params`,
/// sealed. Returns the collection and the seal's build request.
fn sealed_pq_collection(params: HnswParams) -> (VectorCollection, nodedb_vector::BuildRequest) {
    let mut coll = VectorCollection::with_seal_threshold_and_pq_config(8, params, 2, 1024);
    for i in 0..1024u32 {
        coll.insert(vector(i)).unwrap();
    }
    let req = coll
        .seal("pq", test_memory())
        .expect("seal produced request");
    (coll, req)
}

/// [`sealed_pq_collection`] with its build installed.
fn make_built_collection_with_pq_config() -> VectorCollection {
    let (mut coll, req) = sealed_pq_collection(params());
    let segment_id = req.segment_id;
    assert!(coll.complete_build(segment_id, build_now(req)).installed());
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
    let bytes = coll.checkpoint_to_bytes(None).unwrap();
    let restored = VectorCollection::from_checkpoint(&bytes, None, test_memory())
        .expect("checkpoint must deserialize for PQ-configured collection");
    let stats = restored.stats();
    assert_eq!(
        stats.quantization,
        nodedb_types::VectorIndexQuantization::Pq,
        "PQ codec must survive checkpoint roundtrip; got {:?}",
        stats.quantization
    );
}

#[test]
fn hnsw_pq_with_pq_m_not_dividing_dim_installs_the_graph_without_codes() {
    let mut coll = VectorCollection::with_seal_threshold_and_pq_config(8, params(), 3, 16);
    for i in 0..16u32 {
        coll.insert(vector(i)).unwrap();
    }
    let req = coll.seal("pq", test_memory()).expect("seal");
    let segment_id = req.segment_id;
    let built = build_segment(7, req).expect("the graph builds");
    let Err(err) = &built.codec else {
        panic!("pq_m 3 does not divide dim 8");
    };
    assert!(matches!(err, VectorError::InvalidInput { .. }), "{err}");
    assert!(coll.complete_build(segment_id, built).installed());
    assert_eq!(coll.stats().quantization, VectorIndexQuantization::None);
    assert_eq!(coll.stats().building_count, 0, "the graph is installed");
    assert_eq!(coll.search(&vector(5), 1, 64).unwrap()[0].id, 5);
}

#[test]
fn hnsw_pq_segment_with_every_node_deleted_carries_codes() {
    let (mut coll, req) = sealed_pq_collection(params());
    for id in 0..1024u32 {
        assert!(coll.delete(id));
    }
    let segment_id = req.segment_id;
    assert!(coll.complete_build(segment_id, build_now(req)).installed());
    assert_eq!(coll.stats().quantization, VectorIndexQuantization::Pq);
    assert!(coll.search(&vector(3), 5, 64).unwrap().is_empty());
}

#[test]
fn hnsw_pq_bf16_segment_carries_codes() {
    let bf16 = HnswParams {
        dtype: VectorStorageDtype::BF16,
        ..params()
    };
    let (mut coll, req) = sealed_pq_collection(bf16);
    let segment_id = req.segment_id;
    assert!(coll.complete_build(segment_id, build_now(req)).installed());
    assert_eq!(coll.stats().quantization, VectorIndexQuantization::Pq);
    // BF16 rounding can make neighbours equal, so the check is the distance.
    let hits = coll.search(&vector(500), 1, 64).unwrap();
    assert!(hits[0].distance < 1e-3, "{hits:?}");
}
