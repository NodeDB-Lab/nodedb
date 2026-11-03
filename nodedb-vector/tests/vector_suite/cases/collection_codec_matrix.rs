// SPDX-License-Identifier: Apache-2.0

//! Every quantization builds its codec on each sealed segment, and every
//! codec path answers with exact distances, follows deletes, undeletes and
//! compaction, and survives a restore. A graph codec stays exact across
//! repeated compactions and is dropped once stale nodes outnumber the rest.

use nodedb_types::vector_dtype::VectorStorageDtype;
use nodedb_types::{VectorIndexQuantization, VectorQuantization};
use nodedb_vector::collection::{BuildKind, SegmentCodec, VectorCollection};
use nodedb_vector::distance::distance;
use nodedb_vector::hnsw::HnswParams;
use nodedb_vector::rerank::SearchBreadth;
use nodedb_vector::{DistanceMetric, Surrogate};
use roaring::RoaringBitmap;

use crate::support::{build_now, test_memory};

const DIM: usize = 16;
const ROWS: u32 = 1200;

/// A pool that covers the codec stage of a 1200-row segment generously, so
/// these tests check ids and distances, not codec recall.
const BREADTH: SearchBreadth = SearchBreadth {
    ef_search: 128,
    oversample: 20,
};

const ALL: [(VectorQuantization, VectorIndexQuantization); 8] = [
    (VectorQuantization::None, VectorIndexQuantization::None),
    (VectorQuantization::Sq8, VectorIndexQuantization::Sq8),
    (VectorQuantization::Pq, VectorIndexQuantization::Pq),
    (VectorQuantization::RaBitQ, VectorIndexQuantization::RaBitQ),
    (VectorQuantization::Bbq, VectorIndexQuantization::Bbq),
    (VectorQuantization::Binary, VectorIndexQuantization::Binary),
    (
        VectorQuantization::Ternary,
        VectorIndexQuantization::Ternary,
    ),
    (VectorQuantization::Opq, VectorIndexQuantization::Opq),
];

/// Deterministic vector of row `i`, components in `[-1, 1)`.
fn vector(i: u32) -> Vec<f32> {
    let mut state = u64::from(i).wrapping_mul(0x9E37_79B9_7F4A_7C15) | 1;
    (0..DIM)
        .map(|_| {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            (state >> 40) as f32 / (1u64 << 23) as f32 - 1.0
        })
        .collect()
}

fn params(dtype: VectorStorageDtype) -> HnswParams {
    HnswParams {
        m: 8,
        m0: 16,
        ef_construction: 64,
        metric: DistanceMetric::L2,
        dtype,
    }
}

/// `ROWS` rows bound to surrogates `1..=ROWS`, sealed into one segment that
/// carries the codec `q` selects.
fn sealed(q: VectorQuantization, dtype: VectorStorageDtype) -> VectorCollection {
    let mut coll = VectorCollection::with_seal_threshold(DIM, params(dtype), ROWS as usize);
    coll.set_quantization(q);
    for i in 0..ROWS {
        coll.insert_with_surrogate(vector(i), Surrogate::new(i + 1))
            .unwrap();
    }
    let req = coll.seal("k", test_memory()).expect("seal");
    let segment_id = req.segment_id;
    assert!(
        coll.complete_build(segment_id, build_now(req)).installed(),
        "{q:?}"
    );
    coll
}

fn sealed_f32(q: VectorQuantization) -> VectorCollection {
    sealed(q, VectorStorageDtype::F32)
}

#[test]
fn every_quantization_reports_its_codec_after_build() {
    for (q, reported) in ALL {
        let coll = sealed_f32(q);
        assert_eq!(coll.stats().quantization, reported, "{q:?}");
        assert!(coll.sealed_ids_missing_codec().is_empty(), "{q:?}");
    }
}

#[test]
fn every_codec_returns_exact_metric_distances() {
    for (q, _) in ALL {
        let coll = sealed_f32(q);
        let query = vector(321);
        let hits = coll.search(&query, 5, BREADTH).unwrap();
        assert_eq!(hits[0].id, 321, "{q:?}: the row finds itself first");
        for hit in &hits {
            let stored = coll.vector_for_id(hit.id).expect("live hit");
            let exact = distance(&query, &stored, DistanceMetric::L2);
            assert!(
                (hit.distance - exact).abs() < 1e-5,
                "{q:?}: {hit:?} vs {exact}"
            );
        }
    }
}

#[test]
fn codec_search_skips_deleted_rows() {
    for (q, _) in ALL {
        let mut coll = sealed_f32(q);
        assert!(coll.delete(321));
        let hits = coll.search(&vector(321), 10, BREADTH).unwrap();
        assert!(hits.iter().all(|h| h.id != 321), "{q:?}: {hits:?}");
    }
}

#[test]
fn undeleted_row_is_found_again() {
    for (q, _) in ALL {
        let mut coll = sealed_f32(q);
        assert!(coll.delete(321));
        assert!(coll.undelete(321));
        let hits = coll.search(&vector(321), 1, BREADTH).unwrap();
        assert_eq!(hits[0].id, 321, "{q:?}");
    }
}

#[test]
fn codec_search_after_compaction_returns_renumbered_ids() {
    for (q, _) in ALL {
        let mut coll = sealed_f32(q);
        for id in 0..100 {
            assert!(coll.delete(id));
        }
        assert_eq!(coll.compact(), 100, "{q:?}");
        let hits = coll.search(&vector(600), 1, BREADTH).unwrap();
        assert_eq!(hits[0].id, 500, "{q:?}: old id 600 is new id 500");
        assert_eq!(hits[0].distance, 0.0, "{q:?}");
        assert_eq!(coll.get_surrogate(hits[0].id), Some(Surrogate::new(601)));
        assert_eq!(coll.stats().quantization, reported(q), "{q:?}");
    }
}

/// The statistics quantization `q` reports.
fn reported(q: VectorQuantization) -> VectorIndexQuantization {
    ALL.iter()
        .find(|(want, _)| *want == q)
        .map(|(_, reported)| *reported)
        .expect("every quantization is listed")
}

#[test]
fn graph_codec_segment_needs_rebuild_after_restore() {
    for (q, reported) in ALL {
        let coll = sealed_f32(q);
        let bytes = coll.checkpoint_to_bytes(None).unwrap();
        let mut restored = VectorCollection::from_checkpoint(&bytes, None, test_memory()).unwrap();
        let graph = matches!(coll.sealed_segments()[0].codec, SegmentCodec::Graph(_));
        let missing = restored.sealed_ids_missing_codec();
        if !graph {
            assert!(missing.is_empty(), "{q:?}: SQ8 / PQ / none persist");
            continue;
        }
        assert_eq!(missing, vec![0], "{q:?}");
        let req = restored
            .rebuild_request_for("k", 0, test_memory())
            .unwrap()
            .expect("the sealed segment at 0");
        let BuildKind::Rebuild { base_id, len } = req.kind else {
            panic!("a rebuild request");
        };
        let segment_id = req.segment_id;
        assert!(
            restored
                .complete_rebuild(segment_id, base_id, len, build_now(req))
                .installed()
        );
        assert!(restored.sealed_ids_missing_codec().is_empty(), "{q:?}");
        assert_eq!(restored.stats().quantization, reported, "{q:?}");
    }
}

#[test]
fn filtered_and_unfiltered_search_report_the_same_distance() {
    for (q, _) in ALL {
        let coll = sealed_f32(q);
        let query = vector(77);
        let hits = coll.search(&query, 3, BREADTH).unwrap();
        let filter: RoaringBitmap = hits.iter().map(|h| h.id).collect();
        let mut bytes = Vec::new();
        filter.serialize_into(&mut bytes).unwrap();
        let filtered = coll
            .search_with_bitmap_bytes(&query, 3, BREADTH, &bytes)
            .unwrap();
        for hit in &hits {
            let same = filtered
                .iter()
                .find(|f| f.id == hit.id)
                .expect("a filter member is returned");
            assert!((same.distance - hit.distance).abs() < 1e-5, "{q:?}");
        }
    }
}

#[test]
fn metric_override_rescored_on_uncoded_segment() {
    let coll = sealed_f32(VectorQuantization::None);
    let query = vector(42);
    let hits = coll
        .search_with_metric(&query, 5, BREADTH, DistanceMetric::Cosine)
        .unwrap();
    for hit in &hits {
        let stored = coll.vector_for_id(hit.id).expect("live hit");
        let cosine = distance(&query, &stored, DistanceMetric::Cosine);
        assert!((hit.distance - cosine).abs() < 1e-5, "{hit:?} vs {cosine}");
    }
}

#[test]
fn bf16_rabitq_collection_carries_its_codec() {
    let coll = sealed(VectorQuantization::RaBitQ, VectorStorageDtype::BF16);
    assert_eq!(coll.stats().quantization, VectorIndexQuantization::RaBitQ);
    let hits = coll.search(&vector(900), 1, BREADTH).unwrap();
    // BF16 storage rounds the stored vector, so the self-distance is small,
    // not zero.
    assert!(hits[0].distance < 1e-3, "{hits:?}");
}

/// The quantizations that keep a codec graph per segment.
const GRAPH: [VectorQuantization; 5] = [
    VectorQuantization::RaBitQ,
    VectorQuantization::Bbq,
    VectorQuantization::Binary,
    VectorQuantization::Ternary,
    VectorQuantization::Opq,
];

/// Every hit of `query` carries the exact L2 distance to its stored vector.
fn assert_exact(coll: &VectorCollection, query: &[f32], q: VectorQuantization) {
    for hit in coll.search(query, 5, BREADTH).unwrap() {
        let stored = coll.vector_for_id(hit.id).expect("live hit");
        let exact = distance(query, &stored, DistanceMetric::L2);
        assert!(
            (hit.distance - exact).abs() < 1e-5,
            "{q:?}: {hit:?} vs {exact}"
        );
    }
}

/// Rebuild the sealed segment at 0 and install it.
fn rebuild_segment_zero(coll: &mut VectorCollection) {
    let req = coll
        .rebuild_request_for("k", 0, test_memory())
        .unwrap()
        .expect("the sealed segment at 0");
    let BuildKind::Rebuild { base_id, len } = req.kind else {
        panic!("a rebuild request");
    };
    let segment_id = req.segment_id;
    assert!(
        coll.complete_rebuild(segment_id, base_id, len, build_now(req))
            .installed()
    );
}

#[test]
fn two_compactions_in_a_row_keep_graph_codec_ids_exact() {
    for q in GRAPH {
        let mut coll = sealed_f32(q);
        for id in 0..100 {
            assert!(coll.delete(id));
        }
        assert_eq!(coll.compact(), 100, "{q:?}");
        // New ids 0..50 are old ids 100..150.
        for id in 0..50 {
            assert!(coll.delete(id));
        }
        assert_eq!(coll.compact(), 50, "{q:?}");
        // Old ids 160 and 700 are new ids 10 and 550; deleting one hides
        // that row only. New id 10 is an id a stale node once carried.
        assert!(coll.delete(10));
        assert!(coll.delete(550));

        let hits = coll.search(&vector(600), 1, BREADTH).unwrap();
        assert_eq!(hits[0].id, 450, "{q:?}: old id 600 is new id 450");
        assert_eq!(hits[0].distance, 0.0, "{q:?}");
        assert_eq!(coll.get_surrogate(450), Some(Surrogate::new(601)));
        for (old, new) in [(160u32, 10u32), (700, 550)] {
            let hidden = coll.search(&vector(old), 10, BREADTH).unwrap();
            assert!(hidden.iter().all(|h| h.id != new), "{q:?}: {hidden:?}");
        }
        assert_exact(&coll, &vector(900), q);
        assert_eq!(coll.stats().quantization, reported(q), "{q:?}");
    }
}

#[test]
fn a_restored_graph_codec_rebuilt_with_tombstones_hides_deleted_rows() {
    for q in GRAPH {
        let mut coll = sealed_f32(q);
        assert!(coll.delete(10));
        let bytes = coll.checkpoint_to_bytes(None).unwrap();
        let mut restored = VectorCollection::from_checkpoint(&bytes, None, test_memory()).unwrap();
        assert!(restored.delete(20));
        let req = restored
            .rebuild_request_for("k", 0, test_memory())
            .unwrap()
            .expect("the sealed segment at 0");
        // A delete that lands while the rebuild runs is kept too.
        assert!(restored.delete(30));
        let BuildKind::Rebuild { base_id, len } = req.kind else {
            panic!("a rebuild request");
        };
        let segment_id = req.segment_id;
        assert!(
            restored
                .complete_rebuild(segment_id, base_id, len, build_now(req))
                .installed()
        );

        assert_eq!(restored.stats().quantization, reported(q), "{q:?}");
        for gone in [10u32, 20, 30] {
            let hits = restored.search(&vector(gone), 10, BREADTH).unwrap();
            assert!(
                hits.iter().all(|h| h.id != gone),
                "{q:?}: {gone} in {hits:?}"
            );
        }
        assert_eq!(restored.search(&vector(40), 1, BREADTH).unwrap()[0].id, 40);
        assert_exact(&restored, &vector(40), q);
    }
}

#[test]
fn a_compaction_that_removes_most_nodes_drops_the_graph_codec() {
    for q in GRAPH {
        let mut coll = sealed_f32(q);
        for id in 0..700 {
            assert!(coll.delete(id));
        }
        assert_eq!(coll.compact(), 700, "{q:?}");
        // 700 stale routing nodes outnumber the 500 that remain.
        assert_eq!(coll.sealed_ids_missing_codec(), vec![0], "{q:?}");
        assert_eq!(coll.stats().quantization, VectorIndexQuantization::None);
        let hits = coll.search(&vector(900), 1, BREADTH).unwrap();
        assert_eq!(hits[0].id, 200, "{q:?}: old id 900 is new id 200");
        assert_eq!(hits[0].distance, 0.0, "{q:?}");

        rebuild_segment_zero(&mut coll);
        assert!(coll.sealed_ids_missing_codec().is_empty(), "{q:?}");
        assert_eq!(coll.stats().quantization, reported(q), "{q:?}");
        assert_exact(&coll, &vector(900), q);
    }
}
