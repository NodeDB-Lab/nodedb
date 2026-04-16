//! Roaring bitmap pre-filter must use the same ID space across segments.
//!
//! Spec: the query planner builds a Roaring bitmap from GLOBAL vector IDs.
//! `search_with_bitmap_bytes` walks each sealed segment and the segment's
//! HNSW index tests `filter.contains(id)` against the segment-LOCAL id.
//! The collection MUST reconcile the two — either by rewriting the bitmap
//! per-segment (subtract `seg.base_id`) or by applying the offset before
//! `f.contains(id)`. Without that, every segment beyond the first silently
//! drops all filtered candidates because global ≠ local.

#![cfg(feature = "collection")]

use nodedb_vector::DistanceMetric;
use nodedb_vector::collection::VectorCollection;
use nodedb_vector::hnsw::{HnswIndex, HnswParams};
use roaring::RoaringBitmap;

fn params() -> HnswParams {
    HnswParams {
        metric: DistanceMetric::L2,
        ..HnswParams::default()
    }
}

/// Fill a collection's growing segment, seal it, complete the build,
/// so the next inserts land at `base_id == seal_count`.
fn seal_one(coll: &mut VectorCollection, count: usize) {
    for i in 0..count {
        coll.insert(vec![i as f32, 0.0]);
    }
    let req = coll.seal("k").expect("seal produced request");
    let mut idx = HnswIndex::new(req.dim, req.params.clone());
    for v in &req.vectors {
        idx.insert(v.clone()).unwrap();
    }
    coll.complete_build(req.segment_id, idx);
}

fn bitmap_bytes(ids: impl IntoIterator<Item = u32>) -> Vec<u8> {
    let mut bm = RoaringBitmap::new();
    for id in ids {
        bm.insert(id);
    }
    let mut bytes = Vec::new();
    bm.serialize_into(&mut bytes).unwrap();
    bytes
}

#[test]
fn bitmap_filter_targets_second_segment_global_ids() {
    let mut coll = VectorCollection::with_seal_threshold(2, params(), 50);
    seal_one(&mut coll, 50); // segment 0: ids 0..50, base_id = 0
    seal_one(&mut coll, 50); // segment 1: ids 50..100, base_id = 50

    // Query for a point near id=75 (in segment 1). Filter to only global
    // id 75. Correct behavior: returns id=75. Buggy behavior: the second
    // segment's bitmap lookup tests local id 25 against a bitmap that
    // contains global 75 → zero matches.
    let bytes = bitmap_bytes([75u32]);
    let results = coll.search_with_bitmap_bytes(&[75.0, 0.0], 1, 64, &bytes);

    assert_eq!(
        results.len(),
        1,
        "global-id bitmap filter dropped all candidates in segment 1"
    );
    assert_eq!(results[0].id, 75);
}

#[test]
fn bitmap_filter_recovers_many_globals_across_segments() {
    let mut coll = VectorCollection::with_seal_threshold(2, params(), 50);
    seal_one(&mut coll, 50);
    seal_one(&mut coll, 50);

    // Select globals from the second segment only.
    let wanted: Vec<u32> = (60..70).collect();
    let bytes = bitmap_bytes(wanted.iter().copied());

    let results = coll.search_with_bitmap_bytes(&[65.0, 0.0], 10, 128, &bytes);

    assert_eq!(
        results.len(),
        wanted.len(),
        "expected all {} second-segment globals to match; got {}",
        wanted.len(),
        results.len()
    );
    let got: std::collections::HashSet<u32> = results.iter().map(|r| r.id).collect();
    for id in &wanted {
        assert!(
            got.contains(id),
            "missing expected id {id} from filtered results"
        );
    }
}

#[test]
fn bitmap_filter_first_segment_still_works() {
    // Regression guard for the partial-accident: segment 0 has base_id=0 so
    // local==global and filtering appears to work. This test pins that down
    // so a fix to the second-segment path doesn't regress segment 0.
    let mut coll = VectorCollection::with_seal_threshold(2, params(), 50);
    seal_one(&mut coll, 50);
    seal_one(&mut coll, 50);

    let bytes = bitmap_bytes([10u32, 20, 30]);
    let results = coll.search_with_bitmap_bytes(&[20.0, 0.0], 3, 64, &bytes);
    let got: std::collections::HashSet<u32> = results.iter().map(|r| r.id).collect();
    let expected: std::collections::HashSet<u32> = [10u32, 20, 30].into_iter().collect();
    assert_eq!(got, expected);
}
