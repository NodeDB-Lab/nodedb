// SPDX-License-Identifier: Apache-2.0

//! `compact()` must keep `surrogate_map` / `multi_doc_map` consistent with
//! the renumbered HNSW local IDs.
//!
//! Spec: `HnswIndex::compact()` removes tombstoned nodes and renumbers
//! surviving local node ids. The collection stores `surrogate_map` and
//! `multi_doc_map` keyed on GLOBAL ids (`seg.base_id + local`). After
//! compaction those globals shift too — the collection MUST walk both
//! maps and rewrite every entry for the compacted segment to the new
//! `(seg.base_id + new_local)` globals. Without the rewrite,
//! `get_surrogate(vid)` and `delete_multi_vector(doc_surrogate)` point
//! at stale or wrong vectors.

use nodedb_vector::DistanceMetric;
use nodedb_vector::Surrogate;
use nodedb_vector::collection::VectorCollection;
use nodedb_vector::hnsw::HnswParams;

use crate::support::{build_now, test_memory};

fn params() -> HnswParams {
    HnswParams {
        metric: DistanceMetric::L2,
        ..HnswParams::default()
    }
}

fn build_collection_with_docs() -> VectorCollection {
    let mut coll = VectorCollection::with_seal_threshold(2, params(), 6);
    // Six docs, one vector each. Global ids 0..6, surrogates 1..=6.
    for i in 0..6u32 {
        coll.insert_with_surrogate(vec![i as f32, 0.0], Surrogate::new(i + 1))
            .unwrap();
    }
    let req = coll
        .seal("k", test_memory())
        .expect("seal produced request");
    let segment_id = req.segment_id;
    assert!(coll.complete_build(segment_id, build_now(req)).installed());
    coll
}

#[test]
fn surrogate_map_stays_correct_after_compact() {
    let mut coll = build_collection_with_docs();

    // Tombstone two vectors in the middle of the sealed segment.
    assert!(coll.delete(1));
    assert!(coll.delete(3));

    // Sanity: pre-compact, the surviving surrogate mapping still resolves.
    assert_eq!(coll.get_surrogate(0), Some(Surrogate::new(1)));
    assert_eq!(coll.get_surrogate(5), Some(Surrogate::new(6)));

    let removed = coll.compact();
    assert_eq!(removed, 2, "compact should remove 2 tombstoned nodes");

    let results = coll.search(&[0.0, 0.0], 4, 64).unwrap();
    let ids: Vec<u32> = results.iter().map(|r| r.id).collect();
    assert_eq!(ids.len(), 4, "expected 4 live vectors post-compact");

    let observed_surrogates: std::collections::HashSet<u32> = ids
        .iter()
        .filter_map(|id| coll.get_surrogate(*id).map(|s| s.as_u32()))
        .collect();
    let expected_surrogates: std::collections::HashSet<u32> = [1, 3, 5, 6].into_iter().collect();

    assert_eq!(
        observed_surrogates, expected_surrogates,
        "surrogate_map was not rewritten after compact — globals shifted but the map did not"
    );
}

#[test]
fn multi_doc_map_stays_correct_after_compact() {
    let mut coll = VectorCollection::with_seal_threshold(2, params(), 6);

    let doc_a = Surrogate::new(100);
    let doc_b = Surrogate::new(200);

    let a_vecs: Vec<Vec<f32>> = (0..3u32).map(|i| vec![i as f32, 0.0]).collect();
    let a_refs: Vec<&[f32]> = a_vecs.iter().map(|v| v.as_slice()).collect();
    let a_ids = coll.insert_multi_vector(&a_refs, doc_a).unwrap();
    assert_eq!(a_ids, vec![0, 1, 2]);

    let b_vecs: Vec<Vec<f32>> = (3..6u32).map(|i| vec![i as f32, 0.0]).collect();
    let b_refs: Vec<&[f32]> = b_vecs.iter().map(|v| v.as_slice()).collect();
    let b_ids = coll.insert_multi_vector(&b_refs, doc_b).unwrap();
    assert_eq!(b_ids, vec![3, 4, 5]);

    let req = coll
        .seal("k", test_memory())
        .expect("seal produced request");
    let segment_id = req.segment_id;
    assert!(coll.complete_build(segment_id, build_now(req)).installed());

    assert!(coll.delete(1));
    assert!(coll.delete(4));

    coll.compact();

    let deleted_a = coll.delete_multi_vector(doc_a);
    assert_eq!(
        deleted_a, 2,
        "delete_multi_vector(doc_a) must find its 2 remaining vectors after compact"
    );

    let live_after = coll.live_count();
    assert_eq!(
        live_after, 2,
        "post-compact + doc_a delete: only doc_b's 2 remaining vectors survive"
    );
}
