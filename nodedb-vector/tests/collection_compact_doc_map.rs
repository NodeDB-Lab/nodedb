//! `compact()` must keep `doc_id_map` / `multi_doc_map` consistent with the
//! renumbered HNSW local IDs.
//!
//! Spec: `HnswIndex::compact()` removes tombstoned nodes and renumbers
//! surviving local node ids. The collection stores `doc_id_map` and
//! `multi_doc_map` keyed on GLOBAL ids (`seg.base_id + local`). After
//! compaction those globals shift too — the collection MUST walk both
//! maps and rewrite every entry for the compacted segment to the new
//! `(seg.base_id + new_local)` globals. Without the rewrite,
//! `get_doc_id(vid)` and `delete_multi_vector(doc)` point at stale or
//! wrong vectors.

#![cfg(feature = "collection")]

use nodedb_vector::DistanceMetric;
use nodedb_vector::collection::VectorCollection;
use nodedb_vector::hnsw::{HnswIndex, HnswParams};

fn params() -> HnswParams {
    HnswParams {
        metric: DistanceMetric::L2,
        ..HnswParams::default()
    }
}

fn build_collection_with_docs() -> VectorCollection {
    let mut coll = VectorCollection::with_seal_threshold(2, params(), 6);
    // Six docs, one vector each. Global ids 0..6.
    for i in 0..6u32 {
        coll.insert_with_doc_id(vec![i as f32, 0.0], format!("doc_{i}"));
    }
    // Seal + complete → sealed segment with base_id=0, local ids 0..6.
    let req = coll.seal("k").expect("seal produced request");
    let mut idx = HnswIndex::new(req.dim, req.params.clone());
    for v in &req.vectors {
        idx.insert(v.clone()).unwrap();
    }
    coll.complete_build(req.segment_id, idx);
    coll
}

#[test]
fn doc_id_map_stays_correct_after_compact() {
    let mut coll = build_collection_with_docs();

    // Tombstone two vectors in the middle of the sealed segment.
    assert!(coll.delete(1));
    assert!(coll.delete(3));

    // Sanity: pre-compact, the surviving doc mapping still resolves.
    assert_eq!(coll.get_doc_id(0), Some("doc_0"));
    assert_eq!(coll.get_doc_id(5), Some("doc_5"));

    let removed = coll.compact();
    assert_eq!(removed, 2, "compact should remove 2 tombstoned nodes");

    // Spec: the search results (identified by renumbered global ids) still
    // resolve to the original doc strings. For the surviving vectors
    // {0, 2, 4, 5} post-compact globals become {0, 1, 2, 3}. `get_doc_id`
    // MUST map those new globals to "doc_0", "doc_2", "doc_4", "doc_5".
    let results = coll.search(&[0.0, 0.0], 4, 64);
    let ids: Vec<u32> = results.iter().map(|r| r.id).collect();
    assert_eq!(ids.len(), 4, "expected 4 live vectors post-compact");

    let observed_docs: std::collections::HashSet<String> = ids
        .iter()
        .filter_map(|id| coll.get_doc_id(*id).map(|s| s.to_string()))
        .collect();
    let expected_docs: std::collections::HashSet<String> = ["doc_0", "doc_2", "doc_4", "doc_5"]
        .into_iter()
        .map(String::from)
        .collect();

    assert_eq!(
        observed_docs, expected_docs,
        "doc_id_map was not rewritten after compact — globals shifted but the map did not"
    );
}

#[test]
fn multi_doc_map_stays_correct_after_compact() {
    let mut coll = VectorCollection::with_seal_threshold(2, params(), 6);

    // Two multi-vector docs: doc_a owns globals 0,1,2; doc_b owns 3,4,5.
    let a_vecs: Vec<Vec<f32>> = (0..3u32).map(|i| vec![i as f32, 0.0]).collect();
    let a_refs: Vec<&[f32]> = a_vecs.iter().map(|v| v.as_slice()).collect();
    let a_ids = coll.insert_multi_vector(&a_refs, "doc_a".to_string());
    assert_eq!(a_ids, vec![0, 1, 2]);

    let b_vecs: Vec<Vec<f32>> = (3..6u32).map(|i| vec![i as f32, 0.0]).collect();
    let b_refs: Vec<&[f32]> = b_vecs.iter().map(|v| v.as_slice()).collect();
    let b_ids = coll.insert_multi_vector(&b_refs, "doc_b".to_string());
    assert_eq!(b_ids, vec![3, 4, 5]);

    let req = coll.seal("k").expect("seal produced request");
    let mut idx = HnswIndex::new(req.dim, req.params.clone());
    for v in &req.vectors {
        idx.insert(v.clone()).unwrap();
    }
    coll.complete_build(req.segment_id, idx);

    // Tombstone one vector from each doc (middle of each group).
    assert!(coll.delete(1));
    assert!(coll.delete(4));

    coll.compact();

    // Spec: `delete_multi_vector("doc_a")` must reach the two remaining
    // vectors that originally belonged to doc_a, regardless of the local
    // id renumbering performed by HnswIndex::compact.
    let deleted_a = coll.delete_multi_vector("doc_a");
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
