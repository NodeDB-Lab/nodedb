//! Soft-deletes in growing / building segments must survive checkpoint restore.
//!
//! Spec: `delete(id)` on a vector in the growing segment (or a building
//! segment awaiting HNSW completion) tombstones the vector. Checkpoints
//! MUST serialize that tombstone, and `from_checkpoint` MUST apply it so
//! the restored collection reports the same `live_count()` and excludes
//! the deleted vector from `search()` results.
//!
//! Today:
//!   - `FlatIndex::get_vector` returns `Some(..)` even for tombstoned
//!     slots, so `growing_deleted` is serialized as all-false.
//!   - `from_checkpoint` ignores the `growing_deleted` field entirely and
//!     re-inserts every vector as live.
//!
//! Result: crash recovery silently resurrects soft-deleted rows — a
//! correctness regression for any workflow using `valid_until` deletes.

#![cfg(feature = "collection")]

use nodedb_vector::DistanceMetric;
use nodedb_vector::collection::VectorCollection;
use nodedb_vector::hnsw::HnswParams;

fn params() -> HnswParams {
    HnswParams {
        metric: DistanceMetric::L2,
        ..HnswParams::default()
    }
}

#[test]
fn growing_segment_tombstones_survive_checkpoint_roundtrip() {
    let mut coll = VectorCollection::new(2, params());
    for i in 0..10u32 {
        coll.insert(vec![i as f32, 0.0]);
    }
    assert!(coll.delete(3), "delete on live growing vector must succeed");
    assert!(coll.delete(7), "delete on live growing vector must succeed");
    let live_before = coll.live_count();
    assert_eq!(live_before, 8);

    let bytes = coll.checkpoint_to_bytes();
    let restored = VectorCollection::from_checkpoint(&bytes).expect("checkpoint deserializes");

    assert_eq!(
        restored.live_count(),
        live_before,
        "tombstoned growing-segment vectors resurrected on restore"
    );

    let results = restored.search(&[3.0, 0.0], 10, 64);
    let ids: std::collections::HashSet<u32> = results.iter().map(|r| r.id).collect();
    assert!(
        !ids.contains(&3),
        "soft-deleted id=3 reappeared in search after restore"
    );
    assert!(
        !ids.contains(&7),
        "soft-deleted id=7 reappeared in search after restore"
    );
}

#[test]
fn building_segment_tombstones_survive_checkpoint_roundtrip() {
    // Force a seal so the deleted rows live in a building segment at
    // snapshot time, exercising the `building_segments` encode path.
    let mut coll = VectorCollection::with_seal_threshold(2, params(), 20);
    for i in 0..20u32 {
        coll.insert(vec![i as f32, 0.0]);
    }
    let _req = coll.seal("k").expect("seal produced request");
    // Intentionally do NOT complete the build — vectors now sit in the
    // building segment as a FlatIndex.
    assert!(coll.delete(5), "delete on building vector must succeed");
    assert!(coll.delete(15), "delete on building vector must succeed");
    let live_before = coll.live_count();
    assert_eq!(live_before, 18);

    let bytes = coll.checkpoint_to_bytes();
    let restored = VectorCollection::from_checkpoint(&bytes).expect("checkpoint deserializes");

    assert_eq!(
        restored.live_count(),
        live_before,
        "tombstoned building-segment vectors resurrected on restore"
    );

    let results = restored.search(&[5.0, 0.0], 20, 64);
    let ids: std::collections::HashSet<u32> = results.iter().map(|r| r.id).collect();
    assert!(
        !ids.contains(&5),
        "soft-deleted id=5 reappeared after restore"
    );
    assert!(
        !ids.contains(&15),
        "soft-deleted id=15 reappeared after restore"
    );
}
