// SPDX-License-Identifier: Apache-2.0

use super::*;
use crate::test_support::{planar_srid, test_memory};

fn make_entry(id: u64, lng: f64, lat: f64) -> RTreeEntry {
    RTreeEntry {
        id,
        bbox: BoundingBox::from_point(lng, lat),
    }
}

fn raw_checkpoint(
    entries: Vec<RTreeEntry>,
    live_starts: Vec<LiveStart>,
    ended: Vec<EndedEntry>,
) -> Vec<u8> {
    let snapshot = RTreeSnapshotRkyv {
        srid: TreeSrid::WGS84.srid.code(),
        planar: false,
        entries,
        live_starts,
        ended,
    };
    let payload = rkyv::to_bytes::<rkyv::rancor::Error>(&snapshot).unwrap();
    let mut bytes = RTREE_RKYV_MAGIC.to_vec();
    bytes.push(RTREE_FORMAT_VERSION);
    bytes.extend_from_slice(&payload);
    bytes
}

fn invalid_history_detail(bytes: &[u8]) -> String {
    match RTree::from_checkpoint(bytes, None, test_memory()) {
        Err(RTreeCheckpointError::RkyvDeserialize(detail)) => detail,
        Err(other) => panic!("unexpected checkpoint error: {other}"),
        Ok(_) => panic!("malformed live history must return a checkpoint error"),
    }
}

#[test]
fn checkpoint_roundtrip_empty() {
    let tree = RTree::new(test_memory(), TreeSrid::WGS84);
    let bytes = tree.checkpoint_to_bytes(None).unwrap();
    let restored = RTree::from_checkpoint(&bytes, None, test_memory()).unwrap();
    assert_eq!(restored.len(), 0);
}

#[test]
fn checkpoint_roundtrip_entries() {
    let mut tree = RTree::new(test_memory(), TreeSrid::WGS84);
    for i in 0..100 {
        tree.insert(make_entry(i, (i as f64) * 0.5, (i as f64) * 0.3));
    }
    assert_eq!(tree.len(), 100);

    let bytes = tree.checkpoint_to_bytes(None).unwrap();
    let restored = RTree::from_checkpoint(&bytes, None, test_memory()).unwrap();
    assert_eq!(restored.len(), 100);

    // All entries should be searchable.
    let all = restored.search(&BoundingBox::new(-180.0, -90.0, 180.0, 90.0));
    assert_eq!(all.len(), 100);
}

#[test]
fn checkpoint_preserves_ids() {
    let mut tree = RTree::new(test_memory(), TreeSrid::WGS84);
    tree.insert(make_entry(42, 10.0, 20.0));
    tree.insert(make_entry(99, 30.0, 40.0));

    let bytes = tree.checkpoint_to_bytes(None).unwrap();
    let restored = RTree::from_checkpoint(&bytes, None, test_memory()).unwrap();

    let results = restored.search(&BoundingBox::new(5.0, 15.0, 15.0, 25.0));
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].id, 42);
}

#[test]
fn corrupted_bytes_returns_error() {
    assert!(matches!(
        RTree::from_checkpoint(&[0xFF, 0xFF, 0xFF], None, test_memory()),
        Err(RTreeCheckpointError::UnrecognizedFormat)
    ));
}

#[test]
fn meta_roundtrip() {
    let meta = SpatialIndexMeta {
        collection: "buildings".to_string(),
        field: "geom".to_string(),
        index_type: SpatialIndexType::RTree,
        entry_count: 1000,
        extent: Some(BoundingBox::new(-180.0, -90.0, 180.0, 90.0)),
        srid: Srid::new(3375).unwrap(),
    };
    let bytes = serialize_meta(&meta).unwrap();
    let restored = deserialize_meta(&bytes).unwrap();
    assert_eq!(restored.collection, "buildings");
    assert_eq!(restored.entry_count, 1000);
    assert_eq!(restored.index_type, SpatialIndexType::RTree);
    assert_eq!(restored.srid, Srid::new(3375).unwrap());
}

#[test]
fn checkpoint_preserves_srid() {
    let mut tree = RTree::new(test_memory(), planar_srid(3376));
    tree.insert(make_entry(7, 708_697.0, 662_326.0));
    let bytes = tree.checkpoint_to_bytes(None).unwrap();
    let restored = RTree::from_checkpoint(&bytes, None, test_memory()).unwrap();
    assert_eq!(restored.tree_srid(), planar_srid(3376));
    assert_eq!(restored.len(), 1);
}

#[test]
fn storage_key_format() {
    let key = rtree_storage_key("buildings", "geom");
    assert_eq!(key, b"buildings\0geom\0rtree");

    let meta_key = meta_storage_key("buildings", "geom");
    assert_eq!(meta_key, b"buildings\0geom\0meta");
}

#[test]
fn checkpoint_size_reasonable() {
    let mut tree = RTree::new(test_memory(), TreeSrid::WGS84);
    for i in 0..1000 {
        tree.insert(make_entry(i, (i as f64) * 0.01, (i as f64) * 0.01));
    }
    let bytes = tree.checkpoint_to_bytes(None).unwrap();
    // Each entry: id(8) + 4 f64(32) = ~40 bytes + rkyv overhead.
    // 1000 entries ≈ 40-60KB is reasonable.
    assert!(
        bytes.len() < 100_000,
        "checkpoint too large: {} bytes",
        bytes.len()
    );
    assert!(
        bytes.len() > 10_000,
        "checkpoint too small: {} bytes",
        bytes.len()
    );
}

#[test]
fn checkpoint_preserves_history() {
    let mut tree = RTree::new(test_memory(), TreeSrid::WGS84);
    tree.insert_at(make_entry(1, 10.0, 20.0), 10);
    tree.end_at(1, "d1", 20);
    tree.insert_at(make_entry(1, 11.0, 21.0), 20);
    let bytes = tree.checkpoint_to_bytes(None).unwrap();
    let restored = RTree::from_checkpoint(&bytes, None, test_memory()).unwrap();
    assert_eq!(restored.history_rows(), tree.history_rows());
    let world = BoundingBox::new(-180.0, -90.0, 180.0, 90.0);
    assert_eq!(restored.search_at(&world, 15).len(), 1);
    assert!(matches!(
        restored.search_at(&world, 15).as_slice(),
        [crate::rtree::CutCandidate::Ended(e)] if e.sys_to == 20
    ));
}

#[test]
fn plain_deletion_preserves_remaining_checkpoint_history() {
    let mut tree = RTree::new(test_memory(), TreeSrid::WGS84);
    tree.insert_at(make_entry(1, 10.0, 20.0), 10);
    tree.insert_at(make_entry(2, 30.0, 40.0), 20);
    assert!(tree.delete(1));
    assert!(!tree.delete(99));
    let expected = HistoryRows {
        live: vec![LiveStart {
            entry_id: 2,
            sys_from: 20,
        }],
        ended: Vec::new(),
    };
    assert_eq!(tree.history_rows(), expected);

    let bytes = tree.checkpoint_to_bytes(None).unwrap();
    let restored = RTree::from_checkpoint(&bytes, None, test_memory()).unwrap();
    assert_eq!(restored.history_rows(), expected);
    let world = BoundingBox::new(-180.0, -90.0, 180.0, 90.0);
    assert!(restored.search_at(&world, 19).is_empty());
    assert!(matches!(
        restored.search_at(&world, 20).as_slice(),
        [crate::rtree::CutCandidate::Live(2)]
    ));
}

#[test]
fn checkpoint_rejects_duplicate_live_starts() {
    for ordinals in [[10, 20], [20, 10], [10, 10]] {
        let bytes = raw_checkpoint(
            vec![make_entry(7, 10.0, 20.0)],
            ordinals
                .into_iter()
                .map(|sys_from| LiveStart {
                    entry_id: 7,
                    sys_from,
                })
                .collect(),
            Vec::new(),
        );
        assert_eq!(
            invalid_history_detail(&bytes),
            "duplicate live start for entry 7"
        );
    }
}

#[test]
fn checkpoint_rejects_live_starts_without_live_entries() {
    for entries in [Vec::new(), vec![make_entry(7, 10.0, 20.0)]] {
        let bytes = raw_checkpoint(
            entries,
            vec![LiveStart {
                entry_id: 42,
                sys_from: 10,
            }],
            Vec::new(),
        );
        assert_eq!(
            invalid_history_detail(&bytes),
            "live start for entry 42 has no live entry"
        );
    }
}

#[test]
fn checkpoint_rejects_unversioned_live_start_records() {
    let bytes = raw_checkpoint(
        vec![make_entry(7, 10.0, 20.0)],
        vec![LiveStart {
            entry_id: 7,
            sys_from: UNVERSIONED,
        }],
        Vec::new(),
    );
    assert_eq!(
        invalid_history_detail(&bytes),
        "live start for entry 7 uses the unversioned sentinel"
    );
}

#[test]
fn checkpoint_preserves_mixed_live_entry_history() {
    let live = vec![LiveStart {
        entry_id: 1,
        sys_from: 10,
    }];
    let ended = vec![EndedEntry {
        entry_id: 3,
        document_id: "ended".into(),
        bbox: BoundingBox::from_point(1.0, 2.0),
        sys_from: 0,
        sys_to: 5,
    }];
    let bytes = raw_checkpoint(
        vec![make_entry(1, 10.0, 20.0), make_entry(2, 30.0, 40.0)],
        live.clone(),
        ended.clone(),
    );
    let restored = RTree::from_checkpoint(&bytes, None, test_memory()).unwrap();
    assert_eq!(restored.history_rows(), HistoryRows { live, ended });

    let world = BoundingBox::new(-180.0, -90.0, 180.0, 90.0);
    let visible_ids = |at| {
        let mut ids: Vec<_> = restored
            .search_at(&world, at)
            .into_iter()
            .map(|candidate| match candidate {
                crate::rtree::CutCandidate::Live(id) => id,
                crate::rtree::CutCandidate::Ended(entry) => entry.entry_id,
            })
            .collect();
        ids.sort_unstable();
        ids
    };
    assert_eq!(visible_ids(i64::MIN), vec![2]);
    assert_eq!(visible_ids(2), vec![2, 3]);
    assert_eq!(visible_ids(5), vec![2]);
    assert_eq!(visible_ids(10), vec![1, 2]);
}

#[test]
fn golden_header_layout() {
    let mut tree = RTree::new(test_memory(), TreeSrid::WGS84);
    tree.insert(make_entry(1, 10.0, 20.0));
    let bytes = tree.checkpoint_to_bytes(None).unwrap();
    // Magic at bytes[0..6].
    assert_eq!(&bytes[0..6], b"RKSPT\0");
    // Version byte at bytes[6].
    assert_eq!(bytes[6], super::RTREE_FORMAT_VERSION);
    // rkyv payload follows immediately.
    assert!(bytes.len() > 7);
}

#[test]
fn version_mismatch_returns_error() {
    let mut tree = RTree::new(test_memory(), TreeSrid::WGS84);
    tree.insert(make_entry(1, 10.0, 20.0));
    let mut bytes = tree.checkpoint_to_bytes(None).unwrap();
    // Corrupt the version byte to an unsupported value.
    bytes[6] = 0;
    match RTree::from_checkpoint(&bytes, None, test_memory()) {
        Err(RTreeCheckpointError::UnsupportedVersion { found, expected }) => {
            assert_eq!(found, 0);
            assert_eq!(expected, super::RTREE_FORMAT_VERSION);
        }
        Err(other) => panic!("unexpected error: {other}"),
        Ok(_) => panic!("expected UnsupportedVersion error, got Ok"),
    }
}

fn make_test_kek() -> nodedb_wal::crypto::WalEncryptionKey {
    nodedb_wal::crypto::WalEncryptionKey::from_bytes(&[0x42u8; 32]).unwrap()
}

#[test]
fn spatial_rtree_checkpoint_encrypted_at_rest() {
    let kek = make_test_kek();
    let mut tree = RTree::new(test_memory(), TreeSrid::WGS84);
    for i in 0..50 {
        tree.insert(make_entry(i, i as f64, i as f64 * 0.5));
    }

    let enc_bytes = tree.checkpoint_to_bytes(Some(&kek)).unwrap();

    // Encrypted blob must start with SEGV, not RKSPT.
    assert_eq!(&enc_bytes[0..4], b"SEGV");

    // Round-trip: decrypt and verify all entries survive.
    let restored = RTree::from_checkpoint(&enc_bytes, Some(&kek), test_memory()).unwrap();
    assert_eq!(restored.len(), 50);
    let all = restored.search(&BoundingBox::new(-180.0, -90.0, 180.0, 90.0));
    assert_eq!(all.len(), 50);
}

#[test]
fn spatial_rtree_refuses_plaintext_when_kek_required() {
    let kek = make_test_kek();
    let mut tree = RTree::new(test_memory(), TreeSrid::WGS84);
    tree.insert(make_entry(1, 10.0, 20.0));

    // Write plaintext checkpoint.
    let plain_bytes = tree.checkpoint_to_bytes(None).unwrap();

    // Attempting to load with a KEK must be refused.
    assert!(matches!(
        RTree::from_checkpoint(&plain_bytes, Some(&kek), test_memory()),
        Err(RTreeCheckpointError::KekRequired)
    ));
}

#[test]
fn spatial_rtree_refuses_encrypted_without_kek() {
    let kek = make_test_kek();
    let mut tree = RTree::new(test_memory(), TreeSrid::WGS84);
    tree.insert(make_entry(1, 10.0, 20.0));

    let enc_bytes = tree.checkpoint_to_bytes(Some(&kek)).unwrap();

    // Loading without a key must be refused.
    assert!(matches!(
        RTree::from_checkpoint(&enc_bytes, None, test_memory()),
        Err(RTreeCheckpointError::MissingKek)
    ));
}

#[test]
fn spatial_rtree_tampered_ciphertext_rejected() {
    let kek = make_test_kek();
    let mut tree = RTree::new(test_memory(), TreeSrid::WGS84);
    tree.insert(make_entry(1, 10.0, 20.0));

    let mut enc_bytes = tree.checkpoint_to_bytes(Some(&kek)).unwrap();
    // Flip a byte in the ciphertext region after the current preamble.
    enc_bytes[nodedb_wal::crypto::SEGMENT_ENVELOPE_PREAMBLE_SIZE + 2] ^= 0xFF;

    assert!(matches!(
        RTree::from_checkpoint(&enc_bytes, Some(&kek), test_memory()),
        Err(RTreeCheckpointError::DecryptionFailed(_))
    ));
}
