// SPDX-License-Identifier: BUSL-1.1

//! Integration tests for warm-tier storage via `object_store::ObjectStore`.
//!
//! Exercises snapshot write/read/delete and quarantine record/rebuild against
//! both in-memory and local-filesystem backends. Remote (MinIO/S3) verification
//! is an operational concern left to infrastructure-level CI.

use std::sync::Arc;

use object_store::local::LocalFileSystem;
use object_store::memory::InMemory;
use object_store::{ObjectStore, ObjectStoreExt};

// ── Snapshot: InMemory backend ───────────────────────────────────────────────

#[tokio::test]
async fn snapshot_write_read_delete_in_memory() {
    use nodedb::storage::snapshot_writer::{
        create_base_snapshot, delete_snapshot, discover_snapshots, load_core_snapshot,
        load_manifest, rebuild_catalog,
    };

    fn make_core_bytes(watermark: u64) -> Vec<u8> {
        let snap = nodedb::data::snapshot::CoreSnapshot {
            watermark,
            ..nodedb::data::snapshot::CoreSnapshot::empty()
        };
        snap.to_bytes().unwrap()
    }

    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());

    // Write two snapshots.
    let (meta1, prefix1) = create_base_snapshot(
        &store,
        vec![(0, make_core_bytes(10)), (1, make_core_bytes(20))],
        "node-a",
        None,
    )
    .await
    .unwrap();

    let (meta2, prefix2) =
        create_base_snapshot(&store, vec![(0, make_core_bytes(100))], "node-a", None)
            .await
            .unwrap();

    // Read back manifests.
    let m1 = load_manifest(&store, &prefix1).await.unwrap();
    assert_eq!(m1.num_cores, 2);
    assert_eq!(m1.meta.snapshot_id, meta1.snapshot_id);

    let m2 = load_manifest(&store, &prefix2).await.unwrap();
    assert_eq!(m2.num_cores, 1);
    assert_eq!(m2.meta.snapshot_id, meta2.snapshot_id);

    // Read back core snapshots.
    let core0 = load_core_snapshot(&store, &prefix1, 0, None).await.unwrap();
    assert_eq!(core0.watermark, 10);
    let core1 = load_core_snapshot(&store, &prefix1, 1, None).await.unwrap();
    assert_eq!(core1.watermark, 20);

    // Discover and rebuild catalog.
    let found = discover_snapshots(&store).await;
    assert_eq!(found.len(), 2);
    // Sorted by end_lsn.
    assert!(found[0].1.meta.end_lsn <= found[1].1.meta.end_lsn);

    let catalog = rebuild_catalog(&store).await;
    assert_eq!(catalog.len(), 2);

    // Delete the first snapshot.
    delete_snapshot(&store, &prefix1).await.unwrap();

    // After deletion, manifest key must not exist.
    use object_store::path::Path as OPath;
    let key = OPath::from(format!("{prefix1}/manifest.msgpack"));
    assert!(
        store.head(&key).await.is_err(),
        "manifest must be gone after delete"
    );

    // Remaining snapshot still readable.
    let m2_reload = load_manifest(&store, &prefix2).await.unwrap();
    assert_eq!(m2_reload.meta.snapshot_id, meta2.snapshot_id);
}

// ── Snapshot: LocalFileSystem backend ───────────────────────────────────────

#[tokio::test]
async fn snapshot_write_read_local_filesystem() {
    use nodedb::storage::snapshot_writer::{
        create_base_snapshot, load_core_snapshot, load_manifest,
    };

    fn make_core_bytes(watermark: u64) -> Vec<u8> {
        let snap = nodedb::data::snapshot::CoreSnapshot {
            watermark,
            ..nodedb::data::snapshot::CoreSnapshot::empty()
        };
        snap.to_bytes().unwrap()
    }

    let dir = tempfile::tempdir().unwrap();
    let store: Arc<dyn ObjectStore> =
        Arc::new(LocalFileSystem::new_with_prefix(dir.path()).unwrap());

    let (meta, prefix) =
        create_base_snapshot(&store, vec![(0, make_core_bytes(77))], "local-node", None)
            .await
            .unwrap();

    let manifest = load_manifest(&store, &prefix).await.unwrap();
    assert_eq!(manifest.meta.snapshot_id, meta.snapshot_id);
    assert_eq!(manifest.num_cores, 1);

    let core = load_core_snapshot(&store, &prefix, 0, None).await.unwrap();
    assert_eq!(core.watermark, 77);

    // Verify the file actually exists on disk.
    let manifest_path = dir.path().join(&prefix).join("manifest.msgpack");
    assert!(manifest_path.exists(), "manifest must exist on disk");
}

// ── Quarantine: record + rebuild via InMemory ────────────────────────────────

#[tokio::test]
async fn quarantine_record_and_rebuild_in_memory() {
    use nodedb::storage::quarantine::{QuarantineEngine, QuarantineRegistry, SegmentKey};

    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());

    // Simulate a quarantine entry by manually inserting a `.quarantined.<ts>` key.
    let ts = 1_700_001_000_000u64;
    let key_path = object_store::path::Path::from(format!("seg7.quarantined.{ts}"));
    store
        .put(&key_path, object_store::PutPayload::from(b"".as_ref()))
        .await
        .unwrap();

    // Rebuild registry from the store.
    let reg = QuarantineRegistry::new();
    reg.rebuild_from_store(QuarantineEngine::Fts, &store, &|fname| {
        let stem = fname.split(".quarantined.").next()?;
        Some(("testcoll".to_string(), stem.to_string()))
    })
    .await;

    // The registry must immediately block reads on the rebuilt key.
    let k = SegmentKey {
        engine: QuarantineEngine::Fts,
        collection: "testcoll".into(),
        segment_id: "seg7".into(),
    };
    let err = reg.record_failure(k, "crc", None).unwrap_err();
    assert!(
        matches!(
            err,
            nodedb::storage::quarantine::QuarantineError::SegmentQuarantined {
                quarantined_at_unix_ms, ..
            } if quarantined_at_unix_ms == ts
        ),
        "unexpected error: {err}"
    );

    // Snapshot surface must list the quarantined segment.
    let snap = reg.quarantined_snapshot();
    assert_eq!(snap.len(), 1);
    assert_eq!(snap[0].engine, "fts");
    assert_eq!(snap[0].collection, "testcoll");
    assert_eq!(snap[0].segment_id, "seg7");
}

// ── Snapshot bytes round-trip through InMemory ObjectStore ──────────────────
//
// Verifies that create_base_snapshot writes the expected objects into InMemory
// (manifest + per-core .snap blobs), and that execute_restore consumes those
// bytes and materialises the engine state into a fresh data directory —
// including sparse documents, a vector checkpoint file, and a CRDT checkpoint
// file.

#[tokio::test]
async fn snapshot_bytes_roundtrip_write_and_restore() {
    use futures::TryStreamExt;
    use nodedb::data::snapshot::{CoreSnapshot, CrdtSnapshot, HnswSnapshot, KvPair};
    use nodedb::storage::snapshot_executor::execute_restore;
    use nodedb::storage::snapshot_writer::create_base_snapshot;

    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());

    // Build a CoreSnapshot with one sparse document, one HNSW index, and one
    // CRDT state — enough content to verify all restore paths are exercised.
    let hnsw_bytes = vec![0xDE, 0xAD, 0xBE, 0xEF, 0x01, 0x02];
    let crdt_bytes = vec![0xAB, 0xCD, 0xEF];

    let snap = CoreSnapshot {
        watermark: 99,
        sparse_documents: vec![KvPair {
            key: "1:testcoll:row1".into(),
            value: b"payload-a".to_vec(),
        }],
        sparse_indexes: vec![],
        edges: vec![],
        hnsw_indexes: vec![HnswSnapshot {
            database_id: 0,
            tenant_id: 1,
            collection: "embeddings".into(),
            checkpoint_bytes: hnsw_bytes.clone(),
        }],
        crdt_snapshots: vec![CrdtSnapshot {
            database_id: 0,
            tenant_id: 1,
            peer_id: 42,
            collection: "testcoll".into(),
            snapshot_bytes: crdt_bytes.clone(),
        }],
    };

    let snap_bytes = snap.to_bytes().unwrap();
    let (meta, prefix) = create_base_snapshot(&store, vec![(0, snap_bytes)], "test-node", None)
        .await
        .unwrap();

    // ── Verify object-store objects ──────────────────────────────────────────
    // The manifest and exactly one core .snap blob must be present.
    use object_store::path::Path as OPath;
    let list_prefix = OPath::from(format!("{prefix}/"));
    let objects: Vec<_> = store.list(Some(&list_prefix)).try_collect().await.unwrap();

    assert_eq!(
        objects.len(),
        2,
        "expected manifest + 1 core blob, got {}: {:?}",
        objects.len(),
        objects
            .iter()
            .map(|o| o.location.as_ref())
            .collect::<Vec<_>>()
    );

    let paths: Vec<&str> = objects.iter().map(|o| o.location.as_ref()).collect();
    assert!(
        paths.iter().any(|p| p.ends_with("manifest.msgpack")),
        "manifest.msgpack missing"
    );
    assert!(
        paths.iter().any(|p| p.ends_with("core-0.snap")),
        "core-0.snap missing"
    );

    // All objects must be non-empty.
    for obj in &objects {
        assert!(obj.size > 0, "object {} is empty", obj.location);
    }

    // ── Execute restore into a fresh data directory ──────────────────────────
    let dir = tempfile::tempdir().unwrap();
    let data_dir = dir.path().join("restored");
    std::fs::create_dir_all(&data_dir).unwrap();

    let result = execute_restore(&data_dir, &prefix, &store, &store, &[])
        .await
        .unwrap();

    assert_eq!(result.snapshot_id, meta.snapshot_id);
    assert_eq!(result.cores_restored, 1);
    assert_eq!(result.documents_restored, 1);
    assert_eq!(result.vectors_restored, 1);
    assert_eq!(result.wal_records_replayed, 0);

    // ── Verify sparse engine state ───────────────────────────────────────────
    let sparse_path = data_dir.join("sparse/core-0.redb");
    assert!(
        sparse_path.exists(),
        "sparse redb file must exist after restore"
    );
    let sparse = nodedb::engine::sparse::btree::SparseEngine::open(&sparse_path).unwrap();
    assert!(
        sparse.get_raw("1:testcoll:row1").unwrap().is_some(),
        "sparse document must be readable after restore"
    );

    // ── Verify HNSW checkpoint file ──────────────────────────────────────────
    let ckpt_dir = data_dir.join("vector-ckpt").join("core-0");
    let hnsw_ckpt = ckpt_dir.join("0:1:embeddings:emb.ckpt");
    assert!(
        hnsw_ckpt.exists(),
        "HNSW checkpoint file must exist after restore"
    );
    let on_disk = std::fs::read(&hnsw_ckpt).unwrap();
    assert_eq!(on_disk, hnsw_bytes, "HNSW checkpoint bytes must match");

    // ── Verify CRDT checkpoint file ──────────────────────────────────────────
    // CRDT checkpoints are per-collection and per-core:
    // `crdt-ckpt/core-{id}/tenant-{tid}-coll-{hex(collection)}.ckpt`. The hex
    // encoding mirrors the engine's filename scheme (collection bytes, lowercase).
    let crdt_coll_hex: String = "testcoll".bytes().map(|b| format!("{b:02x}")).collect();
    let crdt_ckpt = data_dir
        .join("crdt-ckpt")
        .join("core-0")
        .join(format!("tenant-1-coll-{crdt_coll_hex}.ckpt"));
    assert!(
        crdt_ckpt.exists(),
        "CRDT checkpoint file must exist after restore"
    );
    let on_disk_crdt = std::fs::read(&crdt_ckpt).unwrap();
    assert_eq!(on_disk_crdt, crdt_bytes, "CRDT checkpoint bytes must match");
}

// ── Quarantine: rebuild with multiple engines and keys ───────────────────────

#[tokio::test]
async fn quarantine_rebuild_multi_engine() {
    use nodedb::storage::quarantine::{QuarantineEngine, QuarantineRegistry, SegmentKey};

    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let ts = 1_700_002_000_000u64;

    // Put two quarantined keys for different engines.
    for key_name in &["seg_a.quarantined.", "seg_b.quarantined."] {
        let path = object_store::path::Path::from(format!("{key_name}{ts}"));
        store
            .put(&path, object_store::PutPayload::from(b"".as_ref()))
            .await
            .unwrap();
    }

    let reg = QuarantineRegistry::new();
    reg.rebuild_from_store(QuarantineEngine::Columnar, &store, &|fname| {
        let stem = fname.split(".quarantined.").next()?;
        Some(("col".to_string(), stem.to_string()))
    })
    .await;

    assert_eq!(reg.quarantined_snapshot().len(), 2);

    // Both segments must be blocked.
    for seg_id in &["seg_a", "seg_b"] {
        let k = SegmentKey {
            engine: QuarantineEngine::Columnar,
            collection: "col".into(),
            segment_id: seg_id.to_string(),
        };
        assert!(reg.record_failure(k, "crc", None).is_err());
    }
}
