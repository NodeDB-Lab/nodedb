// SPDX-License-Identifier: BUSL-1.1

//! Integration tests for warm-tier storage via `object_store::ObjectStore`.
//!
//! Exercises snapshot write/read/delete against both in-memory and
//! local-filesystem backends. Remote (MinIO/S3) verification
//! is an operational concern left to infrastructure-level CI.

use std::sync::Arc;

use nodedb_wal::crypto::WalEncryptionKey;
use object_store::local::LocalFileSystem;
use object_store::memory::InMemory;
use object_store::{ObjectStore, ObjectStoreExt};

fn test_encryption_key() -> WalEncryptionKey {
    WalEncryptionKey::from_bytes(&[0x5A; 32]).expect("test encryption key")
}

fn make_core_bytes(floor: u64) -> Vec<u8> {
    nodedb::data::snapshot::CoreSnapshot {
        stamp: nodedb::types::replay_stamp::ReplayStamp::through(floor),
        ..nodedb::data::snapshot::CoreSnapshot::empty()
    }
    .to_bytes()
    .unwrap()
}

fn no_node() -> nodedb::data::snapshot::NodeSnapshot {
    nodedb::data::snapshot::NodeSnapshot::default()
}

// ── Snapshot: InMemory backend ───────────────────────────────────────────────

#[tokio::test]
async fn snapshot_write_read_delete_in_memory() {
    use nodedb::storage::snapshot_writer::{
        create_base_snapshot, delete_snapshot, discover_snapshots, load_core_snapshot,
        load_manifest, rebuild_catalog,
    };

    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let encryption_key = test_encryption_key();

    // Write two snapshots.
    let (meta1, prefix1) = create_base_snapshot(
        &store,
        vec![(0, make_core_bytes(10)), (1, make_core_bytes(20))],
        &no_node(),
        &[],
        "node-a",
        Some(&encryption_key),
    )
    .await
    .unwrap();

    let (meta2, prefix2) = create_base_snapshot(
        &store,
        vec![(0, make_core_bytes(100))],
        &no_node(),
        &[],
        "node-a",
        Some(&encryption_key),
    )
    .await
    .unwrap();

    // Read back manifests.
    let m1 = load_manifest(&store, &prefix1, &encryption_key)
        .await
        .unwrap();
    assert_eq!(m1.num_cores, 2);
    assert_eq!(m1.meta.snapshot_id, meta1.snapshot_id);

    let m2 = load_manifest(&store, &prefix2, &encryption_key)
        .await
        .unwrap();
    assert_eq!(m2.num_cores, 1);
    assert_eq!(m2.meta.snapshot_id, meta2.snapshot_id);

    // Read back core snapshots.
    let core0 = load_core_snapshot(&store, &prefix1, &m1, 0, &encryption_key)
        .await
        .unwrap();
    assert_eq!(core0.replay_floor(), 10);
    let core1 = load_core_snapshot(&store, &prefix1, &m1, 1, &encryption_key)
        .await
        .unwrap();
    assert_eq!(core1.replay_floor(), 20);

    // Discover and rebuild catalog.
    let found = discover_snapshots(&store, &encryption_key).await;
    assert_eq!(found.len(), 2);
    // Sorted by applied_high_lsn.
    assert!(found[0].1.meta.applied_high_lsn <= found[1].1.meta.applied_high_lsn);

    let catalog = rebuild_catalog(&store, &encryption_key).await;
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
    let m2_reload = load_manifest(&store, &prefix2, &encryption_key)
        .await
        .unwrap();
    assert_eq!(m2_reload.meta.snapshot_id, meta2.snapshot_id);
}

// ── Snapshot: LocalFileSystem backend ───────────────────────────────────────

#[tokio::test]
async fn snapshot_write_read_local_filesystem() {
    use nodedb::storage::snapshot_writer::{
        create_base_snapshot, load_core_snapshot, load_manifest,
    };

    let dir = tempfile::tempdir().unwrap();
    let store: Arc<dyn ObjectStore> =
        Arc::new(LocalFileSystem::new_with_prefix(dir.path()).unwrap());
    let encryption_key = test_encryption_key();

    let (meta, prefix) = create_base_snapshot(
        &store,
        vec![(0, make_core_bytes(77))],
        &no_node(),
        &[],
        "local-node",
        Some(&encryption_key),
    )
    .await
    .unwrap();

    let manifest = load_manifest(&store, &prefix, &encryption_key)
        .await
        .unwrap();
    assert_eq!(manifest.meta.snapshot_id, meta.snapshot_id);
    assert_eq!(manifest.num_cores, 1);

    let core = load_core_snapshot(&store, &prefix, &manifest, 0, &encryption_key)
        .await
        .unwrap();
    assert_eq!(core.replay_floor(), 77);

    // Verify the file actually exists on disk.
    let manifest_path = dir.path().join(&prefix).join("manifest.msgpack");
    assert!(manifest_path.exists(), "manifest must exist on disk");
}

// ── Snapshot bytes round-trip through InMemory ObjectStore ──────────────────
//
// Verifies that create_base_snapshot writes the manifest under the snapshot
// prefix and the chunks of the core and node images under `chunks/`, and that
// execute_restore lands every captured file at its
// original path in a fresh data directory and seeds the WAL above the
// snapshot's highest LSN.

#[tokio::test]
async fn snapshot_bytes_roundtrip_write_and_restore() {
    use futures::TryStreamExt;
    use nodedb::data::snapshot::{CoreSnapshot, NodeSnapshot, SnapshotComponent, SnapshotFile};
    use nodedb::storage::snapshot_executor::{RestoreSource, execute_restore};
    use nodedb::storage::snapshot_writer::{
        CHUNK_DIR, create_base_snapshot, list_chunk_ids, load_manifest,
    };
    use nodedb::types::replay_stamp::ReplayStamp;

    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let encryption_key = test_encryption_key();

    let file = |component, path: &str, bytes: &[u8]| SnapshotFile {
        component,
        path: path.into(),
        bytes: bytes.to_vec(),
    };
    let snap = CoreSnapshot {
        stamp: ReplayStamp::through(99),
        files: vec![
            file(SnapshotComponent::Sparse, "sparse/core-0.redb", b"sparse"),
            file(
                SnapshotComponent::Vector,
                "vector-ckpt/core-0/MANIFEST",
                b"vm",
            ),
            file(
                SnapshotComponent::Timeseries,
                "ts/0/1/metrics/p-0/partition.meta",
                b"pm",
            ),
        ],
        dirs: Vec::new(),
    };
    let node = NodeSnapshot {
        files: vec![file(
            SnapshotComponent::SystemCatalog,
            "system.redb",
            b"sys",
        )],
        metadata_applied_index: 0,
        metadata_captured_index: 0,
        metadata_timeline: 0,
    };

    let (meta, prefix) = create_base_snapshot(
        &store,
        vec![(0, snap.to_bytes().unwrap())],
        &node,
        &[],
        "test-node",
        Some(&encryption_key),
    )
    .await
    .unwrap();
    assert_eq!(meta.applied_high_lsn.as_u64(), 99);

    // ── Verify object-store objects ──────────────────────────────────────────
    use object_store::path::Path as OPath;
    let list_prefix = OPath::from(format!("{prefix}/"));
    let objects: Vec<_> = store.list(Some(&list_prefix)).try_collect().await.unwrap();
    let mut names: Vec<&str> = objects
        .iter()
        .filter_map(|o| o.location.as_ref().rsplit('/').next())
        .collect();
    names.sort_unstable();
    assert_eq!(names, ["manifest.msgpack"]);
    let manifest = load_manifest(&store, &prefix, &encryption_key)
        .await
        .unwrap();
    let mut listed: Vec<String> = manifest.chunk_ids().map(str::to_owned).collect();
    listed.sort_unstable();
    listed.dedup();
    let mut stored = list_chunk_ids(&store).await.unwrap();
    stored.sort_unstable();
    assert_eq!(stored, listed, "every listed chunk is stored once");
    let chunk_dir = OPath::from(CHUNK_DIR);
    let chunks: Vec<_> = store.list(Some(&chunk_dir)).try_collect().await.unwrap();
    for obj in objects.iter().chain(&chunks) {
        assert!(obj.size > 0, "object {} is empty", obj.location);
    }

    // ── Execute restore into a fresh data directory ──────────────────────────
    let dir = tempfile::tempdir().unwrap();
    let data_dir = dir.path().join("restored");

    let source = RestoreSource {
        prefix: &prefix,
        snapshot_store: &store,
        cold_store: None,
        encryption_key: &encryption_key,
    };
    let result = execute_restore(&data_dir, &source).await.unwrap();
    assert_eq!(result.snapshot_id, meta.snapshot_id);
    assert_eq!(result.cores_restored, 1);
    assert_eq!(result.applied_high_lsn.as_u64(), 99);
    // Four captured files plus the WAL seed segment.
    assert_eq!(result.files_restored, 5);

    for (path, bytes) in [
        ("sparse/core-0.redb", b"sparse".as_slice()),
        ("vector-ckpt/core-0/MANIFEST", b"vm"),
        ("ts/0/1/metrics/p-0/partition.meta", b"pm"),
        ("system.redb", b"sys"),
    ] {
        assert_eq!(std::fs::read(data_dir.join(path)).unwrap(), bytes, "{path}");
    }
    let segments = nodedb_wal::segment::discover_segments(&data_dir.join("wal")).unwrap();
    assert_eq!(segments.len(), 1);
    assert_eq!(segments[0].first_lsn, 100);
}
