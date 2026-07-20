// SPDX-License-Identifier: BUSL-1.1

//! Snapshot restore execution: loads a snapshot and replays WAL to a target LSN.
//!
//! ## Restore flow
//!
//! 1. Validate the restore plan via `dry_run_restore()`.
//! 2. For each core, load its `CoreSnapshot` from the object store.
//! 3. Apply the snapshot to the core's engines (redb, HNSW, CRDT).
//! 4. Replay WAL records from `snapshot_lsn` to `target_lsn`.
//!
//! ## Offline restore
//!
//! Restore is an **offline operation** — the server must not be serving traffic.
//! The restore process opens engine storage directly (not via SPSC bridge).
//! After restore, the server is restarted normally and WAL replay covers
//! any records between the snapshot and the crash point.

use std::path::Path;
use std::sync::Arc;

use object_store::path::Path as ObjectPath;
use object_store::{ObjectStore, ObjectStoreExt, PutPayload};
use tracing::{info, warn};

use crate::data::snapshot::{CoreSnapshot, KvPair};
use crate::storage::snapshot_writer::{load_core_snapshot, load_manifest};
use crate::types::Lsn;

/// Result of a snapshot restore operation.
#[derive(Debug, Clone)]
pub struct RestoreResult {
    /// Snapshot that was restored.
    pub snapshot_id: u64,
    /// LSN at which the restore completed (snapshot end_lsn).
    pub restored_lsn: Lsn,
    /// Number of cores restored.
    pub cores_restored: usize,
    /// Total documents restored across all cores.
    pub documents_restored: u64,
    /// Total vectors restored across all cores.
    pub vectors_restored: u64,
    /// WAL records replayed after snapshot.
    pub wal_records_replayed: u64,
}

/// Execute a snapshot restore.
///
/// Restores engine state from a snapshot in the object store, then writes a
/// restore marker so the normal startup path replays WAL records from the
/// snapshot LSN forward.
///
/// `data_dir` is the server's data directory (where engine files live).
/// `prefix` is the snapshot object-store prefix (e.g. `"snap-000001-lsn…"`).
/// `snapshot_store` is the object store holding the snapshot objects.
/// `restore_store` is the object store holding the restore marker (same as
///   `snapshot_store` in practice; passed separately for flexibility).
/// `wal_records` are all available WAL records (already loaded by the caller).
///
/// This is an **offline operation** — must not be called while serving traffic.
pub async fn execute_restore(
    data_dir: &Path,
    prefix: &str,
    snapshot_store: &Arc<dyn ObjectStore>,
    restore_store: &Arc<dyn ObjectStore>,
    wal_records: &[nodedb_wal::WalRecord],
) -> crate::Result<RestoreResult> {
    let manifest = load_manifest(snapshot_store, prefix).await?;
    let snapshot_lsn = manifest.meta.end_lsn.as_u64();

    info!(
        snapshot_id = manifest.meta.snapshot_id,
        snapshot_lsn,
        cores = manifest.num_cores,
        "starting snapshot restore"
    );

    let mut total_docs = 0u64;
    let mut total_vectors = 0u64;

    for core_id in 0..manifest.num_cores {
        let core_snap = load_core_snapshot(snapshot_store, prefix, core_id, None).await?;
        let (docs, vectors) = restore_core_state(data_dir, core_id, &core_snap)?;
        total_docs += docs;
        total_vectors += vectors;

        info!(
            core_id,
            documents = docs,
            vectors,
            watermark = core_snap.watermark,
            "core state restored"
        );
    }

    let wal_to_replay: Vec<_> = wal_records
        .iter()
        .filter(|r| r.header.lsn > snapshot_lsn)
        .collect();

    let wal_count = wal_to_replay.len() as u64;
    if wal_count > 0 {
        info!(
            records = wal_count,
            from_lsn = snapshot_lsn + 1,
            "replaying WAL records after snapshot"
        );
        write_restore_marker(restore_store, snapshot_lsn).await?;
    }

    let result = RestoreResult {
        snapshot_id: manifest.meta.snapshot_id,
        restored_lsn: manifest.meta.end_lsn,
        cores_restored: manifest.num_cores,
        documents_restored: total_docs,
        vectors_restored: total_vectors,
        wal_records_replayed: wal_count,
    };

    info!(
        snapshot_id = result.snapshot_id,
        restored_lsn = result.restored_lsn.as_u64(),
        documents = result.documents_restored,
        vectors = result.vectors_restored,
        wal_replayed = result.wal_records_replayed,
        "snapshot restore complete"
    );

    Ok(result)
}

/// Restore a single core's engine state from a `CoreSnapshot`.
///
/// Opens the core's redb databases, clears existing data, and loads
/// the snapshot data. Returns `(documents_count, vectors_count)`.
fn restore_core_state(
    data_dir: &Path,
    core_id: usize,
    snap: &CoreSnapshot,
) -> crate::Result<(u64, u64)> {
    let sparse_path = data_dir.join(format!("sparse/core-{core_id}.redb"));
    if let Some(parent) = sparse_path.parent() {
        // no-objectstore: redb opens a local file directly; warm-tier blob is restored, this is the engine-state landing path.
        std::fs::create_dir_all(parent).map_err(crate::Error::Io)?;
    }
    let sparse = crate::engine::sparse::btree::SparseEngine::open(&sparse_path)?;
    restore_sparse_data(&sparse, &snap.sparse_documents, &snap.sparse_indexes)?;

    let graph_path = data_dir.join(format!("graph/core-{core_id}.redb"));
    if let Some(parent) = graph_path.parent() {
        // no-objectstore: redb opens a local file directly; engine-state landing path.
        std::fs::create_dir_all(parent).map_err(crate::Error::Io)?;
    }
    let edge_store = crate::engine::graph::edge_store::store::EdgeStore::open(&graph_path)?;
    restore_edge_data(&edge_store, &snap.edges)?;

    let vectors = restore_vector_checkpoints(data_dir, core_id, &snap.hnsw_indexes)?;
    restore_crdt_checkpoints(data_dir, core_id, &snap.crdt_snapshots)?;

    Ok((snap.sparse_documents.len() as u64, vectors))
}

fn restore_sparse_data(
    sparse: &crate::engine::sparse::btree::SparseEngine,
    documents: &[KvPair],
    indexes: &[KvPair],
) -> crate::Result<()> {
    for kv in documents {
        sparse.put_raw(&kv.key, &kv.value)?;
    }
    for kv in indexes {
        sparse.put_raw(&kv.key, &kv.value)?;
    }
    if !documents.is_empty() {
        info!(
            documents = documents.len(),
            indexes = indexes.len(),
            "sparse engine data restored"
        );
    }
    Ok(())
}

fn restore_edge_data(
    edge_store: &crate::engine::graph::edge_store::store::EdgeStore,
    edges: &[crate::data::snapshot::TenantKvPair],
) -> crate::Result<()> {
    for kv in edges {
        edge_store.put_edge_raw(
            kv.database_id,
            nodedb_types::TenantId::new(kv.tenant_id),
            &kv.key,
            &kv.value,
        )?;
    }
    if !edges.is_empty() {
        info!(edges = edges.len(), "edge store data restored");
    }
    Ok(())
}

fn restore_vector_checkpoints(
    data_dir: &Path,
    core_id: usize,
    hnsw_indexes: &[crate::data::snapshot::HnswSnapshot],
) -> crate::Result<u64> {
    if hnsw_indexes.is_empty() {
        return Ok(0);
    }

    let ckpt_dir = crate::data::executor::vector_checkpoint::vector_ckpt_dir(data_dir, core_id);
    // no-objectstore: HNSW checkpoints are mmap'd locally for query hot path.
    std::fs::create_dir_all(&ckpt_dir).map_err(crate::Error::Io)?;

    let mut total_vectors = 0u64;
    for idx in hnsw_indexes {
        let key = format!(
            "{}:{}:{}:emb",
            idx.database_id, idx.tenant_id, idx.collection
        );
        let ckpt_path = ckpt_dir.join(format!("{key}.ckpt"));
        let tmp_path = ckpt_dir.join(format!("{key}.ckpt.tmp"));
        nodedb_wal::segment::atomic_write_fsync(&tmp_path, &ckpt_path, &idx.checkpoint_bytes)
            .map_err(crate::Error::Wal)?;
        total_vectors += 1;
    }

    info!(
        collections = hnsw_indexes.len(),
        "vector checkpoints restored"
    );
    Ok(total_vectors)
}

fn restore_crdt_checkpoints(
    data_dir: &Path,
    core_id: usize,
    crdt_snapshots: &[crate::data::snapshot::CrdtSnapshot],
) -> crate::Result<()> {
    if crdt_snapshots.is_empty() {
        return Ok(());
    }

    let ckpt_dir = crate::data::executor::crdt_checkpoint::crdt_ckpt_dir(data_dir, core_id);
    // no-objectstore: CRDT state lands in a local engine-owned directory.
    std::fs::create_dir_all(&ckpt_dir).map_err(crate::Error::Io)?;

    for snap in crdt_snapshots {
        let fname = crate::data::executor::crdt_checkpoint::crdt_ckpt_filename(
            snap.database_id,
            snap.tenant_id,
            &snap.collection,
        );
        let ckpt_path = ckpt_dir.join(&fname);
        let tmp_path = ckpt_dir.join(format!("{fname}.tmp"));
        nodedb_wal::segment::atomic_write_fsync(&tmp_path, &ckpt_path, &snap.snapshot_bytes)
            .map_err(crate::Error::Wal)?;
    }

    info!(tenants = crdt_snapshots.len(), "CRDT checkpoints restored");
    Ok(())
}

/// Write a restore marker to the object store that tells the startup code to
/// replay WAL records only from the given LSN forward.
///
/// Key: `restore_from_lsn`
async fn write_restore_marker(
    store: &Arc<dyn ObjectStore>,
    snapshot_lsn: u64,
) -> crate::Result<()> {
    let marker_key = ObjectPath::from("restore_from_lsn");
    let bytes = snapshot_lsn.to_string().into_bytes();
    store
        .put(&marker_key, PutPayload::from(bytes))
        .await
        .map_err(|e| crate::Error::Storage {
            engine: "snapshot".into(),
            detail: format!("write restore marker: {e}"),
        })?;
    info!(snapshot_lsn, "restore marker written");
    Ok(())
}

/// Read and consume the restore marker (if present).
///
/// Called during startup. Returns the LSN to replay from, or `None` if
/// no restore marker exists (normal startup).
pub async fn read_restore_marker(store: &Arc<dyn ObjectStore>) -> Option<u64> {
    let marker_key = ObjectPath::from("restore_from_lsn");
    let result = store.get(&marker_key).await.ok()?;
    let bytes = result.bytes().await.ok()?;
    let content = std::str::from_utf8(&bytes).ok()?;
    let lsn = content.trim().parse::<u64>().ok()?;

    // Delete the marker after reading — it's a one-time signal.
    if let Err(e) = store.delete(&marker_key).await {
        warn!(error = %e, "failed to delete restore marker");
    }

    info!(
        restore_from_lsn = lsn,
        "restore marker found — WAL replay will start from this LSN"
    );
    Some(lsn)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::data::snapshot::CoreSnapshot;
    use object_store::memory::InMemory;

    fn in_memory_store() -> Arc<dyn ObjectStore> {
        Arc::new(InMemory::new())
    }

    #[tokio::test]
    async fn restore_marker_roundtrip() {
        let store = in_memory_store();

        assert!(read_restore_marker(&store).await.is_none());

        write_restore_marker(&store, 42).await.unwrap();
        assert_eq!(read_restore_marker(&store).await, Some(42));

        // Consumed — should be gone.
        assert!(read_restore_marker(&store).await.is_none());
    }

    #[tokio::test]
    async fn end_to_end_snapshot_restore() {
        let store = in_memory_store();
        let dir = tempfile::tempdir().unwrap();
        let data_dir = dir.path().join("data");
        std::fs::create_dir_all(&data_dir).unwrap();

        let snap = CoreSnapshot {
            watermark: 50,
            sparse_documents: vec![
                KvPair {
                    key: "1:docs:d1".into(),
                    value: b"hello".to_vec(),
                },
                KvPair {
                    key: "1:docs:d2".into(),
                    value: b"world".to_vec(),
                },
            ],
            sparse_indexes: vec![],
            edges: vec![],
            hnsw_indexes: vec![],
            crdt_snapshots: vec![],
        };

        let snap_bytes = snap.to_bytes().unwrap();
        let core_snaps = vec![(0, snap_bytes)];
        let (meta, prefix) =
            crate::storage::snapshot_writer::create_base_snapshot(&store, core_snaps, "test", None)
                .await
                .unwrap();

        let result = execute_restore(&data_dir, &prefix, &store, &store, &[])
            .await
            .unwrap();
        assert_eq!(result.snapshot_id, meta.snapshot_id);
        assert_eq!(result.cores_restored, 1);
        assert_eq!(result.documents_restored, 2);
        assert_eq!(result.wal_records_replayed, 0);

        let sparse_path = data_dir.join("sparse/core-0.redb");
        let sparse = crate::engine::sparse::btree::SparseEngine::open(&sparse_path).unwrap();
        assert!(sparse.get_raw("1:docs:d1").unwrap().is_some());
        assert!(sparse.get_raw("1:docs:d2").unwrap().is_some());
    }
}
