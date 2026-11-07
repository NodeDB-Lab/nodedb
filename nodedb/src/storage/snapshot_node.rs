// SPDX-License-Identifier: BUSL-1.1

//! Capture of the node-level stores for a physical base snapshot.
//!
//! Captured: the system and cluster catalogs, every durable Event Plane and
//! array sync store `SharedState` holds, each Event Plane consumer's action
//! retry store, and the WAL-wrapped CRDT signing root. Each redb image is
//! read under a held write transaction on its database, so no commit lands
//! mid-read. A retry store is read by the consumer task that owns it, on
//! request. The signing root is replaced only by rename, so one read sees a
//! whole file.
//!
//! Not captured:
//! - `event_plane/watermarks.redb`, the per-core LSNs the Event Plane has
//!   processed. Restore seeds the WAL above every LSN the snapshot holds, so
//!   an absent watermark replays exactly the WAL a captured one replays.
//! - `node_incarnation`. A restored directory is a new node life, so its
//!   archive and bases never overwrite those of the life it came from.

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use crate::control::security::catalog::SystemCatalog;
use crate::control::state::SharedState;
use crate::data::executor::snapshot::layout::{
    ARRAY_SYNC_DIR, CRDT_SIGNING_ROOT_FILE, EVENT_PLANE_DIR, component_root,
};
use crate::data::snapshot::{NodeSnapshot, SnapshotComponent, SnapshotFile};
use crate::storage::RedbBacked;
use crate::storage::snapshot_files::{collect_file, read_redb_image, rel_path_string};

pub const TRIGGER_DLQ_FILE: &str = "trigger_dlq.redb";
pub const CONSUMER_OFFSETS_FILE: &str = "consumer_offsets.redb";
pub const CROSS_SHARD_DEDUP_FILE: &str = "cross_shard_dedup.redb";
pub const CROSS_SHARD_DLQ_FILE: &str = "cross_shard_dlq.redb";
pub const JOB_HISTORY_FILE: &str = "job_history.redb";
pub const MV_STATE_FILE: &str = "mv_state.redb";
pub const ARRAY_OP_LOG_FILE: &str = "op_log.redb";
pub const ARRAY_ACKS_FILE: &str = "acks.redb";
pub const ARRAY_SNAPSHOTS_FILE: &str = "snapshots.redb";
/// Opened by `init_prod`, which names the file through this constant.
pub const ARRAY_SCHEMA_DOCS_FILE: &str = "schema_docs.redb";
/// Opened by `init_prod`, which names the file through this constant.
pub const ARRAY_SUBSCRIBER_CURSORS_FILE: &str = "subscriber_cursors.redb";

/// One open node-level redb store under `event_plane/` or `array_sync/`.
pub struct NodeRedbStore<'a> {
    pub component: SnapshotComponent,
    pub file_name: &'static str,
    pub db: &'a redb::Database,
}

/// Read a consistent image of the catalogs and of every store in `stores`.
///
/// Blocks while any of these databases runs a write transaction. Call it
/// from a blocking task, never directly from an async one.
pub fn capture_node_state(
    data_dir: &Path,
    system: &SystemCatalog,
    cluster: Option<&nodedb_cluster::ClusterCatalog>,
    stores: &[NodeRedbStore<'_>],
) -> crate::Result<NodeSnapshot> {
    let mut files = vec![image(
        data_dir,
        SnapshotComponent::SystemCatalog,
        &component_root(SnapshotComponent::SystemCatalog, 0),
        system,
    )?];
    if let Some(cluster) = cluster {
        files.push(image(
            data_dir,
            SnapshotComponent::ClusterCatalog,
            &component_root(SnapshotComponent::ClusterCatalog, 0),
            cluster,
        )?);
    }
    for store in stores {
        let dir = match store.component {
            SnapshotComponent::EventPlane => EVENT_PLANE_DIR,
            SnapshotComponent::ArraySync => ARRAY_SYNC_DIR,
            other => {
                return Err(crate::Error::BadRequest {
                    detail: format!(
                        "{other:?} store {} is not a node-level store",
                        store.file_name
                    ),
                });
            }
        };
        let rel = Path::new(dir).join(store.file_name);
        files.push(image(data_dir, store.component, &rel, store.db)?);
    }
    Ok(NodeSnapshot {
        files,
        metadata_applied_index: 0,
        metadata_captured_index: 0,
        metadata_timeline: system.load_metadata_timeline()?,
    })
}

/// [`capture_node_state`] over every node-level store `state` holds.
pub fn capture_shared_state(
    data_dir: &Path,
    state: &SharedState,
    cluster: Option<&nodedb_cluster::ClusterCatalog>,
) -> crate::Result<NodeSnapshot> {
    // Read before any image: every metadata entry at or below it applied
    // before the catalogs are read, so the images hold it.
    let metadata_index = || {
        cluster.map_or(0, |_| {
            state
                .applied_index_watcher(nodedb_cluster::METADATA_GROUP_ID)
                .current()
        })
    };
    let metadata_applied_index = metadata_index();
    let poisoned = |what: &str| crate::Error::Internal {
        detail: format!("{what} lock is poisoned; its store cannot be captured"),
    };
    let trigger_dlq = match state.trigger_dlq.get() {
        Some(dlq) => Some(dlq.lock().map_err(|_| poisoned("trigger DLQ"))?),
        None => None,
    };
    let cross_shard_dlq = match &state.cross_shard.dlq {
        Some(dlq) => Some(dlq.lock().map_err(|_| poisoned("cross-shard DLQ"))?),
        None => None,
    };

    let event = SnapshotComponent::EventPlane;
    let array = SnapshotComponent::ArraySync;
    let mut stores = vec![
        store(event, CONSUMER_OFFSETS_FILE, &*state.offset_store),
        store(event, JOB_HISTORY_FILE, &*state.job_history),
        store(event, MV_STATE_FILE, &*state.mv_persistence),
        store(array, ARRAY_OP_LOG_FILE, &*state.array_sync_op_log),
        store(array, ARRAY_ACKS_FILE, &*state.array_ack_registry),
        store(array, ARRAY_SNAPSHOTS_FILE, &*state.array_snapshot_store),
        store(array, ARRAY_SCHEMA_DOCS_FILE, &*state.array_sync_schemas),
        store(
            array,
            ARRAY_SUBSCRIBER_CURSORS_FILE,
            &*state.array_subscriber_cursors,
        ),
    ];
    if let Some(dlq) = &trigger_dlq {
        stores.push(store(event, TRIGGER_DLQ_FILE, &**dlq));
    }
    if let Some(dlq) = &cross_shard_dlq {
        stores.push(store(event, CROSS_SHARD_DLQ_FILE, &**dlq));
    }
    if let Some(dedup) = state.cross_shard.dedup.get() {
        stores.push(store(event, CROSS_SHARD_DEDUP_FILE, &**dedup));
    }
    let mut node = capture_node_state(data_dir, state.credentials.catalog(), cluster, &stores)?;
    node.metadata_applied_index = metadata_applied_index;
    node.metadata_captured_index = metadata_index();
    if state.wal.crdt_signing_root()?.is_some() {
        capture_signing_root(data_dir, &mut node.files)?;
    }
    Ok(node)
}

/// Append the WAL-wrapped CRDT signing root. `system.redb` holds its
/// fingerprint, so a restore without it cannot boot. It exists whenever the
/// WAL is encrypted.
pub fn capture_signing_root(data_dir: &Path, out: &mut Vec<SnapshotFile>) -> crate::Result<()> {
    collect_file(
        data_dir,
        Path::new(CRDT_SIGNING_ROOT_FILE),
        SnapshotComponent::WalKeys,
        out,
    )
}

/// Ask every Event Plane consumer for an image of its action retry store.
/// A node with no Event Plane, and a core that never kept an action, add
/// nothing.
pub async fn capture_action_retry_stores(
    state: &SharedState,
    timeout: Duration,
) -> crate::Result<Vec<SnapshotFile>> {
    let Some(inbox) = state.action_requeue.get() else {
        return Ok(Vec::new());
    };
    let mut files = Vec::new();
    for (core_id, image) in inbox.retry_captures().capture_all(timeout).await? {
        let Some(bytes) = image else {
            continue;
        };
        let rel = crate::event::action::ActionStore::path_for(Path::new(""), core_id);
        files.push(SnapshotFile {
            component: SnapshotComponent::EventPlane,
            path: rel_path_string(&rel)?,
            bytes,
        });
    }
    Ok(files)
}

/// The whole node-level image: the action retry stores from their consumers,
/// then every other store read on a blocking thread.
pub async fn capture_node_snapshot(
    data_dir: PathBuf,
    state: Arc<SharedState>,
    cluster: Option<Arc<nodedb_cluster::ClusterCatalog>>,
    consumer_timeout: Duration,
) -> crate::Result<NodeSnapshot> {
    let retry_stores = capture_action_retry_stores(&state, consumer_timeout).await?;
    let mut node = tokio::task::spawn_blocking(move || {
        capture_shared_state(&data_dir, &state, cluster.as_deref())
    })
    .await
    .map_err(|e| crate::Error::Internal {
        detail: format!("node store capture task failed: {e}"),
    })??;
    node.files.extend(retry_stores);
    Ok(node)
}

fn store<'a>(
    component: SnapshotComponent,
    file_name: &'static str,
    source: &'a impl RedbBacked,
) -> NodeRedbStore<'a> {
    NodeRedbStore {
        component,
        file_name,
        db: source.redb_database(),
    }
}

fn image(
    data_dir: &Path,
    component: SnapshotComponent,
    rel: &Path,
    source: &impl RedbBacked,
) -> crate::Result<SnapshotFile> {
    Ok(SnapshotFile {
        component,
        path: rel_path_string(rel)?,
        bytes: read_redb_image(source.redb_database(), &data_dir.join(rel))?,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn catalogs_and_stores_round_trip_through_their_images() {
        let dir = tempfile::tempdir().unwrap();
        let system = SystemCatalog::open(&dir.path().join("system.redb")).unwrap();
        system.put_surrogate_hwm(4_242).unwrap();
        let cluster =
            nodedb_cluster::ClusterCatalog::open(&dir.path().join("cluster.redb")).unwrap();
        cluster.save_cluster_id(77).unwrap();
        let history = crate::event::scheduler::JobHistoryStore::open(dir.path()).unwrap();

        let captured = capture_node_state(
            dir.path(),
            &system,
            Some(&cluster),
            &[NodeRedbStore {
                component: SnapshotComponent::EventPlane,
                file_name: JOB_HISTORY_FILE,
                db: history.redb_database(),
            }],
        )
        .unwrap();
        let paths: Vec<_> = captured.files.iter().map(|f| f.path.as_str()).collect();
        assert_eq!(
            paths,
            [
                "system.redb",
                "cluster.redb",
                "event_plane/job_history.redb"
            ]
        );
        drop((system, cluster, history));

        let target = tempfile::tempdir().unwrap();
        std::fs::create_dir_all(target.path().join("event_plane")).unwrap();
        for file in &captured.files {
            std::fs::write(target.path().join(&file.path), &file.bytes).unwrap();
        }
        let system = SystemCatalog::open(&target.path().join("system.redb")).unwrap();
        assert_eq!(system.get_surrogate_hwm().unwrap(), 4_242);
        let cluster =
            nodedb_cluster::ClusterCatalog::open(&target.path().join("cluster.redb")).unwrap();
        assert_eq!(cluster.load_cluster_id().unwrap(), Some(77));
        assert!(crate::event::scheduler::JobHistoryStore::open(target.path()).is_ok());
    }

    /// Each store names its own file at open. A renamed file leaves the
    /// capture reading a path that no longer exists.
    #[test]
    fn store_file_names_match_what_each_store_opens() {
        let dir = tempfile::tempdir().unwrap();
        let data = dir.path();
        let _dlq = crate::event::trigger::TriggerDlq::open(data).unwrap();
        let _offsets = crate::event::cdc::OffsetStore::open(data).unwrap();
        let _dedup = crate::event::cross_shard::CrossShardDedup::open(data).unwrap();
        let _xdlq = crate::event::cross_shard::CrossShardDlq::open(data).unwrap();
        let _history = crate::event::scheduler::JobHistoryStore::open(data).unwrap();
        let _mv = crate::event::streaming_mv::MvPersistence::open(data).unwrap();
        let _log = crate::control::array_sync::OriginOpLog::open(data).unwrap();
        let _acks = crate::control::array_sync::ArrayAckRegistry::open(data).unwrap();
        let _snaps = crate::control::array_sync::OriginSnapshotStore::open(data).unwrap();
        for file in [
            TRIGGER_DLQ_FILE,
            CONSUMER_OFFSETS_FILE,
            CROSS_SHARD_DEDUP_FILE,
            CROSS_SHARD_DLQ_FILE,
            JOB_HISTORY_FILE,
            MV_STATE_FILE,
        ] {
            assert!(data.join(EVENT_PLANE_DIR).join(file).is_file(), "{file}");
        }
        for file in [ARRAY_OP_LOG_FILE, ARRAY_ACKS_FILE, ARRAY_SNAPSHOTS_FILE] {
            assert!(data.join(ARRAY_SYNC_DIR).join(file).is_file(), "{file}");
        }
    }

    fn wal_key_file(dir: &Path) -> PathBuf {
        use std::io::Write as _;
        use std::os::unix::fs::OpenOptionsExt as _;
        let path = dir.join("wal.key");
        let mut file = std::fs::OpenOptions::new()
            .create_new(true)
            .write(true)
            .mode(0o600)
            .open(&path)
            .unwrap();
        file.write_all(&[0x24; 32]).unwrap();
        path
    }

    /// The captured signing root restores to the path the WAL reads, so the
    /// restored node derives the same root the catalog fingerprint names.
    #[test]
    fn the_signing_root_restores_to_the_path_the_wal_opens() {
        let keys = tempfile::tempdir().unwrap();
        let key = wal_key_file(keys.path());
        let source = tempfile::tempdir().unwrap();
        let root = crate::wal::WalManager::open_encrypted(&source.path().join("wal"), false, &key)
            .unwrap()
            .crdt_signing_root()
            .unwrap()
            .unwrap();

        let mut files = Vec::new();
        capture_signing_root(source.path(), &mut files).unwrap();
        assert_eq!(files.len(), 1);
        let rel = crate::data::executor::snapshot::layout::check_restore_path(
            files[0].component,
            None,
            &files[0].path,
        )
        .unwrap();

        let target = tempfile::tempdir().unwrap();
        std::fs::create_dir_all(target.path().join("wal")).unwrap();
        std::fs::write(target.path().join(&rel), &files[0].bytes).unwrap();
        let restored =
            crate::wal::WalManager::open_encrypted(&target.path().join("wal"), false, &key)
                .unwrap()
                .crdt_signing_root()
                .unwrap();
        assert_eq!(restored, Some(root));
    }

    #[test]
    fn a_missing_signing_root_is_an_error() {
        let dir = tempfile::tempdir().unwrap();
        assert!(capture_signing_root(dir.path(), &mut Vec::new()).is_err());
    }

    #[test]
    fn a_core_component_is_refused_as_a_node_store() {
        let dir = tempfile::tempdir().unwrap();
        let system = SystemCatalog::open(&dir.path().join("system.redb")).unwrap();
        let other = redb::Database::create(dir.path().join("x.redb")).unwrap();
        let stores = [NodeRedbStore {
            component: SnapshotComponent::Kv,
            file_name: "x.redb",
            db: &other,
        }];
        assert!(capture_node_state(dir.path(), &system, None, &stores).is_err());
    }
}
