// SPDX-License-Identifier: BUSL-1.1

//! Point-in-time recovery storage every node of a test cluster shares: one
//! cold store, one snapshot store and one WAL key, as the nodes of a real
//! cluster share them. A node spawned with it opens its WAL encrypted at
//! `<data_dir>/wal`, the production layout a restore writes.

use std::io::Write as _;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};

use nodedb::ServerConfig;
use nodedb::config::server::{
    ClusterSettings, ColdStorageSettings, EncryptionSettings, SnapshotStorageSettings,
};
use nodedb::control::state::SharedState;
use nodedb::ctl::restore::RestoreScope;
use nodedb::wal::WalManager;
use nodedb_cluster::METADATA_GROUP_ID;
use nodedb_cluster::calvin::SEQUENCER_GROUP_ID;
use nodedb_wal::record::{RecordType, RestorePointPayload};

use crate::cluster_harness::cluster::StoppedNodeInfo;
use crate::cluster_harness::node::TestClusterNode;

type HarnessResult<T> = Result<T, Box<dyn std::error::Error + Send + Sync>>;

/// How long a node can take to record and archive a restore point.
const POINT_ARCHIVE_TIMEOUT: Duration = Duration::from_secs(60);

/// The shared PITR storage under one root directory.
#[derive(Clone, Debug)]
pub struct PitrStorage {
    root: PathBuf,
}

impl PitrStorage {
    /// Lay out the stores under `root` and write the WAL key.
    pub fn create(root: &Path) -> HarnessResult<Self> {
        std::fs::create_dir_all(root.join("cold"))?;
        std::fs::create_dir_all(root.join("snapshots"))?;
        let storage = Self {
            root: root.to_path_buf(),
        };
        let mut key = std::fs::OpenOptions::new();
        key.write(true).create_new(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt as _;
            key.mode(0o600);
        }
        key.open(storage.key_path())?.write_all(&[0x5C; 32])?;
        Ok(storage)
    }

    pub fn key_path(&self) -> PathBuf {
        self.root.join("wal.key")
    }

    pub fn key(&self) -> HarnessResult<nodedb_wal::crypto::WalEncryptionKey> {
        Ok(nodedb_wal::crypto::WalEncryptionKey::from_file(
            &self.key_path(),
        )?)
    }

    pub fn cold_settings(&self) -> HarnessResult<ColdStorageSettings> {
        Ok(serde_json::from_value(serde_json::json!({
            "local_dir": self.root.join("cold"),
        }))?)
    }

    pub fn snapshot_settings(&self) -> HarnessResult<SnapshotStorageSettings> {
        Ok(serde_json::from_value(serde_json::json!({
            "local_dir": self.root.join("snapshots"),
        }))?)
    }

    /// The server config of node `node_id` at `data_dir`, as `nodedb restore`
    /// reads it.
    pub fn server_config(
        &self,
        node_id: u64,
        data_dir: &Path,
        listen: SocketAddr,
    ) -> HarnessResult<ServerConfig> {
        let mut config = ServerConfig::default();
        config.server.data_dir = data_dir.to_path_buf();
        config.encryption = Some(EncryptionSettings::file(self.key_path()));
        config.cold_storage = Some(self.cold_settings()?);
        config.snapshot_storage = Some(self.snapshot_settings()?);
        config.pitr.enabled = true;
        config.cluster = Some(ClusterSettings {
            node_id,
            listen,
            seed_nodes: vec![listen],
            num_groups: 2,
            replication_factor: 3,
            force_bootstrap: false,
            tls: None,
            max_active_sessions: 0,
            login_attempts_per_ip_per_min: 30,
            login_attempts_per_user_per_min: 10,
            insecure_transport: true,
            log_compaction_threshold: None,
            join_retry_max_attempts: 8,
            join_retry_max_backoff_secs: 32,
            swim_listen: None,
        });
        Ok(config)
    }

    /// Wait until `node` recorded restore point `id` for every group it
    /// hosts, and cold storage holds those records and the metadata log
    /// through `id`.
    pub async fn await_point_archived(&self, node: &TestClusterNode, id: u64) -> HarnessResult<()> {
        let deadline = Instant::now() + POINT_ARCHIVE_TIMEOUT;
        loop {
            let last = point_archived(&node.shared, id).await;
            if matches!(last, Ok(true)) {
                return Ok(());
            }
            if Instant::now() >= deadline {
                return Err(format!(
                    "node {} did not archive restore point {id} within {POINT_ARCHIVE_TIMEOUT:?}: \
                     {last:?}",
                    node.node_id
                )
                .into());
            }
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
    }

    /// Rewrite a stopped node's data directory as its part of a cluster
    /// restore to `restore_point`: empty it except `tls/`, then restore as
    /// `nodedb restore --cluster --restore-point` does. Returns the report.
    pub async fn restore_node(
        &self,
        node: &StoppedNodeInfo,
        restore_point: u64,
    ) -> HarnessResult<String> {
        for entry in std::fs::read_dir(&node.data_dir)? {
            let entry = entry?;
            if entry.file_name() == "tls" {
                continue;
            }
            if entry.file_type()?.is_dir() {
                std::fs::remove_dir_all(entry.path())?;
            } else {
                std::fs::remove_file(entry.path())?;
            }
        }
        let config = self.server_config(node.node_id, &node.data_dir, node.listen_addr)?;
        let restored = nodedb::ctl::restore::restore_with_config(
            &config,
            &RestoreScope::Cluster { restore_point },
            None,
            false,
        )
        .await?;
        Ok(restored.to_string())
    }

    /// Install the shared cold and snapshot stores on `state`, which the
    /// node spawn already rooted at its data directory with its node-level
    /// stores in the production layout.
    pub(crate) fn install_stores(&self, state: &mut SharedState) -> HarnessResult<()> {
        let cold = nodedb::storage::cold::ColdStorage::new(
            self.cold_settings()?.to_cold_storage_config(),
        )?;
        state.cold_storage = Some(Arc::new(cold));
        state.snapshot_storage = nodedb::storage::snapshot_writer::build_snapshot_store(
            &self.snapshot_settings()?.to_snapshot_storage_config(),
            &state.data_dir,
        )?;
        Ok(())
    }
}

/// Open a node's WAL: with `pitr`, encrypted at `<data_dir>/wal`, the
/// production layout a restore writes; without, at `<data_dir>/test.wal`.
pub(crate) fn open_node_wal(
    pitr: Option<&PitrStorage>,
    data_dir: &Path,
) -> HarnessResult<Arc<WalManager>> {
    let Some(pitr) = pitr else {
        return Ok(Arc::new(WalManager::open_for_testing(
            &data_dir.join("test.wal"),
        )?));
    };
    let mut wal = WalManager::open_for_testing(&data_dir.join("wal"))?;
    wal.set_encryption_ring(nodedb_wal::crypto::KeyRing::new(pitr.key()?))?;
    Ok(Arc::new(wal))
}

/// The boot steps after a node's state is wired and before its Raft groups
/// start: read back the cut barriers its catalog holds, and with `pitr`,
/// seal a restored generation and resolve the node life.
pub(crate) async fn wire_boot(
    pitr: Option<&PitrStorage>,
    shared: &SharedState,
    cluster_catalog: &Arc<nodedb_cluster::ClusterCatalog>,
) -> HarnessResult<()> {
    shared
        .pitr
        .install_recorded_cuts(nodedb::control::pitr::restore_point::load_recorded_cuts(
            shared.credentials.catalog(),
        )?);
    if pitr.is_some() {
        nodedb::control::pitr::seal_restored_generation(shared).await?;
        nodedb::control::pitr::wire_pitr(shared, Arc::clone(cluster_catalog)).await?;
    }
    Ok(())
}

/// Whether `shared` recorded restore point `id` for every group it hosts and
/// archived the records and the metadata log through `id`. Seals and uploads
/// the WAL on the way.
async fn point_archived(shared: &Arc<SharedState>, id: u64) -> HarnessResult<bool> {
    let recorded: std::collections::BTreeSet<u64> = shared
        .wal
        .replay()?
        .iter()
        .filter(|record| {
            RecordType::from_raw(record.logical_record_type()) == Some(RecordType::RestorePoint)
        })
        .filter_map(|record| RestorePointPayload::from_bytes(&record.payload).ok())
        .filter(|point| point.id == id)
        .map(|point| point.group_id)
        .collect();
    let mut expected: std::collections::BTreeSet<u64> = [METADATA_GROUP_ID, SEQUENCER_GROUP_ID]
        .into_iter()
        .collect();
    if let Some(routing) = &shared.cluster_routing {
        let routing = routing.read().unwrap_or_else(|p| p.into_inner());
        for (group_id, info) in routing.group_members() {
            if info.members.contains(&shared.node_id) {
                expected.insert(*group_id);
            }
        }
    }
    if !expected.is_subset(&recorded) {
        return Ok(false);
    }
    if !nodedb::control::pitr::archive_wal_now(shared).await? {
        return Ok(false);
    }
    let life = shared.pitr.life().ok_or("PITR wired no node life")?;
    let cold = shared.cold_storage.as_ref().ok_or("no cold storage")?;
    let timeline = shared.credentials.catalog().load_metadata_timeline()?;
    let chunks = nodedb::storage::raft_log_archive::list_chunks(
        &cold.object_store(),
        cold.prefix(),
        nodedb::storage::raft_log_archive::ArchiveLife {
            timeline,
            node_id: life.node_id,
            incarnation: life.incarnation.as_str(),
        },
    )
    .await?;
    Ok(chunks.last().is_some_and(|chunk| chunk.last >= id))
}
