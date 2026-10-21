// SPDX-License-Identifier: BUSL-1.1

//! What a restore reads from the server config: the data directory it
//! writes, the stores it reads, the WAL key, and the node id.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use object_store::ObjectStore;

use super::error::RestoreError;
use crate::ServerConfig;
use crate::config::server::{SnapshotStorageSettings, apply_env_overrides};
use crate::storage::cold::{ColdStorage, DEFAULT_COLD_LOCAL_DIR};
use crate::storage::snapshot_writer::build_snapshot_store;

pub struct RestoreEnv {
    pub data_dir: PathBuf,
    pub node_id: u64,
    pub key: nodedb_wal::crypto::WalEncryptionKey,
    /// The snapshot store root. Each node life's bases sit under
    /// `{node_id}/{incarnation}/`.
    pub snapshot_root: Arc<dyn ObjectStore>,
    pub cold: ColdStorage,
    /// Block size the cut WAL segment is padded to.
    pub alignment: usize,
}

impl RestoreEnv {
    /// Read `path` the way the server boot reads it: the file, then the
    /// `NODEDB_*` environment overrides.
    pub async fn from_config_file(path: &Path, kept: &[&str]) -> Result<Self, RestoreError> {
        let mut config = ServerConfig::from_file(path)?;
        apply_env_overrides(&mut config)?;
        Self::from_config(&config, kept).await
    }

    /// Refuse a data directory holding any entry other than those named in
    /// `kept`, or a local store inside it, before any store is opened.
    /// The WAL key comes from the source `[encryption]` names, as at boot.
    pub async fn from_config(config: &ServerConfig, kept: &[&str]) -> Result<Self, RestoreError> {
        let data_dir = absolute(&config.server.data_dir)?;
        require_empty_target(&data_dir, kept)?;

        let encryption = config
            .encryption
            .as_ref()
            .ok_or(RestoreError::MissingConfig {
                section: "encryption",
                why: "base snapshots are encrypted with the WAL key",
            })?;
        let cold_settings = config
            .cold_storage
            .as_ref()
            .ok_or(RestoreError::MissingConfig {
                section: "cold_storage",
                why: "the WAL archive lives in cold storage",
            })?;
        let snapshot_config = config
            .snapshot_storage
            .as_ref()
            .map(|s| s.to_snapshot_storage_config())
            .unwrap_or_else(SnapshotStorageSettings::default_storage_config);
        let cold_config = cold_settings.to_cold_storage_config();

        if snapshot_config.endpoint.is_empty() {
            let dir = snapshot_config
                .local_dir
                .clone()
                .unwrap_or_else(|| data_dir.join("snapshots"));
            refuse_inside(&data_dir, &dir, "snapshot_storage")?;
        }
        if cold_config.endpoint.is_empty() {
            let dir = cold_config
                .local_dir
                .clone()
                .unwrap_or_else(|| PathBuf::from(DEFAULT_COLD_LOCAL_DIR));
            refuse_inside(&data_dir, &dir, "cold_storage")?;
        }

        let key = crate::control::security::keystore::load_wal_key(
            encryption,
            std::sync::Arc::new(config.egress.checked_policy()?),
        )
        .await?;
        let snapshot_root = build_snapshot_store(&snapshot_config, &data_dir)?;
        let cold = ColdStorage::new(cold_config)?;
        Ok(Self {
            data_dir,
            node_id: crate::control::cluster::configured_node_id(config),
            key,
            snapshot_root,
            cold,
            alignment: config.tuning.wal.alignment,
        })
    }
}

/// Refuse a data directory that holds any entry not named in `kept`. An
/// absent one is fine: the restore creates it.
pub fn require_empty_target(data_dir: &Path, kept: &[&str]) -> Result<(), RestoreError> {
    // no-objectstore: the restore target is a local data directory.
    let entries = match std::fs::read_dir(data_dir) {
        Ok(entries) => entries,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(e) => return Err(crate::Error::Io(e).into()),
    };
    for entry in entries {
        let entry = entry.map_err(crate::Error::Io)?;
        if !kept.iter().any(|name| entry.file_name() == **name) {
            return Err(crate::Error::RestoreTargetNotEmpty {
                path: data_dir.to_path_buf(),
            }
            .into());
        }
    }
    Ok(())
}

fn refuse_inside(
    data_dir: &Path,
    store_dir: &Path,
    section: &'static str,
) -> Result<(), RestoreError> {
    let store_dir = absolute(store_dir)?;
    if store_dir.starts_with(data_dir) {
        return Err(RestoreError::StoreInsideDataDir {
            section,
            store_dir,
            data_dir: data_dir.to_path_buf(),
        });
    }
    Ok(())
}

fn absolute(path: &Path) -> Result<PathBuf, RestoreError> {
    std::path::absolute(path).map_err(|e| crate::Error::Io(e).into())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_missing_or_empty_target_is_accepted() {
        let dir = tempfile::tempdir().unwrap();
        require_empty_target(&dir.path().join("absent"), &[]).unwrap();
        require_empty_target(dir.path(), &[]).unwrap();
        assert!(
            !dir.path().join("absent").exists(),
            "the check creates nothing"
        );
    }

    #[test]
    fn a_non_empty_target_is_refused() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::create_dir(dir.path().join("tls")).unwrap();
        require_empty_target(dir.path(), &["tls"]).unwrap();
        std::fs::write(dir.path().join("system.redb"), b"x").unwrap();
        for kept in [&[][..], &["tls"][..]] {
            assert!(matches!(
                require_empty_target(dir.path(), kept),
                Err(RestoreError::Node(
                    crate::Error::RestoreTargetNotEmpty { .. }
                ))
            ));
        }
    }

    #[test]
    fn a_store_inside_the_data_directory_is_refused() {
        let data = Path::new("/srv/nodedb");
        assert!(matches!(
            refuse_inside(data, Path::new("/srv/nodedb/snapshots"), "snapshot_storage"),
            Err(RestoreError::StoreInsideDataDir {
                section: "snapshot_storage",
                ..
            })
        ));
        refuse_inside(data, Path::new("/srv/nodedb-snapshots"), "snapshot_storage").unwrap();
    }

    #[tokio::test]
    async fn a_config_without_snapshot_storage_is_refused_before_anything_is_created() {
        let dir = tempfile::tempdir().unwrap();
        let data_dir = dir.path().join("data");
        let mut config = ServerConfig::default();
        config.server.data_dir = data_dir.clone();
        config.encryption = Some(crate::config::server::EncryptionSettings::file(
            dir.path().join("wal.key"),
        ));
        config.cold_storage = Some(
            toml::from_str(&format!(
                "local_dir = \"{}\"",
                dir.path().join("cold").display()
            ))
            .unwrap(),
        );
        assert!(matches!(
            RestoreEnv::from_config(&config, &[]).await,
            Err(RestoreError::StoreInsideDataDir {
                section: "snapshot_storage",
                ..
            })
        ));
        assert!(!data_dir.exists());
    }
}
