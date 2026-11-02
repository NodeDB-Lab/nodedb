// SPDX-License-Identifier: BUSL-1.1

use std::path::PathBuf;

use serde::{Deserialize, Serialize};

/// Snapshot storage configuration for warm-tier snapshot archives.
///
/// Example TOML:
/// ```toml
/// [snapshot_storage]
/// bucket = "my-nodedb-snapshots"
/// region = "us-east-1"
/// ```
///
/// When `endpoint` is empty, snapshots are stored on the local filesystem
/// under `local_dir` (default: `{data_dir}/snapshots`).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SnapshotStorageSettings {
    /// S3-compatible endpoint URL. Empty = local filesystem (default).
    #[serde(default)]
    pub endpoint: String,
    /// Bucket name.
    #[serde(default = "default_snapshot_bucket")]
    pub bucket: String,
    /// Prefix path within the bucket.
    #[serde(default = "default_snapshot_prefix")]
    pub prefix: String,
    /// Access key (empty = IAM role / instance credentials).
    #[serde(default)]
    pub access_key: String,
    /// Secret key.
    #[serde(default)]
    pub secret_key: String,
    /// Region (required for AWS S3; ignored by most S3-compatible stores).
    #[serde(default = "default_snapshot_region")]
    pub region: String,
    /// Local directory for snapshot storage (used when endpoint is empty).
    #[serde(default)]
    pub local_dir: Option<PathBuf>,
}

fn default_snapshot_bucket() -> String {
    "nodedb-snapshots".into()
}

fn default_snapshot_prefix() -> String {
    "snapshots/".into()
}

fn default_snapshot_region() -> String {
    "us-east-1".into()
}

impl SnapshotStorageSettings {
    /// Convert to the `SnapshotStorageConfig` used by the storage layer.
    pub fn to_snapshot_storage_config(
        &self,
    ) -> crate::storage::snapshot_writer::SnapshotStorageConfig {
        crate::storage::snapshot_writer::SnapshotStorageConfig {
            endpoint: self.endpoint.clone(),
            bucket: self.bucket.clone(),
            prefix: self.prefix.clone(),
            access_key: self.access_key.clone(),
            secret_key: self.secret_key.clone(),
            region: self.region.clone(),
            local_dir: self.local_dir.clone(),
        }
    }

    /// Default storage-layer config (local filesystem) used when the
    /// `[snapshot_storage]` section is omitted from the server config.
    pub fn default_storage_config() -> crate::storage::snapshot_writer::SnapshotStorageConfig {
        crate::storage::snapshot_writer::SnapshotStorageConfig {
            endpoint: String::new(),
            bucket: default_snapshot_bucket(),
            prefix: default_snapshot_prefix(),
            access_key: String::new(),
            secret_key: String::new(),
            region: default_snapshot_region(),
            local_dir: None,
        }
    }
}
