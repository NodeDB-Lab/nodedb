// SPDX-License-Identifier: BUSL-1.1

//! Reading the manifest that names the live KV checkpoint generation, plus the
//! shared typed-error constructor for this module's filesystem failures.

use super::format::{KV_CKPT_FORMAT_VERSION, KvCheckpointManifest};
use super::paths::KV_CKPT_MANIFEST;
use crate::data::executor::checkpoint_decode_error::CheckpointDecodeError;
use crate::data::executor::core_loop::CoreLoop;

impl CoreLoop {
    /// Read the live manifest, or `Ok(None)` when the file is genuinely absent.
    ///
    /// A present-but-unreadable, undecodable, or version-mismatched manifest is
    /// `Err`, not `None`: the manifest is the only record of the LSN the
    /// generation it names is durable through, and treating corruption as
    /// "no generation" would let the caller install no floor while the WAL
    /// below that LSN may already be gone.
    pub(super) fn read_kv_manifest(
        &self,
        ckpt_dir: &std::path::Path,
    ) -> Result<Option<KvCheckpointManifest>, CheckpointDecodeError> {
        let path = ckpt_dir.join(KV_CKPT_MANIFEST);
        if !path.exists() {
            return Ok(None);
        }
        let bytes = nodedb_wal::segment::read_checkpoint_framed(&path).map_err(|source| {
            CheckpointDecodeError::ReadFile {
                path: path.clone(),
                source,
            }
        })?;
        let manifest = zerompk::from_msgpack::<KvCheckpointManifest>(&bytes).map_err(|source| {
            CheckpointDecodeError::MsgpackDecode {
                path: path.clone(),
                source,
            }
        })?;
        if manifest.format_version != KV_CKPT_FORMAT_VERSION {
            return Err(CheckpointDecodeError::FormatVersion {
                path: path.clone(),
                found: manifest.format_version,
                expected: KV_CKPT_FORMAT_VERSION,
            });
        }
        manifest
            .replay
            .validate()
            .map_err(|source| CheckpointDecodeError::InvalidReplayStamp {
                path: path.clone(),
                source,
            })?;
        Ok(Some(manifest))
    }
}

/// Wrap a filesystem failure as the KV engine's typed storage error.
pub(super) fn storage_err(
    path: &std::path::Path,
    action: &str,
    e: &dyn std::fmt::Display,
) -> crate::Error {
    crate::Error::Storage {
        engine: "kv".to_string(),
        detail: format!(
            "KV checkpoint: failed to {action} at {}: {e}",
            path.display()
        ),
    }
}
