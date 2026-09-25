// SPDX-License-Identifier: BUSL-1.1

//! Reading the manifest that names the live vector checkpoint generation, plus
//! the shared typed-error constructor for this module's filesystem failures.

use super::format::{VECTOR_CKPT_FORMAT_VERSION, VectorCheckpointManifest};
use super::paths::VECTOR_CKPT_MANIFEST;
use crate::data::executor::checkpoint_decode_error::CheckpointDecodeError;

/// Read the live manifest under `ckpt_dir`, or `Ok(None)` when the file is
/// genuinely absent.
///
/// A present-but-unreadable, undecodable, or version-mismatched manifest is
/// `Err`, not `None`: the manifest is the only record of which generation is
/// live, and treating corruption as "no generation" would let the load path
/// restore nothing while the WAL below that generation's LSN may already be
/// gone.
///
/// A free function rather than a `CoreLoop` method because reclaim — which runs
/// against a data dir and not a live core — resolves the live generation
/// through this same reader, and the two must never diverge on what "live"
/// means.
pub(crate) fn read_vector_manifest_at(
    ckpt_dir: &std::path::Path,
) -> Result<Option<VectorCheckpointManifest>, CheckpointDecodeError> {
    let path = ckpt_dir.join(VECTOR_CKPT_MANIFEST);
    if !path.exists() {
        return Ok(None);
    }
    let bytes = nodedb_wal::segment::read_checkpoint_framed(&path).map_err(|source| {
        CheckpointDecodeError::ReadFile {
            path: path.clone(),
            source,
        }
    })?;
    let manifest = zerompk::from_msgpack::<VectorCheckpointManifest>(&bytes).map_err(|source| {
        CheckpointDecodeError::MsgpackDecode {
            path: path.clone(),
            source,
        }
    })?;
    if manifest.format_version != VECTOR_CKPT_FORMAT_VERSION {
        return Err(CheckpointDecodeError::FormatVersion {
            path,
            found: manifest.format_version,
            expected: VECTOR_CKPT_FORMAT_VERSION,
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

/// Wrap a filesystem failure as the vector engine's typed storage error.
pub(super) fn storage_err(
    path: &std::path::Path,
    action: &str,
    e: &dyn std::fmt::Display,
) -> crate::Error {
    crate::Error::Storage {
        engine: "vector".to_string(),
        detail: format!(
            "vector checkpoint: failed to {action} at {}: {e}",
            path.display()
        ),
    }
}
