// SPDX-License-Identifier: BUSL-1.1

//! Reading the manifest that names the live sparse-vector checkpoint
//! generation, plus the shared typed-error constructor for this module's
//! filesystem failures.

use super::format::{SPARSE_VECTOR_CKPT_FORMAT_VERSION, SparseVectorCheckpointManifest};
use super::paths::SPARSE_VECTOR_CKPT_MANIFEST;
use crate::data::executor::checkpoint_decode_error::CheckpointDecodeError;

/// Read the live manifest under `ckpt_dir`, or `Ok(None)` when the file is
/// genuinely absent.
///
/// A present-but-unreadable, undecodable, or version-mismatched manifest is
/// `Err`, not `None`: the manifest is the only record of the LSN the
/// generation it names is durable through, and treating corruption as "no
/// generation" would let the load path restore nothing while the WAL below
/// that LSN may already be gone. `core_id` is currently unused (kept for
/// interface symmetry with callers that carry it); it never decides absence
/// vs. corruption.
///
/// A free function rather than a `CoreLoop` method because reclaim — which runs
/// against a data dir and not a live core — resolves the live generation through
/// this same reader, and the two must never diverge on what "live" means.
pub(crate) fn read_sparse_vector_manifest_at(
    ckpt_dir: &std::path::Path,
    _core_id: usize,
) -> Result<Option<SparseVectorCheckpointManifest>, CheckpointDecodeError> {
    let path = ckpt_dir.join(SPARSE_VECTOR_CKPT_MANIFEST);
    if !path.exists() {
        return Ok(None);
    }
    let bytes = nodedb_wal::segment::read_checkpoint_framed(&path).map_err(|source| {
        CheckpointDecodeError::ReadFile {
            path: path.clone(),
            source,
        }
    })?;
    let manifest =
        zerompk::from_msgpack::<SparseVectorCheckpointManifest>(&bytes).map_err(|source| {
            CheckpointDecodeError::MsgpackDecode {
                path: path.clone(),
                source,
            }
        })?;
    if manifest.format_version != SPARSE_VECTOR_CKPT_FORMAT_VERSION {
        return Err(CheckpointDecodeError::FormatVersion {
            path: path.clone(),
            found: manifest.format_version,
            expected: SPARSE_VECTOR_CKPT_FORMAT_VERSION,
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

/// Wrap a filesystem failure as the sparse-vector engine's typed storage error.
pub(super) fn storage_err(
    path: &std::path::Path,
    action: &str,
    e: &dyn std::fmt::Display,
) -> crate::Error {
    crate::Error::Storage {
        engine: "sparse_vector".to_string(),
        detail: format!(
            "sparse-vector checkpoint: failed to {action} at {}: {e}",
            path.display()
        ),
    }
}
