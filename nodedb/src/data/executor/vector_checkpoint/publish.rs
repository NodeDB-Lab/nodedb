// SPDX-License-Identifier: BUSL-1.1

//! Publishing a written vector checkpoint generation.
//!
//! Free functions rather than `CoreLoop` methods because the periodic flush is
//! not the only writer: snapshot restore rebuilds this core's whole index set
//! from a `CoreSnapshot` with no `CoreLoop` in hand, and it has to publish
//! through exactly the same manifest the load path reads.

use super::format::{VECTOR_CKPT_FORMAT_VERSION, VectorCheckpointManifest};
use super::manifest::{read_vector_manifest_at, storage_err};
use super::paths::VECTOR_CKPT_MANIFEST;
use crate::types::replay_stamp::ReplayStamp;

/// The generation number the next publish under `ckpt_dir` must use.
///
/// Never reuses a live generation: a reader holding the current manifest must
/// keep seeing an intact generation until the new one is published, so the new
/// files cannot be written over the live ones.
pub(crate) fn next_generation(ckpt_dir: &std::path::Path) -> crate::Result<u64> {
    Ok(read_vector_manifest_at(ckpt_dir)?.map_or(0, |m| m.generation.wrapping_add(1)))
}

/// Publish a written generation by atomically replacing the manifest.
///
/// This single write is the commit point of the whole checkpoint: before it
/// nothing changed; after it the entire generation is live, holding the
/// records `replay` names. It also
/// fsyncs `ckpt_dir`, the same directory holding the `gen-{n}/` entry, so that
/// entry cannot still be pending when the manifest naming it becomes visible.
pub(crate) fn publish_vector_generation(
    ckpt_dir: &std::path::Path,
    generation: u64,
    replay: ReplayStamp,
) -> crate::Result<()> {
    let manifest = VectorCheckpointManifest {
        format_version: VECTOR_CKPT_FORMAT_VERSION,
        generation,
        replay,
    };
    let bytes = zerompk::to_msgpack_vec(&manifest).map_err(|e| crate::Error::Serialization {
        format: "msgpack".to_string(),
        detail: format!("vector checkpoint manifest encode failed: {e}"),
    })?;
    let path = ckpt_dir.join(VECTOR_CKPT_MANIFEST);
    nodedb_wal::segment::write_checkpoint_framed(ckpt_dir, VECTOR_CKPT_MANIFEST, &bytes)
        .map_err(|e| storage_err(&path, "publish manifest", &e))?;
    Ok(())
}
