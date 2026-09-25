// SPDX-License-Identifier: BUSL-1.1

//! On-disk types for the vector checkpoint.
//!
//! Only the manifest is defined here. The per-index files carry the raw bytes
//! `VectorCollection::checkpoint_to_bytes` produces, unwrapped: the collection
//! owns its own encoding (including its optional AES-256-GCM envelope), and the
//! whole generation's version belongs on the manifest that publishes it rather
//! than repeated in every file — a per-file stamp could disagree between
//! indexes, which is exactly the split the generation exists to make
//! unrepresentable.

use serde::{Deserialize, Serialize};

use crate::types::replay_stamp::ReplayStamp;

/// On-disk format version for the manifest and the generation it names.
///
/// A manifest stamped with any other version is refused rather than misparsed.
/// Refusing costs a WAL replay; misparsing would install indexes built from
/// bytes this build cannot read.
pub(crate) const VECTOR_CKPT_FORMAT_VERSION: u16 = 2;

/// Names the live generation. Writing this file is what publishes a checkpoint.
#[derive(
    Debug,
    Clone,
    PartialEq,
    Eq,
    Serialize,
    Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
pub(crate) struct VectorCheckpointManifest {
    /// Always [`VECTOR_CKPT_FORMAT_VERSION`] when written; validated on load.
    pub format_version: u16,
    /// Which `gen-{n}/` directory holds the live per-index files.
    pub generation: u64,
    /// The LSN every index in that generation is durable THROUGH (inclusive).
    ///
    /// Recorded so a restart knows what the PREVIOUS process actually made
    /// durable: it restores `vector_durable_lsn`, which is the point a failed
    /// flush clamps WAL truncation to. Without it the first failed flush after
    /// a restart would pin truncation at zero.
    pub durable_through_lsn: u64,
    /// The records every index in the generation holds. Restart replay skips
    /// a vector record exactly when this stamp names it.
    pub replay: ReplayStamp,
}

/// Encode a manifest publishing `generation`, for tests that need a live
/// generation on disk without driving a whole core through a flush.
///
/// Shares this module's writer so a test's idea of "published" can never drift
/// from the real one — a test fixture that hand-rolled the bytes would keep
/// passing after a format change that broke production.
#[cfg(test)]
pub(crate) fn test_manifest_bytes(generation: u64) -> Vec<u8> {
    zerompk::to_msgpack_vec(&VectorCheckpointManifest {
        format_version: VECTOR_CKPT_FORMAT_VERSION,
        generation,
        durable_through_lsn: 0,
        replay: ReplayStamp::default(),
    })
    .expect("manifest encode is infallible for this fixed struct")
}
