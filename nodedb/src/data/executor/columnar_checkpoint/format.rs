// SPDX-License-Identifier: BUSL-1.1

//! On-disk types for the columnar checkpoint: the manifest that publishes a
//! generation, and the per-collection files it names.

use serde::{Deserialize, Serialize};

use crate::data::executor::applied_prefix::ReplayStamp;

/// On-disk format version for the manifest and the collection files.
///
/// A file stamped with any other version is refused rather than misparsed.
/// Refusing costs a WAL replay; misparsing would install wrong rows AND a floor
/// that suppresses the records which would have corrected them.
pub(crate) const COLUMNAR_CKPT_FORMAT_VERSION: u16 = 3;

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
pub(crate) struct ColumnarCheckpointManifest {
    /// Always [`COLUMNAR_CKPT_FORMAT_VERSION`] when written; validated on load.
    pub format_version: u16,
    /// Which `gen-{n}/` directory holds the live collection files.
    pub generation: u64,
    /// The records the generation holds. Its prefix is also the LSN this core
    /// reports as the columnar engine's truncation floor once it is restored. WAL replay skips exactly the columnar
    /// records [`ReplayStamp::skips`] names and replays every other one.
    ///
    /// A single highest-applied LSN cannot state this. LSNs are node-global and
    /// records reach a core out of mint order, so a record below the highest
    /// applied one can still be on its way when the generation is written.
    pub replay: ReplayStamp,
}

/// One collection's full engine state within a generation.
#[derive(
    Debug, Clone, Serialize, Deserialize, zerompk::ToMessagePack, zerompk::FromMessagePack,
)]
pub(crate) struct ColumnarCheckpointFile {
    /// Always [`COLUMNAR_CKPT_FORMAT_VERSION`] when written; validated on load.
    pub format_version: u16,
    /// The complete `MutationEngine` state: memtable columns and their per-row
    /// surrogates, PK index, delete bitmaps, segment-id counters, schema, and
    /// the flushed segment blobs together with their per-row surrogate sidecar.
    ///
    /// One field, not two, deliberately: the segment blobs and their surrogate
    /// table are held in lockstep by position (outer index == segment index),
    /// and `nodedb_columnar` exports and imports both halves in a single call.
    /// Anything that could restore one without the other would corrupt every
    /// cross-engine prefilter silently, so this format cannot express it.
    ///
    /// The LSN lives in the manifest, not here — see the module docs for why a
    /// per-file stamp is unsound.
    pub engine: nodedb_columnar::ColumnarEngineSnapshot,
}
