// SPDX-License-Identifier: BUSL-1.1

//! Why a published checkpoint generation could not be decoded.
//!
//! One type across every engine's load path because they all answer the same
//! question the same way: a generation decodes WHOLE or not at all, and the
//! reason must name the specific corruption it found. A boot-time restore that
//! reports "the generation is bad" without saying which file, which value, and
//! what was expected leaves an operator staring at a full WAL replay with no way
//! to tell a truncated write from a format skew from a hand-edited data dir.

use std::path::PathBuf;

use crate::bridge::envelope::ErrorCode;

/// A corruption found while decoding one engine's checkpoint generation.
///
/// Every variant names a closed, specific failure — there is deliberately no
/// free-form catch-all, because the caller's only response is to abandon the
/// restore and replay the whole WAL, and the log line it emits is then the sole
/// record of what was actually wrong with the data dir.
#[derive(Debug, thiserror::Error)]
pub(crate) enum CheckpointDecodeError {
    /// The generation directory itself could not be listed.
    #[error("cannot scan {}: {source}", dir.display())]
    ScanDir {
        dir: PathBuf,
        #[source]
        source: std::io::Error,
    },

    /// One entry within an otherwise readable generation directory faulted.
    #[error("cannot read dir entry: {source}")]
    DirEntry {
        #[source]
        source: std::io::Error,
    },

    /// The filename does not encode the key its contents restore under. Guessing
    /// a key would install the state where nothing ever reads it.
    #[error("unparseable checkpoint filename {stem:?}")]
    UnparseableFilename { stem: String },

    /// A checkpoint file could not be read off disk.
    #[error("cannot read {}: {source}", path.display())]
    ReadFile {
        path: PathBuf,
        #[source]
        source: nodedb_wal::WalError,
    },

    /// A checkpoint file's MessagePack body did not decode.
    #[error("cannot decode {}: {source}", path.display())]
    MsgpackDecode {
        path: PathBuf,
        #[source]
        source: zerompk::Error,
    },

    /// The file was written by a build whose layout this one does not share.
    /// Reading it anyway would misinterpret every field after the skew.
    #[error("{} has format version {found}, expected {expected}", path.display())]
    FormatVersion {
        path: PathBuf,
        found: u16,
        expected: u16,
    },

    /// A manifest's replay stamp does not have the shape replay decisions
    /// rely on. Gating replay on it could skip a record no checkpoint holds.
    #[error("{} carries an invalid replay stamp: {source}", path.display())]
    InvalidReplayStamp {
        path: PathBuf,
        #[source]
        source: crate::data::executor::applied_prefix::InvalidReplayStamp,
    },

    /// A sparse-vector index file did not decode into an index. The decoder
    /// reports only success or failure, so there is no inner detail to carry.
    #[error("cannot decode {}", path.display())]
    UndecodableIndex { path: PathBuf },

    /// A columnar snapshot decoded but does not describe a rebuildable engine.
    ///
    /// Boxed: `ColumnarError` is the widest payload any variant here carries, and
    /// these errors travel in a `Result` on the boot path.
    #[error("cannot rebuild engine from {}: {source}", path.display())]
    EngineNotRebuildable {
        path: PathBuf,
        #[source]
        source: Box<nodedb_columnar::ColumnarError>,
    },

    /// The flushed segment blobs and their surrogate sidecar disagree in length.
    /// The two are held in lockstep by index, so a partial sidecar has no
    /// positional meaning at all.
    #[error(
        "{} has {segments} flushed segments but {surrogates} surrogate entries; the two are \
         held in lockstep by index and a partial sidecar cannot be positionally interpreted",
        path.display()
    )]
    SurrogateLockstepMismatch {
        path: PathBuf,
        segments: usize,
        surrogates: usize,
    },

    /// A KV collection file decoded, but its index registrations did not rebuild.
    /// Carries the inner corruption so the file AND the bad value are both named.
    #[error("cannot rebuild indexes from {}: {source}", path.display())]
    KvIndexes {
        path: PathBuf,
        #[source]
        source: Box<CheckpointDecodeError>,
    },

    /// A composite index's positions and fields do not line up, so no position
    /// can be attributed to a field.
    #[error("composite index {fields:?} has {positions} field positions for {field_count} fields")]
    CompositeFieldPositionMismatch {
        fields: Vec<String>,
        positions: usize,
        field_count: usize,
    },

    /// A sort direction outside the closed set the exporter writes. Refused
    /// rather than defaulted: the builder reads every non-"DESC" spelling as
    /// ascending, so accepting one would silently invert the index.
    #[error("sorted index {index:?} column {column:?} has unknown direction {direction:?}")]
    UnknownSortDirection {
        index: String,
        column: String,
        direction: String,
    },

    /// A window type outside the closed set the exporter writes. Refused for the
    /// same reason: an unknown one reads as unwindowed, silently widening the
    /// index to every entry ever written to it.
    #[error("sorted index {index:?} has unknown window type {window_type:?}")]
    UnknownWindowType { index: String, window_type: String },

    /// The exported sort definition was rejected by the same builder the live
    /// registration path uses, so a restored def could not match a registered one.
    ///
    /// The code is boxed for the same reason the bridge's `Response` boxes it.
    #[error("sorted index {index:?} is not rebuildable: {code:?}")]
    SortedIndexNotRebuildable { index: String, code: Box<ErrorCode> },

    /// An on-disk field position does not fit this machine's `usize`. The file
    /// holds `u64` so it does not encode the writer's pointer width; a value that
    /// does not fit is a corrupt file, not something to clamp.
    #[error("index on {field:?} has out-of-range field position {position}")]
    FieldPositionOutOfRange { field: String, position: u64 },
}

impl From<CheckpointDecodeError> for crate::Error {
    fn from(e: CheckpointDecodeError) -> Self {
        crate::Error::SegmentCorrupted {
            detail: e.to_string(),
        }
    }
}
