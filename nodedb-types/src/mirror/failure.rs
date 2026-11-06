// SPDX-License-Identifier: Apache-2.0

use serde::{Deserialize, Serialize};

/// Why a mirror link stopped. Each variant names what an operator acts on.
///
/// Exhaustive matches are required everywhere this enum is matched — no
/// `_ =>` arms.
#[derive(
    Debug,
    Clone,
    PartialEq,
    Eq,
    Serialize,
    Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
    thiserror::Error,
)]
pub enum MirrorFailure {
    /// A source entry did not apply on the mirror.
    #[error("source entry {source_index} of group {source_group} did not apply: {detail}")]
    ApplyError {
        source_group: u64,
        source_index: u64,
        detail: String,
    },
    /// A source entry names a collection state the mirror catalog lacks.
    #[error(
        "source entry {source_index} of group {source_group} names a state of collection \
         '{collection}' the mirror catalog lacks"
    )]
    SchemaMismatch {
        collection: String,
        source_group: u64,
        source_index: u64,
    },
    /// A source entry does not decode on the mirror.
    #[error("source entry {source_index} of group {source_group} does not decode: {detail}")]
    PoisonEntry {
        source_group: u64,
        source_index: u64,
        detail: String,
    },
    /// The source compacted past what the mirror needs. Only an operator's
    /// rebuild clears it, because a rebuild removes the readable copy.
    #[error(
        "source group {group} retains its log from index {first_available}, past this \
         mirror's position"
    )]
    LogTruncated { group: u64, first_available: u64 },
    /// The source restored to another log timeline.
    #[error("the source cluster restored to another log timeline")]
    SourceTimelineChanged,
    /// The source database was dropped. The mirror keeps its data.
    #[error("the source database was dropped")]
    SourceDropped,
    /// The source ran an operation a mirror cannot follow.
    #[error("the source ran an operation a mirror cannot follow: {detail}")]
    UnsupportedSourceOp { detail: String },
    /// The source's retention bound passed before the bootstrap finished.
    #[error("the source's log retention bound passed before the bootstrap finished")]
    BootstrapOutrun,
    /// A capture could not install, or its install stopped part way. A partial
    /// install plus a newer capture leaves rows the source deleted, so only an
    /// operator's rebuild clears it.
    #[error("the bootstrap did not finish: {detail}")]
    BootstrapFailed { detail: String },
}

impl MirrorFailure {
    /// The failure's kind, without its per-occurrence detail.
    pub fn kind(&self) -> &'static str {
        match self {
            Self::ApplyError { .. } => "apply_error",
            Self::SchemaMismatch { .. } => "schema_mismatch",
            Self::PoisonEntry { .. } => "poison_entry",
            Self::LogTruncated { .. } => "log_truncated",
            Self::SourceTimelineChanged => "source_timeline_changed",
            Self::SourceDropped => "source_dropped",
            Self::UnsupportedSourceOp { .. } => "unsupported_source_op",
            Self::BootstrapOutrun => "bootstrap_outrun",
            Self::BootstrapFailed { .. } => "bootstrap_failed",
        }
    }

    /// Whether the replicated positions still name a state the link can resume
    /// from. Any other failure leaves only `REBOOTSTRAP`.
    pub fn is_resumable(&self) -> bool {
        match self {
            Self::ApplyError { .. }
            | Self::SchemaMismatch { .. }
            | Self::PoisonEntry { .. }
            | Self::UnsupportedSourceOp { .. } => true,
            Self::LogTruncated { .. }
            | Self::SourceTimelineChanged
            | Self::SourceDropped
            | Self::BootstrapOutrun
            | Self::BootstrapFailed { .. } => false,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_truncated_log_failure_names_the_group_and_its_first_index() {
        let text = MirrorFailure::LogTruncated {
            group: 3,
            first_available: 90,
        }
        .to_string();
        assert!(
            text.contains("group 3") && text.contains("index 90"),
            "{text}"
        );
    }
}
