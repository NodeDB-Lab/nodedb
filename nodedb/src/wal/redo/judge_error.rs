// SPDX-License-Identifier: BUSL-1.1

//! Why boot can not judge a committed record whose install has no outcome.

/// A committed record boot can not judge. Each one stops the boot: applying
/// or cancelling the record without its verdict can contradict the verdict
/// its live install reaches on every replica.
#[derive(Debug, thiserror::Error)]
pub enum RedoJudgeError {
    /// The record is no Calvin slice, and the WAL holds no complete commit
    /// boundary for it.
    #[error(
        "the committed redo at lsn {lsn} has no outcome and the WAL holds no complete commit \
         boundary for it: boot can not validate it; restore the node from a replica or a backup"
    )]
    BoundaryMissing { lsn: u64 },
    /// The validate failed for a reason of this node, not of the record.
    #[error(
        "the boot validate of the committed redo at lsn {lsn} could not read this node's state \
         ({detail}); check the node's disks and WAL directory, then restart"
    )]
    Unreadable { lsn: u64, detail: String },
    /// The record's header names an event source this build does not know.
    #[error(
        "the committed redo at lsn {lsn} names unknown event source code {code}; start the node \
         with the build that wrote the WAL"
    )]
    EventSourceUnknown { lsn: u64, code: u8 },
}

impl From<RedoJudgeError> for crate::Error {
    fn from(error: RedoJudgeError) -> Self {
        crate::Error::Storage {
            engine: "wal_replay".into(),
            detail: error.to_string(),
        }
    }
}
