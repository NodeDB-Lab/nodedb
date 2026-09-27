// SPDX-License-Identifier: Apache-2.0

//! Tracking of the window between "bytes reached the file" and "the file was
//! fsynced", plus the terminal state a failed fsync puts a writer into.
//!
//! A flush and its fsync are two separate syscalls. Between them the records
//! exist only in the page cache, and the write buffer has already been
//! cleared — so an emptiness check alone cannot tell "nothing to make
//! durable" apart from "everything is written but nothing is durable yet".
//! Conflating the two lets a retry after a failed fsync report success over
//! records that were never persisted.

use std::fs::File;

use crate::error::{Result, WalError};

/// Where the writer stands relative to the last successful fsync.
pub(crate) enum DurabilityState {
    /// Every byte handed to the file has been fsynced.
    Synced,

    /// Bytes reached the file but no successful fsync has followed. A `sync`
    /// in this state must issue a real fsync even with an empty buffer.
    FlushedUnsynced,

    /// An fsync failed. The writer never leaves this state — see
    /// [`WalError::DurabilityLost`] for why the failure is terminal.
    Poisoned { detail: String },
}

impl DurabilityState {
    pub(crate) fn new() -> Self {
        Self::Synced
    }

    /// Reject any further work once an fsync has failed.
    pub(crate) fn check(&self) -> Result<()> {
        match self {
            Self::Poisoned { detail } => Err(WalError::DurabilityLost {
                detail: detail.clone(),
            }),
            Self::Synced | Self::FlushedUnsynced => Ok(()),
        }
    }

    /// An fsync is owed to the file even if the write buffer is empty.
    pub(crate) fn needs_fsync(&self) -> bool {
        matches!(self, Self::FlushedUnsynced)
    }

    /// Whether `sync`/`submit_and_sync` may return early without fsyncing.
    ///
    /// Both writers share this shape: an empty buffer alone doesn't mean
    /// there is nothing to do, because the flush clears the buffer before
    /// the fsync runs. Skipping is safe only when the buffer is empty *and*
    /// no flush is outstanding.
    pub(crate) fn should_skip_sync(&self, buffer_empty: bool) -> bool {
        buffer_empty && !self.needs_fsync()
    }

    /// Record that a write reached the file without an fsync behind it.
    pub(crate) fn record_flush(&mut self) {
        if matches!(self, Self::Synced) {
            *self = Self::FlushedUnsynced;
        }
    }

    /// Record that an fsync completed, clearing the outstanding flush.
    ///
    /// A poisoned writer stays poisoned: its lost records cannot be brought
    /// back by fsyncing whatever came after them.
    pub(crate) fn record_sync_ok(&mut self) {
        if !matches!(self, Self::Poisoned { .. }) {
            *self = Self::Synced;
        }
    }

    /// Enter the terminal state and produce the error every later call repeats.
    pub(crate) fn poison(&mut self, detail: String) -> WalError {
        let err = WalError::DurabilityLost {
            detail: detail.clone(),
        };
        // The single transition into the terminal state, so one lost-durability
        // event files one report no matter how many later calls repeat the
        // error out of [`DurabilityState::check`].
        crate::diag::durability_lost(&err, &detail);
        *self = Self::Poisoned { detail };
        err
    }
}

/// Fsync `file`, poisoning `state` if the kernel reports a failure.
///
/// The caller must have already checked [`DurabilityState::check`]; this
/// helper is the only place a successful fsync clears the outstanding-flush
/// marker, so the two can never drift apart.
pub(crate) fn fsync_and_track(file: &File, state: &mut DurabilityState) -> Result<()> {
    // Crash injection: the kernel reports a writeback error at the fsync.
    // Everything already handed to the page cache is unrecoverable from here.
    #[cfg(feature = "failpoints")]
    if let Some(detail) = nodedb_types::fail_point::eval_fail("wal::fsync_failure") {
        return Err(state.poison(format!(
            "fsync failed: failpoint wal::fsync_failure: {detail}"
        )));
    }

    if let Err(e) = file.sync_all() {
        return Err(state.poison(format!("fsync failed: {e}")));
    }

    state.record_sync_ok();
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn flush_marks_an_outstanding_fsync() {
        let mut state = DurabilityState::new();
        assert!(!state.needs_fsync());
        state.record_flush();
        assert!(state.needs_fsync());
    }

    #[test]
    fn successful_fsync_clears_the_marker() {
        let dir = crate::test_tempdir().unwrap();
        let file = File::create(dir.path().join("f")).unwrap();
        let mut state = DurabilityState::new();
        state.record_flush();

        fsync_and_track(&file, &mut state).unwrap();
        assert!(!state.needs_fsync());
        state.check().unwrap();
    }

    #[test]
    fn poison_is_permanent() {
        let mut state = DurabilityState::new();
        let err = state.poison("EIO".to_string());
        assert!(matches!(err, WalError::DurabilityLost { .. }));
        assert!(matches!(
            state.check(),
            Err(WalError::DurabilityLost { .. })
        ));

        // A later flush must not talk the writer back into a usable state.
        state.record_flush();
        assert!(matches!(
            state.check(),
            Err(WalError::DurabilityLost { .. })
        ));
    }
}
