// SPDX-License-Identifier: BUSL-1.1

//! How an undo restores the core's `last_ts_ingest` timer.

/// How an undo restores the core's `last_ts_ingest` timer.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(in crate::data::executor) enum TimerRestore {
    /// Set the timer to the pre-image's value.
    Exact,
    /// Set the timer to the later of its current and pre-image values. A
    /// write that interleaved with the rolled-back install keeps its timer.
    KeepNewer,
}

impl TimerRestore {
    /// The timer value after restoring `before` over `current`.
    pub(in crate::data::executor) fn resolve(
        self,
        current: Option<std::time::Instant>,
        before: Option<std::time::Instant>,
    ) -> Option<std::time::Instant> {
        match self {
            Self::Exact => before,
            Self::KeepNewer => match (current, before) {
                (Some(now), Some(then)) => Some(now.max(then)),
                (now, then) => now.or(then),
            },
        }
    }
}
