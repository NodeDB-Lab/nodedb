// SPDX-License-Identifier: BUSL-1.1

//! Where the history the sequencer state machine holds begins.
//!
//! A leader seeds its first epoch from the state machine. The seed is exact
//! only when the state holds the effect of every committed entry. That holds
//! for a state built from the log's first entry, and for one restored from a
//! snapshot that itself held complete history. It does not hold for a log
//! whose start was discarded with no snapshot to restore the state from.

use super::core::SequencerStateMachine;

/// Where the state this replica holds begins.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum HistoryOrigin {
    /// Built by applying the sequencer log from its first entry.
    #[default]
    LogStart,
    /// Restored from a snapshot that holds the state through log index
    /// `through`. Entries after it apply on top.
    Snapshot { through: u64 },
    /// The log starts above its first entry, and no snapshot restored the
    /// state the discarded entries built.
    Unknown,
}

impl HistoryOrigin {
    /// Whether the state holds the effect of every committed entry.
    pub fn is_known(self) -> bool {
        !matches!(self, Self::Unknown)
    }
}

impl SequencerStateMachine {
    /// Where the history this state machine holds begins.
    pub fn history_origin(&self) -> HistoryOrigin {
        self.history
    }

    /// Record that the sequencer log starts above its first entry and no
    /// snapshot restored the discarded state. A later snapshot restore
    /// replaces it.
    pub fn mark_history_unknown(&mut self) {
        self.history = HistoryOrigin::Unknown;
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::*;
    use crate::calvin::CalvinCompletionRegistry;

    #[test]
    fn a_fresh_state_machine_holds_history_from_the_log_start() {
        let mut sm =
            SequencerStateMachine::new(HashMap::new(), CalvinCompletionRegistry::new_detached());
        assert_eq!(sm.history_origin(), HistoryOrigin::LogStart);
        assert!(sm.history_origin().is_known());
        sm.mark_history_unknown();
        assert_eq!(sm.history_origin(), HistoryOrigin::Unknown);
        assert!(!sm.history_origin().is_known());
    }
}
