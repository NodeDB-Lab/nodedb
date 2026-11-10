// SPDX-License-Identifier: BUSL-1.1

//! The `RETURNING` sequence transcripts of one Calvin txn on its leader.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex, MutexGuard};

use crate::control::server::shared::returning::transcript::AccessorTranscript;

/// One transcript per `RETURNING` plan of a txn, keyed by the plan's index
/// in the class's plan batch.
///
/// The leader keeps them for every stage of the txn, so a later evaluation
/// replays the sequence results of the first and makes no new sequence
/// call.
#[derive(Debug, Default)]
pub(super) struct TxnTranscripts {
    plans: Mutex<BTreeMap<u32, Arc<AccessorTranscript>>>,
    /// Held for the whole of one evaluation, so two evaluations of the txn
    /// never interleave on one transcript.
    evaluation: Mutex<()>,
}

impl TxnTranscripts {
    /// The transcript of the plan at `index`.
    pub(super) fn plan(&self, index: u32) -> Arc<AccessorTranscript> {
        let mut plans = self
            .plans
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        Arc::clone(plans.entry(index).or_default())
    }

    /// Hold the txn's transcripts for one evaluation.
    pub(super) fn evaluation(&self) -> MutexGuard<'_, ()> {
        self.evaluation
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn each_plan_keeps_one_transcript() {
        let transcripts = TxnTranscripts::default();
        assert!(Arc::ptr_eq(&transcripts.plan(2), &transcripts.plan(2)));
        assert!(!Arc::ptr_eq(&transcripts.plan(2), &transcripts.plan(3)));
    }
}
