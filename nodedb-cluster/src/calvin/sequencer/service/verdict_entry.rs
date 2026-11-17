// SPDX-License-Identifier: BUSL-1.1

//! Encoding of an aggregated commit/abort decision into the sequencer log entry
//! the leader proposes for it.

use crate::calvin::sequencer::entry::SequencerEntry;
use crate::calvin::{AbortCause, TxnId, VerdictOutcome};

/// Encode a decision as the entry the leader proposes. A commit is `Verdict`.
/// An abort is `AbortVerdict`, which carries the reason and the refusal the
/// coordinator reports.
pub(crate) fn verdict_entry(txn: TxnId, outcome: VerdictOutcome) -> SequencerEntry {
    match outcome {
        VerdictOutcome::Commit => SequencerEntry::Verdict {
            epoch: txn.epoch,
            position: txn.position,
        },
        VerdictOutcome::Abort(AbortCause {
            reason,
            refusal,
            other_refusals,
        }) => SequencerEntry::AbortVerdict {
            epoch: txn.epoch,
            position: txn.position,
            reason,
            refusal,
            other_refusals,
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::calvin::AbortReason;

    #[test]
    fn abort_proposes_abort_verdict_with_its_reason() {
        let entry = verdict_entry(
            TxnId::new(4, 1),
            VerdictOutcome::Abort(AbortReason::ParticipantError.into()),
        );
        assert_eq!(
            entry,
            SequencerEntry::AbortVerdict {
                epoch: 4,
                position: 1,
                reason: AbortReason::ParticipantError,
                refusal: None,
                other_refusals: Vec::new(),
            }
        );
    }

    #[test]
    fn abort_proposes_abort_verdict_with_its_refusal() {
        let entry = verdict_entry(
            TxnId::new(4, 3),
            VerdictOutcome::Abort(AbortCause::refused(vec![5, 6])),
        );
        assert_eq!(
            entry,
            SequencerEntry::AbortVerdict {
                epoch: 4,
                position: 3,
                reason: AbortReason::ParticipantError,
                refusal: Some(vec![5, 6]),
                other_refusals: Vec::new(),
            }
        );
    }

    #[test]
    fn commit_proposes_verdict() {
        assert_eq!(
            verdict_entry(TxnId::new(4, 2), VerdictOutcome::Commit),
            SequencerEntry::Verdict {
                epoch: 4,
                position: 2,
            }
        );
    }
}
