// SPDX-License-Identifier: BUSL-1.1

//! The completion registry's side of a multi-part transaction that lost its
//! parts.

use super::completion::{CalvinCompletionRegistry, TxnId, VerdictOutcome};
use super::completion_verdict::VerdictSignal;
use super::sequencer::AbortReason;

impl CalvinCompletionRegistry {
    /// Record that the multi-part transaction `txn` lost its parts, applied
    /// from a `SequencerEntry::TxnPartsAbandoned` on every replica.
    ///
    /// - The stored verdict becomes `Abort(PartsLost)`, pushed to every local
    ///   scheduler. A participant that staged the parts that target it drops
    ///   them at the verdict.
    /// - The coordinator's outcome is `Aborted { PartsLost }` at once. It
    ///   waits for no ack: no participant staged the whole transaction, and
    ///   no verdict can ever commit it.
    ///
    /// Idempotent. A later ack of `txn` lands on the terminal entry, which
    /// the waiterless sweep removes.
    pub fn note_parts_abandoned(&self, txn: TxnId) {
        let verdict = VerdictOutcome::Abort(AbortReason::PartsLost);
        let mut inner = self.inner.lock().unwrap_or_else(|p| p.into_inner());
        {
            let entry = inner.entry_mut(txn);
            entry.abandoned = true;
            entry.verdict = Some(verdict);
        }
        let signal = VerdictSignal {
            epoch: txn.epoch,
            position: txn.position,
            verdict,
        };
        for tx in inner.verdict_signal_senders.values() {
            let _ = tx.try_send(signal);
        }
        // The entry stays: a participant that staged every part targeting
        // it probes the stored verdict when it parks, after this.
        inner.settle(txn);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::calvin::AttemptOutcome;

    #[tokio::test]
    async fn lost_parts_abort_a_registered_waiter_without_acks() {
        let reg = CalvinCompletionRegistry::new_detached();
        let txn = TxnId::new(4, 1);
        let rx = reg.register_completion(txn, 3);
        reg.note_parts_abandoned(txn);
        assert_eq!(
            rx.await.expect("outcome"),
            AttemptOutcome::Aborted {
                reason: AbortReason::PartsLost
            }
        );
        assert_eq!(reg.verdict(txn), Some(false));
    }

    #[tokio::test]
    async fn lost_parts_before_registration_abort_the_later_waiter() {
        let reg = CalvinCompletionRegistry::new_detached();
        let txn = TxnId::new(4, 2);
        reg.note_parts_abandoned(txn);
        let rx = reg.register_completion(txn, 3);
        assert_eq!(
            rx.await.expect("outcome"),
            AttemptOutcome::Aborted {
                reason: AbortReason::PartsLost
            }
        );
    }
}
