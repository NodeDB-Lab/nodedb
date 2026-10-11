// SPDX-License-Identifier: BUSL-1.1

//! The minting half of a leader tick: drain the inbox into one epoch,
//! validate it, and propose the admitted batch.

use std::sync::atomic::Ordering;

use tracing::{debug, warn};

use crate::calvin::TxnId;
use crate::calvin::sequencer::entry::SequencerEntry;
use crate::calvin::sequencer::validator::validate_batch_with_assignments;
use crate::calvin::types::EpochBatch;

use super::core::SequencerService;

impl SequencerService {
    /// The epoch instant the next minted epoch carries, given the wall clock
    /// reads `wall_ms`. Records it as this service's last mint.
    ///
    /// The instant is strictly above every instant this service minted and
    /// every one the state machine applied. The state machine rebuilds its
    /// instant from the replayed log, so the order holds across a wall clock
    /// that steps back, a leader change and a restart.
    pub(super) fn next_epoch_system_ms(&mut self, wall_ms: i64) -> i64 {
        let applied = self
            .state_machine
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .last_epoch_system_ms();
        let minted = monotonic_epoch_ms(wall_ms, self.last_minted_ms.max(applied));
        self.last_minted_ms = Some(minted);
        minted
    }

    /// Sequence the queued submissions into epoch `epoch`.
    ///
    /// Every drained submission leaves with an answer. An admitted one gets
    /// its assignment once the batch is in the Raft log. A rejected one, and
    /// every one of a batch whose proposal failed, has its assignment
    /// dropped, so its caller reads a closed channel at once. None of those
    /// is in the log, so a retry never applies a transaction twice.
    ///
    /// The epoch advances when the batch is proposed, and when every
    /// candidate was rejected. A failed proposal leaves it unchanged, so the
    /// next tick re-attempts the same epoch.
    pub(super) fn mint_epoch(&mut self, epoch: u64) {
        let mut candidates = Vec::new();
        let drained = self.inbox_receiver.drain_into_capped(
            &mut candidates,
            self.config.max_txns_per_epoch,
            self.config.max_bytes_per_epoch,
        );
        if drained == 0 {
            debug!(
                node_id = self.node_id,
                epoch, "epoch tick: inbox empty, no proposal"
            );
            return;
        }

        let (admitted, rejected) = validate_batch_with_assignments(epoch, candidates);

        self.metrics
            .admitted_total
            .fetch_add(admitted.len() as u64, Ordering::Relaxed);

        // Record per-conflict metrics and fail each rejected submission.
        let rejected_count = rejected.len();
        for rejection in rejected {
            self.completion_registry
                .drop_assignment(rejection.admitted.inbox_seq);
            self.metrics
                .rejected_conflict_total
                .fetch_add(1, Ordering::Relaxed);
            if let Some(ctx) = rejection.conflict_context {
                self.metrics.record_conflict(ctx);
            }
        }

        if admitted.is_empty() {
            debug!(
                epoch,
                rejected = rejected_count,
                "epoch tick: all candidates rejected, no proposal"
            );
            self.advance_epoch(epoch + 1);
            return;
        }

        // Read wall clock ONCE on the sequencer leader. This is the single
        // deterministic timestamp source for every transaction in this epoch.
        // All replicas receive this value via Raft replication; engine handlers
        // use it instead of reading the wall clock independently.
        let wall_ms = std::time::SystemTime::now() // no-determinism: read once on leader; replicated to all replicas via Raft
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| i64::try_from(d.as_millis()).unwrap_or(i64::MAX))
            .unwrap_or(0);
        let epoch_system_ms = self.next_epoch_system_ms(wall_ms);

        // A multi-part transaction enters the batch as its header. Its
        // coordinator streams the parts once the batch is proposed, and each
        // is proposed as its own entry.
        let streams: Vec<(TxnId, crate::calvin::types::MultiPartPlans)> = admitted
            .iter()
            .filter_map(|(_, txn)| {
                let manifest = txn.tx_class.multi_part.clone()?;
                Some((TxnId::new(epoch, txn.position), manifest))
            })
            .collect();

        // `(inbox_seq, txn, participants)` of each admitted submission.
        let assignments: Vec<(u64, TxnId, usize)> = admitted
            .iter()
            .map(|(inbox_seq, txn)| {
                (
                    *inbox_seq,
                    TxnId::new(epoch, txn.position),
                    txn.tx_class.participating_vshards().len(),
                )
            })
            .collect();
        let batch = EpochBatch {
            epoch,
            txns: admitted.into_iter().map(|(_, txn)| txn).collect(),
            epoch_system_ms,
        };
        let txns_count = batch.txns.len();
        let entry = SequencerEntry::EpochBatch { batch };
        let _replicate_span =
            tracing::info_span!("sequencer_replicate", epoch, txns_count).entered();
        match self.propose_entry(&entry) {
            Ok(log_index) => {
                // Open each stream before its submitter learns the
                // assignment, so its first offer finds the stream.
                for (txn, manifest) in streams {
                    self.open_part_stream(txn, &manifest);
                }
                for (inbox_seq, txn, participants) in assignments {
                    self.completion_registry
                        .note_assigned(inbox_seq, txn, participants);
                }
                debug!(
                    epoch,
                    log_index,
                    admitted = txns_count,
                    rejected = rejected_count,
                    "sequencer proposed epoch batch"
                );
                self.advance_epoch(epoch + 1);
            }
            Err(e) => {
                warn!(
                    epoch,
                    error = %e,
                    "sequencer propose failed; the batch's submissions are failed and \
                     the epoch is retried on the next tick if still leader"
                );
                for (inbox_seq, _, _) in assignments {
                    self.completion_registry.drop_assignment(inbox_seq);
                }
            }
        }
    }
}

/// The epoch instant a mint carries: the wall clock, raised strictly above
/// `floor`, the highest instant minted or applied before it.
fn monotonic_epoch_ms(wall_ms: i64, floor: Option<i64>) -> i64 {
    match floor {
        Some(floor) => wall_ms.max(floor.saturating_add(1)),
        None => wall_ms,
    }
}

#[cfg(test)]
mod tests {
    use super::monotonic_epoch_ms;

    #[test]
    fn the_first_mint_takes_the_wall_clock() {
        assert_eq!(monotonic_epoch_ms(1_000, None), 1_000);
    }

    #[test]
    fn a_wall_clock_that_steps_back_still_mints_above_the_floor() {
        assert_eq!(monotonic_epoch_ms(900, Some(1_000)), 1_001);
        assert_eq!(monotonic_epoch_ms(1_000, Some(1_000)), 1_001);
        assert_eq!(monotonic_epoch_ms(5_000, Some(1_000)), 5_000);
    }
}
