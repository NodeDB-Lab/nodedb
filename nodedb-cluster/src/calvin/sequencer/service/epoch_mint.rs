// SPDX-License-Identifier: BUSL-1.1

//! The minting half of a leader tick: drain the inbox into one epoch,
//! validate it, and propose the admitted batch.

use std::sync::atomic::Ordering;

use tracing::{debug, warn};

use crate::calvin::TxnId;
use crate::calvin::sequencer::entry::SequencerEntry;
use crate::calvin::sequencer::validator::validate_batch_with_assignments;
use crate::calvin::types::EpochBatch;
use crate::error::ClusterError;

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
        match self.propose_epoch_batch(entry) {
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
                self.last_proposed_index = log_index;
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

impl SequencerService {
    /// Append the epoch batch `entry` to the sequencer log under the
    /// `MultiRaft` lock.
    ///
    /// A cut marker appended after the batch's instant was minted can carry
    /// a floor at or above it (`MultiRaft::propose_to_sequencer`). The batch
    /// then carries the instant one millisecond above that floor. The check
    /// and the append share one lock hold, so no marker enters the log
    /// between them.
    fn propose_epoch_batch(&mut self, mut entry: SequencerEntry) -> Result<u64, ClusterError> {
        let encode = |entry: &SequencerEntry| {
            zerompk::to_msgpack_vec(entry).map_err(|e| ClusterError::Codec {
                detail: format!("sequencer encode: {e}"),
            })
        };
        let mut bytes = encode(&entry)?;
        let mut mr = self.multi_raft.lock().unwrap_or_else(|p| p.into_inner());
        let floor_ms = mr.sequencer_cut_floor_ms();
        if let SequencerEntry::EpochBatch { batch } = &mut entry
            && batch.epoch_system_ms <= floor_ms
        {
            batch.epoch_system_ms = floor_ms.saturating_add(1);
            self.last_minted_ms = self.last_minted_ms.max(Some(batch.epoch_system_ms));
            bytes = encode(&entry)?;
        }
        mr.propose_to_sequencer(bytes, 0)
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
    use super::super::core::tests::{elect, make_harness};
    use super::monotonic_epoch_ms;
    use crate::calvin::sequencer::entry::SequencerEntry;
    use crate::calvin::types::EpochBatch;

    fn cut_marker(hlc: u64, floor: i64) -> Vec<u8> {
        zerompk::to_msgpack_vec(&SequencerEntry::CutMarker {
            hlc,
            restore_point: 0,
            barrier: None,
            floor,
        })
        .expect("encode")
    }

    /// A closed read cut whose floor leads the leader's clock. The leader
    /// appended the marker after it minted the next epoch's instant. The
    /// epoch it appends after the marker still carries a later instant.
    #[test]
    fn an_epoch_appended_after_a_cut_marker_stamps_above_its_floor() {
        let mut harness = make_harness();
        elect(&harness.multi_raft);
        let floor_ms = 2_000_000_000_000;
        let floor = nodedb_types::ms_to_ordinal_upper(floor_ms) - 3;
        // Minted before the marker reached the log, from a slow clock.
        let minted = harness.service.next_epoch_system_ms(1_600_000_000_000);
        harness
            .multi_raft
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .propose_to_sequencer(cut_marker(7, floor), floor)
            .expect("append the marker");
        harness
            .service
            .propose_epoch_batch(SequencerEntry::EpochBatch {
                batch: EpochBatch {
                    epoch: 0,
                    txns: Vec::new(),
                    epoch_system_ms: minted,
                },
            })
            .expect("append the epoch");
        assert_eq!(harness.service.last_minted_ms, Some(floor_ms + 1));
        assert!(
            nodedb_types::calvin_txn_ordinal(floor_ms + 1, 0) > floor,
            "every version of the epoch lies above the closed cut"
        );
        // The next mint stays above it.
        assert!(harness.service.next_epoch_system_ms(1_600_000_000_000) > floor_ms + 1);
    }

    /// A new leader never saw the marker appended: it applied it. Applying
    /// the marker raises the applied instant, and the leader mints above it.
    #[test]
    fn a_new_leader_mints_above_an_applied_cut_floor() {
        let mut successor = make_harness();
        let floor_ms = 2_100_000_000_000;
        successor
            .state_machine
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .apply(
                1,
                &cut_marker(9, nodedb_types::ms_to_ordinal_upper(floor_ms)),
            );
        assert_eq!(
            successor.service.next_epoch_system_ms(1_600_000_000_000),
            floor_ms + 1
        );
    }

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
