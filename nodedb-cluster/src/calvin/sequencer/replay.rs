// SPDX-License-Identifier: BUSL-1.1

//! Sequencer Raft-log replay: the scheduler-side catch-up decode path.
//!
//! When the live fan-out in [`super::state_machine::SequencerStateMachine::apply`]
//! drops an input on a full/closed vShard channel, the scheduler re-reads the
//! committed sequencer Raft log and decodes it back into the SAME
//! [`SchedulerInput`] stream the live path delivers, then feeds it through the
//! same idempotent scheduler processing. This module owns that decode.
//!
//! DETERMINISM CONTRACT: the [`SchedulerInput`] values produced here MUST be
//! bit-for-bit identical to what the live `apply` fan-out produced for the same
//! entries — same per-vShard `epoch_vshard_txn_count`, same `epoch_system_ms`
//! stamping, same order. Any divergence would let a replica that took the replay
//! path reach a different lock-table state than one that took the live path.

use std::collections::HashMap;

use crate::calvin::sequencer::entry::SequencerEntry;
use crate::calvin::sequencer::state_machine::SequencerStateMachine;
use crate::calvin::types::{EpochBatch, SchedulerInput};

/// Compute the per-vShard count of how many of `batch`'s positions target
/// each vShard: every position of an epoch targeting a given vShard is
/// stamped with the same count of that epoch's positions targeting the
/// vShard.
///
/// DETERMINISM CONTRACT: this is the single source of truth for the count
/// used to stamp `epoch_vshard_txn_count` on both the live
/// [`SequencerStateMachine::apply`] fan-out path and this module's
/// [`SequencerStateMachine::replay_epochs_for_vshard`] replay path. The two
/// MUST always call this same helper rather than recomputing the loop
/// independently, or the two paths could silently drift.
pub(crate) fn compute_vshard_txn_counts(batch: &EpochBatch) -> HashMap<u32, u32> {
    let mut vshard_txn_counts: HashMap<u32, u32> = HashMap::new();
    for txn in &batch.txns {
        for vshard_id in txn.tx_class.participating_vshards() {
            *vshard_txn_counts.entry(vshard_id.as_u32()).or_insert(0) += 1;
        }
    }
    vshard_txn_counts
}

impl SequencerStateMachine {
    /// Decode committed Raft log entries into the per-vShard [`SchedulerInput`]
    /// stream for `vshard_id`, in committed-log order.
    ///
    /// The caller (the Calvin scheduler's catch-up drain) passes raw Raft log
    /// entries obtained via `MultiRaft::read_committed_entries`. Each entry is
    /// decoded as a [`SequencerEntry`] and, if it targets `vshard_id`, converted
    /// into the exact `SchedulerInput` the live fan-out would have delivered:
    ///
    /// * `EpochBatch` (epoch in `[from_epoch, to_epoch]`) → one
    ///   [`SchedulerInput::Txn`] per participating position, re-stamped with the
    ///   batch's `epoch_system_ms` and the per-vShard `epoch_vshard_txn_count`
    ///   computed identically to the live [`SequencerStateMachine::apply`] path.
    /// * `ReserveRead`  targeting `vshard_id` → [`SchedulerInput::Reserve`].
    /// * `ReleaseReservation` targeting `vshard_id` → [`SchedulerInput::Release`].
    /// * `CutMarker` → [`SchedulerInput::CutMarker`], for every vShard.
    /// * All other variants carry no per-vShard scheduler input.
    ///
    /// Entries are emitted in Raft-log order (and, within an epoch batch, in
    /// position order) — the same order the live path delivered them — so the
    /// scheduler applies an identical sequence. Entries that fail to decode are
    /// logged and skipped (same policy as the live apply path).
    ///
    /// Unlike live `apply`, this is a pure read (`&self`): it does NOT re-seed the
    /// completion registry or advance any watermark — those side effects already
    /// happened when the entry was first applied live; only the channel fan-out
    /// was missed, and that is what replay reconstructs.
    pub fn replay_epochs_for_vshard(
        &self,
        entries: &[nodedb_raft::LogEntry],
        vshard_id: u32,
        from_epoch: u64,
        to_epoch: u64,
    ) -> Vec<SchedulerInput> {
        let mut result: Vec<SchedulerInput> = Vec::new();

        for entry in entries {
            if entry.data.is_empty() {
                // No-op entry (newly elected leader heartbeat).
                continue;
            }
            if crate::conf_change::ConfChange::is_conf_change(&entry.data) {
                // A membership change of the sequencer group, applied by the
                // Raft layer; it carries no sequencer input.
                continue;
            }
            let seq_entry: SequencerEntry = match zerompk::from_msgpack(&entry.data) {
                Ok(e) => e,
                Err(err) => {
                    tracing::warn!(
                        raft_index = entry.index,
                        error = %err,
                        "calvin replay: failed to decode sequencer entry; skipping"
                    );
                    continue;
                }
            };
            match seq_entry {
                SequencerEntry::EpochBatch { mut batch } => {
                    if batch.epoch < from_epoch || batch.epoch > to_epoch {
                        continue;
                    }
                    // Re-derive participating_vshards (skipped during serialization)
                    // exactly as the live apply path does before fan-out.
                    for txn in &mut batch.txns {
                        txn.tx_class.restore_derived();
                    }

                    // Shared with the live `apply` EpochBatch arm via
                    // `compute_vshard_txn_counts` so the two paths can never drift.
                    let vshard_txn_counts = compute_vshard_txn_counts(&batch);

                    for txn in &batch.txns {
                        let participates = txn
                            .tx_class
                            .participating_vshards()
                            .iter()
                            .any(|v| v.as_u32() == vshard_id);
                        if !participates {
                            continue;
                        }
                        // Same stamping as the live fan-out: `epoch_system_ms`
                        // from the batch (deterministic time anchor) and the
                        // per-vShard position count for THIS vShard.
                        let mut per_vshard = txn.clone();
                        per_vshard.epoch_system_ms = batch.epoch_system_ms;
                        per_vshard.epoch_vshard_txn_count =
                            vshard_txn_counts.get(&vshard_id).copied().unwrap_or(0);
                        result.push(SchedulerInput::Txn(per_vshard));
                    }
                }
                // Re-fan the read reservation exactly as the live `ReserveRead`
                // arm does, filtered to this vShard.
                SequencerEntry::ReserveRead { owner, vshard, key } if vshard == vshard_id => {
                    result.push(SchedulerInput::Reserve { owner, key });
                }
                // Re-fan the reservation release exactly as the live
                // `ReleaseReservation` arm does, filtered to this vShard.
                SequencerEntry::ReleaseReservation {
                    owner,
                    vshard,
                    reason,
                } if vshard == vshard_id => {
                    result.push(SchedulerInput::Release { owner, reason });
                }
                // A cut marker reaches every vShard, exactly as the live
                // `CutMarker` arm fans it out.
                SequencerEntry::CutMarker { hlc } => {
                    result.push(SchedulerInput::CutMarker { hlc });
                }
                // Reservation entries for a different vShard carry nothing for us.
                SequencerEntry::ReserveRead { .. } => {}
                SequencerEntry::ReleaseReservation { .. } => {}
                // Coordinator-side notification signals carry no per-vShard
                // scheduler input; they are re-derived via live `apply` on every
                // replica's completion registry, not replayed to a scheduler.
                SequencerEntry::CompletionAck { .. } => {}
                SequencerEntry::OllpMismatch { .. } => {}
                SequencerEntry::TxnRoutingFailed { .. } => {}
                SequencerEntry::Vote { .. } => {}
                SequencerEntry::Verdict { .. } => {}
                SequencerEntry::AbortVote { .. } => {}
                SequencerEntry::AbortVerdict { .. } => {}
            }
        }

        result
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::calvin::CalvinCompletionRegistry;
    use crate::calvin::types::{
        EngineKeySet, EpochBatch, LockKeyWire, ReadWriteSet, ReleaseReason, SequencedTxn,
        SortedVec, TxClass, TxnIdWire,
    };
    use nodedb_types::{
        TenantId,
        id::{DatabaseId, VShardId},
    };
    use std::collections::HashMap;
    use tokio::sync::mpsc;

    fn find_two_distinct_collections() -> (String, String) {
        let mut first: Option<(String, u32)> = None;
        for i in 0u32..512 {
            let name = format!("col_{i}");
            let vshard = VShardId::from_collection_in_database(DatabaseId::DEFAULT, &name).as_u32();
            if let Some((ref fname, fv)) = first {
                if fv != vshard {
                    return (fname.clone(), name);
                }
            } else {
                first = Some((name, vshard));
            }
        }
        panic!("could not find two distinct-vshard collections in 512 tries");
    }

    fn make_batch_with_two_vshards() -> (EpochBatch, u32, u32) {
        let (col_a, col_b) = find_two_distinct_collections();
        let real_va = VShardId::from_collection_in_database(DatabaseId::DEFAULT, &col_a).as_u32();
        let real_vb = VShardId::from_collection_in_database(DatabaseId::DEFAULT, &col_b).as_u32();
        let write_set = ReadWriteSet::new(vec![
            EngineKeySet::Document {
                collection: col_a,
                surrogates: SortedVec::new(vec![1]),
            },
            EngineKeySet::Document {
                collection: col_b,
                surrogates: SortedVec::new(vec![2]),
            },
        ]);
        let tx_class = TxClass::new(
            ReadWriteSet::new(vec![]),
            write_set,
            vec![],
            TenantId::new(1),
            None,
            crate::calvin::types::VersionedReadSet::default(),
        )
        .expect("valid TxClass");
        let batch = EpochBatch {
            epoch: 0,
            txns: vec![SequencedTxn {
                epoch: 0,
                position: 0,
                tx_class,
                epoch_system_ms: 1_700_000_000_000,
                epoch_vshard_txn_count: 1,
                lock_owner: None,
            }],
            epoch_system_ms: 1_700_000_000_000,
        };
        (batch, real_va, real_vb)
    }

    fn encode_entry(entry: &SequencerEntry) -> Vec<u8> {
        zerompk::to_msgpack_vec(entry).expect("encode")
    }

    fn log_entry(index: u64, data: Vec<u8>) -> nodedb_raft::LogEntry {
        nodedb_raft::LogEntry {
            term: 1,
            index,
            data,
        }
    }

    /// The replay decode of a mixed EpochBatch + ReserveRead + ReleaseReservation
    /// stream must reproduce, bit-for-bit and in order, exactly what the live
    /// `apply` fan-out delivered to the same vShard's channel.
    #[test]
    fn replay_mixed_sequence_matches_live_fanout() {
        let (batch, va, vb) = make_batch_with_two_vshards();
        let owner = TxnIdWire {
            epoch: 0,
            position: 0,
        };
        let entries = [
            SequencerEntry::EpochBatch {
                batch: batch.clone(),
            },
            SequencerEntry::ReserveRead {
                owner,
                vshard: va,
                key: LockKeyWire::Kv {
                    collection: "sessions".to_owned(),
                    key: b"hot".to_vec(),
                },
            },
            SequencerEntry::ReleaseReservation {
                owner,
                vshard: va,
                reason: ReleaseReason::Commit,
            },
            // A reservation for a DIFFERENT vShard must not appear in va's stream.
            SequencerEntry::ReserveRead {
                owner,
                vshard: vb,
                key: LockKeyWire::Kv {
                    collection: "other".to_owned(),
                    key: b"cold".to_vec(),
                },
            },
        ];

        // LIVE: apply each entry and drain va's channel to capture what the live
        // fan-out delivered.
        let (tx_a, mut rx_a) = mpsc::channel(64);
        let (tx_b, mut _rx_b) = mpsc::channel(64);
        let mut senders = HashMap::new();
        senders.insert(va, tx_a);
        senders.insert(vb, tx_b);
        let mut sm = SequencerStateMachine::new(senders, CalvinCompletionRegistry::new_detached());
        for (i, entry) in entries.iter().enumerate() {
            sm.apply(i as u64, &encode_entry(entry));
        }
        let mut live: Vec<SchedulerInput> = Vec::new();
        while let Ok(input) = rx_a.try_recv() {
            live.push(input);
        }

        // REPLAY: decode the same entries from the Raft log for va.
        let log_entries: Vec<nodedb_raft::LogEntry> = entries
            .iter()
            .enumerate()
            .map(|(i, entry)| log_entry(i as u64, encode_entry(entry)))
            .collect();
        let replayed = sm.replay_epochs_for_vshard(&log_entries, va, 0, u64::MAX);

        // Stream shape: Txn, Reserve, Release (the cross-vShard reserve excluded).
        assert_eq!(live.len(), 3, "live delivered Txn + Reserve + Release");
        assert_eq!(replayed.len(), live.len());
        // `SchedulerInput` is a channel payload (no PartialEq); its Debug form
        // captures order + every stamped field (epoch_system_ms,
        // epoch_vshard_txn_count, owner, key, reason), so equal Debug ⇒ identical.
        assert_eq!(format!("{live:?}"), format!("{replayed:?}"));

        // Explicitly pin the stamped Txn fields the determinism contract hinges on.
        match &replayed[0] {
            SchedulerInput::Txn(txn) => {
                assert_eq!(txn.epoch_system_ms, batch.epoch_system_ms);
                assert_eq!(txn.epoch_vshard_txn_count, 1);
                assert_eq!(txn.position, 0);
            }
            other => panic!("expected Txn first, got {other:?}"),
        }
        assert!(matches!(replayed[1], SchedulerInput::Reserve { .. }));
        assert!(matches!(replayed[2], SchedulerInput::Release { .. }));
    }

    /// Epochs outside `[from_epoch, to_epoch]` are excluded, mirroring the
    /// bounded rebuild range the scheduler drains.
    #[test]
    fn replay_filters_epoch_range() {
        let (batch, va, _vb) = make_batch_with_two_vshards();
        let entry = SequencerEntry::EpochBatch { batch };
        let log_entries = vec![log_entry(0, encode_entry(&entry))];
        let sm =
            SequencerStateMachine::new(HashMap::new(), CalvinCompletionRegistry::new_detached());

        // Batch epoch is 0; a range starting at 1 excludes it.
        let out = sm.replay_epochs_for_vshard(&log_entries, va, 1, u64::MAX);
        assert!(out.is_empty());
    }
}
