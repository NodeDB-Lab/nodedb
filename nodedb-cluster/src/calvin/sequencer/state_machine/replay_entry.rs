// SPDX-License-Identifier: BUSL-1.1

//! One committed sequencer entry in the decoded form the catch-up replay
//! projects.
//!
//! [`ReplayEntry::decode`] runs once per entry on this node. Then
//! [`ReplayEntry::project`] yields each vShard's [`SchedulerInput`]s from it,
//! without decoding again.
//!
//! DETERMINISM CONTRACT: `project` yields, bit for bit and in order, the inputs
//! the live `apply` fan-out delivers for the same entry: the same
//! `epoch_system_ms` and `epoch_vshard_txn_count` stamps, in position order.

use std::collections::HashMap;
use std::sync::Arc;

use crate::calvin::sequencer::entry::SequencerEntry;
use crate::calvin::sequencer::replay::compute_vshard_txn_counts;
use crate::calvin::types::{
    CutBarrierWire, EpochBatch, LockKeyWire, ReleaseReason, SchedulerInput, TaskChunk, TxnIdWire,
};

/// The decoded form of one committed sequencer entry, as the replay projects
/// it.
#[derive(Debug)]
pub(crate) enum ReplayEntry {
    /// The entry carries no scheduler input: a no-op, a membership change,
    /// an entry that fails to decode, or a coordinator-side signal.
    Nothing,
    /// An epoch batch with its participants restored.
    Batch {
        batch: EpochBatch,
        /// Per-vShard count of the batch's positions, from
        /// [`compute_vshard_txn_counts`].
        counts: HashMap<u32, u32>,
    },
    /// A read reservation for `vshard`.
    Reserve {
        owner: TxnIdWire,
        vshard: u32,
        key: LockKeyWire,
    },
    /// A reservation release for `vshard`.
    Release {
        owner: TxnIdWire,
        vshard: u32,
        reason: ReleaseReason,
    },
    /// A cut marker. Every vShard receives it.
    CutMarker {
        hlc: u64,
        restore_point: u64,
        barrier: Option<Arc<CutBarrierWire>>,
    },
    /// A part of a multi-part transaction. Only its `targets` receive it.
    Part {
        txn: TxnIdWire,
        index: u32,
        first_task: u32,
        /// The vShards the part routes to, sorted.
        targets: Vec<u32>,
        plans: Arc<Vec<u8>>,
        chunk: Option<TaskChunk>,
    },
    /// An abandonment of a multi-part transaction. Every vShard receives it.
    Abandoned { txn: TxnIdWire },
}

impl ReplayEntry {
    /// Decode the committed log `entry`.
    ///
    /// A no-op or a membership change yields `Nothing`. An entry that fails
    /// to decode, or a batch with underivable participants, is logged and
    /// yields `Nothing`, as the live apply path skips it.
    pub(crate) fn decode(entry: &nodedb_raft::LogEntry) -> Self {
        if entry.data.is_empty() {
            // A newly elected leader's no-op entry.
            return Self::Nothing;
        }
        if crate::conf_change::ConfChange::is_conf_change(&entry.data) {
            // The Raft layer applies a membership change. It carries no input.
            return Self::Nothing;
        }
        let decoded: Result<SequencerEntry, _> = zerompk::from_msgpack(&entry.data);
        match decoded {
            Ok(seq_entry) => Self::from_entry(entry.index, seq_entry),
            Err(err) => {
                tracing::warn!(
                    raft_index = entry.index,
                    error = %err,
                    "calvin replay: failed to decode sequencer entry; skipping"
                );
                Self::Nothing
            }
        }
    }

    /// Convert the decoded `seq_entry` at Raft `index`.
    fn from_entry(index: u64, seq_entry: SequencerEntry) -> Self {
        match seq_entry {
            SequencerEntry::EpochBatch { batch } => Self::from_batch(index, batch),
            SequencerEntry::ReserveRead { owner, vshard, key } => {
                Self::Reserve { owner, vshard, key }
            }
            SequencerEntry::ReleaseReservation {
                owner,
                vshard,
                reason,
            } => Self::Release {
                owner,
                vshard,
                reason,
            },
            SequencerEntry::CutMarker {
                hlc,
                restore_point,
                barrier,
            } => Self::CutMarker {
                hlc,
                restore_point,
                barrier: barrier.map(Arc::new),
            },
            SequencerEntry::TxnPart {
                epoch,
                position,
                index,
                first_task,
                mut targets,
                plans,
                chunk,
            } => {
                targets.sort_unstable();
                Self::Part {
                    txn: TxnIdWire { epoch, position },
                    index,
                    first_task,
                    targets,
                    plans: Arc::new(plans),
                    chunk,
                }
            }
            SequencerEntry::TxnPartsAbandoned { epoch, position } => Self::Abandoned {
                txn: TxnIdWire { epoch, position },
            },
            // An epoch floor only moves the next epoch the sequencer proposes.
            SequencerEntry::EpochFloor { .. } => Self::Nothing,
            // Coordinator-side signals reach the completion registry through
            // live `apply` on every replica. No scheduler receives them.
            SequencerEntry::CompletionAck { .. }
            | SequencerEntry::Vote { .. }
            | SequencerEntry::Verdict { .. }
            | SequencerEntry::AbortVote { .. }
            | SequencerEntry::AbortVerdict { .. } => Self::Nothing,
        }
    }

    /// Restore the participants of `batch`, decoded at Raft `index`, and
    /// count its positions per vShard.
    ///
    /// The live path skips a batch whose participants cannot be derived, so
    /// the replay skips it too.
    fn from_batch(index: u64, mut batch: EpochBatch) -> Self {
        for txn in &mut batch.txns {
            // A freshly decoded class is unshared: no copy.
            if let Err(err) = Arc::make_mut(&mut txn.tx_class).restore_derived() {
                tracing::warn!(
                    raft_index = index,
                    epoch = batch.epoch,
                    error = %err,
                    "calvin replay: epoch batch carries a transaction with \
                     underivable participants; skipping"
                );
                return Self::Nothing;
            }
        }
        Self::restored_batch(batch)
    }

    /// Wrap `batch`, whose participants are restored, with its per-vShard
    /// position counts.
    pub(super) fn restored_batch(batch: EpochBatch) -> Self {
        let counts = compute_vshard_txn_counts(&batch);
        Self::Batch { batch, counts }
    }

    /// Push onto `out` the inputs this entry carries for `vshard`, for
    /// transactions of epochs in `from_epoch ..= to_epoch`.
    pub(crate) fn project(
        &self,
        vshard: u32,
        from_epoch: u64,
        to_epoch: u64,
        out: &mut Vec<SchedulerInput>,
    ) {
        let in_range = |epoch: u64| epoch >= from_epoch && epoch <= to_epoch;
        match self {
            Self::Nothing => {}
            Self::Batch { batch, counts } => {
                if !in_range(batch.epoch) {
                    return;
                }
                let count = counts.get(&vshard).copied().unwrap_or(0);
                for txn in &batch.txns {
                    let participates = txn
                        .tx_class
                        .participating_vshards()
                        .binary_search_by_key(&vshard, |v| v.as_u32())
                        .is_ok();
                    if !participates {
                        continue;
                    }
                    // The live fan-out stamps the batch instant and this
                    // vShard's position count. The copy shares the class.
                    let mut per_vshard = txn.clone();
                    per_vshard.epoch_system_ms = batch.epoch_system_ms;
                    per_vshard.epoch_vshard_txn_count = count;
                    out.push(SchedulerInput::Txn(Box::new(per_vshard)));
                }
            }
            Self::Reserve {
                owner,
                vshard: target,
                key,
            } => {
                if *target == vshard {
                    out.push(SchedulerInput::Reserve {
                        owner: *owner,
                        key: key.clone(),
                    });
                }
            }
            Self::Release {
                owner,
                vshard: target,
                reason,
            } => {
                if *target == vshard {
                    out.push(SchedulerInput::Release {
                        owner: *owner,
                        reason: *reason,
                    });
                }
            }
            Self::CutMarker {
                hlc,
                restore_point,
                barrier,
            } => out.push(SchedulerInput::CutMarker {
                hlc: *hlc,
                restore_point: *restore_point,
                barrier: barrier.clone(),
            }),
            // The scheduler ignores a part of a transaction it holds no
            // header for.
            Self::Part {
                txn,
                index,
                first_task,
                targets,
                plans,
                chunk,
            } => {
                if in_range(txn.epoch) && targets.binary_search(&vshard).is_ok() {
                    out.push(SchedulerInput::TxnPart {
                        txn: *txn,
                        index: *index,
                        first_task: *first_task,
                        plans: Arc::clone(plans),
                        chunk: *chunk,
                    });
                }
            }
            // The scheduler acts only on a transaction it holds, which it
            // holds only when it participates.
            Self::Abandoned { txn } => {
                if in_range(txn.epoch) {
                    out.push(SchedulerInput::PartsAbandoned { txn: *txn });
                }
            }
        }
    }
}
