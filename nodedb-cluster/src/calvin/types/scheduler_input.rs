// SPDX-License-Identifier: BUSL-1.1

//! [`SchedulerInput`]: the per-vShard fan-out payload the sequencer state machine
//! delivers to each Calvin scheduler.
//!
//! The sequencer state machine fans committed [`SequencerEntry`] variants out to
//! per-vShard channels carrying this enum. A scheduler applies each input in
//! sequencer-committed order, so every replica performs identical
//! `process`/`acquire_shared`/`release` calls in identical order — the
//! determinism contract.
//!
//! [`SequencerEntry`]: super::super::sequencer::entry::SequencerEntry

use std::sync::Arc;

use super::lock_wire::{LockKeyWire, ReleaseReason, TxnIdWire};
use super::multi_part::TaskChunk;
use super::sequencer::SequencedTxn;

/// One item in a per-vShard scheduler input stream.
///
/// This is a purely in-process channel payload (never serialized) — the wire
/// form is the replicated `SequencerEntry`, decoded and fanned out into these.
#[derive(Debug)]
pub enum SchedulerInput {
    /// A sequenced transaction to process (lock-acquire + dispatch). Boxed
    /// so the other, small variants do not carry its size.
    Txn(Box<SequencedTxn>),
    /// Install a SHARED reservation on `key` for interactive txn `owner`.
    Reserve { owner: TxnIdWire, key: LockKeyWire },
    /// Release ALL of `owner`'s shared reservations on this vShard.
    Release {
        owner: TxnIdWire,
        reason: ReleaseReason,
    },
    /// A backup's consistent-cut marker carrying its watermark `hlc`. Every
    /// transaction delivered before it must finish before the scheduler
    /// reports it; every transaction delivered after it commits above `hlc`.
    /// `restore_point` names the cluster restore point the cut takes, `0`
    /// for none. `barrier` is set when the cut places a barrier in every data
    /// group: the leader holds the redo of every later transaction until it
    /// applied its group's barrier. Shared by every vShard of this node.
    CutMarker {
        hlc: u64,
        restore_point: u64,
        barrier: Option<Arc<super::cut_barrier::CutBarrierWire>>,
    },
    /// Part `index` of the plans of the multi-part transaction `txn`, whose
    /// first task is the transaction's task `first_task`. The bytes are
    /// shared by every scheduler of this node the part targets. `chunk` is
    /// set on a part that holds one byte range of one task.
    TxnPart {
        txn: TxnIdWire,
        index: u32,
        first_task: u32,
        plans: Arc<Vec<u8>>,
        chunk: Option<TaskChunk>,
    },
    /// The multi-part transaction `txn` lost its parts and aborts.
    PartsAbandoned { txn: TxnIdWire },
}
