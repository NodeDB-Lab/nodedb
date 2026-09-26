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

use super::lock_wire::{LockKeyWire, ReleaseReason, TxnIdWire};
use super::sequencer::SequencedTxn;

/// One item in a per-vShard scheduler input stream.
///
/// This is a purely in-process channel payload (never serialized) — the wire
/// form is the replicated `SequencerEntry`, decoded and fanned out into these.
#[derive(Debug)]
pub enum SchedulerInput {
    /// A sequenced transaction to process (lock-acquire + dispatch).
    Txn(SequencedTxn),
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
    CutMarker { hlc: u64 },
}
