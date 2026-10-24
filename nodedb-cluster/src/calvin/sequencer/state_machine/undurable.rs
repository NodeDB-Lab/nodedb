// SPDX-License-Identifier: BUSL-1.1

//! Transaction inputs this node's schedulers received and may not have made
//! durable yet.
//!
//! A scheduler that restarts catches up from the first index the sequencer
//! log still holds, and skips every position its applied state names. An
//! input it received live, and had not installed when the node stopped, is
//! in neither place once the log compacted past it: the transaction is lost
//! on this replica. The sequencer log therefore keeps every such input. The
//! state machine records each `Txn` input it hands a scheduler, live or by
//! the catch-up replay, and the log compactor drops the ones the scheduler
//! made durable before it reads the lowest index left.
//!
//! The record holds one entry per input not yet seen durable, so it is
//! bounded by the inputs in flight on this node's schedulers.

use std::collections::{BTreeMap, BTreeSet};

use super::core::SequencerStateMachine;

/// One received input: its vShard, epoch and position.
type Input = (u32, u64, u32);

/// The inputs not yet seen durable, by sequencer log index.
#[derive(Debug, Default)]
pub(super) struct UndurableInputs {
    by_index: BTreeMap<u64, BTreeSet<Input>>,
}

impl UndurableInputs {
    /// Record the input of `vshard` at `index`. A repeat is a no-op.
    pub(super) fn note(&mut self, index: u64, vshard: u32, epoch: u64, position: u32) {
        self.by_index
            .entry(index)
            .or_default()
            .insert((vshard, epoch, position));
    }

    /// Drop every input `durable` answers true for, then return the lowest
    /// index that still holds one.
    fn prune(&mut self, durable: impl Fn(u32, u64, u32) -> bool) -> Option<u64> {
        self.by_index.retain(|_, inputs| {
            inputs.retain(|&(vshard, epoch, position)| !durable(vshard, epoch, position));
            !inputs.is_empty()
        });
        self.by_index.keys().next().copied()
    }
}

impl SequencerStateMachine {
    /// Record that the catch-up replay handed the `Txn` input of `vshard` at
    /// `index` to its scheduler.
    pub fn note_replayed_txn(&mut self, index: u64, vshard: u32, epoch: u64, position: u32) {
        self.undurable.note(index, vshard, epoch, position);
    }

    /// The lowest sequencer index whose `Txn` input a scheduler here received
    /// and has not made durable, after dropping every input `durable` answers
    /// true for. `durable(vshard, epoch, position)` must also answer true for
    /// a vShard this node no longer serves: its replica holds no state.
    pub fn undurable_floor(&mut self, durable: impl Fn(u32, u64, u32) -> bool) -> Option<u64> {
        self.undurable.prune(durable)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_floor_is_the_lowest_input_not_yet_durable() {
        let mut inputs = UndurableInputs::default();
        inputs.note(10, 1, 4, 0);
        inputs.note(10, 2, 4, 0);
        inputs.note(12, 1, 5, 0);
        inputs.note(12, 1, 5, 0);
        assert_eq!(inputs.prune(|_, _, _| false), Some(10));

        // vShard 1 installed epoch 4; vShard 2 has not.
        assert_eq!(
            inputs.prune(|vshard, epoch, _| vshard == 1 && epoch == 4),
            Some(10)
        );
        assert_eq!(inputs.prune(|vshard, _, _| vshard == 2), Some(12));
        assert_eq!(inputs.prune(|_, _, _| true), None);
    }
}
