// SPDX-License-Identifier: BUSL-1.1

//! Completion-ack recording and reservation forwarding for the sequencer apply path.

use tracing::warn;

use crate::calvin::TxnId;
use crate::calvin::types::{SchedulerInput, TxnIdWire};

use super::core::{Delivery, SequencerStateMachine};

impl SequencerStateMachine {
    /// Record `vshard_id`'s completion ack for `txn`, with its apply result,
    /// and log the ack at its Raft `index`.
    ///
    /// The first ack of a vShard in log order answers for it, on every node
    /// alike: the result depends on the log alone. Only the vShard's
    /// data-group leader proposes an ack, so a node that left the group adds
    /// none. `from_node` names the proposer for tracing.
    pub(super) fn apply_completion_ack(
        &self,
        index: u64,
        txn: TxnId,
        vshard_id: u32,
        from_node: u64,
        result: Vec<u8>,
    ) {
        tracing::trace!(
            epoch = txn.epoch,
            position = txn.position,
            vshard_id,
            from_node,
            "sequencer apply: completion ack"
        );
        self.completion_registry
            .note_completion_ack_with(txn, vshard_id, result);
        self.completion_registry
            .applied_acks
            .record(crate::calvin::AppliedCompletionAck {
                index,
                txn,
                vshard_id,
            });
    }

    /// Send a reservation `input` for `owner` to `vshard`'s scheduler.
    ///
    /// Same delivery as the epoch-batch fan-out: an armed vShard, or a full
    /// or closed channel, leaves the input to the catch-up replay. This node
    /// may not host the vShard. Then nothing is sent. `what` names the input
    /// in the warnings.
    pub(super) fn forward_reservation(
        &self,
        index: u64,
        vshard: u32,
        owner: TxnIdWire,
        input: SchedulerInput,
        what: &'static str,
    ) {
        match self.deliver(index, vshard, input) {
            Delivery::NotHosted | Delivery::Sent | Delivery::Deferred => {}
            Delivery::DroppedFull => warn!(
                vshard,
                owner_epoch = owner.epoch,
                owner_position = owner.position,
                what,
                "sequencer apply: vshard channel full (backpressure); \
                 reservation input left to the catch-up replay"
            ),
            Delivery::DroppedClosed => warn!(
                vshard,
                what, "sequencer apply: vshard sender gone; scheduler may have exited"
            ),
        }
    }
}
