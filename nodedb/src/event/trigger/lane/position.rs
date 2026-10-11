// SPDX-License-Identifier: BUSL-1.1

//! The replica-independent position of an event whose actions fire.
//!
//! The lane names an event as the CDC router does: by the change-feed
//! partition of its write and a position in that partition. The position is
//! the write's replicated position (a Raft entry) with the event's ordinal
//! among the write's firing events. One partition's
//! events come from one Data-Plane core in apply order, so every replica
//! numbers them alike.
//!
//! Every firing event takes an ordinal, whether or not this node's catalog
//! holds an action for its collection. The numbering then never depends on
//! when a replica applied a trigger's DDL.

use crate::event::cdc::position::{PartitionTail, PositionSequencer};
use crate::event::cdc::{CdcOffset, CdcRouter};
use crate::event::types::WriteEvent;

/// The position allocator of the lane's firing events.
#[derive(Debug, Default)]
pub struct ActionPositions {
    sequencer: PositionSequencer,
}

impl ActionPositions {
    pub fn new() -> Self {
        Self::default()
    }

    /// The partition and position of `event`, a firing event. `tail` yields
    /// the highest position this node holds for a partition. It runs once
    /// per partition, on the partition's first firing event since this
    /// process started.
    pub fn next(
        &self,
        event: &WriteEvent,
        router: &CdcRouter,
        tail: impl FnOnce(u32) -> Option<CdcOffset>,
    ) -> (u32, CdcOffset) {
        let record_lsn = event
            .record
            .map_or(event.lsn.as_u64(), |record| record.lsn.as_u64());
        let (partition, source) = router.index_source(event.vshard_id.as_u32(), record_lsn);
        let position = self.sequencer.next(partition, source, record_lsn, || {
            tail(partition).map(|position| PartitionTail {
                position,
                record_lsn: 0,
            })
        });
        (partition, position)
    }
}

/// The `(source_lsn, source_sequence)` a fired action names its event by:
/// the position's index and sequence. Every replica derives the same pair.
///
/// Every position has epoch `0` (see `cdc::offset`), so the pair names the
/// event within its vShard.
pub fn action_identity(position: CdcOffset) -> (u64, u64) {
    (position.index, position.sequence)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn an_action_identity_is_the_index_and_sequence() {
        let position = CdcOffset::data_event(0, 12, 1);
        assert_eq!(action_identity(position), (12, position.sequence));
    }
}
