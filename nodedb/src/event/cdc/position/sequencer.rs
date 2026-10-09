// SPDX-License-Identifier: BUSL-1.1

//! Assign each change event its position within its partition.
//!
//! One write can produce several change events. They share the write's index
//! and take consecutive ordinals in arrival order. One partition's events come
//! from one Data-Plane core, which applies writes in log order and emits a
//! write's events in execution order, so every replica numbers them alike.
//!
//! Only events that match a change stream take an ordinal. Each such event
//! lands in a stream buffer, so after a restart the highest buffered event of
//! a partition tells the sequencer where the write in progress stopped.
//!
//! The ordinal continues only while the events come from the same local WAL
//! record. A Raft entry re-applied after a restart writes a new record, so its
//! events restart at ordinal 1, take the positions they took before, and the
//! stream buffer drops them as duplicates.

use std::collections::HashMap;
use std::sync::Mutex;

use crate::event::cdc::offset::CdcOffset;

/// Where the position of an event comes from.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IndexSource {
    /// A replicated write: a Raft entry at `(epoch, index)`.
    Replicated { epoch: u64, index: u64 },
    /// The event's write has no known replicated position on a node that
    /// positions by it. The event joins the partition's current write.
    Unmapped,
    /// The local WAL LSN, on a node that applies no replicated writes.
    Local(u64),
}

/// The last position a partition assigned, and the local WAL record of the
/// event that took it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PartitionTail {
    pub position: CdcOffset,
    pub record_lsn: u64,
}

/// Per-partition position allocator. Holds one tail per partition, so it is
/// bounded by the partition count.
#[derive(Debug, Default)]
pub struct PositionSequencer {
    partitions: Mutex<HashMap<u32, PartitionTail>>,
}

impl PositionSequencer {
    pub fn new() -> Self {
        Self::default()
    }

    /// The position of the next data event of `partition`, whose write is the
    /// local WAL record at `record_lsn`. `seed` yields the highest event
    /// already buffered for the partition. It runs once per partition, on the
    /// partition's first event since this process started.
    pub fn next(
        &self,
        partition: u32,
        source: IndexSource,
        record_lsn: u64,
        seed: impl FnOnce() -> Option<PartitionTail>,
    ) -> CdcOffset {
        let mut partitions = self.partitions.lock().unwrap_or_else(|p| p.into_inner());
        let tail = partitions.entry(partition).or_insert_with(|| {
            seed().unwrap_or(PartitionTail {
                position: CdcOffset::ZERO,
                record_lsn: 0,
            })
        });
        let last = tail.position;
        let (epoch, index, continues) = match source {
            IndexSource::Replicated { epoch, index } => (
                epoch,
                index,
                (epoch, index) == (last.epoch, last.index) && record_lsn == tail.record_lsn,
            ),
            IndexSource::Local(index) => (
                0,
                index,
                (0, index) == (last.epoch, last.index) && record_lsn == tail.record_lsn,
            ),
            IndexSource::Unmapped => (last.epoch, last.index, true),
        };
        let ordinal = if continues {
            last.ordinal().saturating_add(1)
        } else {
            1
        };
        let position = CdcOffset::data_event(epoch, index, ordinal);
        *tail = PartitionTail {
            position,
            record_lsn,
        };
        position
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn none() -> Option<PartitionTail> {
        None
    }

    fn entry(index: u64) -> IndexSource {
        IndexSource::Replicated { epoch: 0, index }
    }

    #[test]
    fn events_of_one_write_take_consecutive_ordinals() {
        let sequencer = PositionSequencer::new();
        assert_eq!(
            sequencer.next(0, entry(7), 70, none),
            CdcOffset::data_event(0, 7, 1)
        );
        assert_eq!(
            sequencer.next(0, entry(7), 70, none),
            CdcOffset::data_event(0, 7, 2)
        );
        assert_eq!(
            sequencer.next(0, entry(8), 71, none),
            CdcOffset::data_event(0, 8, 1)
        );
    }

    #[test]
    fn partitions_number_independently() {
        let sequencer = PositionSequencer::new();
        sequencer.next(0, entry(7), 70, none);
        assert_eq!(
            sequencer.next(1, entry(7), 70, none),
            CdcOffset::data_event(0, 7, 1)
        );
    }

    #[test]
    fn an_unmapped_event_joins_the_current_write() {
        let sequencer = PositionSequencer::new();
        sequencer.next(0, entry(9), 90, none);
        assert_eq!(
            sequencer.next(0, IndexSource::Unmapped, 95, none),
            CdcOffset::data_event(0, 9, 2)
        );
    }

    #[test]
    fn a_seeded_partition_continues_the_record_it_stopped_in() {
        let sequencer = PositionSequencer::new();
        // The buffer holds the correction of the second event of write 12,
        // from local record 500.
        let seed = || {
            Some(PartitionTail {
                position: CdcOffset::data_event(0, 12, 2).correction(),
                record_lsn: 500,
            })
        };
        assert_eq!(
            sequencer.next(0, entry(12), 500, seed),
            CdcOffset::data_event(0, 12, 3)
        );
    }

    #[test]
    fn a_reapplied_entry_restarts_at_its_first_position() {
        let sequencer = PositionSequencer::new();
        let seed = || {
            Some(PartitionTail {
                position: CdcOffset::data_event(0, 12, 2),
                record_lsn: 500,
            })
        };
        // Entry 12 applied again after a restart writes record 900.
        assert_eq!(
            sequencer.next(0, entry(12), 900, seed),
            CdcOffset::data_event(0, 12, 1)
        );
        assert_eq!(
            sequencer.next(0, entry(12), 900, none),
            CdcOffset::data_event(0, 12, 2)
        );
    }

    #[test]
    fn two_replicas_that_see_the_same_writes_assign_the_same_positions() {
        let leader = PositionSequencer::new();
        let follower = PositionSequencer::new();
        let writes = [(4, 1), (5, 3), (6, 2)];
        for (index, events) in writes {
            for _ in 0..events {
                // Each replica holds the entry at its own local WAL LSN.
                assert_eq!(
                    leader.next(2, entry(index), index * 10, none),
                    follower.next(2, entry(index), index * 1_000, none)
                );
            }
        }
    }

    #[test]
    fn a_group_move_never_makes_positions_go_backwards() {
        let sequencer = PositionSequencer::new();
        let before = sequencer.next(4, entry(9_000), 1, none);
        // The new group numbers its log from a low index, in a higher epoch.
        let after = sequencer.next(
            4,
            IndexSource::Replicated {
                epoch: 55,
                index: 3,
            },
            2,
            none,
        );
        assert!(after > before);
        assert_eq!(after, CdcOffset::data_event(55, 3, 1));
    }
}
