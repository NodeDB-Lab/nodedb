// SPDX-License-Identifier: BUSL-1.1

//! The leader's read of one partition for a follower: the events past the
//! follower's position, across every tenant and database, from this node's
//! own stream. This read is internal to the feed. A consumer's replay stays
//! scoped to its tenant (see [`super::catch_up`]).
//!
//! The ring serves the stretch it holds. Below the ring, the settled events
//! pending for the journal and the journal's rows serve the stretch the
//! ring evicted. The read stops at the feed's `through`, the settled
//! frontier, and returns at most a bounded number of events.
//!
//! A follower position below what this node holds, after retention or a
//! snapshot install, cannot be served. The run then starts at the lowest
//! position above which this node holds every event, and the follower
//! records the hole. That is the one hole the feed makes, and every cursor
//! below it resets.

use crate::event::cdc::CdcOffset;

use super::bus::ChangeStream;
use super::catch_up::merge;
use super::ring::AppendedRun;
use super::{ChangePartition, SequencedChangeEvent};

/// Events one forwarded run carries at most.
pub const FORWARD_RUN_EVENTS: usize = 1024;

impl ChangeStream {
    /// The next run of `partition` for a follower that holds every event up
    /// to `after`, with at most `limit` events. `None` when this node holds
    /// nothing past `after`.
    ///
    /// The run's `after` is `after`, or the lowest position above which
    /// this node holds every event when that lies above `after`: a hole. A
    /// run with no event covers a stretch with none, up to the frontier.
    pub(crate) fn forward_run(
        &self,
        partition: ChangePartition,
        after: CdcOffset,
        limit: usize,
    ) -> crate::Result<Option<AppendedRun>> {
        let limit = limit.max(1);
        let mut run_after = after;
        let mut from = after;
        loop {
            let (floor, held_from) = {
                let state = self.lock();
                let Some((floor, through)) = state.ring.feed_continuity(partition) else {
                    return Ok(None);
                };
                if from < floor {
                    // This node lacks every event up to its floor.
                    run_after = floor;
                    from = floor;
                }
                let held_from = state
                    .ring
                    .feed_bounds(partition)
                    .map_or(floor, |(_, held_from)| held_from);
                if from >= held_from {
                    let mut events = state.ring.events_after(partition, from);
                    let mut run_through = through.max(from);
                    if events.len() > limit {
                        events.truncate(limit);
                        if let Some(last) = events.last() {
                            run_through = last.position();
                        }
                    }
                    if events.is_empty() && run_through == after {
                        return Ok(None);
                    }
                    return Ok(Some(AppendedRun {
                        partition,
                        after: run_after,
                        through: run_through,
                        events,
                    }));
                }
                (floor, held_from)
            };
            // The ring evicted events in `(from, held_from]`.
            let Some(journal) = self.journal.get() else {
                // No journal keeps them: the run starts above them.
                run_after = held_from;
                from = held_from;
                continue;
            };
            // Pending first: an event the journal takes meanwhile is in the
            // journal read below.
            let (pending, pending_through) =
                self.pending_between(partition, from, held_from, limit);
            let read = journal.read(partition, from, held_from, None, limit)?;
            if let Some(kept_after) = read.after.filter(|kept_after| from < *kept_after) {
                // Retention dropped the journal's rows up to `kept_after`.
                run_after = kept_after;
                from = kept_after;
                continue;
            }
            let journaled = read
                .changes
                .into_iter()
                .map(|change| {
                    SequencedChangeEvent::new(
                        partition,
                        change.position,
                        floor,
                        change.database_id,
                        change.event,
                    )
                })
                .collect();
            let mut scanned = pending_through.min(read.scanned_through);
            let mut events = merge(pending, journaled);
            events.retain(|event| event.position() <= scanned);
            if events.len() >= limit {
                events.truncate(limit);
                if let Some(last) = events.last() {
                    scanned = last.position();
                }
            }
            if events.is_empty() {
                // No event lies in `(from, held_from]`: a bounded read
                // stops early only past an event it took. The run goes on
                // above the stretch.
                from = held_from;
                continue;
            }
            return Ok(Some(AppendedRun {
                partition,
                after: run_after,
                through: scanned,
                events,
            }));
        }
    }

    /// The pending events of `partition` in `(from, upto]`, at most
    /// `limit`, and the position up to which they are every such event.
    fn pending_between(
        &self,
        partition: ChangePartition,
        from: CdcOffset,
        upto: CdcOffset,
        limit: usize,
    ) -> (Vec<SequencedChangeEvent>, CdcOffset) {
        let ChangePartition(group_id) = partition;
        let state = self.lock();
        let mut taken: Vec<SequencedChangeEvent> = Vec::new();
        let Some(pending) = state.group_pending.get(&group_id) else {
            return (taken, upto);
        };
        let start = pending.partition_point(|event| event.position() <= from);
        for event in &pending[start..] {
            if event.position() > upto {
                break;
            }
            if taken.len() == limit {
                let through = taken.last().map_or(from, SequencedChangeEvent::position);
                return (taken, through);
            }
            taken.push(event.clone());
        }
        (taken, upto)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use nodedb_types::RowIdentity;

    use super::*;
    use crate::control::change_stream::journal::ChangeJournal;
    use crate::control::change_stream::{ChangeEvent, ChangeOperation};
    use crate::types::{DatabaseId, Lsn, TenantId};

    const GROUP: ChangePartition = ChangePartition(3);

    fn event(index: u64, tenant: u64) -> ChangeEvent {
        ChangeEvent {
            lsn: Lsn::new(index),
            tenant_id: TenantId::new(tenant),
            collection: "orders".into(),
            document_id: RowIdentity::from_user_key(format!("row-{index}")),
            operation: ChangeOperation::Insert,
            timestamp_ms: 1,
            after: None,
        }
    }

    /// Every run the leader reads from `after` on, until it holds nothing
    /// more.
    fn read_all(leader: &ChangeStream, mut after: CdcOffset, limit: usize) -> Vec<AppendedRun> {
        let mut runs = Vec::new();
        while let Some(run) = leader.forward_run(GROUP, after, limit).expect("read") {
            after = run.through;
            runs.push(run);
        }
        runs
    }

    /// The read crosses the journal, the pending events, and the ring, for
    /// every tenant, in position order, each event once. Each run holds at
    /// most `limit` events, and the runs chain with no gap.
    #[test]
    fn a_read_crosses_the_journal_pending_and_ring_unscoped() {
        let dir = tempfile::tempdir().expect("tempdir");
        let leader = ChangeStream::new(4);
        let journal = Arc::new(ChangeJournal::open(dir.path(), 1 << 10).expect("open"));
        leader.attach_journal(journal).expect("attach");
        for index in 1..=12 {
            leader.settle_entry(3, index, DatabaseId::DEFAULT, vec![event(index, index % 3)]);
            if index == 6 {
                leader.journal_group(3, 6).expect("journal the first half");
            }
        }
        let runs = read_all(&leader, CdcOffset::ZERO, 5);
        let mut after = CdcOffset::whole_index(0);
        let mut positions = Vec::new();
        for run in &runs {
            assert!(run.events.len() <= 5, "a run stays bounded");
            assert_eq!(run.after.max(after), after, "the runs chain with no gap");
            after = run.through;
            positions.extend(run.events.iter().map(SequencedChangeEvent::position));
        }
        let expected: Vec<CdcOffset> = (1..=12)
            .map(|index| CdcOffset::data_event(0, index, 1))
            .collect();
        assert_eq!(positions, expected);
        assert_eq!(
            after,
            CdcOffset::whole_index(12),
            "the read reaches the frontier"
        );
        assert!(leader.forward_run(GROUP, after, 5).expect("read").is_none());
    }

    /// A stretch with no event is one empty run up to the frontier. A
    /// position below the floor reads from the floor: a hole.
    #[test]
    fn idle_stretches_and_expired_positions() {
        let leader = ChangeStream::new(8);
        leader.settle_entry(3, 1, DatabaseId::DEFAULT, vec![event(1, 1)]);
        for index in 2..=4 {
            leader.settle_group(3, index, index);
        }
        let idle = leader
            .forward_run(GROUP, CdcOffset::whole_index(1), 8)
            .expect("read")
            .expect("the frontier moved");
        assert!(idle.events.is_empty());
        assert_eq!(idle.after, CdcOffset::whole_index(1));
        assert_eq!(idle.through, CdcOffset::whole_index(4));

        leader.install_group_floor(3, 6);
        leader.settle_entry(3, 7, DatabaseId::DEFAULT, vec![event(7, 1)]);
        let holed = leader
            .forward_run(GROUP, CdcOffset::whole_index(4), 8)
            .expect("read")
            .expect("a run past the floor");
        assert_eq!(
            holed.after,
            CdcOffset::whole_index(6),
            "the hole starts at the floor"
        );
        assert_eq!(holed.events.len(), 1);
    }
}
