// SPDX-License-Identifier: BUSL-1.1

//! The bounded replay ring behind the change stream, and what this node
//! holds of each partition's feed.
//!
//! A partition's events enter the ring in position order. The feed records
//! `through`, the position up to which this node saw the partition, and
//! `floor`, the position above which it holds every event. A run whose
//! publisher vouches continuity from a position above `through` leaves a
//! hole: the floor rises to that position, and a cursor below it must reset.

use std::collections::{BTreeMap, VecDeque};
use std::ops::Deref;

use crate::event::cdc::CdcOffset;
use crate::types::{DatabaseId, TenantId};

use super::{ChangeCursor, ChangeEvent, ChangePartition, SequencedChangeEvent};

/// One change at its position, before it enters a ring.
#[derive(Debug, Clone)]
pub(crate) struct PositionedChange {
    pub position: CdcOffset,
    pub database_id: DatabaseId,
    pub event: ChangeEvent,
}

/// A publisher's contiguous stretch of one partition's feed.
#[derive(Debug, Clone)]
pub(crate) struct ChangeRun {
    pub partition: ChangePartition,
    /// The publisher holds every event of the partition in `(after,
    /// through]`, and `changes` are all of them. `None` continues the
    /// publisher's previous run of the partition.
    pub after: Option<CdcOffset>,
    pub through: CdcOffset,
    /// In position order.
    pub changes: Vec<PositionedChange>,
}

/// What a ring took from a run: the events it did not hold yet, and the
/// stretch they cover.
#[derive(Debug, Clone)]
pub(crate) struct AppendedRun {
    pub partition: ChangePartition,
    /// The ring holds every event of the partition in `(after, through]`.
    pub after: CdcOffset,
    pub through: CdcOffset,
    pub events: Vec<SequencedChangeEvent>,
}

/// Replay starts after an acknowledged cursor or at a timestamp.
#[derive(Clone, Debug)]
pub enum ReplayStart {
    Cursor(ChangeCursor),
    Timestamp(u64),
}

/// A replayed event and the cursor that resumes right after it.
#[derive(Clone, Debug)]
pub struct ReplayedChange {
    pub event: SequencedChangeEvent,
    pub cursor: ChangeCursor,
}

impl Deref for ReplayedChange {
    type Target = SequencedChangeEvent;

    fn deref(&self) -> &Self::Target {
        &self.event
    }
}

/// A consistent replay of the ring.
#[derive(Clone, Debug)]
pub struct ReplaySnapshot {
    pub events: Vec<ReplayedChange>,
    /// Resumes right after the replay: past every returned event and every
    /// held event the replay passed over.
    pub cursor: ChangeCursor,
    /// The limit stopped the replay before the ring's end.
    pub has_more: bool,
}

/// A cursor this node cannot resume from; the client must reset.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ReplayError {
    Expired,
}

/// Which events a replay returns.
#[derive(Clone, Copy, Debug)]
pub(crate) struct ReplayScope<'a> {
    pub tenant_id: TenantId,
    pub database_id: DatabaseId,
    pub collection: Option<&'a str>,
}

impl ReplayScope<'_> {
    pub(crate) fn matches(&self, event: &SequencedChangeEvent) -> bool {
        self.admits(event.database_id(), event.tenant_id, &event.collection)
    }

    /// Whether a change of `collection` in `database_id` for `tenant_id` is
    /// in scope.
    pub(crate) fn admits(
        &self,
        database_id: DatabaseId,
        tenant_id: TenantId,
        collection: &str,
    ) -> bool {
        database_id == self.database_id
            && tenant_id == self.tenant_id
            && self.collection.is_none_or(|name| collection == name)
    }
}

/// A partition whose cursor sits below what the ring holds of it: the ring
/// evicted events the cursor has not consumed.
#[derive(Clone, Copy, Debug)]
pub(crate) struct Lag {
    pub partition: ChangePartition,
    /// The cursor's position in the partition.
    pub from: CdcOffset,
    /// The highest position the ring evicted. The ring holds every event
    /// of the partition above it.
    pub to: CdcOffset,
    /// The feed's floor, which every replayed event carries.
    pub floor: CdcOffset,
}

#[derive(Debug, Clone, Copy)]
struct PartitionFeed {
    through: CdcOffset,
    floor: CdcOffset,
    /// The highest position the ring evicted.
    evicted: Option<CdcOffset>,
}

pub(super) struct ReplayRing {
    events: VecDeque<SequencedChangeEvent>,
    feeds: BTreeMap<ChangePartition, PartitionFeed>,
    capacity: usize,
}

impl ReplayRing {
    pub fn new(capacity: usize) -> Self {
        Self {
            events: VecDeque::with_capacity(capacity),
            feeds: BTreeMap::new(),
            capacity: capacity.max(1),
        }
    }

    /// Where `run` starts on this ring, and the feed's floor once the ring
    /// took it.
    fn start_and_floor(&self, run: &ChangeRun) -> (CdcOffset, CdcOffset) {
        match (self.feeds.get(&run.partition), run.after) {
            (Some(feed), None) => (feed.through, feed.floor),
            (Some(feed), Some(after)) if after <= feed.through => (feed.through, feed.floor),
            // A hole between what this node saw and what the run vouches.
            (Some(_), Some(after)) | (None, Some(after)) => (after, after),
            (None, None) => {
                let start = run
                    .changes
                    .first()
                    .map_or(run.through, |change| before(change.position));
                (start, start)
            }
        }
    }

    /// The floor of `run`'s partition once the ring takes `run`.
    pub fn floor_after(&self, run: &ChangeRun) -> CdcOffset {
        self.start_and_floor(run).1
    }

    /// The changes of `run` that [`Self::append`] takes: those above where
    /// the run starts on this ring. A run the ring took before has none.
    pub fn fresh_in<'a>(&self, run: &'a ChangeRun) -> &'a [PositionedChange] {
        let (start, _) = self.start_and_floor(run);
        let first = run
            .changes
            .partition_point(|change| change.position <= start);
        &run.changes[first..]
    }

    /// Append the events of `run` the ring does not hold yet.
    pub fn append(&mut self, run: ChangeRun) -> AppendedRun {
        let (start, floor) = self.start_and_floor(&run);
        let ChangeRun {
            partition,
            through,
            changes,
            ..
        } = run;
        let prior = self.feeds.get(&partition).copied();
        let mut last = start;
        let mut events = Vec::with_capacity(changes.len());
        for change in changes {
            if change.position <= last {
                continue;
            }
            last = change.position;
            events.push(SequencedChangeEvent::new(
                partition,
                change.position,
                floor,
                change.database_id,
                change.event,
            ));
        }
        let through = through.max(last);
        let evicted = prior.and_then(|feed| feed.evicted);
        self.feeds.insert(
            partition,
            PartitionFeed {
                through,
                floor,
                evicted,
            },
        );
        for event in &events {
            self.push(event.clone());
        }
        AppendedRun {
            partition,
            after: start,
            through,
            events,
        }
    }

    fn push(&mut self, event: SequencedChangeEvent) {
        if self.events.len() == self.capacity
            && let Some(evicted) = self.events.pop_front()
            && let Some(feed) = self.feeds.get_mut(&evicted.partition())
        {
            feed.evicted = Some(
                feed.evicted
                    .map_or(evicted.position(), |held| held.max(evicted.position())),
            );
        }
        self.events.push_back(event);
    }

    /// Record that this node lacks every event of `partition` at or below
    /// `floor`.
    pub fn raise_floor(&mut self, partition: ChangePartition, floor: CdcOffset) {
        let feed = self.feeds.entry(partition).or_insert(PartitionFeed {
            through: floor,
            floor,
            evicted: None,
        });
        feed.floor = feed.floor.max(floor);
        feed.through = feed.through.max(floor);
    }

    /// Record that the ring holds no event of `partition` at or below
    /// `position`, as if it evicted them: boot loads a partition's newest
    /// journaled events only.
    pub fn note_evicted(&mut self, partition: ChangePartition, position: CdcOffset) {
        if let Some(feed) = self.feeds.get_mut(&partition) {
            feed.evicted = Some(feed.evicted.map_or(position, |held| held.max(position)));
        }
    }

    /// The partitions of `cursor` the ring evicted unconsumed events of.
    /// Fails when the cursor sits below a partition's floor: the node lacks
    /// events there.
    pub fn lagging(&self, cursor: &ChangeCursor) -> Result<Vec<Lag>, ReplayError> {
        let mut lags = Vec::new();
        for (partition, position) in cursor.entries() {
            let Some(feed) = self.feeds.get(&partition) else {
                continue;
            };
            if position < feed.floor {
                return Err(ReplayError::Expired);
            }
            if let Some(evicted) = feed.evicted
                && position < evicted
            {
                lags.push(Lag {
                    partition,
                    from: position,
                    to: evicted,
                    floor: feed.floor,
                });
            }
        }
        Ok(lags)
    }

    /// `(through, held_from)` of `partition`: the position up to which this
    /// node saw it, and the position above which the ring holds every event
    /// of it.
    pub fn feed_bounds(&self, partition: ChangePartition) -> Option<(CdcOffset, CdcOffset)> {
        let feed = self.feeds.get(&partition)?;
        let held_from = feed
            .evicted
            .map_or(feed.floor, |evicted| evicted.max(feed.floor));
        Some((feed.through, held_from))
    }

    /// Every held event of `partition` above `after`, in position order.
    pub fn events_after(
        &self,
        partition: ChangePartition,
        after: CdcOffset,
    ) -> Vec<SequencedChangeEvent> {
        let mut events: Vec<SequencedChangeEvent> = Vec::new();
        for event in self.events.iter().rev() {
            if event.partition() != partition {
                continue;
            }
            // A partition's events sit in position order.
            if event.position() <= after {
                break;
            }
            events.push(event.clone());
        }
        events.reverse();
        events
    }

    /// `(floor, through)` of `partition`: the position above which this
    /// node holds or held every event of it, and the position up to which it
    /// saw it.
    pub fn feed_continuity(&self, partition: ChangePartition) -> Option<(CdcOffset, CdcOffset)> {
        let feed = self.feeds.get(&partition)?;
        Some((feed.floor, feed.through))
    }

    /// Empty the ring: its events in ring order, and each partition's
    /// `(partition, floor, through)`.
    pub fn drain(
        &mut self,
    ) -> (
        Vec<SequencedChangeEvent>,
        Vec<(ChangePartition, CdcOffset, CdcOffset)>,
    ) {
        let events = self.events.drain(..).collect();
        let feeds = std::mem::take(&mut self.feeds)
            .into_iter()
            .map(|(partition, feed)| (partition, feed.floor, feed.through))
            .collect();
        (events, feeds)
    }

    /// Every partition this node saw.
    pub fn partitions(&self) -> Vec<ChangePartition> {
        self.feeds.keys().copied().collect()
    }

    /// The cursor past every event this node saw.
    pub fn head(&self) -> ChangeCursor {
        let mut cursor = ChangeCursor::default();
        for (partition, feed) in &self.feeds {
            cursor.raise(*partition, feed.through);
        }
        cursor
    }

    /// Whether this node can resume `cursor` without a hole. A replicated
    /// partition the node never saw has no hole to report.
    pub fn validate(&self, cursor: &ChangeCursor) -> Result<(), ReplayError> {
        for (partition, position) in cursor.entries() {
            let Some(feed) = self.feeds.get(&partition) else {
                continue;
            };
            if position < feed.floor || feed.evicted.is_some_and(|evicted| position < evicted) {
                return Err(ReplayError::Expired);
            }
        }
        Ok(())
    }

    /// Replay the ring's events in `scope` from `start`, at most `limit`.
    pub fn replay(
        &self,
        scope: ReplayScope<'_>,
        start: &ReplayStart,
        limit: usize,
    ) -> Result<ReplaySnapshot, ReplayError> {
        let mut cursor = match start {
            ReplayStart::Cursor(cursor) => {
                self.validate(cursor)?;
                cursor.clone()
            }
            ReplayStart::Timestamp(_) => ChangeCursor::default(),
        };
        let mut events = Vec::new();
        let mut has_more = false;
        for event in &self.events {
            let eligible = scope.matches(event)
                && match start {
                    ReplayStart::Cursor(start) => !start.covers(event),
                    ReplayStart::Timestamp(since) => event.timestamp_ms >= *since,
                };
            if eligible && events.len() == limit {
                has_more = true;
                break;
            }
            cursor.raise(event.partition(), event.position());
            if eligible {
                events.push(ReplayedChange {
                    event: event.clone(),
                    cursor: cursor.clone(),
                });
            }
        }
        Ok(ReplaySnapshot {
            events,
            cursor,
            has_more,
        })
    }
}

/// The largest position below `position`.
fn before(position: CdcOffset) -> CdcOffset {
    if position.sequence > 0 {
        CdcOffset::at(position.epoch, position.index, position.sequence - 1)
    } else if position.index > 0 {
        CdcOffset::at(position.epoch, position.index - 1, u64::MAX)
    } else {
        position
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::control::change_stream::ChangeOperation;
    use crate::types::Lsn;

    const GROUP: ChangePartition = ChangePartition(4);

    fn change(index: u64, ordinal: u64) -> PositionedChange {
        PositionedChange {
            position: CdcOffset::data_event(0, index, ordinal),
            database_id: DatabaseId::DEFAULT,
            event: ChangeEvent {
                lsn: Lsn::new(index),
                tenant_id: TenantId::new(1),
                collection: "orders".into(),
                document_id: nodedb_types::RowIdentity::from_user_key(format!("{index}.{ordinal}")),
                operation: ChangeOperation::Insert,
                timestamp_ms: index,
                after: None,
            },
        }
    }

    fn run(after: u64, through: u64, changes: Vec<PositionedChange>) -> ChangeRun {
        ChangeRun {
            partition: GROUP,
            after: Some(CdcOffset::whole_index(after)),
            through: CdcOffset::whole_index(through),
            changes,
        }
    }

    fn scope() -> ReplayScope<'static> {
        ReplayScope {
            tenant_id: TenantId::new(1),
            database_id: DatabaseId::DEFAULT,
            collection: None,
        }
    }

    fn at(cursor: &ChangeCursor) -> ReplayStart {
        ReplayStart::Cursor(cursor.clone())
    }

    #[test]
    fn a_repeated_run_appends_nothing() {
        let mut ring = ReplayRing::new(16);
        let first = ring.append(run(9, 11, vec![change(10, 1), change(11, 1)]));
        assert_eq!(first.events.len(), 2);
        // The same stretch from another replica after a leader change.
        let again = ring.append(run(
            9,
            12,
            vec![change(10, 1), change(11, 1), change(12, 1)],
        ));
        assert_eq!(again.events.len(), 1);
        assert_eq!(again.after, CdcOffset::whole_index(11));
        assert_eq!(again.events[0].position(), CdcOffset::data_event(0, 12, 1));
    }

    #[test]
    fn a_cursor_from_another_node_resumes_at_the_same_event() {
        let mut leader = ReplayRing::new(16);
        let mut follower = ReplayRing::new(16);
        for ring in [&mut leader, &mut follower] {
            ring.append(run(0, 3, vec![change(1, 1), change(2, 1), change(3, 1)]));
        }
        let page = leader
            .replay(scope(), &ReplayStart::Timestamp(0), 2)
            .expect("replay");
        assert!(page.has_more);
        let resumed = follower
            .replay(scope(), &at(&page.cursor), 16)
            .expect("the follower holds the same feed");
        assert_eq!(resumed.events.len(), 1);
        assert_eq!(resumed.events[0].position(), CdcOffset::data_event(0, 3, 1));
    }

    #[test]
    fn a_hole_expires_every_cursor_below_it() {
        let mut ring = ReplayRing::new(16);
        ring.append(run(0, 2, vec![change(1, 1), change(2, 1)]));
        let page = ring
            .replay(scope(), &ReplayStart::Timestamp(0), 16)
            .expect("replay");
        // Entries 3..=5 never reached this node.
        let appended = ring.append(run(5, 6, vec![change(6, 1)]));
        assert_eq!(appended.events[0].floor(), CdcOffset::whole_index(5));
        assert_eq!(
            ring.replay(scope(), &at(&page.cursor), 16).err(),
            Some(ReplayError::Expired)
        );
        let mut past = ChangeCursor::default();
        past.raise(GROUP, CdcOffset::whole_index(5));
        assert_eq!(
            ring.replay(scope(), &at(&past), 16)
                .expect("replay")
                .events
                .len(),
            1
        );
    }

    #[test]
    fn an_install_floor_and_eviction_expire_older_cursors() {
        let mut ring = ReplayRing::new(2);
        ring.append(run(0, 3, vec![change(1, 1), change(2, 1), change(3, 1)]));
        let mut cursor = ChangeCursor::default();
        cursor.raise(GROUP, CdcOffset::whole_index(0));
        assert_eq!(
            ring.replay(scope(), &at(&cursor), 16).err(),
            Some(ReplayError::Expired),
            "event 1 was evicted before this cursor consumed it"
        );
        cursor.raise(GROUP, CdcOffset::data_event(0, 1, 1));
        assert!(ring.replay(scope(), &at(&cursor), 16).is_ok());
        ring.raise_floor(GROUP, CdcOffset::whole_index(8));
        assert_eq!(
            ring.replay(scope(), &at(&cursor), 16).err(),
            Some(ReplayError::Expired)
        );
    }

    #[test]
    fn a_timestamp_replay_resumes_past_the_events_it_skipped() {
        let mut ring = ReplayRing::new(16);
        ring.append(run(0, 3, vec![change(1, 1), change(2, 1), change(3, 1)]));
        let page = ring
            .replay(scope(), &ReplayStart::Timestamp(3), 16)
            .expect("replay");
        assert_eq!(page.events.len(), 1);
        assert_eq!(
            page.cursor.position(GROUP),
            Some(CdcOffset::data_event(0, 3, 1))
        );
        assert!(
            ring.replay(scope(), &at(&page.cursor), 16)
                .expect("replay")
                .events
                .is_empty()
        );
    }
}
