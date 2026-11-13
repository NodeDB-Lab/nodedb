// SPDX-License-Identifier: BUSL-1.1

//! The change stream's side of the durable journal: rebuild the ring at
//! boot, and journal each feed at the point its durability contract names.
//! See [`crate::control::change_stream::journal`] for the contract.

use std::sync::Arc;

use crate::control::change_stream::journal::ChangeJournal;
use crate::control::change_stream::journal::codec::JournalRow;
use crate::event::cdc::CdcOffset;

use super::bus::{BusState, ChangeStream};
use super::ring::{AppendedRun, ChangeRun, PositionedChange};
use super::{ChangePartition, SequencedChangeEvent};

impl ChangeStream {
    /// Rebuild the replay ring from `journal`, and journal every later
    /// publish there.
    ///
    /// The ring takes each partition's newest journaled events, at most its
    /// capacity. It treats the older ones as evicted, and a cursor below
    /// them catches up from the journal.
    ///
    /// Events this process published before the journal attached, such as
    /// entries the apply loop settled first, go back on top of the rebuilt
    /// feeds and are journaled with the next batch of their feed.
    pub fn attach_journal(&self, journal: Arc<ChangeJournal>) -> crate::Result<()> {
        let feeds = journal.load(self.capacity())?;
        {
            let mut guard = self.lock();
            let state = &mut *guard;
            let (early_events, early_feeds) = state.ring.drain();
            for feed in feeds {
                state.ring.append(ChangeRun {
                    partition: feed.partition,
                    after: Some(feed.after),
                    through: feed.through,
                    changes: feed.changes,
                });
                if let Some(skipped) = feed.skipped {
                    state.ring.note_evicted(feed.partition, skipped);
                }
            }
            for (partition, floor, through) in early_feeds {
                let changes = early_events
                    .iter()
                    .filter(|event| event.partition() == partition)
                    .map(|event| PositionedChange {
                        position: event.position(),
                        database_id: event.database_id(),
                        event: event.event().clone(),
                    })
                    .collect();
                let appended = state.ring.append(ChangeRun {
                    partition,
                    after: Some(floor),
                    through,
                    changes,
                });
                let ChangePartition(group_id) = partition;
                state.held.add(appended.events.len());
                state
                    .group_pending
                    .entry(group_id)
                    .or_default()
                    .extend(appended.events);
            }
        }
        self.journal
            .set(journal)
            .map_err(|_| crate::Error::Internal {
                detail: "the change-feed journal was already attached".into(),
            })?;
        Ok(())
    }

    /// Hold a settled run of a data group until the group's applied floor
    /// covers it.
    pub(super) fn queue_group(&self, state: &mut BusState, group_id: u64, run: &AppendedRun) {
        if self.journal.get().is_none() || run.events.is_empty() {
            return;
        }
        state.held.add(run.events.len());
        state
            .group_pending
            .entry(group_id)
            .or_default()
            .extend(run.events.iter().cloned());
    }

    /// Journal `group_id`'s changes of every entry at or below `floor`, the
    /// applied floor the apply loop is about to save. Every entry the floor
    /// covers then has its changes on disk: the pending ones, and the
    /// spilled pages the journal counts from now on.
    ///
    /// Fails when the journal did not take them. The events stay pending,
    /// and the floor must not save: the next pass journals them again.
    pub(crate) fn journal_group(&self, group_id: u64, floor: u64) -> crate::Result<()> {
        let Some(journal) = self.journal.get() else {
            return Ok(());
        };
        let through = CdcOffset::whole_index(floor);
        let partition = ChangePartition(group_id);
        let (feed_after, events) = {
            let mut guard = self.lock();
            let state = &mut *guard;
            let Some((feed_after, _)) = state.ring.feed_continuity(partition) else {
                return Ok(());
            };
            let pending = state.group_pending.entry(group_id).or_default();
            let covered = pending.partition_point(|event| event.position() <= through);
            let events: Vec<SequencedChangeEvent> = pending.drain(..covered).collect();
            state.held.sub(events.len());
            (feed_after, events)
        };
        let written = journal.persist(
            partition,
            feed_after,
            through,
            events.iter().map(JournalRow::of_sequenced),
        );
        let mut guard = self.lock();
        let state = &mut *guard;
        match &written {
            Ok(()) => state.debt.paid(group_id),
            Err(error) => {
                state.debt.failed(group_id, error);
                state.held.add(events.len());
                let pending = state.group_pending.entry(group_id).or_default();
                let mut kept = events;
                kept.append(pending);
                *pending = kept;
            }
        }
        written
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::control::change_stream::{ChangeEvent, ChangeOperation, ReplayStart};
    use crate::types::{DatabaseId, Lsn, TenantId};
    use nodedb_types::RowIdentity;

    fn event(document: &str) -> ChangeEvent {
        ChangeEvent {
            lsn: Lsn::new(1),
            tenant_id: TenantId::new(1),
            collection: "orders".into(),
            document_id: RowIdentity::from_user_key(document),
            operation: ChangeOperation::Insert,
            timestamp_ms: 1,
            after: None,
        }
    }

    fn open(dir: &std::path::Path) -> ChangeStream {
        let stream = ChangeStream::new(64);
        let journal = Arc::new(ChangeJournal::open(dir, 64).expect("open journal"));
        stream.attach_journal(journal).expect("attach");
        stream
    }

    /// A cursor taken before a restart resumes after it at the next event:
    /// the group's journaled changes come back.
    #[test]
    fn a_cursor_resumes_across_a_restart() {
        let dir = tempfile::tempdir().expect("tempdir");
        let cursor = {
            let stream = open(dir.path());
            for (index, document) in [(1, "g1"), (2, "g2"), (3, "g3"), (4, "g4")] {
                stream.stage(4, index, DatabaseId::DEFAULT, vec![event(document)]);
            }
            stream.settle_group(4, 1, 4);
            stream.journal_group(4, 4).expect("journal");
            let page = stream
                .query_changes(TenantId::new(1), None, ReplayStart::Timestamp(0), 2)
                .expect("replay");
            page.cursor
        };

        let stream = open(dir.path());
        let resumed = stream
            .query_changes(TenantId::new(1), None, ReplayStart::Cursor(cursor), 64)
            .expect("replay");
        let documents: Vec<&str> = resumed
            .events
            .iter()
            .map(|change| change.document_id.as_str())
            .collect();
        assert_eq!(documents, vec!["g3", "g4"]);
    }

    /// Settled entries above the saved applied floor are not journaled, so
    /// they are delivered again after a restart and publish again. A cursor
    /// inside the journaled stretch resumes.
    #[test]
    fn entries_above_the_saved_floor_are_not_journaled() {
        let dir = tempfile::tempdir().expect("tempdir");
        let cursor = {
            let stream = open(dir.path());
            stream.stage(4, 1, DatabaseId::DEFAULT, vec![event("g1")]);
            stream.stage(4, 2, DatabaseId::DEFAULT, vec![event("g2")]);
            stream.settle_group(4, 1, 2);
            stream.journal_group(4, 1).expect("journal");
            stream
                .query_changes(TenantId::new(1), None, ReplayStart::Timestamp(0), 1)
                .expect("replay")
                .cursor
        };

        let stream = open(dir.path());
        let resumed = stream
            .query_changes(TenantId::new(1), None, ReplayStart::Cursor(cursor), 8)
            .expect("the journaled stretch resumes");
        assert!(resumed.events.is_empty());
    }
}
