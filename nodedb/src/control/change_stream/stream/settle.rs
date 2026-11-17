// SPDX-License-Identifier: BUSL-1.1

//! Publishing a data group's settled entries, in log order.
//!
//! An entry's events become visible only when the apply loop settles the
//! entry, after every earlier entry of its group. Consecutive entries held
//! in memory publish as one run. A spilled entry publishes page by page
//! from the journal, then its held tail. Every run is bounded by one page or
//! by the entries held in memory, so a settle holds at most one page of a
//! large entry.
//!
//! A run never claims a position it has not appended: an intermediate run's
//! `through` is the last position it carries, so the next run's events stay
//! above the feed's `through` and the ring takes them.
//!
//! The ring stays bounded. A page past its capacity evicts older events,
//! and a cursor below them catches up from the journal (see
//! [`super::catch_up`]). A live subscriber that lags resets, as on any
//! overflow of the broadcast channel.

use std::collections::BTreeSet;

use crate::event::cdc::CdcOffset;

use super::ChangePartition;
use super::bus::{BusState, ChangeStream};
use super::ring::{AppendedRun, ChangeRun, PositionedChange};
use super::staged::SPILL_EVENTS;

/// What a settle published.
#[derive(Debug, Default)]
pub struct Settled {
    /// Events the ring took that it did not hold yet.
    pub events: usize,
    /// The collections those events name.
    pub collections: BTreeSet<String>,
}

impl Settled {
    fn note(&mut self, run: &AppendedRun) {
        self.events += run.events.len();
        for event in &run.events {
            if !self.collections.contains(&event.collection) {
                self.collections.insert(event.collection.clone());
            }
        }
    }
}

/// The next spilled entry of a settle, with the tail it holds in memory.
struct SpilledEntry {
    log_index: u64,
    spilled_through: CdcOffset,
    tail: Vec<PositionedChange>,
}

impl ChangeStream {
    /// Whether a settle of `group_id`'s entries up to `last` reads spilled
    /// pages back from the journal.
    pub(crate) fn settles_from_journal(&self, group_id: u64, last: u64) -> bool {
        self.lock()
            .staged
            .range((group_id, 0)..=(group_id, last))
            .any(|(_, entry)| entry.spilled_through.is_some())
    }

    /// Publish the staged changes of `group_id`'s entries `first..=last`,
    /// which the apply loop settled in log order, with no forwarding.
    #[cfg(test)]
    pub(crate) fn settle_group(&self, group_id: u64, first: u64, last: u64) -> Settled {
        self.settle_group_with(group_id, first, last, || {})
    }

    /// Publish the staged changes of `group_id`'s entries `first..=last`,
    /// which the apply loop settled in log order. Calls `after_run` once
    /// the ring took each run, outside the stream's lock: the forwarder
    /// sends each run before the ring evicts it.
    pub(crate) fn settle_group_with(
        &self,
        group_id: u64,
        first: u64,
        last: u64,
        mut after_run: impl FnMut(),
    ) -> Settled {
        let partition = ChangePartition(group_id);
        let after = Some(CdcOffset::whole_index(first.saturating_sub(1)));
        let mut settled = Settled::default();
        let mut carried: Vec<PositionedChange> = Vec::new();
        loop {
            let spilled = {
                let mut guard = self.lock();
                let state = &mut *guard;
                let mut changes = std::mem::take(&mut carried);
                let spilled = take_until_spilled(state, group_id, first, last, &mut changes);
                let through = spilled
                    .as_ref()
                    .map_or(CdcOffset::whole_index(last), |entry| {
                        CdcOffset::whole_index(entry.log_index.saturating_sub(1))
                    });
                let appended = self.append_locked(
                    state,
                    ChangeRun {
                        partition,
                        after,
                        through,
                        changes,
                    },
                );
                settled.note(&appended);
                self.queue_group(state, group_id, &appended);
                spilled
            };
            after_run();
            let Some(entry) = spilled else {
                return settled;
            };
            self.publish_spilled(partition, after, &entry, &mut settled, &mut after_run);
            carried = entry.tail;
        }
    }

    /// Publish the spilled pages of `entry` from the journal, in order. The
    /// journal holds them already, so none waits for the floor save.
    ///
    /// A page that does not read back leaves a hole at the entry's spilled
    /// stretch: the floor rises past it, and every cursor below it resets.
    fn publish_spilled(
        &self,
        partition: ChangePartition,
        after: Option<CdcOffset>,
        entry: &SpilledEntry,
        settled: &mut Settled,
        after_run: &mut impl FnMut(),
    ) {
        let Some(journal) = self.journal.get() else {
            return;
        };
        let mut from = CdcOffset::whole_index(entry.log_index.saturating_sub(1));
        while from < entry.spilled_through {
            let page =
                match journal.read(partition, from, entry.spilled_through, None, SPILL_EVENTS) {
                    Ok(page) => page,
                    Err(error) => {
                        let ChangePartition(group_id) = partition;
                        tracing::error!(
                            group_id,
                            log_index = entry.log_index,
                            %error,
                            "a spilled page of change events did not read back from the change \
                             journal; the feed records a hole over it"
                        );
                        crate::diag::change_journal_failed(group_id, "spilled page read", &error);
                        self.lock()
                            .ring
                            .raise_floor(partition, entry.spilled_through);
                        return;
                    }
                };
            {
                let mut state = self.lock();
                let appended = self.append_locked(
                    &mut state,
                    ChangeRun {
                        partition,
                        after,
                        through: page.scanned_through,
                        changes: page.changes,
                    },
                );
                settled.note(&appended);
            }
            after_run();
            from = page.scanned_through;
        }
    }
}

/// Take `group_id`'s staged entries up to `last` in log order, moving the
/// held events of each into `changes`, until a spilled entry. Returns that
/// entry. An entry below `first` settled in an earlier pass without this
/// node ever publishing it, so it is dropped.
fn take_until_spilled(
    state: &mut BusState,
    group_id: u64,
    first: u64,
    last: u64,
    changes: &mut Vec<PositionedChange>,
) -> Option<SpilledEntry> {
    while let Some(key) = state
        .staged
        .range((group_id, 0)..=(group_id, last))
        .next()
        .map(|(key, _)| *key)
    {
        let entry = state.staged.remove(&key)?;
        state.held.sub(entry.held.len());
        if key.1 < first {
            continue;
        }
        if let Some(spilled_through) = entry.spilled_through {
            return Some(SpilledEntry {
                log_index: key.1,
                spilled_through,
                tail: entry.held,
            });
        }
        changes.extend(entry.held);
    }
    None
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use nodedb_types::RowIdentity;

    use super::*;
    use crate::control::change_stream::journal::ChangeJournal;
    use crate::control::change_stream::{
        ChangeCursor, ChangeEvent, ChangeOperation, ReplayStart, ReplayedChange,
    };
    use crate::types::{DatabaseId, Lsn, TenantId};

    const GROUP: u64 = 1;
    const RING: usize = 64;
    /// Rows the journal keeps: every event these tests publish, so a read
    /// from the first entry finds it.
    const RETAIN: usize = 1 << 16;
    /// Events one stage call carries: one page of a large entry, past the
    /// spill bound.
    const PAGE: usize = SPILL_EVENTS + 476;

    fn event(document: String) -> ChangeEvent {
        ChangeEvent {
            lsn: Lsn::new(9),
            tenant_id: TenantId::new(1),
            collection: "orders".into(),
            document_id: RowIdentity::from_user_key(document),
            operation: ChangeOperation::Insert,
            timestamp_ms: 1,
            after: None,
        }
    }

    fn open(dir: &std::path::Path) -> ChangeStream {
        let stream = ChangeStream::new(RING);
        let journal = Arc::new(ChangeJournal::open(dir, RETAIN).expect("open journal"));
        stream.attach_journal(journal).expect("attach");
        stream
    }

    /// Stage `pages` pages of [`PAGE`] events under entry `log_index`.
    fn stage_large(stream: &ChangeStream, log_index: u64, pages: usize) {
        for page in 0..pages {
            let events = (0..PAGE)
                .map(|row| event(format!("e{log_index}-{}", page * PAGE + row)))
                .collect();
            stream
                .stage_page_now(GROUP, log_index, DatabaseId::DEFAULT, events)
                .expect("the page spills");
        }
    }

    fn stage_small(stream: &ChangeStream, log_index: u64) {
        stream.stage(
            GROUP,
            log_index,
            DatabaseId::DEFAULT,
            vec![
                event(format!("e{log_index}-0")),
                event(format!("e{log_index}-1")),
            ],
        );
    }

    /// Every event past `after` in group 1, page by page.
    fn read_from(stream: &ChangeStream, after: u64) -> Vec<ReplayedChange> {
        let mut cursor = ChangeCursor::default();
        cursor.raise(ChangePartition(GROUP), CdcOffset::whole_index(after));
        let mut out = Vec::new();
        loop {
            let page = stream
                .query_changes(
                    TenantId::new(1),
                    Some("orders"),
                    ReplayStart::Cursor(cursor),
                    1000,
                )
                .expect("the cursor catches up");
            cursor = page.cursor;
            let more = page.has_more;
            out.extend(page.events);
            if !more {
                return out;
            }
        }
    }

    /// The positions of `(log index, events)` entries, in log order.
    fn expected(entries: &[(u64, usize)]) -> Vec<CdcOffset> {
        entries
            .iter()
            .flat_map(|&(index, events)| {
                (1..=events as u64).map(move |ordinal| CdcOffset::data_event(0, index, ordinal))
            })
            .collect()
    }

    /// A large entry stages in pages at its exact positions, between two
    /// small entries. It publishes past the ring's capacity, and a cursor
    /// before it reads every event once, in order, from the journal and
    /// the ring. Its spilled pages never wait in memory for the floor.
    #[test]
    fn a_large_entry_publishes_in_order_past_the_ring() {
        let dir = tempfile::tempdir().expect("tempdir");
        let stream = open(dir.path());
        stage_small(&stream, 4);
        stage_large(&stream, 5, 3);
        stage_small(&stream, 6);
        assert_eq!(stream.held_events(), 4, "the large entry's pages spilled");

        let settled = stream.settle_group(GROUP, 4, 6);
        assert_eq!(settled.events, 4 + 3 * PAGE);
        assert_eq!(
            stream.held_events(),
            4,
            "only the small entries wait for the floor"
        );
        stream.journal_group(GROUP, 6).expect("journal");
        assert_eq!(stream.held_events(), 0);

        let read = read_from(&stream, 3);
        let positions: Vec<CdcOffset> = read.iter().map(|change| change.position()).collect();
        assert_eq!(positions, expected(&[(4, 2), (5, 3 * PAGE), (6, 2)]));
        let large: Vec<&str> = read
            .iter()
            .filter(|change| change.position().index == 5)
            .map(|change| change.document_id.as_str())
            .collect();
        let staged: Vec<String> = (0..3 * PAGE).map(|row| format!("e5-{row}")).collect();
        assert_eq!(large, staged, "each event keeps its staging order");
    }

    /// The settled events a floor has not covered yet catch up from memory:
    /// the small entries from the pending events, the large one from its
    /// spilled pages.
    #[test]
    fn a_cursor_catches_up_before_the_floor_save() {
        let dir = tempfile::tempdir().expect("tempdir");
        let stream = open(dir.path());
        stage_small(&stream, 4);
        stage_large(&stream, 5, 2);
        stage_small(&stream, 6);
        stream.settle_group(GROUP, 4, 6);
        let positions: Vec<CdcOffset> = read_from(&stream, 3)
            .iter()
            .map(|change| change.position())
            .collect();
        assert_eq!(positions, expected(&[(4, 2), (5, 2 * PAGE), (6, 2)]));
    }

    /// The most the stream holds in memory for one entry is one page,
    /// however many pages the entry stages.
    #[test]
    fn an_entry_holds_one_page_in_memory_whatever_its_size() {
        let peak = |pages: usize| {
            let dir = tempfile::tempdir().expect("tempdir");
            let stream = open(dir.path());
            stage_large(&stream, 5, pages);
            stream.settle_group(GROUP, 5, 5);
            stream.journal_group(GROUP, 5).expect("journal");
            stream.held_events_peak()
        };
        let small = peak(2);
        let large = peak(12);
        assert_eq!(small, large, "the bound does not grow with the entry");
        assert!(large <= PAGE, "at most one page: {large}");
    }

    /// A crash while a large entry stages, and one after it settled and
    /// before its floor saved, leave nothing visible twice. The restart
    /// stages the entry again at the same positions, and it publishes once.
    #[test]
    fn a_crash_mid_entry_restages_exactly_once() {
        for settled_before_crash in [false, true] {
            let dir = tempfile::tempdir().expect("tempdir");
            {
                let stream = open(dir.path());
                stage_small(&stream, 4);
                stream.settle_group(GROUP, 4, 4);
                stream.journal_group(GROUP, 4).expect("journal");
                stage_large(&stream, 5, 2);
                if settled_before_crash {
                    stream.settle_group(GROUP, 5, 5);
                } else {
                    assert!(
                        read_from(&stream, 4).is_empty(),
                        "no event of an entry that has not settled is visible"
                    );
                }
            }
            let stream = open(dir.path());
            assert!(
                read_from(&stream, 4).is_empty(),
                "the restart drops the staged pages of an entry above the floor"
            );
            stage_large(&stream, 5, 2);
            stream.settle_group(GROUP, 5, 5);
            stream.journal_group(GROUP, 5).expect("journal");
            let positions: Vec<CdcOffset> = read_from(&stream, 3)
                .iter()
                .map(|change| change.position())
                .collect();
            assert_eq!(
                positions,
                expected(&[(4, 2), (5, 2 * PAGE)]),
                "settled before the crash: {settled_before_crash}"
            );
        }
    }
}
