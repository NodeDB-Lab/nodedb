// SPDX-License-Identifier: BUSL-1.1

//! The durable change-feed journal.
//!
//! The journal keeps what this node published on each partition, bounded per
//! partition, so the replay ring comes back after a restart, and a cursor
//! the ring no longer serves catches up from it.
//!
//! - A data group's changes are journaled when the apply loop saves the
//!   group's applied floor, before the floor: every entry at or below a
//!   saved floor has its changes on disk. Entries above it are delivered
//!   again after a restart and publish again. A committed Calvin slice
//!   installs from a data-group entry, so its changes follow this rule.
//! - A forwarded run is journaled before the ring takes it, and before the
//!   receiver acks it. A run the journal does not write is refused, never
//!   acked: the ring takes none of it, and the leader sends the stretch
//!   again from the receiver's position (see `stream::forwarded`).
//! - A large entry's staged pages are written as it stages, above the
//!   partition's `through` (see [`ChangeJournal::spill`]). They are not
//!   counted until the floor save that covers the entry journals it.
//!
//! Each partition keeps `after`, the position above which it holds every
//! event, and `through`, the position up to which it holds the partition. A
//! write whose `after` lies above the stored `through` is a hole: the
//! journal drops the partition's rows up to it and restarts it there.
//!
//! Retention drops the oldest rows past the partition's bound, whole entries
//! at a time. The entry that holds the first row kept stays whole, so a
//! consumer at its start reads every event of it.

use std::ops::Bound;
use std::path::Path;

use redb::{Database, Durability, ReadableTable, Table, TableDefinition};

use crate::control::change_stream::ChangePartition;
use crate::event::cdc::CdcOffset;

use super::codec::{
    FeedRecord, JournalRow, StoredChange, partition_key, row_bounds, row_key, row_position,
};

pub(super) const ROWS: TableDefinition<&[u8], &[u8]> = TableDefinition::new("change_feed_rows");
pub(super) const FEEDS: TableDefinition<&[u8], &[u8]> = TableDefinition::new("change_feed_feeds");

pub(super) fn storage(detail: impl std::fmt::Display) -> crate::Error {
    crate::Error::Storage {
        engine: "change_stream".into(),
        detail: format!("change-feed journal: {detail}"),
    }
}

/// The durable journal of the Control-Plane change feeds.
pub struct ChangeJournal {
    pub(super) db: Database,
    /// Rows each partition keeps, rounded up to a whole entry.
    retain: u64,
    /// The number of coming persists that raise. Only tests set it.
    #[cfg(test)]
    refused_persists: std::sync::atomic::AtomicU32,
}

impl ChangeJournal {
    /// Open or create the journal at `{dir}/change_feed.redb`, keeping
    /// `retain` rows per partition.
    pub fn open(dir: &Path, retain: usize) -> crate::Result<Self> {
        std::fs::create_dir_all(dir)
            .map_err(|e| storage(format!("create {}: {e}", dir.display())))?;
        let path = dir.join("change_feed.redb");
        let db = Database::create(&path)
            .map_err(|e| storage(format!("open {}: {e}", path.display())))?;
        let txn = db.begin_write().map_err(storage)?;
        {
            txn.open_table(ROWS).map_err(storage)?;
            txn.open_table(FEEDS).map_err(storage)?;
        }
        txn.commit().map_err(storage)?;
        Ok(Self {
            db,
            retain: u64::try_from(retain.max(1)).unwrap_or(u64::MAX),
            #[cfg(test)]
            refused_persists: std::sync::atomic::AtomicU32::new(0),
        })
    }

    /// Make the next `persists` calls of [`Self::persist`] raise before
    /// they write, so a caller's error path runs.
    #[cfg(test)]
    pub(crate) fn fail_next_persists_for_test(&self, persists: u32) {
        self.refused_persists
            .store(persists, std::sync::atomic::Ordering::SeqCst);
    }

    /// Journal `rows` of `partition`, durably. The publisher holds every
    /// event of the partition above `feed_after`, and has seen it up to
    /// `through`. Every staged row up to `through` counts from now on.
    pub(crate) fn persist<'a>(
        &self,
        partition: ChangePartition,
        feed_after: CdcOffset,
        through: CdcOffset,
        rows: impl IntoIterator<Item = JournalRow<'a>>,
    ) -> crate::Result<()> {
        #[cfg(test)]
        if self
            .refused_persists
            .try_update(
                std::sync::atomic::Ordering::SeqCst,
                std::sync::atomic::Ordering::SeqCst,
                |left| left.checked_sub(1),
            )
            .is_ok()
        {
            return Err(storage("injected persist failure"));
        }
        let key = partition_key(partition);
        let (first, _) = row_bounds(partition);
        let txn = self.db.begin_write().map_err(storage)?;
        {
            let mut feeds = txn.open_table(FEEDS).map_err(storage)?;
            let mut table = txn.open_table(ROWS).map_err(storage)?;
            let stored = feeds
                .get(key.as_slice())
                .map_err(storage)?
                .and_then(|bytes| FeedRecord::decode(bytes.value()));
            let mut feed = match stored {
                None => FeedRecord {
                    after: feed_after,
                    through: feed_after,
                    rows: 0,
                },
                Some(stored) if feed_after <= stored.through => stored,
                Some(_) => {
                    // A hole: no cursor below it can be served, so the rows
                    // up to it go. A row above it is a staged page of an
                    // entry that settles later.
                    let hole = row_key(partition, feed_after);
                    table
                        .retain_in(first.as_slice()..=hole.as_slice(), |_, _| false)
                        .map_err(storage)?;
                    FeedRecord {
                        after: feed_after,
                        through: feed_after,
                        rows: 0,
                    }
                }
            };
            let counted = feed.through;
            for row in rows {
                if row.position <= feed.after {
                    continue;
                }
                let value = zerompk::to_msgpack_vec(&StoredChange::of(row)).map_err(storage)?;
                let at = row_key(partition, row.position);
                let fresh = table
                    .insert(at.as_slice(), value.as_slice())
                    .map_err(storage)?
                    .is_none();
                // A row above the counted stretch counts with the stretch
                // below.
                if fresh && row.position <= counted {
                    feed.rows += 1;
                }
            }
            feed.through = counted.max(through);
            if feed.through > counted {
                feed.rows += count_rows(&table, partition, counted, feed.through)?;
            }
            if feed.rows > self.retain {
                trim(&mut table, partition, &mut feed, self.retain)?;
            }
            feeds
                .insert(key.as_slice(), feed.encode().as_slice())
                .map_err(storage)?;
        }
        txn.commit().map_err(storage)
    }

    /// Write `rows`, a staged page of an entry that has not settled, at
    /// their positions above the partition's `through`. A catch-up read
    /// serves them once the entry settled, and the floor save that covers
    /// the entry counts them.
    ///
    /// The commit is not durable alone: the next durable commit, the floor
    /// save's, makes it durable. A crash before loses the page, and the
    /// restart stages the entry again. Boot drops every row above its
    /// partition's `through` (see [`ChangeJournal::load`]).
    pub(crate) fn spill<'a>(
        &self,
        partition: ChangePartition,
        rows: impl IntoIterator<Item = JournalRow<'a>>,
    ) -> crate::Result<()> {
        let mut txn = self.db.begin_write().map_err(storage)?;
        txn.set_durability(Durability::None).map_err(storage)?;
        {
            let mut table = txn.open_table(ROWS).map_err(storage)?;
            for row in rows {
                let value = zerompk::to_msgpack_vec(&StoredChange::of(row)).map_err(storage)?;
                let at = row_key(partition, row.position);
                table
                    .insert(at.as_slice(), value.as_slice())
                    .map_err(storage)?;
            }
        }
        txn.commit().map_err(storage)
    }
}

/// Rows of `partition` in `(after, through]`.
fn count_rows(
    table: &Table<'_, &'static [u8], &'static [u8]>,
    partition: ChangePartition,
    after: CdcOffset,
    through: CdcOffset,
) -> crate::Result<u64> {
    let low = row_key(partition, after);
    let high = row_key(partition, through);
    let mut rows = 0u64;
    for row in table
        .range((
            Bound::Excluded(low.as_slice()),
            Bound::Included(high.as_slice()),
        ))
        .map_err(storage)?
    {
        row.map_err(storage)?;
        rows += 1;
    }
    Ok(rows)
}

/// Drop the oldest rows of `partition` past `retain`, whole entries at a
/// time: the entry that holds the first row kept stays whole.
fn trim(
    table: &mut Table<'_, &'static [u8], &'static [u8]>,
    partition: ChangePartition,
    feed: &mut FeedRecord,
    retain: u64,
) -> crate::Result<()> {
    let excess = feed.rows.saturating_sub(retain);
    let (first, _) = row_bounds(partition);
    let through = row_key(partition, feed.through);
    // The rows before the entry that holds the first row kept, and the
    // last of them.
    let mut dropped = 0u64;
    let mut drop_through: Option<CdcOffset> = None;
    {
        let mut entry: Option<(u64, u64)> = None;
        let mut last: Option<CdcOffset> = None;
        for (seen, row) in (0u64..).zip(
            table
                .range(first.as_slice()..=through.as_slice())
                .map_err(storage)?,
        ) {
            let (key, _) = row.map_err(storage)?;
            let position =
                row_position(key.value()).ok_or_else(|| storage("a row key did not decode"))?;
            if entry != Some((position.epoch, position.index)) {
                entry = Some((position.epoch, position.index));
                dropped = seen;
                drop_through = last;
            }
            if seen == excess {
                break;
            }
            last = Some(position);
        }
    }
    let Some(drop_through) = drop_through.filter(|_| dropped > 0) else {
        return Ok(());
    };
    let upto = row_key(partition, drop_through);
    table
        .retain_in(first.as_slice()..=upto.as_slice(), |_, _| false)
        .map_err(storage)?;
    feed.after = feed.after.max(drop_through);
    feed.rows -= dropped;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::control::change_stream::{ChangeEvent, ChangeOperation, SequencedChangeEvent};
    use crate::types::{DatabaseId, Lsn, TenantId};

    const GROUP: ChangePartition = ChangePartition(2);

    fn event_at(index: u64, ordinal: u64) -> SequencedChangeEvent {
        SequencedChangeEvent::new(
            GROUP,
            CdcOffset::data_event(0, index, ordinal),
            CdcOffset::ZERO,
            DatabaseId::DEFAULT,
            ChangeEvent {
                lsn: Lsn::new(index),
                tenant_id: TenantId::new(1),
                collection: "orders".into(),
                document_id: nodedb_types::RowIdentity::from_user_key(format!(
                    "row-{index}.{ordinal}"
                )),
                operation: ChangeOperation::Update,
                timestamp_ms: index * 10,
                after: None,
            },
        )
    }

    fn event(index: u64) -> SequencedChangeEvent {
        event_at(index, 1)
    }

    fn persist(journal: &ChangeJournal, after: u64, through: u64, events: &[SequencedChangeEvent]) {
        journal
            .persist(
                GROUP,
                CdcOffset::whole_index(after),
                CdcOffset::whole_index(through),
                events.iter().map(JournalRow::of_sequenced),
            )
            .expect("persist");
    }

    #[test]
    fn a_journaled_feed_loads_back_in_position_order() {
        let dir = tempfile::tempdir().expect("tempdir");
        let journal = ChangeJournal::open(dir.path(), 8).expect("open");
        persist(&journal, 0, 2, &[event(1), event(2)]);
        persist(&journal, 0, 5, &[event(4)]);
        drop(journal);

        let journal = ChangeJournal::open(dir.path(), 8).expect("reopen");
        let feeds = journal.load(8).expect("load");
        assert_eq!(feeds.len(), 1);
        let feed = &feeds[0];
        assert_eq!(feed.after, CdcOffset::whole_index(0));
        assert_eq!(feed.through, CdcOffset::whole_index(5));
        assert_eq!(feed.skipped, None);
        let positions: Vec<CdcOffset> = feed.changes.iter().map(|c| c.position).collect();
        assert_eq!(
            positions,
            vec![
                CdcOffset::data_event(0, 1, 1),
                CdcOffset::data_event(0, 2, 1),
                CdcOffset::data_event(0, 4, 1),
            ]
        );
        assert_eq!(feed.changes[1].event.document_id.as_str(), "row-2.1");
        assert_eq!(feed.changes[1].event.operation, ChangeOperation::Update);
        assert_eq!(feed.changes[1].event.timestamp_ms, 20);

        // A tail of two leaves the oldest row to a catch-up read.
        let feed = &journal.load(2).expect("load")[0];
        assert_eq!(feed.changes.len(), 2);
        assert_eq!(feed.skipped, Some(CdcOffset::data_event(0, 1, 1)));
    }

    #[test]
    fn retention_and_holes_raise_the_journal_floor() {
        let dir = tempfile::tempdir().expect("tempdir");
        let journal = ChangeJournal::open(dir.path(), 2).expect("open");
        persist(&journal, 0, 3, &[event(1), event(2), event(3)]);
        let feed = &journal.load(8).expect("load")[0];
        assert_eq!(feed.changes.len(), 2, "retention keeps two rows");
        assert_eq!(feed.after, CdcOffset::data_event(0, 1, 1));

        persist(&journal, 9, 10, &[event(10)]);
        let feed = &journal.load(8).expect("load")[0];
        assert_eq!(feed.after, CdcOffset::whole_index(9));
        assert_eq!(feed.changes.len(), 1);
    }

    /// Retention keeps the newest entry whole when it alone passes the
    /// bound, so a consumer at its start reads every event of it.
    #[test]
    fn retention_keeps_the_entry_of_the_first_kept_row_whole() {
        let dir = tempfile::tempdir().expect("tempdir");
        let journal = ChangeJournal::open(dir.path(), 2).expect("open");
        let large: Vec<SequencedChangeEvent> =
            (1..=5).map(|ordinal| event_at(3, ordinal)).collect();
        persist(&journal, 0, 2, &[event(1), event(2)]);
        persist(&journal, 0, 3, &large);
        let feed = &journal.load(16).expect("load")[0];
        assert_eq!(feed.changes.len(), 5, "entry 3 stays whole");
        assert_eq!(feed.after, CdcOffset::data_event(0, 2, 1));
    }

    /// A staged page sits above `through` uncounted. The persist whose
    /// `through` covers it counts it, and a load drops one it never covered.
    #[test]
    fn staged_rows_count_once_their_floor_covers_them() {
        let dir = tempfile::tempdir().expect("tempdir");
        let journal = ChangeJournal::open(dir.path(), 64).expect("open");
        persist(&journal, 0, 1, &[event(1)]);
        let page = [event_at(2, 1), event_at(2, 2)];
        let staged = [event_at(3, 1)];
        journal
            .spill(GROUP, page.iter().map(JournalRow::of_sequenced))
            .expect("spill");
        journal
            .spill(GROUP, staged.iter().map(JournalRow::of_sequenced))
            .expect("spill");
        let read = journal
            .read(
                GROUP,
                CdcOffset::whole_index(1),
                CdcOffset::whole_index(2),
                None,
                16,
            )
            .expect("read");
        assert_eq!(read.changes.len(), 2, "a catch-up read serves the page");
        assert_eq!(read.scanned_through, CdcOffset::whole_index(2));

        persist(&journal, 0, 2, &[]);
        let feed = &journal.load(16).expect("load")[0];
        assert_eq!(
            feed.changes.len(),
            3,
            "entry 3 never settled: boot drops it"
        );
        let read = journal
            .read(
                GROUP,
                CdcOffset::whole_index(2),
                CdcOffset::whole_index(3),
                None,
                16,
            )
            .expect("read");
        assert!(read.changes.is_empty());
    }
}
