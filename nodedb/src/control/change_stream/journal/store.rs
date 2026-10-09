// SPDX-License-Identifier: BUSL-1.1

//! The durable change-feed journal.
//!
//! The journal keeps what this node published on each partition, bounded per
//! partition, so the replay ring comes back after a restart and a cursor
//! inside retention resumes with no reset.
//!
//! - A data group's changes are journaled when the apply loop saves the
//!   group's applied floor, before the floor: every entry at or below a
//!   saved floor has its changes on disk. Entries above it are delivered
//!   again after a restart and publish again. A committed Calvin slice
//!   installs from a data-group entry, so its changes follow this rule.
//! - A forwarded run is journaled before the receiver acks it.
//!
//! Each partition keeps `after`, the position above which it holds every
//! event, and `through`, the position up to which it holds the partition. A
//! write whose `after` lies above the stored `through` is a hole: the
//! journal drops the partition's rows and restarts it there.

use std::path::Path;

use redb::{Database, ReadableDatabase, ReadableTable, TableDefinition};

use crate::control::change_stream::{ChangePartition, PositionedChange, SequencedChangeEvent};
use crate::event::cdc::CdcOffset;

use super::codec::{
    FeedRecord, StoredChange, decode_partition, partition_key, row_bounds, row_key, row_position,
};

const ROWS: TableDefinition<&[u8], &[u8]> = TableDefinition::new("change_feed_rows");
const FEEDS: TableDefinition<&[u8], &[u8]> = TableDefinition::new("change_feed_feeds");

fn storage(detail: impl std::fmt::Display) -> crate::Error {
    crate::Error::Storage {
        engine: "change_stream".into(),
        detail: format!("change-feed journal: {detail}"),
    }
}

/// One partition as the journal holds it.
#[derive(Debug, Clone)]
pub(crate) struct JournalFeed {
    pub partition: ChangePartition,
    pub after: CdcOffset,
    pub through: CdcOffset,
    /// In position order.
    pub changes: Vec<PositionedChange>,
}

/// The durable journal of the Control-Plane change feeds.
pub struct ChangeJournal {
    db: Database,
    /// Rows each partition keeps.
    retain: u64,
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
        })
    }

    /// Journal `events` of `partition`, durably. The publisher holds every
    /// event of the partition above `feed_after`, and has seen it up to
    /// `through`.
    pub fn persist(
        &self,
        partition: ChangePartition,
        feed_after: CdcOffset,
        through: CdcOffset,
        events: &[SequencedChangeEvent],
    ) -> crate::Result<()> {
        let key = partition_key(partition);
        let (first, last) = row_bounds(partition);
        let txn = self.db.begin_write().map_err(storage)?;
        {
            let mut feeds = txn.open_table(FEEDS).map_err(storage)?;
            let mut rows = txn.open_table(ROWS).map_err(storage)?;
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
                    // below it go.
                    rows.retain_in(first.as_slice()..=last.as_slice(), |_, _| false)
                        .map_err(storage)?;
                    FeedRecord {
                        after: feed_after,
                        through: feed_after,
                        rows: 0,
                    }
                }
            };
            for event in events {
                if event.position() <= feed.after {
                    continue;
                }
                let value = zerompk::to_msgpack_vec(&StoredChange::of(event)).map_err(storage)?;
                let row = row_key(partition, event.position());
                if rows
                    .insert(row.as_slice(), value.as_slice())
                    .map_err(storage)?
                    .is_none()
                {
                    feed.rows += 1;
                }
            }
            feed.through = feed.through.max(through);
            if feed.rows > self.retain {
                let excess = usize::try_from(feed.rows - self.retain).unwrap_or(usize::MAX);
                let mut doomed = Vec::with_capacity(excess);
                for entry in rows
                    .range(first.as_slice()..=last.as_slice())
                    .map_err(storage)?
                    .take(excess)
                {
                    let (row, _) = entry.map_err(storage)?;
                    doomed.push(row.value().to_vec());
                }
                for row in doomed {
                    rows.remove(row.as_slice()).map_err(storage)?;
                    if let Some(position) = row_position(&row) {
                        feed.after = feed.after.max(position);
                    }
                    feed.rows -= 1;
                }
            }
            feeds
                .insert(key.as_slice(), feed.encode().as_slice())
                .map_err(storage)?;
        }
        txn.commit().map_err(storage)
    }

    /// Every journaled partition with its rows.
    pub(crate) fn load(&self) -> crate::Result<Vec<JournalFeed>> {
        let txn = self.db.begin_read().map_err(storage)?;
        let feeds = txn.open_table(FEEDS).map_err(storage)?;
        let rows = txn.open_table(ROWS).map_err(storage)?;
        let mut out = Vec::new();
        for entry in feeds.iter().map_err(storage)? {
            let (key, value) = entry.map_err(storage)?;
            let partition = decode_partition(key.value())
                .ok_or_else(|| storage("a feed key did not decode"))?;
            let feed = FeedRecord::decode(value.value())
                .ok_or_else(|| storage("a feed record did not decode"))?;
            let (first, last) = row_bounds(partition);
            let mut changes = Vec::new();
            for row in rows
                .range(first.as_slice()..=last.as_slice())
                .map_err(storage)?
            {
                let (row, value) = row.map_err(storage)?;
                let position =
                    row_position(row.value()).ok_or_else(|| storage("a row key did not decode"))?;
                let stored: StoredChange = zerompk::from_msgpack(value.value()).map_err(storage)?;
                changes.push(stored.at(position));
            }
            out.push(JournalFeed {
                partition,
                after: feed.after,
                through: feed.through,
                changes,
            });
        }
        Ok(out)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::control::change_stream::{ChangeEvent, ChangeOperation};
    use crate::types::{DatabaseId, Lsn, TenantId};

    const GROUP: ChangePartition = ChangePartition(2);

    fn event(index: u64) -> SequencedChangeEvent {
        SequencedChangeEvent::new(
            GROUP,
            CdcOffset::data_event(0, index, 1),
            CdcOffset::ZERO,
            DatabaseId::DEFAULT,
            ChangeEvent {
                lsn: Lsn::new(index),
                tenant_id: TenantId::new(1),
                collection: "orders".into(),
                document_id: nodedb_types::RowIdentity::from_user_key(format!("row-{index}")),
                operation: ChangeOperation::Update,
                timestamp_ms: index * 10,
                after: None,
            },
        )
    }

    #[test]
    fn a_journaled_feed_loads_back_in_position_order() {
        let dir = tempfile::tempdir().expect("tempdir");
        let journal = ChangeJournal::open(dir.path(), 8).expect("open");
        journal
            .persist(
                GROUP,
                CdcOffset::whole_index(0),
                CdcOffset::whole_index(2),
                &[event(1), event(2)],
            )
            .expect("persist");
        journal
            .persist(
                GROUP,
                CdcOffset::whole_index(0),
                CdcOffset::whole_index(5),
                &[event(4)],
            )
            .expect("persist");
        drop(journal);

        let journal = ChangeJournal::open(dir.path(), 8).expect("reopen");
        let feeds = journal.load().expect("load");
        assert_eq!(feeds.len(), 1);
        let feed = &feeds[0];
        assert_eq!(feed.after, CdcOffset::whole_index(0));
        assert_eq!(feed.through, CdcOffset::whole_index(5));
        let positions: Vec<CdcOffset> = feed.changes.iter().map(|c| c.position).collect();
        assert_eq!(
            positions,
            vec![
                CdcOffset::data_event(0, 1, 1),
                CdcOffset::data_event(0, 2, 1),
                CdcOffset::data_event(0, 4, 1),
            ]
        );
        assert_eq!(feed.changes[1].event.document_id.as_str(), "row-2");
        assert_eq!(feed.changes[1].event.operation, ChangeOperation::Update);
        assert_eq!(feed.changes[1].event.timestamp_ms, 20);
    }

    #[test]
    fn retention_and_holes_raise_the_journal_floor() {
        let dir = tempfile::tempdir().expect("tempdir");
        let journal = ChangeJournal::open(dir.path(), 2).expect("open");
        journal
            .persist(
                GROUP,
                CdcOffset::whole_index(0),
                CdcOffset::whole_index(3),
                &[event(1), event(2), event(3)],
            )
            .expect("persist");
        let feed = &journal.load().expect("load")[0];
        assert_eq!(feed.changes.len(), 2, "retention keeps two rows");
        assert_eq!(feed.after, CdcOffset::data_event(0, 1, 1));

        journal
            .persist(
                GROUP,
                CdcOffset::whole_index(9),
                CdcOffset::whole_index(10),
                &[event(10)],
            )
            .expect("persist past a hole");
        let feed = &journal.load().expect("load")[0];
        assert_eq!(feed.after, CdcOffset::whole_index(9));
        assert_eq!(feed.changes.len(), 1);
    }
}
