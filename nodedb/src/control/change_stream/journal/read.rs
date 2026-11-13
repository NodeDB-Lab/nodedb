// SPDX-License-Identifier: BUSL-1.1

//! Reads of the change-feed journal: boot's rebuild of the replay ring, and
//! the catch-up of a cursor the ring no longer serves.

use std::ops::Bound;

use redb::{ReadableDatabase, ReadableTable};

use crate::control::change_stream::stream::ring::ReplayScope;
use crate::control::change_stream::{ChangePartition, PositionedChange};
use crate::event::cdc::CdcOffset;
use crate::types::{DatabaseId, TenantId};

use super::codec::{
    FeedRecord, StoredChange, decode_partition, partition_key, row_bounds, row_key, row_position,
};
use super::store::{ChangeJournal, FEEDS, ROWS, storage};

/// The newest rows of one partition, as boot rebuilds the ring from them.
#[derive(Debug, Clone)]
pub(crate) struct JournalFeed {
    pub partition: ChangePartition,
    pub after: CdcOffset,
    pub through: CdcOffset,
    /// The newest rows at or below `through`, in position order.
    pub changes: Vec<PositionedChange>,
    /// The newest row older than `changes`. The ring treats every row up to
    /// it as evicted, and a cursor below it catches up from the journal.
    pub skipped: Option<CdcOffset>,
}

/// What a catch-up read of one partition found.
#[derive(Debug, Clone)]
pub(crate) struct JournalRead {
    /// The journal holds every counted event of the partition above this
    /// position. `None` when it never journaled the partition.
    pub after: Option<CdcOffset>,
    /// The rows in scope, in position order.
    pub changes: Vec<PositionedChange>,
    /// The read saw every row up to this position.
    pub scanned_through: CdcOffset,
}

impl ChangeJournal {
    /// Every journaled partition with its newest `tail` rows.
    ///
    /// Drops every row above its partition's `through` first: a staged page
    /// of an entry that did not settle before the restart. The restart
    /// stages that entry again, at the same positions.
    pub(crate) fn load(&self, tail: usize) -> crate::Result<Vec<JournalFeed>> {
        let feeds = self.discard_unsettled()?;
        let txn = self.db.begin_read().map_err(storage)?;
        let rows = txn.open_table(ROWS).map_err(storage)?;
        let mut out = Vec::with_capacity(feeds.len());
        for (partition, feed) in feeds {
            let (first, _) = row_bounds(partition);
            let through = row_key(partition, feed.through);
            let mut changes = Vec::new();
            let mut skipped = None;
            for row in rows
                .range(first.as_slice()..=through.as_slice())
                .map_err(storage)?
                .rev()
            {
                let (key, value) = row.map_err(storage)?;
                let position =
                    row_position(key.value()).ok_or_else(|| storage("a row key did not decode"))?;
                if changes.len() == tail {
                    skipped = Some(position);
                    break;
                }
                let stored: StoredChange = zerompk::from_msgpack(value.value()).map_err(storage)?;
                changes.push(stored.at(position));
            }
            changes.reverse();
            out.push(JournalFeed {
                partition,
                after: feed.after,
                through: feed.through,
                changes,
                skipped,
            });
        }
        Ok(out)
    }

    /// Drop the rows above each partition's `through`, and return every
    /// partition's record.
    fn discard_unsettled(&self) -> crate::Result<Vec<(ChangePartition, FeedRecord)>> {
        let txn = self.db.begin_write().map_err(storage)?;
        let mut feeds = Vec::new();
        {
            let table = txn.open_table(FEEDS).map_err(storage)?;
            for entry in table.iter().map_err(storage)? {
                let (key, value) = entry.map_err(storage)?;
                let partition = decode_partition(key.value())
                    .ok_or_else(|| storage("a feed key did not decode"))?;
                let feed = FeedRecord::decode(value.value())
                    .ok_or_else(|| storage("a feed record did not decode"))?;
                feeds.push((partition, feed));
            }
        }
        {
            let mut rows = txn.open_table(ROWS).map_err(storage)?;
            for (partition, feed) in &feeds {
                let (_, last) = row_bounds(*partition);
                let through = row_key(*partition, feed.through);
                rows.retain_in(
                    (
                        Bound::Excluded(through.as_slice()),
                        Bound::Included(last.as_slice()),
                    ),
                    |_, _| false,
                )
                .map_err(storage)?;
            }
        }
        txn.commit().map_err(storage)?;
        Ok(feeds)
    }

    /// The rows of `partition` in `(from, upto]` that `scope` admits, at
    /// most `limit`. Reads staged rows above the stored `through` too: the
    /// caller bounds `upto` by what settled.
    pub(crate) fn read(
        &self,
        partition: ChangePartition,
        from: CdcOffset,
        upto: CdcOffset,
        scope: Option<ReplayScope<'_>>,
        limit: usize,
    ) -> crate::Result<JournalRead> {
        let txn = self.db.begin_read().map_err(storage)?;
        let feeds = txn.open_table(FEEDS).map_err(storage)?;
        let rows = txn.open_table(ROWS).map_err(storage)?;
        let after = feeds
            .get(partition_key(partition).as_slice())
            .map_err(storage)?
            .and_then(|bytes| FeedRecord::decode(bytes.value()))
            .map(|feed| feed.after);
        let low = row_key(partition, from);
        let high = row_key(partition, upto);
        let mut changes = Vec::new();
        let mut scanned_through = upto;
        for row in rows
            .range((
                Bound::Excluded(low.as_slice()),
                Bound::Included(high.as_slice()),
            ))
            .map_err(storage)?
        {
            let (key, value) = row.map_err(storage)?;
            let position =
                row_position(key.value()).ok_or_else(|| storage("a row key did not decode"))?;
            let stored: StoredChange = zerompk::from_msgpack(value.value()).map_err(storage)?;
            let admitted = scope.is_none_or(|scope| {
                scope.admits(
                    DatabaseId::new(stored.database_id),
                    TenantId::new(stored.tenant_id),
                    &stored.collection,
                )
            });
            if !admitted {
                continue;
            }
            changes.push(stored.at(position));
            if changes.len() == limit {
                scanned_through = position;
                break;
            }
        }
        Ok(JournalRead {
            after,
            changes,
            scanned_through,
        })
    }
}
