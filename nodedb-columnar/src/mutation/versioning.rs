// SPDX-License-Identifier: Apache-2.0

//! Version stamping on the write path: the `sys_from` a new row takes, and
//! the end a superseded or deleted row records.

use nodedb_types::value::Value;

use crate::pk_index::RowLocation;
use crate::versions::{SegmentVersions, VersionChains};
use crate::wal_record::ColumnarWalRecord;

use super::engine::MutationEngine;

impl MutationEngine {
    /// `sys_from` of the row at `loc`. `None` for a row with no stamp.
    pub fn sys_from_at(&self, loc: RowLocation) -> Option<i64> {
        self.versions.get(&loc.segment_id)?.sys_from(loc.row_index)
    }

    /// `sys_to` of the row at `loc`. `None` while the row is open, and
    /// after a purge removed it.
    pub fn sys_to_at(&self, loc: RowLocation) -> Option<i64> {
        self.versions.get(&loc.segment_id)?.sys_to(loc.row_index)
    }

    /// The stamps of segment `segment_id`. The memtable's virtual segment
    /// id names the memtable.
    pub fn segment_versions(&self, segment_id: u64) -> Option<&SegmentVersions> {
        self.versions.get(&segment_id)
    }

    /// The per-key chains of ended versions.
    pub fn version_chains(&self) -> &VersionChains {
        &self.chains
    }

    /// The highest ordinal any stamp of this engine holds. A snapshot
    /// install or a restore folds it into the clock, so a later write
    /// stamps above every stored version.
    pub fn newest_ordinal(&self) -> Option<i64> {
        self.versions
            .values()
            .filter_map(SegmentVersions::newest_ordinal)
            .max()
    }

    /// The newest ordinal any version of key `pk_bytes` holds, and whether
    /// the key binds a current row. The newest ordinal is the newest
    /// `sys_from` or `sys_to` of the bound row, the closed-key tombstone and
    /// every ended version of the key.
    pub fn key_newest_ordinal(&self, pk_bytes: &[u8]) -> (Option<i64>, bool) {
        let bound = self.pk_index.get(pk_bytes).copied();
        let closed = self.closed_index.get(pk_bytes).copied();
        let stamps = |loc: RowLocation| [self.sys_from_at(loc), self.sys_to_at(loc)];
        let newest = bound
            .into_iter()
            .chain(closed)
            .chain(self.chains.get(pk_bytes).iter().copied())
            .flat_map(stamps)
            .flatten()
            .max();
        (newest, bound.is_some())
    }

    /// Bytes the version stamps and chains hold.
    pub fn version_memory_bytes(&self) -> usize {
        let stamps: usize = self
            .versions
            .values()
            .map(SegmentVersions::memory_bytes)
            .sum();
        stamps + self.chains.location_count() * std::mem::size_of::<RowLocation>()
    }

    /// The `sys_from` a new row of `values` takes.
    ///
    /// A bitemporal row takes its `_ts_system` cell: the caller stamped it
    /// by the bitemporal rule, raised per key. Any other row takes `stamp`,
    /// raised to the floor of every key in `keys` ([`Self::key_floor`]), so
    /// each version of a key sorts after the versions before it.
    pub(super) fn version_stamp(&self, values: &[Value], stamp: i64, keys: &[&[u8]]) -> i64 {
        if let Some(idx) = self.schema.ts_system_idx()
            && let Some(Value::Integer(cell)) = values.get(idx)
        {
            return *cell;
        }
        keys.iter()
            .filter_map(|pk| self.key_floor(pk))
            .fold(stamp, i64::max)
    }

    /// The lowest stamp a new version of `pk` can take: one above the
    /// `sys_from` of its bound row, else the end of its newest ended
    /// version (and one above that version's `sys_from`). `None` for a key
    /// with neither.
    fn key_floor(&self, pk: &[u8]) -> Option<i64> {
        if let Some(loc) = self.pk_index.get(pk).copied() {
            return self.sys_from_at(loc).map(|from| from.saturating_add(1));
        }
        let loc = self.chains.last(pk)?;
        let above = self.sys_from_at(loc)?.saturating_add(1);
        Some(self.sys_to_at(loc).map_or(above, |to| to.max(above)))
    }

    /// The `sys_to` a delete stamped `stamp` ends the row at `loc` at:
    /// `stamp`, raised one above the row's `sys_from`.
    pub(super) fn end_stamp(&self, loc: RowLocation, stamp: i64) -> i64 {
        self.sys_from_at(loc)
            .map_or(stamp, |from| stamp.max(from.saturating_add(1)))
    }

    /// Stamp the memtable row being appended with `sys_from`.
    pub(super) fn stamp_memtable_row(&mut self, sys_from: i64) {
        self.versions
            .entry(self.memtable_segment_id)
            .or_default()
            .push(sys_from);
    }

    /// End the row of `pk` at `loc` at `sys_to`: mark it in the delete
    /// bitmap, record its `sys_to`, and append it to `pk`'s chain. Returns
    /// the WAL record of the end.
    pub(super) fn end_row(
        &mut self,
        pk: Vec<u8>,
        loc: RowLocation,
        sys_to: i64,
    ) -> ColumnarWalRecord {
        self.delete_bitmaps
            .entry(loc.segment_id)
            .or_default()
            .mark_deleted(loc.row_index);
        self.versions
            .entry(loc.segment_id)
            .or_default()
            .end(loc.row_index, sys_to);
        self.chains.push(pk, loc);
        ColumnarWalRecord::DeleteRows {
            collection: self.collection.clone(),
            segment_id: loc.segment_id,
            row_indices: vec![loc.row_index],
            sys_to,
        }
    }

    /// Undo [`Self::end_row`] for the row of `pk` at `loc`: clear its
    /// delete-bitmap bit, drop its `sys_to`, and remove it from the chain.
    pub(super) fn reopen_row(&mut self, pk: &[u8], loc: RowLocation) {
        if let Some(bm) = self.delete_bitmaps.get_mut(&loc.segment_id) {
            bm.unmark_deleted(loc.row_index);
        }
        if let Some(stamps) = self.versions.get_mut(&loc.segment_id) {
            stamps.reopen(loc.row_index);
        }
        self.chains.remove(pk, loc);
    }
}
