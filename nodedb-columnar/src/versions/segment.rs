// SPDX-License-Identifier: Apache-2.0

//! The system-time stamps of the rows of one segment, or of the memtable.

use std::collections::HashMap;

use serde::{Deserialize, Serialize};
use zerompk::{FromMessagePack, ToMessagePack};

use crate::format::BLOCK_SIZE;

/// Per-row `sys_from`, the `sys_to` of every ended row that stays as
/// history, and the lowest `sys_from` of each 1024-row block.
///
/// A row the delete bitmap marks and that has a `sys_to` here is history. A
/// row the delete bitmap marks without one is purged.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SegmentVersions {
    sys_from: Vec<i64>,
    sys_to: HashMap<u32, i64>,
    block_min: Vec<i64>,
}

/// The serialisable form of [`SegmentVersions`] for one segment.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToMessagePack, FromMessagePack)]
pub struct SegmentVersionsSnapshot {
    /// The segment id the stamps belong to. The memtable's virtual segment
    /// id names the memtable.
    pub segment_id: u64,
    /// `sys_from` of every row, in row order.
    pub sys_from: Vec<i64>,
    /// `(row, sys_to)` of every ended row that stays as history, ascending
    /// by row.
    pub sys_to: Vec<(u32, i64)>,
}

impl SegmentVersions {
    /// Stamps with no row.
    pub fn new() -> Self {
        Self::default()
    }

    /// Rebuild stamps from their parts. Ended entries past the last row are
    /// dropped.
    pub fn from_parts(sys_from: Vec<i64>, sys_to: impl IntoIterator<Item = (u32, i64)>) -> Self {
        let rows = sys_from.len();
        let sys_to = sys_to
            .into_iter()
            .filter(|(row, _)| (*row as usize) < rows)
            .collect();
        let block_min = sys_from
            .chunks(BLOCK_SIZE)
            .map(|block| block.iter().copied().min().unwrap_or(i64::MAX))
            .collect();
        Self {
            sys_from,
            sys_to,
            block_min,
        }
    }

    /// The serialisable form, under `segment_id`.
    pub fn to_snapshot(&self, segment_id: u64) -> SegmentVersionsSnapshot {
        let mut sys_to: Vec<(u32, i64)> = self.sys_to.iter().map(|(r, t)| (*r, *t)).collect();
        sys_to.sort_unstable();
        SegmentVersionsSnapshot {
            segment_id,
            sys_from: self.sys_from.clone(),
            sys_to,
        }
    }

    /// Rebuild from the serialisable form.
    pub fn from_snapshot(snap: SegmentVersionsSnapshot) -> Self {
        Self::from_parts(snap.sys_from, snap.sys_to)
    }

    /// Number of stamped rows.
    pub fn len(&self) -> usize {
        self.sys_from.len()
    }

    /// Whether no row is stamped.
    pub fn is_empty(&self) -> bool {
        self.sys_from.is_empty()
    }

    /// Stamp the next row with `sys_from`.
    pub fn push(&mut self, sys_from: i64) {
        let block = self.sys_from.len() / BLOCK_SIZE;
        self.sys_from.push(sys_from);
        match self.block_min.get_mut(block) {
            Some(min) => *min = (*min).min(sys_from),
            None => self.block_min.push(sys_from),
        }
    }

    /// `sys_from` of `row`. `None` past the last row.
    pub fn sys_from(&self, row: u32) -> Option<i64> {
        self.sys_from.get(row as usize).copied()
    }

    /// `sys_to` of `row`. `None` while the row is open or after a purge.
    pub fn sys_to(&self, row: u32) -> Option<i64> {
        self.sys_to.get(&row).copied()
    }

    /// End `row` at `sys_to`.
    pub fn end(&mut self, row: u32, sys_to: i64) {
        self.sys_to.insert(row, sys_to);
    }

    /// Drop the end of `row`. Returns the `sys_to` it had.
    pub fn reopen(&mut self, row: u32) -> Option<i64> {
        self.sys_to.remove(&row)
    }

    /// Cut the stamps back to `len` rows.
    pub fn truncate(&mut self, len: usize) {
        if len >= self.sys_from.len() {
            return;
        }
        self.sys_from.truncate(len);
        self.sys_to.retain(|row, _| (*row as usize) < len);
        let blocks = len.div_ceil(BLOCK_SIZE);
        self.block_min.truncate(blocks);
        if let Some(last) = blocks.checked_sub(1)
            && let Some(slot) = self.block_min.get_mut(last)
        {
            *slot = self
                .sys_from
                .get(last * BLOCK_SIZE..)
                .and_then(|block| block.iter().copied().min())
                .unwrap_or(i64::MAX);
        }
    }

    /// The lowest `sys_from` of the rows. `None` with no row.
    pub fn min_sys_from(&self) -> Option<i64> {
        self.block_min.iter().copied().min()
    }

    /// The lowest `sys_from` of block `block` (rows
    /// `block * BLOCK_SIZE ..`). `None` past the last block.
    pub fn block_min_sys_from(&self, block: usize) -> Option<i64> {
        self.block_min.get(block).copied()
    }

    /// The highest ordinal a stamp holds: a `sys_from` or a `sys_to`.
    pub fn newest_ordinal(&self) -> Option<i64> {
        let from = self.sys_from.iter().copied().max();
        let to = self.sys_to.values().copied().max();
        from.max(to)
    }

    /// Every ended row that stays as history, with its `sys_to`.
    pub fn ended(&self) -> impl Iterator<Item = (u32, i64)> + '_ {
        self.sys_to.iter().map(|(row, to)| (*row, *to))
    }

    /// Drop the end of every row whose `sys_to` is at or below `horizon`.
    /// Returns those rows, ascending.
    pub fn purge_below(&mut self, horizon: i64) -> Vec<u32> {
        let mut purged: Vec<u32> = self
            .sys_to
            .iter()
            .filter(|(_, to)| **to <= horizon)
            .map(|(row, _)| *row)
            .collect();
        purged.sort_unstable();
        for row in &purged {
            self.sys_to.remove(row);
        }
        purged
    }

    /// Bytes the stamps hold, for memory accounting.
    pub fn memory_bytes(&self) -> usize {
        self.sys_from.capacity() * std::mem::size_of::<i64>()
            + self.block_min.capacity() * std::mem::size_of::<i64>()
            + self.sys_to.capacity() * (std::mem::size_of::<u32>() + std::mem::size_of::<i64>())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn block_minimum_follows_pushes_and_cuts() {
        let mut v = SegmentVersions::new();
        for i in 0..(BLOCK_SIZE as i64 + 2) {
            v.push(1_000 - i);
        }
        assert_eq!(
            v.block_min_sys_from(0),
            Some(1_000 - (BLOCK_SIZE as i64 - 1))
        );
        assert_eq!(
            v.block_min_sys_from(1),
            Some(1_000 - (BLOCK_SIZE as i64 + 1))
        );
        v.truncate(BLOCK_SIZE + 1);
        assert_eq!(v.block_min_sys_from(1), Some(1_000 - BLOCK_SIZE as i64));
        v.truncate(3);
        assert_eq!(v.block_min_sys_from(0), Some(998));
        assert_eq!(v.block_min_sys_from(1), None);
    }

    #[test]
    fn purge_drops_ends_at_or_below_the_horizon() {
        let mut v = SegmentVersions::from_parts(vec![1, 2, 3], [(0, 2), (1, 5)]);
        assert_eq!(v.purge_below(2), vec![0]);
        assert_eq!(v.sys_to(0), None);
        assert_eq!(v.sys_to(1), Some(5));
    }

    #[test]
    fn snapshot_round_trips() {
        let v = SegmentVersions::from_parts(vec![7, 9], [(0, 9)]);
        let back = SegmentVersions::from_snapshot(v.to_snapshot(3));
        assert_eq!(back, v);
    }
}
