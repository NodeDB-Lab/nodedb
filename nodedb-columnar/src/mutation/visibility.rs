// SPDX-License-Identifier: Apache-2.0

//! Read-side version selection: which stored rows a read at a
//! [`VersionCut`] sees, by row, by block, and by primary key.

use nodedb_types::surrogate::Surrogate;
use nodedb_types::value::Value;

use crate::delete_bitmap::DeleteBitmap;
use crate::error::ColumnarError;
use crate::format::BLOCK_SIZE;
use crate::pk_index::RowLocation;
use crate::reader::BlockSelection;
use crate::versions::{VersionCut, version_visible};

use super::engine::MutationEngine;

impl MutationEngine {
    /// Whether a read at `cut` sees row `row` of segment `segment_id`.
    ///
    /// - `Current`: the row is not in the delete bitmap. One bitmap probe.
    /// - `At(s)`: `sys_from <= s`, and the row is open or its `sys_to` is
    ///   above `s`. A purged row is never visible.
    /// - `All`: the row is open, or ended and kept as history.
    pub fn row_visible(&self, segment_id: u64, row: u32, cut: VersionCut) -> bool {
        let ended = self
            .delete_bitmaps
            .get(&segment_id)
            .is_some_and(|bm| bm.is_deleted(row));
        match cut {
            VersionCut::Current => !ended,
            VersionCut::All => !ended || self.ended_sys_to(segment_id, row).is_some(),
            VersionCut::At(at) => {
                let sys_to = if ended {
                    match self.ended_sys_to(segment_id, row) {
                        Some(to) => Some(to),
                        None => return false,
                    }
                } else {
                    None
                };
                let sys_from = self
                    .versions
                    .get(&segment_id)
                    .and_then(|v| v.sys_from(row))
                    .unwrap_or(i64::MIN);
                version_visible(sys_from, sys_to, at)
            }
        }
    }

    fn ended_sys_to(&self, segment_id: u64, row: u32) -> Option<i64> {
        self.versions.get(&segment_id)?.sys_to(row)
    }

    /// Whether a purge removed the row: it is ended and keeps no `sys_to`.
    pub fn is_purged(&self, segment_id: u64, row: u32) -> bool {
        !self.row_visible(segment_id, row, VersionCut::All)
    }

    /// The rows of segment `segment_id` a purge removed: no read at any cut
    /// sees them. A backup that carries history keeps every other row.
    pub fn purged_rows(&self, segment_id: u64) -> DeleteBitmap {
        let mut purged = DeleteBitmap::new();
        if let Some(bm) = self.delete_bitmaps.get(&segment_id) {
            for row in bm.iter() {
                if self.ended_sys_to(segment_id, row).is_none() {
                    purged.mark_deleted(row);
                }
            }
        }
        purged
    }

    /// Whether a read at `cut` sees no row of segment `segment_id`, judged
    /// by stamps alone: every row was created after the cut.
    pub fn segment_skippable(&self, segment_id: u64, cut: VersionCut) -> bool {
        let Some(at) = cut.ordinal() else {
            return false;
        };
        self.versions
            .get(&segment_id)
            .and_then(|v| v.min_sys_from())
            .is_some_and(|min| min > at)
    }

    /// Whether a read at `cut` sees no row of block `block` (rows
    /// `block * BLOCK_SIZE ..`) of segment `segment_id`, judged by stamps
    /// alone: every row of the block was created after the cut.
    pub fn block_skippable(&self, segment_id: u64, block: usize, cut: VersionCut) -> bool {
        let Some(at) = cut.ordinal() else {
            return false;
        };
        self.versions
            .get(&segment_id)
            .and_then(|v| v.block_min_sys_from(block))
            .is_some_and(|min| min > at)
    }

    /// The blocks of segment `segment_id` (`row_count` rows) that hold a row
    /// a read at `cut` sees, judged by stamps and the delete bitmap alone.
    /// A block outside the selection holds no row the read sees, so a scan
    /// never decodes it. A block created after an `At` cut is ruled out by
    /// its stamp minimum without a row probe.
    pub fn visible_blocks(
        &self,
        segment_id: u64,
        row_count: usize,
        cut: VersionCut,
    ) -> BlockSelection {
        let blocks = BlockSelection::block_count_for_rows(row_count);
        BlockSelection::from_fn(blocks, |block| {
            if self.block_skippable(segment_id, block, cut) {
                return false;
            }
            let start = block * BLOCK_SIZE;
            let end = start.saturating_add(BLOCK_SIZE).min(row_count);
            (start..end).any(|row| {
                u32::try_from(row).is_ok_and(|row| self.row_visible(segment_id, row, cut))
            })
        })
    }

    /// The location of the version of `pk` a read at `cut` sees.
    ///
    /// `Current` and `All` answer the PK index binding. `At(s)` checks the
    /// bound row, then walks the key's chain of ended versions newest
    /// first. The walk reads stamps only, never a segment.
    pub fn version_at(&self, pk: &[u8], cut: VersionCut) -> Option<RowLocation> {
        let bound = self.pk_index.get(pk).copied();
        let VersionCut::At(_) = cut else {
            return bound;
        };
        bound
            .into_iter()
            .chain(self.chains.get(pk).iter().rev().copied())
            .find(|loc| self.row_visible(loc.segment_id, loc.row_index, cut))
    }

    /// Iterate the memtable rows a read at `cut` sees, in row order.
    ///
    /// Yields `(row_index, surrogate, values)`. The surrogate is `None` for
    /// rows inserted without one. A row the cut does not see is never
    /// decoded. At an `At` cut a whole block created after the cut is
    /// skipped by its stamp minimum. Yields `Err` for a visible row with a
    /// corrupt cell.
    pub fn scan_memtable_versions(
        &self,
        cut: VersionCut,
    ) -> impl Iterator<Item = Result<(u32, Option<Surrogate>, Vec<Value>), ColumnarError>> + '_
    {
        let segment_id = self.memtable_segment_id;
        let row_count = self.memtable.row_count();
        (0..row_count)
            .filter(move |&idx| !self.block_skippable(segment_id, idx / BLOCK_SIZE, cut))
            .filter_map(move |idx| {
                let row = u32::try_from(idx).ok()?;
                if !self.row_visible(segment_id, row, cut) {
                    return None;
                }
                let surrogate = self.memtable_surrogates.get(idx).copied().flatten();
                match self.memtable.get_row(idx) {
                    Ok(Some(values)) => Some(Ok((row, surrogate, values))),
                    Ok(None) => None,
                    Err(e) => Some(Err(self.memtable_read_fault(e))),
                }
            })
    }

    /// Memtable row `row_idx` when a read at `cut` sees it. `Ok(None)` when
    /// the cut does not see it or it is past the end. `Err` when a cell of
    /// the row is corrupt.
    pub fn get_memtable_version(
        &self,
        row_idx: usize,
        cut: VersionCut,
    ) -> Result<Option<Vec<Value>>, ColumnarError> {
        let Ok(row) = u32::try_from(row_idx) else {
            return Ok(None);
        };
        if !self.row_visible(self.memtable_segment_id, row, cut) {
            return Ok(None);
        }
        self.memtable
            .get_row(row_idx)
            .map_err(|e| self.memtable_read_fault(e))
    }
}

#[cfg(test)]
mod tests {
    use nodedb_types::columnar::{ColumnDef, ColumnType, ColumnarSchema};
    use nodedb_types::value::Value;

    use super::*;

    /// 2100 memtable rows: block 0 stamped 10, blocks 1 and 2 stamped 20,
    /// and every row of block 1 deleted at 30.
    fn engine() -> MutationEngine {
        let schema = ColumnarSchema::new(vec![
            ColumnDef::required("id", ColumnType::Int64).with_primary_key(),
            ColumnDef::required("v", ColumnType::Int64),
        ])
        .expect("valid");
        let mut engine = MutationEngine::with_flush_threshold("t".into(), schema, usize::MAX);
        for i in 0..2100i64 {
            let stamp = if i < 1024 { 10 } else { 20 };
            engine
                .insert(&[Value::Integer(i), Value::Integer(i)], stamp)
                .expect("insert");
        }
        for i in 1024..2048i64 {
            engine.delete(&Value::Integer(i), 30).expect("delete");
        }
        engine
    }

    #[test]
    fn visible_blocks_follow_the_cut() {
        let engine = engine();
        let seg = engine.memtable_segment_id();
        let rows = 2100;
        let current = engine.visible_blocks(seg, rows, VersionCut::Current);
        assert!(current.is_selected(0) && !current.is_selected(1) && current.is_selected(2));

        let early = engine.visible_blocks(seg, rows, VersionCut::At(15));
        assert_eq!(early.selected_count(), 1, "blocks 1 and 2 start at 20");
        assert!(early.is_selected(0));

        let before_delete = engine.visible_blocks(seg, rows, VersionCut::At(25));
        assert_eq!(before_delete.selected_count(), 3);

        let all = engine.visible_blocks(seg, rows, VersionCut::All);
        assert_eq!(all.selected_count(), 3, "ended rows stay as history");
    }
}
