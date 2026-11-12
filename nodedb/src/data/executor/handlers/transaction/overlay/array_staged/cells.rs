// SPDX-License-Identifier: BUSL-1.1

//! Cell staging and read-your-own-writes accessors for [`ArrayTxnOverlay`].
//! Every read merges the hot tier with the spilled runs.

use std::borrow::Cow;

use nodedb_array::types::ArrayId;
use nodedb_array::types::coord::value::CoordValue;

use super::txn_overlay::ArrayTxnOverlay;
use super::types::{CellSlot, StagedCellPut};

impl ArrayTxnOverlay {
    /// Stage a cell put. It replaces any pending tombstone for the same
    /// coordinate (last-writer-wins within the transaction).
    pub fn stage_cell_put(
        &mut self,
        array_id: ArrayId,
        coord: Vec<CoordValue>,
        put: StagedCellPut,
    ) -> crate::Result<()> {
        self.stage_cell(array_id, coord, CellSlot::Put(put))
    }

    /// Stage a cell delete. It replaces any pending put for the same
    /// coordinate.
    pub fn stage_cell_delete(
        &mut self,
        array_id: ArrayId,
        coord: Vec<CoordValue>,
    ) -> crate::Result<()> {
        self.stage_cell(array_id, coord, CellSlot::Tombstone)
    }

    fn stage_cell(
        &mut self,
        array_id: ArrayId,
        coord: Vec<CoordValue>,
        slot: CellSlot,
    ) -> crate::Result<()> {
        let seq = self.record_cell_undo(&array_id, &coord)?;
        self.arrays
            .entry(array_id)
            .or_default()
            .cells
            .insert(coord, slot, seq);
        self.spill_past_threshold()
    }

    fn cell(
        &self,
        array_id: &ArrayId,
        coord: &[CoordValue],
    ) -> crate::Result<Option<Cow<'_, CellSlot>>> {
        self.guard()?;
        let Some(overlay) = self.arrays.get(array_id) else {
            return Ok(None);
        };
        overlay.cells.get(coord, self.space.reader())
    }

    /// Whether `coord` is currently staged in this transaction's own overlay:
    /// `Some(true)` when staged as a put, `Some(false)` when staged as a
    /// tombstone, `None` when this transaction has not touched the cell. The
    /// caller must then resolve it against BASE state.
    pub fn staged_cell_presence(
        &self,
        array_id: &ArrayId,
        coord: &[CoordValue],
    ) -> crate::Result<Option<bool>> {
        Ok(self
            .cell(array_id, coord)?
            .map(|slot| matches!(*slot, CellSlot::Put(_))))
    }

    /// True if `coord` has been staged-deleted in this transaction.
    pub fn is_cell_tombstoned(
        &self,
        array_id: &ArrayId,
        coord: &[CoordValue],
    ) -> crate::Result<bool> {
        Ok(matches!(
            self.cell(array_id, coord)?.as_deref(),
            Some(CellSlot::Tombstone)
        ))
    }

    /// Every staged cell slot of `array_id`, in key order.
    fn scan_cells<'a>(
        &'a self,
        array_id: &ArrayId,
    ) -> impl Iterator<Item = crate::Result<(Vec<CoordValue>, Cow<'a, CellSlot>)>> + 'a {
        let poisoned = self.guard().err();
        let scan = match poisoned {
            Some(_) => None,
            None => self
                .arrays
                .get(array_id)
                .map(|overlay| overlay.cells.iter()),
        };
        poisoned
            .map(Err)
            .into_iter()
            .chain(scan.into_iter().flatten())
    }

    /// Every staged cell put on `array_id` whose coordinate satisfies `pred`.
    /// Staging never leaves a coordinate both put and tombstoned, so no
    /// tombstone filter is needed here.
    pub fn staged_cells_matching<'a, P>(
        &'a self,
        array_id: &ArrayId,
        pred: P,
    ) -> impl Iterator<Item = crate::Result<(Vec<CoordValue>, StagedCellPut)>> + 'a
    where
        P: Fn(&[CoordValue]) -> bool + 'a,
    {
        self.scan_cells(array_id)
            .filter_map(move |item| match item {
                Err(e) => Some(Err(e)),
                Ok((coord, slot)) => match slot {
                    Cow::Borrowed(CellSlot::Put(put)) if pred(&coord) => {
                        Some(Ok((coord, put.clone())))
                    }
                    Cow::Owned(CellSlot::Put(put)) if pred(&coord) => Some(Ok((coord, put))),
                    Cow::Borrowed(_) | Cow::Owned(_) => None,
                },
            })
    }

    /// Every staged cell put on `array_id`.
    pub fn staged_cells<'a>(
        &'a self,
        array_id: &ArrayId,
    ) -> impl Iterator<Item = crate::Result<(Vec<CoordValue>, StagedCellPut)>> + 'a {
        self.staged_cells_matching(array_id, |_| true)
    }

    /// Every staged tombstone coordinate on `array_id`.
    pub fn tombstoned_coords<'a>(
        &'a self,
        array_id: &ArrayId,
    ) -> impl Iterator<Item = crate::Result<Vec<CoordValue>>> + 'a {
        self.scan_cells(array_id).filter_map(|item| match item {
            Err(e) => Some(Err(e)),
            Ok((coord, slot)) => match &*slot {
                CellSlot::Tombstone => Some(Ok(coord)),
                CellSlot::Put(_) => None,
            },
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use nodedb_array::types::cell_value::value::CellValue;
    use nodedb_types::{Surrogate, TenantId};

    fn aid(name: &str) -> ArrayId {
        ArrayId::new(TenantId::new(1), name)
    }

    fn coord(x: i64, y: i64) -> Vec<CoordValue> {
        vec![CoordValue::Int64(x), CoordValue::Int64(y)]
    }

    fn put(v: f64) -> StagedCellPut {
        StagedCellPut {
            attrs: vec![CellValue::Float64(v)],
            surrogate: Surrogate::new(1),
            system_from_ms: 0,
            valid_from_ms: 0,
            valid_until_ms: i64::MAX,
        }
    }

    #[test]
    fn stage_cell_put_then_visible() {
        let mut overlay = ArrayTxnOverlay::new();
        overlay
            .stage_cell_put(aid("a"), coord(1, 2), put(3.5))
            .unwrap();
        let out: Vec<_> = overlay
            .staged_cells(&aid("a"))
            .collect::<crate::Result<_>>()
            .unwrap();
        assert_eq!(out, vec![(coord(1, 2), put(3.5))]);
        assert_eq!(
            overlay
                .staged_cell_presence(&aid("a"), &coord(1, 2))
                .unwrap(),
            Some(true)
        );
        assert!(!overlay.is_cell_tombstoned(&aid("a"), &coord(1, 2)).unwrap());
        assert!(overlay.touches(&aid("a")));
        assert!(!overlay.touches(&aid("b")));
    }

    #[test]
    fn stage_cell_delete_tombstones_and_clears_put() {
        let mut overlay = ArrayTxnOverlay::new();
        overlay
            .stage_cell_put(aid("a"), coord(1, 2), put(1.0))
            .unwrap();
        overlay.stage_cell_delete(aid("a"), coord(1, 2)).unwrap();
        assert!(overlay.is_cell_tombstoned(&aid("a"), &coord(1, 2)).unwrap());
        assert_eq!(
            overlay
                .staged_cell_presence(&aid("a"), &coord(1, 2))
                .unwrap(),
            Some(false)
        );
        assert_eq!(overlay.staged_cells(&aid("a")).count(), 0);
        let tombs: Vec<_> = overlay
            .tombstoned_coords(&aid("a"))
            .collect::<crate::Result<_>>()
            .unwrap();
        assert_eq!(tombs, vec![coord(1, 2)]);
    }

    #[test]
    fn stage_put_after_delete_clears_tombstone() {
        let mut overlay = ArrayTxnOverlay::new();
        overlay.stage_cell_delete(aid("a"), coord(1, 2)).unwrap();
        overlay
            .stage_cell_put(aid("a"), coord(1, 2), put(9.0))
            .unwrap();
        assert!(!overlay.is_cell_tombstoned(&aid("a"), &coord(1, 2)).unwrap());
        assert_eq!(overlay.staged_cells(&aid("a")).count(), 1);
    }

    #[test]
    fn untouched_cell_reports_none() {
        let mut overlay = ArrayTxnOverlay::new();
        overlay
            .stage_cell_put(aid("a"), coord(1, 2), put(1.0))
            .unwrap();
        assert_eq!(
            overlay
                .staged_cell_presence(&aid("a"), &coord(2, 2))
                .unwrap(),
            None
        );
        assert_eq!(
            overlay
                .staged_cell_presence(&aid("b"), &coord(1, 2))
                .unwrap(),
            None
        );
    }

    #[test]
    fn staged_cells_matching_applies_predicate() {
        let mut overlay = ArrayTxnOverlay::new();
        overlay
            .stage_cell_put(aid("a"), coord(1, 1), put(1.0))
            .unwrap();
        overlay
            .stage_cell_put(aid("a"), coord(5, 5), put(5.0))
            .unwrap();
        let out: Vec<_> = overlay
            .staged_cells_matching(&aid("a"), |c| c[0] == CoordValue::Int64(5))
            .collect::<crate::Result<_>>()
            .unwrap();
        assert_eq!(out, vec![(coord(5, 5), put(5.0))]);
    }

    #[test]
    fn arrays_are_isolated() {
        let mut overlay = ArrayTxnOverlay::new();
        overlay
            .stage_cell_put(aid("a"), coord(1, 1), put(1.0))
            .unwrap();
        assert_eq!(overlay.staged_cells(&aid("b")).count(), 0);
        assert_eq!(overlay.tombstoned_coords(&aid("b")).count(), 0);
    }
}
