// SPDX-License-Identifier: BUSL-1.1

//! Cell staging and read-your-own-writes accessors for [`ArrayTxnOverlay`].

use nodedb_array::types::ArrayId;
use nodedb_array::types::coord::value::CoordValue;

use super::txn_overlay::ArrayTxnOverlay;
use super::types::StagedCellPut;

impl ArrayTxnOverlay {
    /// Stage a cell put: adds to the pending put-set and clears any pending
    /// tombstone for the same coordinate (last-writer-wins within the
    /// transaction).
    pub fn stage_cell_put(
        &mut self,
        array_id: ArrayId,
        coord: Vec<CoordValue>,
        put: StagedCellPut,
    ) {
        self.record_cell_undo(&array_id, &coord);
        let overlay = self.arrays.entry(array_id).or_default();
        overlay.pending_tombstones.remove(&coord);
        overlay.pending_cells.insert(coord, put);
    }

    /// Stage a cell delete: adds a tombstone and clears any pending put for
    /// the same coordinate.
    pub fn stage_cell_delete(&mut self, array_id: ArrayId, coord: Vec<CoordValue>) {
        self.record_cell_undo(&array_id, &coord);
        let overlay = self.arrays.entry(array_id).or_default();
        overlay.pending_cells.remove(&coord);
        overlay.pending_tombstones.insert(coord);
    }

    /// Whether `coord` is currently staged in this transaction's own overlay:
    /// `Some(true)` when staged as a put, `Some(false)` when staged as a
    /// tombstone, `None` when this transaction has not touched the cell. The
    /// caller must then resolve it against BASE state.
    pub fn staged_cell_presence(&self, array_id: &ArrayId, coord: &[CoordValue]) -> Option<bool> {
        let overlay = self.arrays.get(array_id)?;
        if overlay.pending_cells.contains_key(coord) {
            Some(true)
        } else if overlay.pending_tombstones.contains(coord) {
            Some(false)
        } else {
            None
        }
    }

    /// True if `coord` has been staged-deleted in this transaction.
    pub fn is_cell_tombstoned(&self, array_id: &ArrayId, coord: &[CoordValue]) -> bool {
        self.arrays
            .get(array_id)
            .is_some_and(|overlay| overlay.pending_tombstones.contains(coord))
    }

    /// Every staged cell put on `array_id` whose coordinate satisfies `pred`.
    /// Staging never leaves a coordinate in both sets, so no tombstone filter
    /// is needed here.
    pub fn staged_cells_matching<'a, P>(
        &'a self,
        array_id: &ArrayId,
        pred: P,
    ) -> impl Iterator<Item = (&'a [CoordValue], &'a StagedCellPut)>
    where
        P: Fn(&[CoordValue]) -> bool + 'a,
    {
        self.arrays
            .get(array_id)
            .map(|overlay| overlay.pending_cells.iter())
            .into_iter()
            .flatten()
            .filter(move |(coord, _)| pred(coord.as_slice()))
            .map(|(coord, put)| (coord.as_slice(), put))
    }

    /// Every staged cell put on `array_id`.
    pub fn staged_cells<'a>(
        &'a self,
        array_id: &ArrayId,
    ) -> impl Iterator<Item = (&'a [CoordValue], &'a StagedCellPut)> {
        self.staged_cells_matching(array_id, |_| true)
    }

    /// Every staged tombstone coordinate on `array_id`.
    pub fn tombstoned_coords<'a>(
        &'a self,
        array_id: &ArrayId,
    ) -> impl Iterator<Item = &'a [CoordValue]> {
        self.arrays
            .get(array_id)
            .into_iter()
            .flat_map(|overlay| overlay.pending_tombstones.iter().map(Vec::as_slice))
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
            surrogate: Surrogate::ZERO,
            system_from_ms: 0,
            valid_from_ms: 0,
            valid_until_ms: i64::MAX,
        }
    }

    #[test]
    fn stage_cell_put_then_visible() {
        let mut overlay = ArrayTxnOverlay::new();
        overlay.stage_cell_put(aid("a"), coord(1, 2), put(3.5));
        let out: Vec<_> = overlay.staged_cells(&aid("a")).collect();
        assert_eq!(out, vec![(coord(1, 2).as_slice(), &put(3.5))]);
        assert_eq!(
            overlay.staged_cell_presence(&aid("a"), &coord(1, 2)),
            Some(true)
        );
        assert!(!overlay.is_cell_tombstoned(&aid("a"), &coord(1, 2)));
        assert!(overlay.touches(&aid("a")));
        assert!(!overlay.touches(&aid("b")));
    }

    #[test]
    fn stage_cell_delete_tombstones_and_clears_put() {
        let mut overlay = ArrayTxnOverlay::new();
        overlay.stage_cell_put(aid("a"), coord(1, 2), put(1.0));
        overlay.stage_cell_delete(aid("a"), coord(1, 2));
        assert!(overlay.is_cell_tombstoned(&aid("a"), &coord(1, 2)));
        assert_eq!(
            overlay.staged_cell_presence(&aid("a"), &coord(1, 2)),
            Some(false)
        );
        assert_eq!(overlay.staged_cells(&aid("a")).count(), 0);
        let tombs: Vec<_> = overlay.tombstoned_coords(&aid("a")).collect();
        assert_eq!(tombs, vec![coord(1, 2).as_slice()]);
    }

    #[test]
    fn stage_put_after_delete_clears_tombstone() {
        let mut overlay = ArrayTxnOverlay::new();
        overlay.stage_cell_delete(aid("a"), coord(1, 2));
        overlay.stage_cell_put(aid("a"), coord(1, 2), put(9.0));
        assert!(!overlay.is_cell_tombstoned(&aid("a"), &coord(1, 2)));
        assert_eq!(overlay.staged_cells(&aid("a")).count(), 1);
    }

    #[test]
    fn untouched_cell_reports_none() {
        let mut overlay = ArrayTxnOverlay::new();
        overlay.stage_cell_put(aid("a"), coord(1, 2), put(1.0));
        assert_eq!(overlay.staged_cell_presence(&aid("a"), &coord(2, 2)), None);
        assert_eq!(overlay.staged_cell_presence(&aid("b"), &coord(1, 2)), None);
    }

    #[test]
    fn staged_cells_matching_applies_predicate() {
        let mut overlay = ArrayTxnOverlay::new();
        overlay.stage_cell_put(aid("a"), coord(1, 1), put(1.0));
        overlay.stage_cell_put(aid("a"), coord(5, 5), put(5.0));
        let out: Vec<_> = overlay
            .staged_cells_matching(&aid("a"), |c| c[0] == CoordValue::Int64(5))
            .collect();
        assert_eq!(out, vec![(coord(5, 5).as_slice(), &put(5.0))]);
    }

    #[test]
    fn arrays_are_isolated() {
        let mut overlay = ArrayTxnOverlay::new();
        overlay.stage_cell_put(aid("a"), coord(1, 1), put(1.0));
        assert_eq!(overlay.staged_cells(&aid("b")).count(), 0);
        assert_eq!(overlay.tombstoned_coords(&aid("b")).count(), 0);
    }
}
