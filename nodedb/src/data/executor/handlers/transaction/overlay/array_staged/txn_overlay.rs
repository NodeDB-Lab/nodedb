// SPDX-License-Identifier: BUSL-1.1

//! Per-transaction staging overlay for ARRAY writes.
//!
//! An array cell's identity is its full coordinate tuple, not a surrogate,
//! so the surrogate-keyed `super::TxnOverlay` does not fit. This is a
//! parallel, independent overlay type held alongside it and the GRAPH
//! overlay on `CoreLoop` (`array_txn_overlays`), keyed by `ArrayId`.
//!
//! Scope: this overlay serves read-your-own-writes for Slice / Project /
//! Aggregate / Elementwise reads and the statement-time affected count of
//! `ArrayOp::Put` / `ArrayOp::Delete`. COMMIT serializes the buffered
//! `ArrayOp` plan into the transaction's redo record, which the redo install
//! applies. The overlay is dropped at commit or rollback, same lifecycle as
//! `super::TxnOverlay`.
//!
//! Cells live in tiered maps: a hot tier in RAM and sorted runs in the
//! transaction's spill directory. Past `[tuning.query]
//! max_txn_overlay_bytes` of hot entries, the overlay spills.
//!
//! This file owns the type itself plus the savepoint undo journal
//! (`record_cell_undo` / `rollback_to`). Cell staging lives in the sibling
//! `cells` module; hot-tier accounting lives in `memory`.

use std::collections::HashMap;

use nodedb_array::types::ArrayId;
use nodedb_array::types::coord::value::CoordValue;

use super::super::lease::{LeaseStamp, SystemFromLatch};
use super::types::{ArrayCollectionOverlay, CellKey, CellSlot};
use crate::data::executor::handlers::spill::tiered::{SpillSpace, SpillWeight, UndoJournal};

/// One cell slot's hot state captured immediately before a staged mutation
/// overwrote it, with the hot version's seq. A put and a tombstone of one
/// coordinate replace each other, so restoring the slot restores both.
#[derive(Debug, Clone, zerompk::ToMessagePack, zerompk::FromMessagePack)]
pub(super) struct ArrayOverlayUndo {
    array_id: ArrayId,
    coord: CellKey,
    prev: Option<(CellSlot, u64)>,
}

impl SpillWeight for ArrayOverlayUndo {
    fn spill_weight(&self) -> usize {
        std::mem::size_of::<ArrayOverlayUndo>()
            + self.coord.spill_weight()
            + self
                .prev
                .as_ref()
                .map_or(0, |(slot, _)| slot.spill_weight())
    }
}

/// Per-transaction ARRAY staging overlay: holds not-yet-durable cell puts
/// and tombstones for every array touched by the transaction.
#[derive(Debug)]
pub struct ArrayTxnOverlay {
    pub(super) arrays: HashMap<ArrayId, ArrayCollectionOverlay>,
    /// The undo journal. The two mutators are the only writers of the cell
    /// maps and each appends here first, so no mutation escapes it.
    pub(super) journal: UndoJournal<ArrayOverlayUndo>,
    /// Ordinal-clock stamp of the last time this transaction touched its
    /// ARRAY overlay. Read by the lease reaper alongside the other overlays'
    /// stamps: a refresh on ANY overlay keeps the transaction alive.
    last_touch: LeaseStamp,
    /// Frozen system-time ordinal used by both live transaction apply and WAL
    /// redo. Separate from lease liveness so refreshes cannot change history.
    resolved_system_from: SystemFromLatch,
    /// The spill directory, the hot-tier threshold, and the lookup reader.
    pub(super) space: SpillSpace,
    /// Set when a spill or a rollback failed part way: every later call
    /// fails with this text.
    pub(super) poisoned: Option<String>,
}

#[cfg(test)]
impl Default for ArrayTxnOverlay {
    fn default() -> Self {
        Self::new()
    }
}

impl ArrayTxnOverlay {
    /// An overlay that spills into `space`.
    pub(in crate::data::executor) fn with_spill(space: SpillSpace) -> Self {
        Self {
            arrays: HashMap::new(),
            journal: UndoJournal::default(),
            last_touch: LeaseStamp::default(),
            resolved_system_from: SystemFromLatch::default(),
            space,
            poisoned: None,
        }
    }

    /// An overlay that never spills, for tests.
    #[cfg(test)]
    pub fn new() -> Self {
        Self::with_spill(SpillSpace::never_spills())
    }

    /// Refresh the array overlay's lease stamp to `ord`. See
    /// `super::super::staged::TxnOverlay::touch`.
    pub fn touch(&self, ord: i64) {
        self.last_touch.touch(ord);
    }

    /// The array overlay's last lease stamp (0 if never touched).
    pub fn last_touch(&self) -> i64 {
        self.last_touch.last_touch()
    }

    /// Freeze the array transaction's system-time ordinal on first resolve.
    /// Retries return the same value byte-for-byte.
    pub fn freeze_system_from(&self, candidate: i64) -> i64 {
        self.resolved_system_from.freeze(candidate)
    }

    pub fn resolved_system_from(&self) -> Option<i64> {
        self.resolved_system_from.resolved()
    }

    /// True once this transaction has staged anything on `array_id`.
    pub fn touches(&self, array_id: &ArrayId) -> bool {
        self.arrays.contains_key(array_id)
    }

    /// Fail when an earlier spill or rollback left the overlay unusable.
    pub(super) fn guard(&self) -> crate::Result<()> {
        match &self.poisoned {
            None => Ok(()),
            Some(reason) => Err(crate::Error::Internal {
                detail: format!(
                    "array staging overlay is unusable after a spill error: {reason}; \
                     roll the transaction back"
                ),
            }),
        }
    }

    /// Record a cell slot's hot state before a staged mutation overwrites
    /// it. Single chokepoint shared by `stage_cell_put` and
    /// `stage_cell_delete`. Returns the journal position the mutation's
    /// version carries.
    pub(super) fn record_cell_undo(
        &mut self,
        array_id: &ArrayId,
        coord: &[CoordValue],
    ) -> crate::Result<u64> {
        self.guard()?;
        let prev = self.arrays.get(array_id).and_then(|overlay| {
            overlay
                .cells
                .hot(coord)
                .map(|hot| (hot.value.clone(), hot.seq))
        });
        let coord = coord.to_vec();
        Ok(self.journal.push(ArrayOverlayUndo {
            array_id: array_id.clone(),
            coord,
            prev,
        }))
    }

    /// Current length of the array overlay undo journal: the savepoint
    /// marker a later `rollback_to` rewinds toward. `MetaOp::MarkSavepoint`
    /// records it on the core beside the other overlays' markers.
    pub fn journal_len(&self) -> usize {
        self.journal.next_seq() as usize
    }

    /// Revert every staged cell mutation recorded at or after `marker`.
    /// Every version written since is dropped from the hot tier and the
    /// runs, and the journal restores the hot versions the mutations
    /// overwrote, newest first. A `marker` at or beyond the current length
    /// is a no-op. A spill-file error part way leaves the overlay unusable.
    pub fn rollback_to(&mut self, marker: usize) -> crate::Result<()> {
        self.guard()?;
        if marker >= self.journal_len() {
            return Ok(());
        }
        let result = self.rollback_tiers(marker as u64);
        if let Err(e) = &result {
            self.poisoned = Some(e.to_string());
        }
        result
    }

    fn rollback_tiers(&mut self, bound: u64) -> crate::Result<()> {
        for overlay in self.arrays.values_mut() {
            overlay.rollback(bound, &mut self.space)?;
        }
        let arrays = &mut self.arrays;
        self.journal.rollback(bound, &self.space, |undo| {
            if let Some(overlay) = arrays.get_mut(&undo.array_id) {
                overlay.cells.restore(undo.coord, undo.prev);
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use super::super::types::StagedCellPut;
    use super::*;
    use nodedb_array::types::cell_value::value::CellValue;
    use nodedb_types::{Surrogate, TenantId};

    fn aid() -> ArrayId {
        ArrayId::new(TenantId::new(1), "a")
    }

    fn coord(x: i64) -> Vec<CoordValue> {
        vec![CoordValue::Int64(x)]
    }

    fn put(v: i64) -> StagedCellPut {
        StagedCellPut {
            attrs: vec![CellValue::Int64(v)],
            surrogate: Surrogate::new(1),
            system_from_ms: 0,
            valid_from_ms: 0,
            valid_until_ms: i64::MAX,
        }
    }

    #[test]
    fn rollback_to_removes_put_added_after_marker() {
        let mut overlay = ArrayTxnOverlay::new();
        overlay.stage_cell_put(aid(), coord(1), put(1)).unwrap();
        let marker = overlay.journal_len();
        overlay.stage_cell_put(aid(), coord(2), put(2)).unwrap();
        assert_eq!(overlay.staged_cells(&aid()).count(), 2);

        overlay.rollback_to(marker).unwrap();

        let out: Vec<_> = overlay
            .staged_cells(&aid())
            .collect::<crate::Result<_>>()
            .unwrap();
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].0, coord(1));
        assert_eq!(overlay.journal_len(), marker);
    }

    #[test]
    fn rollback_to_restores_prior_body_for_reoverwritten_cell() {
        let mut overlay = ArrayTxnOverlay::new();
        overlay.stage_cell_put(aid(), coord(1), put(1)).unwrap();
        let marker = overlay.journal_len();
        overlay.stage_cell_put(aid(), coord(1), put(9)).unwrap();

        overlay.rollback_to(marker).unwrap();

        let out: Vec<_> = overlay
            .staged_cells(&aid())
            .collect::<crate::Result<_>>()
            .unwrap();
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].1, put(1));
    }

    #[test]
    fn rollback_to_restores_tombstone_cleared_by_reput() {
        let mut overlay = ArrayTxnOverlay::new();
        overlay.stage_cell_delete(aid(), coord(1)).unwrap();
        let marker = overlay.journal_len();
        overlay.stage_cell_put(aid(), coord(1), put(7)).unwrap();
        assert!(!overlay.is_cell_tombstoned(&aid(), &coord(1)).unwrap());

        overlay.rollback_to(marker).unwrap();

        assert!(overlay.is_cell_tombstoned(&aid(), &coord(1)).unwrap());
        assert_eq!(overlay.staged_cells(&aid()).count(), 0);
    }

    #[test]
    fn rollback_to_restores_put_cleared_by_delete() {
        let mut overlay = ArrayTxnOverlay::new();
        overlay.stage_cell_put(aid(), coord(1), put(5)).unwrap();
        let marker = overlay.journal_len();
        overlay.stage_cell_delete(aid(), coord(1)).unwrap();

        overlay.rollback_to(marker).unwrap();

        assert!(!overlay.is_cell_tombstoned(&aid(), &coord(1)).unwrap());
        let out: Vec<_> = overlay
            .staged_cells(&aid())
            .collect::<crate::Result<_>>()
            .unwrap();
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].1, put(5));
    }

    #[test]
    fn rollback_to_current_len_is_noop() {
        let mut overlay = ArrayTxnOverlay::new();
        overlay.stage_cell_put(aid(), coord(1), put(1)).unwrap();
        let marker = overlay.journal_len();
        overlay.rollback_to(marker).unwrap();
        assert_eq!(overlay.staged_cells(&aid()).count(), 1);
    }

    #[test]
    fn freeze_system_from_is_sticky() {
        let overlay = ArrayTxnOverlay::new();
        assert_eq!(overlay.resolved_system_from(), None);
        assert_eq!(overlay.freeze_system_from(42), 42);
        assert_eq!(overlay.freeze_system_from(99), 42);
        assert_eq!(overlay.resolved_system_from(), Some(42));
    }
}
