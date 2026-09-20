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
//! `ArrayOp::Put` / `ArrayOp::Delete`. COMMIT durability is unchanged: the
//! buffered `ArrayOp` plan is replayed through the real `handle_array_put` /
//! `handle_array_delete` handlers inside the COMMIT `TransactionBatch`. This
//! overlay is in-memory only and is dropped at commit or rollback, same
//! lifecycle as `super::TxnOverlay`.
//!
//! This file owns the type itself plus the savepoint undo journal
//! (`record_cell_undo` / `rollback_to`). Cell staging lives in the sibling
//! `cells` module; memory accounting lives in `memory`.

use std::collections::HashMap;

use nodedb_array::types::ArrayId;
use nodedb_array::types::coord::value::CoordValue;

use super::super::lease::{LeaseStamp, SystemFromLatch};
use super::types::{ArrayCollectionOverlay, CellKey, StagedCellPut};

/// One cell slot's state captured immediately before a staged mutation
/// overwrote it. Every mutator is last-writer-wins with CROSS-SET CLEARING
/// (staging a put removes the coordinate from the tombstone set and
/// vice-versa), so `ROLLBACK TO SAVEPOINT` restores the recorded prior
/// membership of BOTH sets rather than dropping post-savepoint keys.
#[derive(Debug, Clone)]
struct ArrayOverlayUndo {
    array_id: ArrayId,
    coord: CellKey,
    /// Prior `pending_cells` entry, or `None` if the slot was absent.
    prev_put: Option<StagedCellPut>,
    /// Prior membership in `pending_tombstones`.
    prev_tombstoned: bool,
}

/// Per-transaction ARRAY staging overlay: holds not-yet-durable cell puts
/// and tombstones for every array touched by the transaction.
#[derive(Debug, Default)]
pub struct ArrayTxnOverlay {
    pub(super) arrays: HashMap<ArrayId, ArrayCollectionOverlay>,
    /// Append-only undo journal recording each cell slot's prior state before
    /// a staged mutation overwrote it. `journal_len` reads its length (the
    /// array savepoint marker); `rollback_to` replays it in reverse down to
    /// a marker. The two mutators are the ONLY writers of the private
    /// cell / tombstone sets and each appends here first, so no mutation
    /// escapes the journal. Dropped with the overlay when the transaction
    /// resolves.
    journal: Vec<ArrayOverlayUndo>,
    /// Ordinal-clock stamp of the last time this transaction touched its
    /// ARRAY overlay. Read by the lease reaper alongside the other overlays'
    /// stamps: a refresh on ANY overlay keeps the transaction alive.
    last_touch: LeaseStamp,
    /// Frozen system-time ordinal used by both live transaction apply and WAL
    /// redo. Separate from lease liveness so refreshes cannot change history.
    resolved_system_from: SystemFromLatch,
}

impl ArrayTxnOverlay {
    pub fn new() -> Self {
        Self::default()
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

    /// Record a cell slot's prior state across BOTH sets before a staged
    /// mutation overwrites it. Single chokepoint shared by `stage_cell_put`
    /// and `stage_cell_delete`, so no cell-set mutation escapes the journal.
    pub(super) fn record_cell_undo(&mut self, array_id: &ArrayId, coord: &[CoordValue]) {
        let (prev_put, prev_tombstoned) = match self.arrays.get(array_id) {
            Some(overlay) => (
                overlay.pending_cells.get(coord).cloned(),
                overlay.pending_tombstones.contains(coord),
            ),
            None => (None, false),
        };
        self.journal.push(ArrayOverlayUndo {
            array_id: array_id.clone(),
            coord: coord.to_vec(),
            prev_put,
            prev_tombstoned,
        });
    }

    /// Current length of the array overlay undo journal: the savepoint
    /// marker a later `rollback_to` rewinds toward. Returned to the Control
    /// Plane by `MetaOp::MarkSavepoint` alongside the other overlays' markers.
    pub fn journal_len(&self) -> usize {
        self.journal.len()
    }

    /// Revert every staged cell mutation recorded after `marker`, restoring
    /// each slot's prior membership across BOTH sets (or removing it when the
    /// prior slot was absent), then truncate the journal to `marker`.
    ///
    /// Entries are replayed strictly in reverse so repeated writes to one
    /// slot unwind to the exact state present at the marked point. A
    /// `marker` at or beyond the current length is a no-op.
    pub fn rollback_to(&mut self, marker: usize) {
        while self.journal.len() > marker {
            let Some(undo) = self.journal.pop() else {
                break;
            };
            let ArrayOverlayUndo {
                array_id,
                coord,
                prev_put,
                prev_tombstoned,
            } = undo;
            let Some(overlay) = self.arrays.get_mut(&array_id) else {
                continue;
            };
            match prev_put {
                Some(put) => {
                    overlay.pending_cells.insert(coord.clone(), put);
                }
                None => {
                    overlay.pending_cells.remove(&coord);
                }
            }
            if prev_tombstoned {
                overlay.pending_tombstones.insert(coord);
            } else {
                overlay.pending_tombstones.remove(&coord);
            }
        }
    }
}

#[cfg(test)]
mod tests {
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
            surrogate: Surrogate::ZERO,
            system_from_ms: 0,
            valid_from_ms: 0,
            valid_until_ms: i64::MAX,
        }
    }

    #[test]
    fn rollback_to_removes_put_added_after_marker() {
        let mut overlay = ArrayTxnOverlay::new();
        overlay.stage_cell_put(aid(), coord(1), put(1));
        let marker = overlay.journal_len();
        overlay.stage_cell_put(aid(), coord(2), put(2));
        assert_eq!(overlay.staged_cells(&aid()).count(), 2);

        overlay.rollback_to(marker);

        let out: Vec<_> = overlay.staged_cells(&aid()).collect();
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].0, coord(1).as_slice());
        assert_eq!(overlay.journal_len(), marker);
    }

    #[test]
    fn rollback_to_restores_prior_body_for_reoverwritten_cell() {
        let mut overlay = ArrayTxnOverlay::new();
        overlay.stage_cell_put(aid(), coord(1), put(1));
        let marker = overlay.journal_len();
        overlay.stage_cell_put(aid(), coord(1), put(9));

        overlay.rollback_to(marker);

        let out: Vec<_> = overlay.staged_cells(&aid()).collect();
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].1, &put(1));
    }

    #[test]
    fn rollback_to_restores_tombstone_cleared_by_reput() {
        let mut overlay = ArrayTxnOverlay::new();
        overlay.stage_cell_delete(aid(), coord(1));
        let marker = overlay.journal_len();
        overlay.stage_cell_put(aid(), coord(1), put(7));
        assert!(!overlay.is_cell_tombstoned(&aid(), &coord(1)));

        overlay.rollback_to(marker);

        assert!(overlay.is_cell_tombstoned(&aid(), &coord(1)));
        assert_eq!(overlay.staged_cells(&aid()).count(), 0);
    }

    #[test]
    fn rollback_to_restores_put_cleared_by_delete() {
        let mut overlay = ArrayTxnOverlay::new();
        overlay.stage_cell_put(aid(), coord(1), put(5));
        let marker = overlay.journal_len();
        overlay.stage_cell_delete(aid(), coord(1));

        overlay.rollback_to(marker);

        assert!(!overlay.is_cell_tombstoned(&aid(), &coord(1)));
        let out: Vec<_> = overlay.staged_cells(&aid()).collect();
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].1, &put(5));
    }

    #[test]
    fn rollback_to_current_len_is_noop() {
        let mut overlay = ArrayTxnOverlay::new();
        overlay.stage_cell_put(aid(), coord(1), put(1));
        let marker = overlay.journal_len();
        overlay.rollback_to(marker);
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
