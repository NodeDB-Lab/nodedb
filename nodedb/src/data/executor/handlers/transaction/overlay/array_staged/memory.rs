// SPDX-License-Identifier: BUSL-1.1

//! Hot-tier accounting and spilling of the ARRAY overlay.
//!
//! `[tuning.query] max_txn_overlay_bytes` is the overlay's hot-tier
//! threshold. A staged mutation that lifts the hot weight past it writes
//! every hot tier and the in-RAM journal to the spill directory.

use super::txn_overlay::ArrayTxnOverlay;

impl ArrayTxnOverlay {
    /// Weight of every hot tier and of the in-RAM journal.
    pub fn hot_bytes(&self) -> usize {
        self.arrays
            .values()
            .map(|overlay| overlay.cells.hot_bytes())
            .sum::<usize>()
            + self.journal.tail_bytes()
    }

    /// RAM the spilled tiers keep: blooms, run indexes and the journal index.
    pub fn spilled_heap_bytes(&self) -> usize {
        self.arrays
            .values()
            .map(|overlay| overlay.cells.run_heap_bytes())
            .sum::<usize>()
            + self.journal.spilled_heap_bytes()
    }

    /// Spill when the hot weight passed the threshold. Called by every
    /// mutator after it staged. An error part way leaves the overlay
    /// unusable.
    pub(super) fn spill_past_threshold(&mut self) -> crate::Result<()> {
        if self.hot_bytes() <= self.space.threshold() {
            return Ok(());
        }
        let result = self.spill_tiers();
        if let Err(e) = &result {
            self.poisoned = Some(e.to_string());
        }
        result
    }

    fn spill_tiers(&mut self) -> crate::Result<()> {
        for overlay in self.arrays.values_mut() {
            overlay.spill(&mut self.space)?;
        }
        self.journal.spill(&mut self.space)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::data::executor::handlers::spill::tiered::SpillSpace;
    use crate::data::executor::handlers::transaction::overlay::StagedCellPut;
    use nodedb_array::types::ArrayId;
    use nodedb_array::types::cell_value::value::CellValue;
    use nodedb_array::types::coord::value::CoordValue;
    use nodedb_types::{Surrogate, TenantId};

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
    fn hot_bytes_counts_staged_cells() {
        let mut overlay = ArrayTxnOverlay::new();
        assert_eq!(overlay.hot_bytes(), 0);
        overlay
            .stage_cell_put(
                ArrayId::new(TenantId::new(1), "a"),
                vec![CoordValue::Int64(1)],
                put(7),
            )
            .unwrap();
        assert!(overlay.hot_bytes() > 0);
    }

    #[test]
    fn spilled_cells_read_back_and_roll_back() {
        let dir = tempfile::tempdir().unwrap();
        let mut overlay =
            ArrayTxnOverlay::with_spill(SpillSpace::new(dir.path().join("t"), "array", 0));
        let array = ArrayId::new(TenantId::new(1), "a");
        for x in 0..30i64 {
            overlay
                .stage_cell_put(array.clone(), vec![CoordValue::Int64(x)], put(x))
                .unwrap();
        }
        let marker = overlay.journal_len();
        overlay
            .stage_cell_delete(array.clone(), vec![CoordValue::Int64(4)])
            .unwrap();
        overlay
            .stage_cell_put(array.clone(), vec![CoordValue::Int64(100)], put(100))
            .unwrap();
        assert!(overlay.spilled_heap_bytes() > 0);
        assert!(
            overlay
                .is_cell_tombstoned(&array, &[CoordValue::Int64(4)])
                .unwrap()
        );
        assert_eq!(overlay.staged_cells(&array).count(), 30);

        overlay.rollback_to(marker).unwrap();

        assert_eq!(
            overlay
                .staged_cell_presence(&array, &[CoordValue::Int64(4)])
                .unwrap(),
            Some(true)
        );
        assert_eq!(
            overlay
                .staged_cell_presence(&array, &[CoordValue::Int64(100)])
                .unwrap(),
            None
        );
        assert_eq!(overlay.staged_cells(&array).count(), 30);
        assert_eq!(overlay.tombstoned_coords(&array).count(), 0);
    }
}
