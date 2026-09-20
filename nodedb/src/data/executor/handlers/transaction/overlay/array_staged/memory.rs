// SPDX-License-Identifier: BUSL-1.1

//! Per-transaction ARRAY overlay memory-footprint accounting, reused by the
//! staging handler to enforce `MAX_TXN_OVERLAY_BYTES`.

use super::txn_overlay::ArrayTxnOverlay;
use super::types::ArrayCollectionOverlay;

impl ArrayTxnOverlay {
    /// Sum of staged cell / tombstone byte footprint across every array this
    /// transaction has touched.
    pub fn memory_size_estimate(&self) -> usize {
        self.arrays
            .values()
            .map(ArrayCollectionOverlay::memory_size_estimate)
            .sum()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::data::executor::handlers::transaction::overlay::StagedCellPut;
    use nodedb_array::types::ArrayId;
    use nodedb_array::types::cell_value::value::CellValue;
    use nodedb_array::types::coord::value::CoordValue;
    use nodedb_types::{Surrogate, TenantId};

    #[test]
    fn memory_size_estimate_counts_bytes() {
        let mut overlay = ArrayTxnOverlay::new();
        assert_eq!(overlay.memory_size_estimate(), 0);
        overlay.stage_cell_put(
            ArrayId::new(TenantId::new(1), "a"),
            vec![CoordValue::Int64(1)],
            StagedCellPut {
                attrs: vec![CellValue::Int64(7)],
                surrogate: Surrogate::ZERO,
                system_from_ms: 0,
                valid_from_ms: 0,
                valid_until_ms: i64::MAX,
            },
        );
        assert!(overlay.memory_size_estimate() > 0);
    }
}
