// SPDX-License-Identifier: BUSL-1.1

//! Per-transaction GRAPH overlay memory-footprint accounting, reused by the
//! staging handlers to enforce `[tuning.query] max_txn_overlay_bytes`.

use super::txn_overlay::GraphTxnOverlay;
use super::types::GraphCollectionOverlay;

impl GraphTxnOverlay {
    /// Sum of staged edge/tombstone/label-delta byte footprint across every
    /// collection this transaction has touched.
    pub fn memory_size_estimate(&self) -> usize {
        self.collections
            .values()
            .map(GraphCollectionOverlay::memory_size_estimate)
            .sum()
    }
}

#[cfg(test)]
mod tests {
    use super::super::types::GraphCollKey;
    use super::*;
    use crate::types::{DatabaseId, TenantId};

    fn key(coll: &str) -> GraphCollKey {
        (DatabaseId::new(1), TenantId::new(1), coll.to_string())
    }

    #[test]
    fn memory_size_estimate_counts_bytes() {
        let mut overlay = GraphTxnOverlay::new();
        assert_eq!(overlay.memory_size_estimate(), 0);
        overlay.stage_edge_put(key("g"), "a", "l", "b", vec![1, 2, 3]);
        assert!(overlay.memory_size_estimate() > 0);
    }
}
