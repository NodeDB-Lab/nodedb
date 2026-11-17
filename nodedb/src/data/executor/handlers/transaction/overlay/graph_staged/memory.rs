// SPDX-License-Identifier: BUSL-1.1

//! Hot-tier accounting and spilling of the GRAPH overlay.
//!
//! `[tuning.query] max_txn_overlay_bytes` is the overlay's hot-tier
//! threshold. A staged mutation that lifts the hot weight past it writes
//! every hot tier and the in-RAM journal to the spill directory.

use super::txn_overlay::GraphTxnOverlay;

impl GraphTxnOverlay {
    /// Weight of every hot tier and of the in-RAM journal.
    pub fn hot_bytes(&self) -> usize {
        self.collections
            .values()
            .map(super::types::GraphCollectionOverlay::hot_bytes)
            .sum::<usize>()
            + self.journal.tail_bytes()
    }

    /// RAM the spilled tiers keep: blooms, run indexes and the journal index.
    pub fn spilled_heap_bytes(&self) -> usize {
        self.collections
            .values()
            .map(super::types::GraphCollectionOverlay::spilled_heap_bytes)
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
        for overlay in self.collections.values_mut() {
            overlay.spill(&mut self.space)?;
        }
        self.journal.spill(&mut self.space)
    }
}

#[cfg(test)]
mod tests {
    use super::super::types::GraphCollKey;
    use super::*;
    use crate::data::executor::handlers::spill::tiered::SpillSpace;
    use crate::types::{DatabaseId, TenantId};

    fn key(coll: &str) -> GraphCollKey {
        (DatabaseId::new(1), TenantId::new(1), coll.to_string())
    }

    #[test]
    fn hot_bytes_counts_staged_edges() {
        let mut overlay = GraphTxnOverlay::new();
        assert_eq!(overlay.hot_bytes(), 0);
        overlay
            .stage_edge_put(key("g"), "a", "l", "b", vec![1, 2, 3])
            .unwrap();
        assert!(overlay.hot_bytes() > 0);
    }

    #[test]
    fn spilled_edges_and_labels_read_back_and_roll_back() {
        let dir = tempfile::tempdir().unwrap();
        let mut overlay =
            GraphTxnOverlay::with_spill(SpillSpace::new(dir.path().join("t"), "graph", 0));
        let g = key("g");
        for n in 0..40u32 {
            overlay
                .stage_edge_put(g.clone(), "a", "knows", &format!("n{n}"), vec![n as u8])
                .unwrap();
        }
        overlay
            .stage_node_labels_set(g.clone(), "a", &["Person".to_string()])
            .unwrap();
        let marker = overlay.journal_len();
        overlay
            .stage_edge_delete(g.clone(), "a", "knows", "n3")
            .unwrap();
        overlay
            .stage_edge_put(g.clone(), "z", "knows", "a", vec![9])
            .unwrap();
        overlay
            .stage_node_labels_remove(g.clone(), "a", &["Person".to_string()])
            .unwrap();
        assert!(overlay.spilled_heap_bytes() > 0);

        assert_eq!(overlay.edges_for_src(&g, "a").unwrap().len(), 39);
        assert!(overlay.is_edge_tombstoned(&g, "a", "knows", "n3").unwrap());
        assert_eq!(overlay.edges_for_dst(&g, "a").unwrap().len(), 1);
        assert!(
            overlay
                .labels_delta(&g, "a")
                .unwrap()
                .unwrap()
                .removed
                .contains("Person")
        );

        overlay.rollback_to(marker).unwrap();

        assert_eq!(overlay.edges_for_src(&g, "a").unwrap().len(), 40);
        assert!(!overlay.is_edge_tombstoned(&g, "a", "knows", "n3").unwrap());
        assert!(overlay.edges_for_dst(&g, "a").unwrap().is_empty());
        let delta = overlay.labels_delta(&g, "a").unwrap().unwrap();
        assert!(delta.added.contains("Person"));
        assert!(delta.removed.is_empty());
    }
}
