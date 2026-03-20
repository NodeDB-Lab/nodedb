//! Memory estimation, access tracking, and prefetching for the CSR index.
//!
//! Provides RAM usage estimation for SpillController integration,
//! hot/cold node identification via access counting, and CPU cache
//! prefetch hints for BFS traversal optimization.

use super::index::CsrIndex;

impl CsrIndex {
    /// Record a node access (called during traversals for hot/cold tracking).
    pub fn record_access(&self, node_id: u32) {
        let idx = node_id as usize;
        if idx < self.access_counts.len() {
            let c = &self.access_counts[idx];
            c.set(c.get().saturating_add(1));
        }
    }

    /// Get nodes with access count below `threshold` (cold nodes).
    pub fn cold_nodes(&self, threshold: u32) -> Vec<u32> {
        self.access_counts
            .iter()
            .enumerate()
            .filter(|(_, c)| c.get() <= threshold)
            .map(|(i, _)| i as u32)
            .collect()
    }

    /// Number of hot nodes (access count > 0).
    pub fn hot_node_count(&self) -> usize {
        self.access_counts.iter().filter(|c| c.get() > 0).count()
    }

    /// Current query epoch (incremented per traversal call).
    pub fn query_epoch(&self) -> u64 {
        self.query_epoch
    }

    /// Reset access counters (called during compaction or periodically).
    pub fn reset_access_counts(&mut self) {
        self.access_counts.iter().for_each(|c| c.set(0));
        self.query_epoch = 0;
    }

    /// Predictive prefetch: hint the OS to load a node's adjacency data
    /// into the page cache before the traversal touches it.
    ///
    /// For in-memory dense CSR, this prefetches the cache line containing
    /// the node's offset/target entries. For future mmap'd cold segments,
    /// this would call `madvise(MADV_WILLNEED)` on the relevant pages.
    ///
    /// Called during BFS planning: when adding nodes to the next frontier,
    /// prefetch their neighbors' data so it's resident when the BFS loop
    /// reaches them on the next iteration.
    #[inline]
    pub fn prefetch_node(&self, node_id: u32) {
        let idx = node_id as usize;
        // For in-memory CSR: prefetch the offset entry into L1 cache.
        // This is a software prefetch hint — no-op on cache hit, ~200ns
        // savings on cache miss for large graphs.
        if idx + 1 < self.out_offsets.len() {
            // SAFETY: We're just hinting the CPU to load this address.
            // The offset is within bounds (checked above). This is a
            // performance hint, not a correctness requirement.
            #[cfg(target_arch = "x86_64")]
            unsafe {
                let ptr = self.out_offsets.as_ptr().add(idx) as *const u8;
                std::arch::x86_64::_mm_prefetch(ptr as *const i8, std::arch::x86_64::_MM_HINT_T0);
            }
        }
    }

    /// Prefetch a batch of nodes (called during BFS frontier expansion).
    pub fn prefetch_batch(&self, node_ids: &[u32]) {
        for &id in node_ids {
            self.prefetch_node(id);
        }
    }

    /// Evaluate graph memory pressure and return promotion/demotion hints.
    ///
    /// Uses SpillController-compatible thresholds (90%/75% hysteresis):
    /// - Above spill threshold: demote cold nodes (spill to potential mmap)
    /// - Below restore threshold: promote warm nodes back to hot RAM
    ///
    /// `utilization` = estimated memory usage as percentage (0-100).
    /// Returns `(nodes_to_demote, nodes_to_promote)` counts.
    pub fn evaluate_memory_pressure(
        &self,
        utilization: u8,
        spill_threshold: u8,
        restore_threshold: u8,
    ) -> (usize, usize) {
        if utilization >= spill_threshold {
            // Above spill threshold: identify cold nodes to demote.
            let cold = self.cold_nodes(0); // nodes with zero access
            (cold.len(), 0)
        } else if utilization <= restore_threshold {
            // Below restore threshold: all nodes can stay hot.
            (0, self.node_count())
        } else {
            // In hysteresis band: no action.
            (0, 0)
        }
    }

    /// Estimated memory usage of the dense CSR in bytes.
    ///
    /// Used for SpillController utilization calculation.
    pub fn estimated_memory_bytes(&self) -> usize {
        let offsets = (self.out_offsets.len() + self.in_offsets.len()) * 4;
        let targets = (self.out_targets.len() + self.in_targets.len()) * 4;
        let labels = (self.out_labels.len() + self.in_labels.len()) * 2;
        let buffer: usize = self
            .buffer_out
            .iter()
            .chain(self.buffer_in.iter())
            .map(|b| b.len() * 6) // (u16 + u32) per entry
            .sum();
        let interning = self.id_to_node.iter().map(|s| s.len() + 24).sum::<usize>()
            + self.id_to_label.iter().map(|s| s.len() + 24).sum::<usize>();
        offsets + targets + labels + buffer + interning
    }
}
