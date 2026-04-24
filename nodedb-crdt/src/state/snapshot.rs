//! Snapshot export/import, history compaction, memory estimation.

use loro::LoroDoc;

use crate::error::{CrdtError, Result};

use super::core::CrdtState;

impl CrdtState {
    /// Export the current state as bytes for sync.
    pub fn export_snapshot(&self) -> Result<Vec<u8>> {
        self.doc
            .export(loro::ExportMode::Snapshot)
            .map_err(|e| CrdtError::Loro(format!("snapshot export failed: {e}")))
    }

    /// Import remote updates.
    pub fn import(&self, data: &[u8]) -> Result<()> {
        self.doc
            .import(data)
            .map_err(|e| CrdtError::DeltaApplyFailed(e.to_string()))?;
        Ok(())
    }

    /// Compact the CRDT history by replacing the internal LoroDoc with a
    /// shallow snapshot.
    ///
    /// A shallow snapshot contains the current state but discards the
    /// full operation history. This is the CRDT equivalent of WAL
    /// truncation after checkpoint.
    ///
    /// After compaction:
    /// - All current state is preserved (reads return same values).
    /// - New deltas can still be applied and merged.
    /// - Historical operations before the snapshot point are gone.
    /// - Peers that sync after compaction receive a full snapshot
    ///   instead of incremental deltas (acceptable for long-offline peers).
    ///
    /// Call this periodically (e.g., every 30 minutes or when memory
    /// pressure exceeds threshold) to prevent unbounded history growth.
    pub fn compact_history(&mut self) -> Result<()> {
        // Export a shallow snapshot at the current frontiers.
        let frontiers = self.doc.oplog_frontiers();
        let snapshot = self
            .doc
            .export(loro::ExportMode::shallow_snapshot(&frontiers))
            .map_err(|e| CrdtError::Loro(format!("shallow snapshot export: {e}")))?;

        // Replace the doc with a fresh one loaded from the snapshot.
        let new_doc = LoroDoc::new();
        new_doc
            .set_peer_id(self.peer_id)
            .map_err(|e| CrdtError::Loro(format!("failed to set peer_id on compacted doc: {e}")))?;
        new_doc
            .import(&snapshot)
            .map_err(|e| CrdtError::Loro(format!("shallow snapshot import: {e}")))?;

        self.doc = new_doc;
        Ok(())
    }

    /// Estimated memory usage of the CRDT state (bytes).
    ///
    /// Includes operation history, current state, and internal caches.
    /// Use this to decide when to trigger `compact_history()`.
    pub fn estimated_memory_bytes(&self) -> usize {
        // Loro doesn't expose a direct memory metric.
        // Use snapshot size as a proxy — it's proportional to state size.
        // This is not precise but good enough for pressure monitoring.
        self.doc
            .export(loro::ExportMode::Snapshot)
            .map(|s| s.len())
            .unwrap_or(0)
    }
}
