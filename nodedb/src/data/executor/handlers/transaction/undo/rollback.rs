// SPDX-License-Identifier: BUSL-1.1

//! Rollback driver for the undo log.

use super::UndoEntry;
use crate::data::executor::core_loop::CoreLoop;

impl CoreLoop {
    /// Roll back completed writes in reverse order.
    ///
    /// Returns `Ok(())` if all undo entries were applied successfully.
    ///
    /// Returns `Err((entry_index, detail))` on the first undo failure —
    /// the entry index is the original forward-order position of the failed
    /// entry (before reversal). On failure the caller **must** return a
    /// `RollbackFailed` error to the client; the shard state is unknown
    /// and requires a restart to restore consistency via WAL replay.
    pub(in crate::data::executor::handlers) fn rollback_undo_log(
        &mut self,
        did: u64,
        tid: u64,
        undo_log: Vec<UndoEntry>,
    ) -> Result<(), (usize, String)> {
        let total = undo_log.len();
        for (rev_idx, entry) in undo_log.into_iter().rev().enumerate() {
            // Convert reversed index back to original forward-order index for
            // diagnostics (makes it easier to correlate with the sub-plan that
            // produced this undo entry).
            let original_idx = total.saturating_sub(1 + rev_idx);
            self.apply_undo_entry(did, tid, original_idx, entry)?;
        }
        Ok(())
    }

    /// Apply a single undo entry. Returns `Err((entry_index, detail))` if the
    /// undo cannot be applied — this is a fatal condition: the shard's in-memory
    /// state is now partially rolled back and must not serve writes.
    fn apply_undo_entry(
        &mut self,
        did: u64,
        tid: u64,
        entry_index: usize,
        entry: UndoEntry,
    ) -> Result<(), (usize, String)> {
        match entry {
            UndoEntry::PutDocument { .. } | UndoEntry::DeleteDocument { .. } => {
                self.apply_undo_document(did, tid, entry_index, entry)
            }
            UndoEntry::InsertVector { .. } | UndoEntry::DeleteVector { .. } => {
                self.apply_undo_vector(tid, entry_index, entry)
            }
            UndoEntry::SpatialInsert { .. } | UndoEntry::SpatialDelete { .. } => {
                self.apply_undo_spatial(entry_index, entry)
            }
            UndoEntry::PutEdge { .. } | UndoEntry::DeleteEdge { .. } => {
                self.apply_undo_edge(did, tid, entry_index, entry)
            }
            UndoEntry::KvPut { .. }
            | UndoEntry::KvDelete { .. }
            | UndoEntry::KvBatchPut { .. }
            | UndoEntry::KvTransfer { .. }
            | UndoEntry::KvTransferItem { .. }
            | UndoEntry::KvTtl { .. }
            | UndoEntry::SortedIndexDdl { .. } => self.apply_undo_kv(did, tid, entry_index, entry),
            UndoEntry::ColumnarInsert { .. }
            | UndoEntry::ColumnarUpdate { .. }
            | UndoEntry::ColumnarDelete { .. } => self.apply_undo_columnar(entry_index, entry),
            UndoEntry::TimeseriesIngest { .. } => self.apply_undo_timeseries(entry_index, entry),
            UndoEntry::StatsRestore { .. } => self.apply_undo_stats(entry_index, entry),
            UndoEntry::MarkNodeDeleted { .. } => self.apply_undo_mark_node(entry_index, entry),
        }
    }
}
