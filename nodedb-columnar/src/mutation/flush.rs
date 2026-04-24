//! Post-write coordination: memtable flush + compaction commit.

use std::collections::HashMap;

use crate::pk_index::RowLocation;
use crate::wal_record::ColumnarWalRecord;

use super::engine::{MutationEngine, MutationResult};

impl MutationEngine {
    /// Notify the engine that the memtable was flushed to a new segment.
    ///
    /// Updates the PK index to remap memtable entries to the new segment.
    /// Returns the WAL record for the flush event.
    pub fn on_memtable_flushed(&mut self, new_segment_id: u32) -> MutationResult {
        let row_count = self.memtable_row_counter;

        // Remap PK index entries from virtual memtable segment to real segment.
        self.pk_index
            .remap_segment(self.memtable_segment_id, |old_row| {
                Some(RowLocation {
                    segment_id: new_segment_id,
                    row_index: old_row,
                })
            });

        // Reset memtable tracking.
        self.memtable_segment_id = self.next_segment_id;
        self.next_segment_id += 1;
        self.memtable_row_counter = 0;

        let wal = ColumnarWalRecord::MemtableFlushed {
            collection: self.collection.clone(),
            segment_id: new_segment_id,
            row_count: row_count as u64,
        };

        MutationResult {
            wal_records: vec![wal],
        }
    }

    /// Notify the engine that compaction completed.
    ///
    /// Remaps PK index entries and removes old delete bitmaps.
    pub fn on_compaction_complete(
        &mut self,
        old_segment_ids: &[u32],
        new_segment_id: u32,
        row_mapping: &HashMap<(u32, u32), u32>,
    ) -> MutationResult {
        // Remap PK index for each old segment.
        for &old_seg in old_segment_ids {
            self.pk_index.remap_segment(old_seg, |old_row| {
                row_mapping
                    .get(&(old_seg, old_row))
                    .map(|&new_row| RowLocation {
                        segment_id: new_segment_id,
                        row_index: new_row,
                    })
            });

            // Remove old delete bitmap.
            self.delete_bitmaps.remove(&old_seg);
        }

        let wal = ColumnarWalRecord::CompactionCommit {
            collection: self.collection.clone(),
            old_segment_ids: old_segment_ids.to_vec(),
            new_segment_ids: vec![new_segment_id],
        };

        MutationResult {
            wal_records: vec![wal],
        }
    }
}
