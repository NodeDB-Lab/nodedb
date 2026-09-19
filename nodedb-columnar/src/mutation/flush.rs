// SPDX-License-Identifier: Apache-2.0

//! Post-write coordination: memtable flush.

use crate::error::ColumnarError;
use crate::pk_index::RowLocation;
use crate::wal_record::ColumnarWalRecord;

use super::engine::{MutationEngine, MutationResult};

impl MutationEngine {
    /// Notify the engine that the memtable was flushed to a new segment.
    ///
    /// Remaps the PK index entries of the memtable's virtual segment to
    /// `new_segment_id` and moves the memtable's delete bitmap with them, so
    /// a row tombstoned while in the memtable stays tombstoned in the
    /// segment. The virtual segment id itself never changes: it names the
    /// memtable, not a flushed segment, so no flushed segment can ever share
    /// it and a later remap can never sweep flushed rows along.
    ///
    /// Returns the WAL record for the flush event, or `SegmentIdExhausted`
    /// if the u64 segment ID counter has wrapped past its maximum.
    pub fn on_memtable_flushed(
        &mut self,
        new_segment_id: u64,
    ) -> Result<MutationResult, ColumnarError> {
        let row_count = self.memtable_row_counter;

        // Remap PK index entries from virtual memtable segment to real segment.
        self.pk_index
            .remap_segment(self.memtable_segment_id, |old_row| {
                Some(RowLocation {
                    segment_id: new_segment_id,
                    row_index: old_row,
                })
            });
        if let Some(bitmap) = self.delete_bitmaps.remove(&self.memtable_segment_id) {
            self.delete_bitmaps.insert(new_segment_id, bitmap);
        }

        // Advance the segment ID counter with overflow protection.
        let next = self
            .next_segment_id
            .checked_add(1)
            .ok_or(ColumnarError::SegmentIdExhausted)?;

        // Reset memtable tracking.
        self.next_segment_id = next;
        self.memtable_row_counter = 0;
        self.memtable_surrogates.clear();

        let wal = ColumnarWalRecord::MemtableFlushed {
            collection: self.collection.clone(),
            segment_id: new_segment_id,
            row_count: row_count as u64,
        };

        Ok(MutationResult {
            wal_records: vec![wal],
        })
    }
}

#[cfg(test)]
mod tests {
    use nodedb_types::columnar::{ColumnDef, ColumnType, ColumnarSchema};
    use nodedb_types::value::Value;

    use super::*;
    use crate::pk_index::encode_pk;

    fn engine() -> MutationEngine {
        let schema = ColumnarSchema::new(vec![
            ColumnDef::required("id", ColumnType::Int64).with_primary_key(),
        ])
        .expect("valid");
        MutationEngine::new("t".into(), schema)
    }

    /// Two flushes must land in two distinct segments: the memtable keeps its
    /// virtual id, so the second remap touches only the rows that were still
    /// in the memtable.
    #[test]
    fn second_flush_leaves_first_segment_rows_in_place() {
        let mut e = engine();
        e.insert(&[Value::Integer(1)]).expect("insert");
        let first = e.next_segment_id();
        e.on_memtable_flushed(first).expect("flush 1");
        e.insert(&[Value::Integer(2)]).expect("insert");
        let second = e.next_segment_id();
        e.on_memtable_flushed(second).expect("flush 2");

        assert_ne!(first, second);
        assert_eq!(e.memtable_segment_id(), 0);
        assert_eq!(
            e.pk_index()
                .get(&encode_pk(&Value::Integer(1)))
                .map(|l| l.segment_id),
            Some(first)
        );
        assert_eq!(
            e.pk_index()
                .get(&encode_pk(&Value::Integer(2)))
                .map(|l| l.segment_id),
            Some(second)
        );
    }

    /// A row tombstoned in the memtable stays tombstoned once flushed.
    #[test]
    fn memtable_tombstones_follow_the_flushed_segment() {
        let mut e = engine();
        e.insert(&[Value::Integer(1)]).expect("insert");
        e.insert(&[Value::Integer(2)]).expect("insert");
        e.delete(&Value::Integer(1)).expect("delete");
        let seg = e.next_segment_id();
        e.on_memtable_flushed(seg).expect("flush");

        assert!(e.delete_bitmap(0).is_none(), "memtable bitmap moved out");
        assert!(
            e.delete_bitmap(seg).is_some_and(|bm| bm.is_deleted(0)),
            "row 0 of the flushed segment is the tombstoned row"
        );
        assert!(!e.delete_bitmap(seg).is_some_and(|bm| bm.is_deleted(1)));
    }
}
