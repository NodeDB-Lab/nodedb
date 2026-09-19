// SPDX-License-Identifier: Apache-2.0

//! Whole-collection `TRUNCATE` on a `MutationEngine`, reversible for a
//! transaction batch that aborts after it.

use std::collections::HashMap;

use nodedb_types::surrogate::Surrogate;

use crate::delete_bitmap::DeleteBitmap;
use crate::memtable::ColumnarMemtable;
use crate::pk_index::PkIndex;

use super::engine::MutationEngine;

/// Every row-bearing part of a `MutationEngine`, taken out by
/// [`MutationEngine::truncate`] and put back by
/// [`MutationEngine::restore_truncated`]. Holds every bitemporal version and
/// every tombstone exactly as they were, so a restore is exact.
pub struct TruncatedRows {
    memtable: ColumnarMemtable,
    pk_index: PkIndex,
    delete_bitmaps: HashMap<u64, DeleteBitmap>,
    memtable_row_counter: u32,
    memtable_surrogates: Vec<Option<Surrogate>>,
}

impl MutationEngine {
    /// Number of live rows: one per bound primary key. Historical bitemporal
    /// versions and tombstoned rows are not counted.
    pub fn live_row_count(&self) -> usize {
        self.pk_index.len()
    }

    /// Remove every row, every bitemporal version, and every tombstone, and
    /// hand the pre-image back. Segment ids stay monotonic: the caller drops
    /// its flushed segment bytes for the same collection, and the next flush
    /// takes the next unused id rather than reusing one a stale reader could
    /// still name.
    pub fn truncate(&mut self) -> TruncatedRows {
        let threshold = self.memtable.flush_threshold();
        let memtable = std::mem::replace(
            &mut self.memtable,
            ColumnarMemtable::with_threshold(&self.schema, threshold),
        );
        TruncatedRows {
            memtable,
            pk_index: std::mem::take(&mut self.pk_index),
            delete_bitmaps: std::mem::take(&mut self.delete_bitmaps),
            memtable_row_counter: std::mem::replace(&mut self.memtable_row_counter, 0),
            memtable_surrogates: std::mem::take(&mut self.memtable_surrogates),
        }
    }

    /// Put back what [`Self::truncate`] took out. Used exclusively by the
    /// transaction undo log; never called on the normal write path.
    pub fn restore_truncated(&mut self, rows: TruncatedRows) {
        let TruncatedRows {
            memtable,
            pk_index,
            delete_bitmaps,
            memtable_row_counter,
            memtable_surrogates,
        } = rows;
        self.memtable = memtable;
        self.pk_index = pk_index;
        self.delete_bitmaps = delete_bitmaps;
        self.memtable_row_counter = memtable_row_counter;
        self.memtable_surrogates = memtable_surrogates;
    }
}

#[cfg(test)]
mod tests {
    use nodedb_types::columnar::{ColumnDef, ColumnType, ColumnarSchema};
    use nodedb_types::value::Value;

    use super::*;

    fn schema() -> ColumnarSchema {
        ColumnarSchema::new(vec![
            ColumnDef::required("id", ColumnType::Int64).with_primary_key(),
            ColumnDef::required("name", ColumnType::String),
        ])
        .expect("valid")
    }

    fn seeded() -> MutationEngine {
        let mut engine = MutationEngine::with_flush_threshold("t".into(), schema(), 7);
        for i in 0..4 {
            engine
                .insert_with_surrogate(
                    &[Value::Integer(i), Value::String(format!("r{i}"))],
                    Surrogate(100 + i as u32),
                )
                .expect("insert");
        }
        engine.delete(&Value::Integer(1)).expect("delete");
        engine
    }

    #[test]
    fn truncate_empties_every_row_bearing_part_and_keeps_the_threshold() {
        let mut engine = seeded();
        assert_eq!(engine.live_row_count(), 3);

        let _pre = engine.truncate();

        assert_eq!(engine.live_row_count(), 0);
        assert_eq!(engine.memtable().row_count(), 0);
        assert!(engine.delete_bitmaps().is_empty());
        assert!(engine.memtable_surrogates().is_empty());
        assert_eq!(engine.scan_memtable_rows().count(), 0);
        assert_eq!(engine.memtable().flush_threshold(), 7);
        assert_eq!(engine.next_segment_id(), 1);

        engine
            .insert(&[Value::Integer(9), Value::String("z".into())])
            .expect("insert after truncate");
        assert_eq!(engine.live_row_count(), 1);
        assert_eq!(engine.scan_memtable_rows().count(), 1);
    }

    #[test]
    fn restore_truncated_brings_back_rows_and_tombstones_exactly() {
        let mut engine = seeded();
        let before: Vec<Vec<Value>> = engine.scan_memtable_rows().collect();

        let pre = engine.truncate();
        engine
            .insert(&[Value::Integer(9), Value::String("z".into())])
            .expect("insert after truncate");
        engine.restore_truncated(pre);

        let after: Vec<Vec<Value>> = engine.scan_memtable_rows().collect();
        assert_eq!(
            after, before,
            "restore must reproduce the pre-truncate rows"
        );
        assert_eq!(engine.live_row_count(), 3);
        assert!(
            engine
                .pk_index()
                .get(&crate::pk_index::encode_pk(&Value::Integer(1)))
                .is_none(),
            "the tombstone deleted before the truncate stays deleted"
        );
        assert_eq!(engine.memtable_surrogates().len(), 4);
    }

    #[test]
    fn truncate_keeps_segment_ids_monotonic_after_a_flush() {
        let mut engine = seeded();
        engine.on_memtable_flushed(1).expect("flush");
        assert_eq!(engine.next_segment_id(), 2);
        let _pre = engine.truncate();
        assert_eq!(engine.next_segment_id(), 2);
    }
}
