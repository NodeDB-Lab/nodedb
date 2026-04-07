//! Columnar mutation engine: coordinates PK index, delete bitmaps,
//! memtable, and WAL records for full INSERT/UPDATE/DELETE.
//!
//! The MutationEngine is the single point of coordination for all
//! columnar write operations. It produces WAL records that must be
//! persisted before the mutation is considered durable.

use std::collections::HashMap;

use nodedb_types::columnar::ColumnarSchema;
use nodedb_types::value::Value;

use crate::delete_bitmap::DeleteBitmap;
use crate::error::ColumnarError;
use crate::memtable::ColumnarMemtable;
use crate::pk_index::{PkIndex, RowLocation, encode_pk};
use crate::wal_record::{ColumnarWalRecord, encode_row_for_wal};

/// Coordinates all columnar mutations for a single collection.
///
/// Owns the memtable, PK index, and per-segment delete bitmaps.
/// Produces WAL records for each mutation that the caller must persist.
pub struct MutationEngine {
    collection: String,
    schema: ColumnarSchema,
    memtable: ColumnarMemtable,
    pk_index: PkIndex,
    /// Per-segment delete bitmaps. Key = segment_id.
    delete_bitmaps: HashMap<u32, DeleteBitmap>,
    /// PK column indices in the schema.
    pk_col_indices: Vec<usize>,
    /// Counter for assigning segment IDs.
    next_segment_id: u32,
    /// Current "memtable segment ID" — a virtual segment ID for rows
    /// that are still in the memtable (not yet flushed).
    memtable_segment_id: u32,
    /// Row counter within the current memtable (resets on flush).
    memtable_row_counter: u32,
}

/// Result of a mutation operation, including the WAL record to persist.
pub struct MutationResult {
    /// WAL record(s) to persist before the mutation is considered durable.
    pub wal_records: Vec<ColumnarWalRecord>,
}

impl MutationEngine {
    /// Create a new mutation engine for a collection.
    pub fn new(collection: String, schema: ColumnarSchema) -> Self {
        let pk_col_indices: Vec<usize> = schema
            .columns
            .iter()
            .enumerate()
            .filter(|(_, c)| c.primary_key)
            .map(|(i, _)| i)
            .collect();

        let memtable = ColumnarMemtable::new(&schema);
        // Reserve segment_id 0 for the first memtable. Real segments start at 1.
        let memtable_segment_id = 0;

        Self {
            collection,
            schema,
            memtable,
            pk_index: PkIndex::new(),
            delete_bitmaps: HashMap::new(),
            pk_col_indices,
            next_segment_id: 1,
            memtable_segment_id,
            memtable_row_counter: 0,
        }
    }

    /// Insert a row. Returns WAL record to persist.
    ///
    /// Validates schema, checks PK uniqueness, adds to memtable and PK index.
    pub fn insert(&mut self, values: &[Value]) -> Result<MutationResult, ColumnarError> {
        // Extract PK bytes for uniqueness check.
        let pk_bytes = self.extract_pk_bytes(values)?;

        // Check for duplicate PK.
        if self.pk_index.contains(&pk_bytes) {
            return Err(ColumnarError::DuplicatePrimaryKey);
        }

        // Generate WAL record BEFORE applying the mutation.
        let row_data = encode_row_for_wal(values);
        let wal = ColumnarWalRecord::InsertRow {
            collection: self.collection.clone(),
            row_data,
        };

        // Add to memtable.
        self.memtable.append_row(values)?;

        // Add to PK index (pointing to memtable virtual segment).
        let location = RowLocation {
            segment_id: self.memtable_segment_id,
            row_index: self.memtable_row_counter,
        };
        self.pk_index.upsert(pk_bytes, location);
        self.memtable_row_counter += 1;

        Ok(MutationResult {
            wal_records: vec![wal],
        })
    }

    /// Delete a row by PK value. Returns WAL record to persist.
    ///
    /// Looks up PK in the index to find the segment + row, then marks
    /// the row in the segment's delete bitmap.
    pub fn delete(&mut self, pk_value: &Value) -> Result<MutationResult, ColumnarError> {
        let pk_bytes = encode_pk(pk_value);

        let location = self
            .pk_index
            .get(&pk_bytes)
            .copied()
            .ok_or(ColumnarError::PrimaryKeyNotFound)?;

        // Generate WAL record BEFORE applying.
        let wal = ColumnarWalRecord::DeleteRows {
            collection: self.collection.clone(),
            segment_id: location.segment_id,
            row_indices: vec![location.row_index],
        };

        // Mark in delete bitmap.
        let bitmap = self.delete_bitmaps.entry(location.segment_id).or_default();
        bitmap.mark_deleted(location.row_index);

        // Remove from PK index.
        self.pk_index.remove(&pk_bytes);

        Ok(MutationResult {
            wal_records: vec![wal],
        })
    }

    /// Update a row by PK: DELETE old + INSERT new.
    ///
    /// `updates` maps column names to new values. Columns not in the map
    /// retain their existing values from the old row.
    ///
    /// Returns WAL records for both the delete and the insert.
    ///
    /// NOTE: The caller must provide the full old row values for the re-insert.
    /// This method takes the complete new row (already merged with old values).
    pub fn update(
        &mut self,
        old_pk: &Value,
        new_values: &[Value],
    ) -> Result<MutationResult, ColumnarError> {
        // Delete the old row.
        let delete_result = self.delete(old_pk)?;

        // Insert the new row.
        let insert_result = self.insert(new_values)?;

        // Combine WAL records.
        let mut wal_records = delete_result.wal_records;
        wal_records.extend(insert_result.wal_records);

        Ok(MutationResult { wal_records })
    }

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

    // -- Accessors --

    /// Access the memtable.
    pub fn memtable(&self) -> &ColumnarMemtable {
        &self.memtable
    }

    /// Mutable access to the memtable (for drain on flush).
    pub fn memtable_mut(&mut self) -> &mut ColumnarMemtable {
        &mut self.memtable
    }

    /// Access the PK index.
    pub fn pk_index(&self) -> &PkIndex {
        &self.pk_index
    }

    /// Mutable access to the PK index (for cold-start rebuild).
    pub fn pk_index_mut(&mut self) -> &mut PkIndex {
        &mut self.pk_index
    }

    /// Access a segment's delete bitmap.
    pub fn delete_bitmap(&self, segment_id: u32) -> Option<&DeleteBitmap> {
        self.delete_bitmaps.get(&segment_id)
    }

    /// Access all delete bitmaps.
    pub fn delete_bitmaps(&self) -> &HashMap<u32, DeleteBitmap> {
        &self.delete_bitmaps
    }

    /// The collection name.
    pub fn collection(&self) -> &str {
        &self.collection
    }

    /// The schema.
    pub fn schema(&self) -> &ColumnarSchema {
        &self.schema
    }

    /// Whether the memtable should be flushed.
    pub fn should_flush(&self) -> bool {
        self.memtable.should_flush()
    }

    /// Iterate non-deleted rows in the memtable as `Vec<Value>`.
    ///
    /// Skips rows marked as deleted in the memtable's virtual segment
    /// delete bitmap. For rows in flushed segments, use `SegmentReader`.
    pub fn scan_memtable_rows(&self) -> impl Iterator<Item = Vec<Value>> + '_ {
        let deletes = self.delete_bitmaps.get(&self.memtable_segment_id);
        self.memtable
            .iter_rows()
            .enumerate()
            .filter_map(move |(row_idx, row)| {
                if deletes.is_some_and(|bm| bm.is_deleted(row_idx as u32)) {
                    None
                } else {
                    Some(row)
                }
            })
    }

    /// Get a single row from the memtable by index (None if deleted).
    pub fn get_memtable_row(&self, row_idx: usize) -> Option<Vec<Value>> {
        if self
            .delete_bitmaps
            .get(&self.memtable_segment_id)
            .is_some_and(|bm| bm.is_deleted(row_idx as u32))
        {
            return None;
        }
        self.memtable.get_row(row_idx)
    }

    /// Whether a segment should be compacted based on its delete ratio.
    pub fn should_compact(&self, segment_id: u32, total_rows: u64) -> bool {
        self.delete_bitmaps
            .get(&segment_id)
            .is_some_and(|bm| bm.should_compact(total_rows, 0.2))
    }

    // -- Internal helpers --

    /// Extract PK bytes from a row of values.
    fn extract_pk_bytes(&self, values: &[Value]) -> Result<Vec<u8>, ColumnarError> {
        if values.len() != self.schema.columns.len() {
            return Err(ColumnarError::SchemaMismatch {
                expected: self.schema.columns.len(),
                got: values.len(),
            });
        }

        if self.pk_col_indices.len() == 1 {
            Ok(encode_pk(&values[self.pk_col_indices[0]]))
        } else {
            let pk_values: Vec<&Value> = self.pk_col_indices.iter().map(|&i| &values[i]).collect();
            Ok(crate::pk_index::encode_composite_pk(&pk_values))
        }
    }
}

#[cfg(test)]
mod tests {
    use nodedb_types::columnar::{ColumnDef, ColumnType, ColumnarSchema};
    use nodedb_types::value::Value;

    use super::*;

    fn test_schema() -> ColumnarSchema {
        ColumnarSchema::new(vec![
            ColumnDef::required("id", ColumnType::Int64).with_primary_key(),
            ColumnDef::required("name", ColumnType::String),
            ColumnDef::nullable("score", ColumnType::Float64),
        ])
        .expect("valid")
    }

    #[test]
    fn insert_and_pk_check() {
        let mut engine = MutationEngine::new("test".into(), test_schema());

        let result = engine
            .insert(&[
                Value::Integer(1),
                Value::String("Alice".into()),
                Value::Float(0.75),
            ])
            .expect("insert");

        assert_eq!(result.wal_records.len(), 1);
        assert!(matches!(
            &result.wal_records[0],
            ColumnarWalRecord::InsertRow { .. }
        ));

        assert_eq!(engine.pk_index().len(), 1);
        assert_eq!(engine.memtable().row_count(), 1);
    }

    #[test]
    fn delete_by_pk() {
        let mut engine = MutationEngine::new("test".into(), test_schema());

        engine
            .insert(&[
                Value::Integer(1),
                Value::String("Alice".into()),
                Value::Null,
            ])
            .expect("insert");

        let result = engine.delete(&Value::Integer(1)).expect("delete");
        assert_eq!(result.wal_records.len(), 1);
        assert!(matches!(
            &result.wal_records[0],
            ColumnarWalRecord::DeleteRows { .. }
        ));

        // PK should be removed from index.
        assert!(engine.pk_index().is_empty());
    }

    #[test]
    fn delete_nonexistent_pk() {
        let mut engine = MutationEngine::new("test".into(), test_schema());

        let err = engine.delete(&Value::Integer(999));
        assert!(matches!(err, Err(ColumnarError::PrimaryKeyNotFound)));
    }

    #[test]
    fn update_row() {
        let mut engine = MutationEngine::new("test".into(), test_schema());

        engine
            .insert(&[
                Value::Integer(1),
                Value::String("Alice".into()),
                Value::Float(0.5),
            ])
            .expect("insert");

        // Update: change name and score, keep same PK.
        let result = engine
            .update(
                &Value::Integer(1),
                &[
                    Value::Integer(1),
                    Value::String("Alice Updated".into()),
                    Value::Float(0.75),
                ],
            )
            .expect("update");

        // Should produce 2 WAL records: delete + insert.
        assert_eq!(result.wal_records.len(), 2);
        assert!(matches!(
            &result.wal_records[0],
            ColumnarWalRecord::DeleteRows { .. }
        ));
        assert!(matches!(
            &result.wal_records[1],
            ColumnarWalRecord::InsertRow { .. }
        ));

        // PK index should still have 1 entry.
        assert_eq!(engine.pk_index().len(), 1);
        // Memtable should have 2 rows (original + updated).
        assert_eq!(engine.memtable().row_count(), 2);
    }

    #[test]
    fn memtable_flush_remaps_pk() {
        let mut engine = MutationEngine::new("test".into(), test_schema());

        for i in 0..5 {
            engine
                .insert(&[
                    Value::Integer(i),
                    Value::String(format!("u{i}")),
                    Value::Null,
                ])
                .expect("insert");
        }

        // Simulate flush: memtable becomes segment 1.
        let result = engine.on_memtable_flushed(1);
        assert_eq!(result.wal_records.len(), 1);
        assert!(matches!(
            &result.wal_records[0],
            ColumnarWalRecord::MemtableFlushed {
                segment_id: 1,
                row_count: 5,
                ..
            }
        ));

        // PK index entries should now point to segment 1.
        let pk = encode_pk(&Value::Integer(3));
        let loc = engine.pk_index().get(&pk).expect("pk exists");
        assert_eq!(loc.segment_id, 1);
        assert_eq!(loc.row_index, 3);
    }

    #[test]
    fn multiple_inserts_and_deletes() {
        let mut engine = MutationEngine::new("test".into(), test_schema());

        for i in 0..10 {
            engine
                .insert(&[
                    Value::Integer(i),
                    Value::String(format!("u{i}")),
                    Value::Null,
                ])
                .expect("insert");
        }

        // Delete odd-numbered rows.
        for i in (1..10).step_by(2) {
            engine.delete(&Value::Integer(i)).expect("delete");
        }

        assert_eq!(engine.pk_index().len(), 5); // 0, 2, 4, 6, 8.
    }

    #[test]
    fn should_compact_threshold() {
        let mut engine = MutationEngine::new("test".into(), test_schema());

        // Insert and flush to create a real segment.
        for i in 0..10 {
            engine
                .insert(&[
                    Value::Integer(i),
                    Value::String(format!("u{i}")),
                    Value::Null,
                ])
                .expect("insert");
        }
        engine.on_memtable_flushed(1);

        // Delete 3 out of 10 rows = 30% > 20% threshold.
        for i in 0..3 {
            engine.delete(&Value::Integer(i)).expect("delete");
        }

        assert!(engine.should_compact(1, 10));
    }
}
