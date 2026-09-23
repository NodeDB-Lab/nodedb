// SPDX-License-Identifier: Apache-2.0

//! MutationEngine struct, constructor, accessors, and shared helpers.

use std::collections::HashMap;

use nodedb_types::columnar::ColumnarSchema;
use nodedb_types::surrogate::Surrogate;
use nodedb_types::value::Value;

use crate::delete_bitmap::DeleteBitmap;
use crate::error::ColumnarError;
use crate::memtable::ColumnarMemtable;
use crate::pk_index::{PkIndex, encode_pk};
use crate::wal_record::ColumnarWalRecord;

/// Coordinates all columnar mutations for a single collection.
///
/// Owns the memtable, PK index, and per-segment delete bitmaps.
/// Produces WAL records for each mutation that the caller must persist.
pub struct MutationEngine {
    pub(super) collection: String,
    pub(super) schema: ColumnarSchema,
    pub(super) memtable: ColumnarMemtable,
    pub(super) pk_index: PkIndex,
    /// Per-segment delete bitmaps. Key = segment_id.
    pub(super) delete_bitmaps: HashMap<u64, DeleteBitmap>,
    /// PK column indices in the schema.
    pub(super) pk_col_indices: Vec<usize>,
    /// Counter for assigning segment IDs.
    pub(super) next_segment_id: u64,
    /// Current "memtable segment ID" — a virtual segment ID for rows
    /// that are still in the memtable (not yet flushed).
    pub(super) memtable_segment_id: u64,
    /// Row counter within the current memtable (resets on flush).
    pub(super) memtable_row_counter: u32,
    /// Per-row surrogate identities, parallel to the memtable rows.
    ///
    /// Entry `i` is the surrogate assigned to memtable row `i` at insert
    /// time, or `None` if no surrogate was supplied (test fixtures, legacy
    /// inserts). The vec is drained alongside the memtable on flush.
    pub(super) memtable_surrogates: Vec<Option<Surrogate>>,
}

/// Result of a mutation operation, including the WAL record to persist.
#[derive(Debug)]
pub struct MutationResult {
    /// WAL record(s) to persist before the mutation is considered durable.
    pub wal_records: Vec<ColumnarWalRecord>,
}

impl MutationEngine {
    /// Create a new mutation engine for a collection with the default flush threshold.
    pub fn new(collection: String, schema: ColumnarSchema) -> Self {
        Self::with_flush_threshold(collection, schema, crate::memtable::DEFAULT_FLUSH_THRESHOLD)
    }

    /// Create a new mutation engine with a custom memtable flush threshold.
    ///
    /// The threshold is clamped to a minimum of 1 to prevent a zero-threshold
    /// from causing unbounded flush loops. Use this when the node-level
    /// `QueryTuning::columnar_flush_threshold` config overrides the default.
    pub fn with_flush_threshold(
        collection: String,
        schema: ColumnarSchema,
        flush_threshold: usize,
    ) -> Self {
        let pk_col_indices: Vec<usize> = schema
            .columns
            .iter()
            .enumerate()
            .filter(|(_, c)| c.primary_key)
            .map(|(i, _)| i)
            .collect();

        let memtable = ColumnarMemtable::with_threshold(&schema, flush_threshold.max(1));
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
            memtable_surrogates: Vec::new(),
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
    pub fn delete_bitmap(&self, segment_id: u64) -> Option<&DeleteBitmap> {
        self.delete_bitmaps.get(&segment_id)
    }

    /// Mutable access to a segment's delete bitmap. Creates an empty one
    /// on first access so callers can `mark_deleted_batch` unconditionally.
    /// Used by temporal-purge paths that tombstone superseded row positions
    /// without going through the single-row `insert` / `delete` paths.
    pub fn delete_bitmap_mut(&mut self, segment_id: u64) -> &mut DeleteBitmap {
        self.delete_bitmaps.entry(segment_id).or_default()
    }

    /// The virtual segment id used for rows still in the memtable.
    pub fn memtable_segment_id(&self) -> u64 {
        self.memtable_segment_id
    }

    /// The schema's primary-key column indices, in schema order.
    pub fn pk_col_indices(&self) -> &[usize] {
        &self.pk_col_indices
    }

    /// Access all delete bitmaps.
    pub fn delete_bitmaps(&self) -> &HashMap<u64, DeleteBitmap> {
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

    /// Access the per-row surrogate table for the memtable.
    ///
    /// Index matches memtable row order; `None` entries indicate rows
    /// inserted without a surrogate (test fixtures, legacy paths).
    pub fn memtable_surrogates(&self) -> &[Option<Surrogate>] {
        &self.memtable_surrogates
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

    /// Iterate non-deleted rows paired with their surrogate identity.
    ///
    /// Yields `(Option<Surrogate>, Vec<Value>)`. The surrogate is `None`
    /// for rows inserted without one (test fixtures, legacy paths). Deleted
    /// rows are filtered out exactly as in [`Self::scan_memtable_rows`].
    pub fn scan_memtable_rows_with_surrogates(
        &self,
    ) -> impl Iterator<Item = (Option<Surrogate>, Vec<Value>)> + '_ {
        let deletes = self.delete_bitmaps.get(&self.memtable_segment_id);
        let surrogates = &self.memtable_surrogates;
        self.memtable
            .iter_rows()
            .enumerate()
            .filter_map(move |(row_idx, row)| {
                if deletes.is_some_and(|bm| bm.is_deleted(row_idx as u32)) {
                    return None;
                }
                let surrogate = surrogates.get(row_idx).copied().flatten();
                Some((surrogate, row))
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

    /// Roll back in-memory inserts to `row_count_before`.
    ///
    /// Undoes the effect of one or more inserts that appended rows starting
    /// at `row_count_before`. For each inserted row:
    /// - The corresponding PK entry is removed from the PK index.
    /// - If the insert displaced a prior row (upsert tombstone), that prior
    ///   row's PK index entry is restored and its tombstone bit cleared.
    ///
    /// The memtable is then truncated to `row_count_before`. Used exclusively
    /// by the transaction undo log; never called on the normal write path.
    pub fn rollback_memtable_inserts(
        &mut self,
        row_count_before: usize,
        inserted_pks: &[Vec<u8>],
        displaced: &[(Vec<u8>, crate::pk_index::RowLocation)],
    ) {
        // 1. Remove newly inserted PK entries.
        for pk in inserted_pks {
            self.pk_index.remove(pk);
        }
        // 2. Restore displaced prior-row PK entries and clear tombstones.
        for (pk, prior_location) in displaced {
            self.pk_index.upsert(pk.clone(), *prior_location);
            // Clear the tombstone bit that the insert set on the prior row.
            if let Some(bm) = self.delete_bitmaps.get_mut(&prior_location.segment_id) {
                bm.unmark_deleted(prior_location.row_index);
            }
        }
        // 3. Truncate memtable and surrogate list.
        self.memtable.truncate_to(row_count_before);
        self.memtable_surrogates.truncate(row_count_before);
        self.memtable_row_counter = row_count_before as u32;
    }

    /// Reverse one or more positional deletes: for each `(pk_bytes, location)`
    /// clear the row's delete-bitmap bit and re-bind the PK index to that
    /// location.
    ///
    /// Undoes the effect of [`Self::delete`] (and the delete-half of
    /// [`Self::update`]) so a rolled-back mutation leaves the affected rows
    /// present with their original values. Used exclusively by the transaction
    /// undo log; never called on the normal write path. Mirrors the
    /// displaced-row restore in [`Self::rollback_memtable_inserts`].
    pub fn restore_deleted_rows(&mut self, rows: &[(Vec<u8>, crate::pk_index::RowLocation)]) {
        for (pk_bytes, location) in rows {
            if let Some(bm) = self.delete_bitmaps.get_mut(&location.segment_id) {
                bm.unmark_deleted(location.row_index);
            }
            self.pk_index.upsert(pk_bytes.clone(), *location);
        }
    }

    /// The segment ID that will be assigned to the next flushed segment.
    ///
    /// Use this to obtain the ID to pass to `on_memtable_flushed`.
    pub fn next_segment_id(&self) -> u64 {
        self.next_segment_id
    }

    /// Encode a PK value as index bytes. Exposed for callers that need
    /// to probe the PK index (e.g. `ON CONFLICT DO UPDATE` routing).
    pub fn encode_pk_from_row(&self, values: &[Value]) -> Result<Vec<u8>, ColumnarError> {
        self.extract_pk_bytes(values)
    }

    // -- Internal helpers --

    /// Extract PK bytes from a row of values.
    pub(super) fn extract_pk_bytes(&self, values: &[Value]) -> Result<Vec<u8>, ColumnarError> {
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
    use nodedb_types::surrogate_bitmap::SurrogateBitmap;

    use super::*;

    fn minimal_schema() -> ColumnarSchema {
        ColumnarSchema {
            columns: vec![ColumnDef::required("id", ColumnType::Int64).with_primary_key()],
            version: 1,
        }
    }

    #[test]
    fn segment_id_allocator_returns_err_at_u64_max() {
        let mut engine = MutationEngine::new("test".to_string(), minimal_schema());
        engine.next_segment_id = u64::MAX;
        let result = engine.on_memtable_flushed(u64::MAX - 1);
        assert!(
            matches!(result, Err(ColumnarError::SegmentIdExhausted)),
            "expected SegmentIdExhausted, got: {result:?}"
        );
    }

    #[test]
    fn with_flush_threshold_triggers_should_flush_at_threshold() {
        use nodedb_types::value::Value;

        let schema = minimal_schema();
        // Threshold of 2: after 2 inserts the engine must report should_flush.
        let mut engine = MutationEngine::with_flush_threshold("col".to_string(), schema, 2);
        assert!(!engine.should_flush(), "empty engine should not need flush");

        engine.insert(&[Value::Integer(1)]).expect("insert row 1");
        assert!(!engine.should_flush(), "1 row below threshold of 2");

        engine.insert(&[Value::Integer(2)]).expect("insert row 2");
        assert!(
            engine.should_flush(),
            "2 rows at threshold of 2 must trigger flush"
        );
    }

    #[test]
    fn with_flush_threshold_zero_is_clamped_to_one() {
        use nodedb_types::value::Value;

        let schema = minimal_schema();
        let mut engine = MutationEngine::with_flush_threshold("col".to_string(), schema, 0);
        engine.insert(&[Value::Integer(1)]).expect("insert");
        // A zero threshold would be pathological; clamped to 1, so 1 row = flush.
        assert!(
            engine.should_flush(),
            "clamped-to-1 threshold: 1 row must flush"
        );
    }

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
                None,
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
        let result = engine.on_memtable_flushed(1).expect("flush");
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
    fn prefilter_row_boundary_membership() {
        // Insert 5 rows with surrogates 10, 20, 30, 40, 50.
        // A prefilter bitmap containing only {20, 40} should yield exactly
        // the two matching rows and skip the other three.
        let mut engine = MutationEngine::new("col".into(), test_schema());

        let surrogates = [10u32, 20, 30, 40, 50];
        for (i, &s) in surrogates.iter().enumerate() {
            engine
                .insert_with_surrogate(
                    &[
                        Value::Integer(i as i64),
                        Value::String(format!("r{i}")),
                        Value::Null,
                    ],
                    Surrogate(s),
                )
                .expect("insert_with_surrogate");
        }

        assert_eq!(engine.memtable_surrogates().len(), 5);

        let bitmap = SurrogateBitmap::from_iter([Surrogate(20), Surrogate(40)]);

        // Simulate block-boundary check: memtable surrogate range is [10, 50];
        // bitmap range is [20, 40] — they intersect, so block is NOT skipped.
        let surrogates_slice = engine.memtable_surrogates();
        let (mt_min, mt_max) = surrogates_slice
            .iter()
            .flatten()
            .fold((u32::MAX, u32::MIN), |(lo, hi), s| {
                (lo.min(s.0), hi.max(s.0))
            });
        assert_eq!(mt_min, 10);
        assert_eq!(mt_max, 50);
        let bm_min = bitmap.0.min().unwrap();
        let bm_max = bitmap.0.max().unwrap();
        // Ranges overlap — block must NOT be skipped.
        assert!(!(bm_max < mt_min || bm_min > mt_max));

        // Row-boundary: count rows that pass the bitmap membership check.
        let passing: Vec<_> = engine
            .scan_memtable_rows_with_surrogates()
            .filter(|(sur, _)| sur.is_some_and(|s| bitmap.contains(s)))
            .collect();
        assert_eq!(passing.len(), 2);
        // The two matched rows should be at surrogate positions 20 and 40 (indices 1 and 3).
        assert!(passing[0].0 == Some(Surrogate(20)));
        assert!(passing[1].0 == Some(Surrogate(40)));
    }

    #[test]
    fn prefilter_block_boundary_skip() {
        // Insert rows with surrogates 100, 200, 300.
        // A prefilter bitmap containing only {10, 20} (below the memtable range)
        // should cause the block-boundary check to flag a skip.
        let mut engine = MutationEngine::new("col".into(), test_schema());

        for (i, s) in [100u32, 200, 300].iter().enumerate() {
            engine
                .insert_with_surrogate(
                    &[
                        Value::Integer(i as i64),
                        Value::String(format!("r{i}")),
                        Value::Null,
                    ],
                    Surrogate(*s),
                )
                .expect("insert_with_surrogate");
        }

        let surrogates_slice = engine.memtable_surrogates();
        let (mt_min, mt_max) = surrogates_slice
            .iter()
            .flatten()
            .fold((u32::MAX, u32::MIN), |(lo, hi), s| {
                (lo.min(s.0), hi.max(s.0))
            });
        assert_eq!(mt_min, 100);
        assert_eq!(mt_max, 300);

        let bitmap = SurrogateBitmap::from_iter([Surrogate(10), Surrogate(20)]);
        let bm_min = bitmap.0.min().unwrap();
        let bm_max = bitmap.0.max().unwrap();

        // bm_max (20) < mt_min (100) → disjoint ranges → block MUST be skipped.
        assert!(bm_max < mt_min || bm_min > mt_max);
    }

    #[test]
    fn insert_without_surrogate_stores_none() {
        let mut engine = MutationEngine::new("col".into(), test_schema());
        engine
            .insert(&[Value::Integer(1), Value::String("x".into()), Value::Null])
            .expect("insert");
        assert_eq!(engine.memtable_surrogates(), &[None]);
    }

    #[test]
    fn an_update_keeps_the_old_rows_surrogate() {
        let mut engine = MutationEngine::new("col".into(), test_schema());
        engine
            .insert_with_surrogate(
                &[Value::Integer(1), Value::String("x".into()), Value::Null],
                Surrogate(42),
            )
            .expect("insert");
        engine
            .update(
                &Value::Integer(1),
                &[Value::Integer(2), Value::String("y".into()), Value::Null],
                Some(Surrogate(7)),
            )
            .expect("update");
        let live: Vec<Option<Surrogate>> = engine
            .scan_memtable_rows_with_surrogates()
            .map(|(surrogate, _)| surrogate)
            .collect();
        assert_eq!(
            live,
            vec![Some(Surrogate(42))],
            "the replacement row carries the identity the memtable row had"
        );
    }

    #[test]
    fn an_update_of_a_flushed_row_carries_the_segment_surrogate() {
        let mut engine = MutationEngine::new("col".into(), test_schema());
        engine
            .insert_with_surrogate(
                &[Value::Integer(1), Value::String("x".into()), Value::Null],
                Surrogate(42),
            )
            .expect("insert");
        let segment_id = engine.next_segment_id();
        let _drained = engine.memtable_mut().drain_optimized();
        engine.on_memtable_flushed(segment_id).expect("flush");
        engine
            .update(
                &Value::Integer(1),
                &[Value::Integer(1), Value::String("y".into()), Value::Null],
                Some(Surrogate(42)),
            )
            .expect("update");
        let live: Vec<Option<Surrogate>> = engine
            .scan_memtable_rows_with_surrogates()
            .map(|(surrogate, _)| surrogate)
            .collect();
        assert_eq!(
            live,
            vec![Some(Surrogate(42))],
            "the replacement row carries the identity the flushed row had"
        );
    }

    #[test]
    fn flush_clears_surrogate_table() {
        let mut engine = MutationEngine::new("col".into(), test_schema());
        engine
            .insert_with_surrogate(
                &[Value::Integer(1), Value::String("x".into()), Value::Null],
                Surrogate(42),
            )
            .expect("insert");
        assert_eq!(engine.memtable_surrogates().len(), 1);
        engine.on_memtable_flushed(1).expect("flush");
        assert!(engine.memtable_surrogates().is_empty());
    }

    /// Every WAL record a mutation can produce must be one the replay path can
    /// act on. A segment-rewrite ("compaction commit") record has no producer and
    /// no replay handler, so the mutation engine must never emit one.
    #[test]
    fn mutations_only_emit_replayable_wal_records() {
        let mut engine = MutationEngine::new("test".into(), test_schema());

        let mut records = Vec::new();
        for i in 0..10 {
            records.extend(
                engine
                    .insert(&[
                        Value::Integer(i),
                        Value::String(format!("u{i}")),
                        Value::Null,
                    ])
                    .expect("insert")
                    .wal_records,
            );
        }
        records.extend(engine.on_memtable_flushed(1).expect("flush").wal_records);
        for i in 0..3 {
            records.extend(
                engine
                    .delete(&Value::Integer(i))
                    .expect("delete")
                    .wal_records,
            );
        }

        assert!(!records.is_empty());
        for rec in &records {
            assert!(
                matches!(
                    rec,
                    ColumnarWalRecord::InsertRow { .. }
                        | ColumnarWalRecord::DeleteRows { .. }
                        | ColumnarWalRecord::MemtableFlushed { .. }
                ),
                "unexpected WAL record shape: {rec:?}"
            );
        }
    }

    /// Both segment counters at `u64::MAX` must fail the flush, not wrap.
    #[test]
    fn segment_id_allocator_returns_err_when_both_counters_are_exhausted() {
        let mut engine = MutationEngine::new("test".into(), test_schema());

        engine.next_segment_id = u64::MAX;
        engine.memtable_segment_id = u64::MAX;

        let err = engine
            .on_memtable_flushed(u64::MAX)
            .expect_err("should be exhausted");
        assert!(
            matches!(err, ColumnarError::SegmentIdExhausted),
            "expected SegmentIdExhausted, got {err:?}"
        );
    }
}
