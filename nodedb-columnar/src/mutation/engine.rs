//! MutationEngine struct, constructor, accessors, and shared helpers.

use std::collections::HashMap;

use nodedb_types::columnar::ColumnarSchema;
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
    pub(super) delete_bitmaps: HashMap<u32, DeleteBitmap>,
    /// PK column indices in the schema.
    pub(super) pk_col_indices: Vec<usize>,
    /// Counter for assigning segment IDs.
    pub(super) next_segment_id: u32,
    /// Current "memtable segment ID" — a virtual segment ID for rows
    /// that are still in the memtable (not yet flushed).
    pub(super) memtable_segment_id: u32,
    /// Row counter within the current memtable (resets on flush).
    pub(super) memtable_row_counter: u32,
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

    /// The segment ID that will be assigned to the next flushed segment.
    ///
    /// Use this to obtain the ID to pass to `on_memtable_flushed`.
    pub fn next_segment_id(&self) -> u32 {
        self.next_segment_id
    }

    /// Whether a segment should be compacted based on its delete ratio.
    pub fn should_compact(&self, segment_id: u32, total_rows: u64) -> bool {
        self.delete_bitmaps
            .get(&segment_id)
            .is_some_and(|bm| bm.should_compact(total_rows, 0.2))
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
