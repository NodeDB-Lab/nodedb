// SPDX-License-Identifier: Apache-2.0

//! `ColumnarMemtable` struct, construction, and plain accessors.

use nodedb_types::columnar::{ColumnDef, ColumnType, ColumnarSchema};

use super::column_data::ColumnData;

/// Default flush threshold: 64K rows per memtable.
pub const DEFAULT_FLUSH_THRESHOLD: usize = 65_536;

/// In-memory columnar buffer that accumulates INSERTs.
///
/// Each column is stored as a typed vector. The memtable flushes to a
/// compressed segment when the row count reaches the threshold.
pub struct ColumnarMemtable {
    pub(super) schema: ColumnarSchema,
    pub(super) columns: Vec<ColumnData>,
    pub(super) row_count: usize,
    pub(super) flush_threshold: usize,
}

impl ColumnarMemtable {
    /// Create a new empty memtable for the given schema.
    pub fn new(schema: &ColumnarSchema) -> Self {
        Self::with_threshold(schema, DEFAULT_FLUSH_THRESHOLD)
    }

    /// Create with a custom flush threshold.
    pub fn with_threshold(schema: &ColumnarSchema, flush_threshold: usize) -> Self {
        let columns = schema
            .columns
            .iter()
            .map(|col| ColumnData::new(&col.column_type, col.nullable))
            .collect();
        Self {
            schema: schema.clone(),
            columns,
            row_count: 0,
            flush_threshold,
        }
    }

    /// Number of rows currently buffered.
    pub fn row_count(&self) -> usize {
        self.row_count
    }

    /// Whether the memtable has reached its flush threshold.
    pub fn should_flush(&self) -> bool {
        self.row_count >= self.flush_threshold
    }

    /// Row count at which [`Self::should_flush`] turns true.
    pub fn flush_threshold(&self) -> usize {
        self.flush_threshold
    }

    /// Whether the memtable is empty.
    pub fn is_empty(&self) -> bool {
        self.row_count == 0
    }

    /// Access the schema.
    pub fn schema(&self) -> &ColumnarSchema {
        &self.schema
    }

    /// Access the raw column data (for the segment writer).
    pub fn columns(&self) -> &[ColumnData] {
        &self.columns
    }

    /// Reconstruct a memtable directly from already-validated column data.
    ///
    /// Used by `MutationEngine::from_snapshot` to restore a memtable from a
    /// backup without re-running per-row validation. The caller must guarantee
    /// that `columns` is parallel to `schema.columns` and that every column
    /// contains exactly `row_count` rows. The flush threshold is set to the
    /// default value.
    pub(crate) fn from_raw_columns(
        schema: &ColumnarSchema,
        columns: Vec<ColumnData>,
        row_count: usize,
    ) -> Self {
        Self {
            schema: schema.clone(),
            columns,
            row_count,
            flush_threshold: DEFAULT_FLUSH_THRESHOLD,
        }
    }

    /// Add a new column to the schema, backfilling existing rows with nulls/defaults.
    pub fn add_column(&mut self, name: String, column_type: ColumnType, nullable: bool) {
        if self.schema.columns.iter().any(|c| c.name == name) {
            return;
        }

        let existing_rows = self.row_count;
        let mut col = ColumnData::new(&column_type, nullable);
        if existing_rows > 0 {
            col.backfill_nulls(existing_rows);
        }

        self.columns.push(col);
        let col_def = if nullable {
            ColumnDef::nullable(name, column_type)
        } else {
            ColumnDef::required(name, column_type)
        };
        self.schema.columns.push(col_def);
    }
}
