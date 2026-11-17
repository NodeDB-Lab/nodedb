// SPDX-License-Identifier: BUSL-1.1

//! Schema evolution on a live [`ColumnarMemtable`].

use nodedb_types::timeseries::SymbolDictionary;

use super::super::types::{ColumnData, ColumnType, ColumnarSchema};
use super::table::ColumnarMemtable;

impl ColumnarMemtable {
    /// Add a new column to the memtable schema, backfilling existing rows
    /// with NULL-equivalent values.
    ///
    /// Used for ILP schema evolution: when a new field appears in a later
    /// batch, the column is added and old rows get its NULL cell
    /// ([`ColumnData::nulls`]).
    pub fn add_column(&mut self, name: String, col_type: ColumnType) {
        if !self.schema.evolve(name, col_type) {
            return;
        }
        let existing_rows = self.row_count as usize;
        let col = ColumnData::nulls(col_type, existing_rows);
        let idx = self.columns.len();
        self.columns.push(col);
        if col_type.is_dictionary() {
            self.symbol_dicts.insert(idx, SymbolDictionary::new());
        }
    }
}

impl ColumnarSchema {
    /// Add column `name` as a live memtable evolves its schema. A name the
    /// schema holds is not added again, and neither is a second time
    /// column. Returns whether the column was added.
    pub fn evolve(&mut self, name: String, col_type: ColumnType) -> bool {
        if col_type.is_time() || self.columns.iter().any(|(n, _)| n == &name) {
            return false;
        }
        self.columns.push((name, col_type));
        self.codecs.push(nodedb_codec::ColumnCodec::Auto);
        true
    }
}
