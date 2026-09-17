// SPDX-License-Identifier: BUSL-1.1

//! Schema evolution on a live [`ColumnarMemtable`].

use nodedb_types::timeseries::SymbolDictionary;

use super::super::types::{ColumnData, ColumnType};
use super::table::ColumnarMemtable;

impl ColumnarMemtable {
    /// Add a new column to the memtable schema, backfilling existing rows
    /// with NULL-equivalent values.
    ///
    /// Used for ILP schema evolution: when a new field appears in a later
    /// batch, the column is added and old rows get NaN/0/null-symbol.
    pub fn add_column(&mut self, name: String, col_type: ColumnType) {
        // Don't add duplicates.
        if self.schema.columns.iter().any(|(n, _)| n == &name) {
            return;
        }
        let existing_rows = self.row_count as usize;
        let col = match col_type {
            ColumnType::Float64 => ColumnData::Float64(vec![f64::NAN; existing_rows]),
            ColumnType::Int64 => ColumnData::Int64(vec![0; existing_rows]),
            ColumnType::Symbol => ColumnData::Symbol(vec![u32::MAX; existing_rows]),
            ColumnType::Timestamp(_) => return, // never add a second timestamp
        };
        let idx = self.columns.len();
        self.columns.push(col);
        self.schema.columns.push((name, col_type));
        self.schema.codecs.push(nodedb_codec::ColumnCodec::Auto);
        if col_type == ColumnType::Symbol {
            self.symbol_dicts.insert(idx, SymbolDictionary::new());
        }
    }
}
