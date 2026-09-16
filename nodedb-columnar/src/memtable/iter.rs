// SPDX-License-Identifier: Apache-2.0

//! Row-oriented iteration and single-row lookup over the memtable.

use nodedb_types::columnar::ColumnDef;
use nodedb_types::value::Value;

use super::column_data::ColumnData;
use super::core::ColumnarMemtable;

impl ColumnarMemtable {
    /// Iterate rows as `Vec<Value>`. For scan/read operations.
    ///
    /// Every cell is typed by its column's declared type, so a time cell
    /// comes back as the instant variant the schema declares.
    pub fn iter_rows(&self) -> MemtableRowIter<'_> {
        MemtableRowIter {
            columns: &self.columns,
            column_defs: &self.schema.columns,
            row_count: self.row_count,
            current: 0,
        }
    }

    /// Get a single row by index as `Vec<Value>`.
    pub fn get_row(&self, row_idx: usize) -> Option<Vec<Value>> {
        if row_idx >= self.row_count {
            return None;
        }
        let mut row = Vec::with_capacity(self.columns.len());
        for (col, def) in self.columns.iter().zip(&self.schema.columns) {
            row.push(col.get_value(row_idx, &def.column_type));
        }
        Some(row)
    }
}

/// Row iterator over a columnar memtable.
pub struct MemtableRowIter<'a> {
    columns: &'a [ColumnData],
    column_defs: &'a [ColumnDef],
    row_count: usize,
    current: usize,
}

impl Iterator for MemtableRowIter<'_> {
    type Item = Vec<Value>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.current >= self.row_count {
            return None;
        }
        let mut row = Vec::with_capacity(self.columns.len());
        for (col, def) in self.columns.iter().zip(self.column_defs) {
            row.push(col.get_value(self.current, &def.column_type));
        }
        self.current += 1;
        Some(row)
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let remaining = self.row_count - self.current;
        (remaining, Some(remaining))
    }
}

impl ExactSizeIterator for MemtableRowIter<'_> {}

#[cfg(test)]
mod tests {
    use nodedb_types::NdbDateTime;
    use nodedb_types::columnar::{ColumnDef, ColumnType, ColumnarSchema};
    use nodedb_types::value::Value;

    use super::super::core::ColumnarMemtable;

    const MICROS: i64 = 1_583_402_400_000_000;

    /// A row read back from the memtable types each time cell by its declared
    /// column: `Timestamp` is a naive instant, `Timestamptz` a UTC instant,
    /// `SystemTimestamp` the integer stored.
    #[test]
    fn a_time_cell_reads_back_as_its_declared_type() {
        let schema = ColumnarSchema::new(vec![
            ColumnDef::required("id", ColumnType::Int64).with_primary_key(),
            ColumnDef::required("at", ColumnType::Timestamp),
            ColumnDef::required("at_tz", ColumnType::Timestamptz),
            ColumnDef::required("sys", ColumnType::SystemTimestamp),
        ])
        .expect("valid schema");
        let mut mt = ColumnarMemtable::new(&schema);
        let dt = NdbDateTime::from_micros(MICROS);
        mt.append_row(&[
            Value::Integer(1),
            Value::NaiveDateTime(dt),
            Value::DateTime(dt),
            Value::Integer(7),
        ])
        .expect("append");

        let expected = vec![
            Value::Integer(1),
            Value::NaiveDateTime(dt),
            Value::DateTime(dt),
            Value::Integer(7),
        ];
        assert_eq!(mt.get_row(0), Some(expected.clone()));
        assert_eq!(mt.iter_rows().collect::<Vec<_>>(), vec![expected]);
        assert_eq!(mt.get_row(1), None);
    }
}
