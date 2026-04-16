//! Columnar memtable: in-memory row buffer with typed column vectors.
//!
//! Each column is stored as a typed vector (Vec<i64>, Vec<f64>, etc.) rather
//! than Vec<Value> to avoid enum overhead and enable SIMD-friendly memory layout.
//! The memtable accumulates INSERTs and flushes to a segment when the row count
//! reaches the configured threshold.
//!
//! NOT thread-safe — lives on a single Data Plane core (!Send by design in Origin,
//! Mutex-wrapped in Lite).

mod column_data;

pub use column_data::{ColumnData, DICT_ENCODE_MAX_CARDINALITY};

use nodedb_types::columnar::{ColumnDef, ColumnType, ColumnarSchema};
use nodedb_types::value::Value;

use crate::error::ColumnarError;

/// Default flush threshold: 64K rows per memtable.
pub const DEFAULT_FLUSH_THRESHOLD: usize = 65_536;

/// In-memory columnar buffer that accumulates INSERTs.
///
/// Each column is stored as a typed vector. The memtable flushes to a
/// compressed segment when the row count reaches the threshold.
pub struct ColumnarMemtable {
    schema: ColumnarSchema,
    columns: Vec<ColumnData>,
    row_count: usize,
    flush_threshold: usize,
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

    /// Append a row of values. Validates types and nullability.
    pub fn append_row(&mut self, values: &[Value]) -> Result<(), ColumnarError> {
        if values.len() != self.schema.columns.len() {
            return Err(ColumnarError::SchemaMismatch {
                expected: self.schema.columns.len(),
                got: values.len(),
            });
        }

        for (i, (col_def, value)) in self.schema.columns.iter().zip(values.iter()).enumerate() {
            if matches!(value, Value::Null) && !col_def.nullable {
                return Err(ColumnarError::NullViolation(col_def.name.clone()));
            }
            self.columns[i].push(value, &col_def.name)?;
        }

        self.row_count += 1;
        debug_assert!(
            self.columns.iter().all(|c| c.len() == self.row_count),
            "column lengths must stay aligned with row_count"
        );
        Ok(())
    }

    /// Number of rows currently buffered.
    pub fn row_count(&self) -> usize {
        self.row_count
    }

    /// Whether the memtable has reached its flush threshold.
    pub fn should_flush(&self) -> bool {
        self.row_count >= self.flush_threshold
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

    /// Convert low-cardinality `String` columns to `DictEncoded` in-place.
    pub fn try_dict_encode_columns(&mut self, max_cardinality: u32) {
        for col in &mut self.columns {
            if let ColumnData::String { .. } = col
                && let Some(encoded) = ColumnData::try_dict_encode(col, max_cardinality)
            {
                *col = encoded;
            }
        }
    }

    /// Iterate rows as `Vec<Value>`. For scan/read operations.
    pub fn iter_rows(&self) -> MemtableRowIter<'_> {
        MemtableRowIter {
            columns: &self.columns,
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
        for col in &self.columns {
            row.push(col.get_value(row_idx));
        }
        Some(row)
    }

    /// Drain the memtable: return all column data and reset to empty.
    pub fn drain(&mut self) -> (ColumnarSchema, Vec<ColumnData>, usize) {
        let columns = std::mem::replace(
            &mut self.columns,
            self.schema
                .columns
                .iter()
                .map(|col| ColumnData::new(&col.column_type, col.nullable))
                .collect(),
        );
        let row_count = self.row_count;
        self.row_count = 0;
        (self.schema.clone(), columns, row_count)
    }

    /// Drain with automatic dictionary encoding for low-cardinality String columns.
    pub fn drain_optimized(&mut self) -> (ColumnarSchema, Vec<ColumnData>, usize) {
        self.try_dict_encode_columns(DICT_ENCODE_MAX_CARDINALITY);
        self.drain()
    }

    /// Zero-copy row ingest for timeseries and high-throughput paths.
    ///
    /// Accepts borrowed values via `IngestValue<'_>`, avoiding string cloning
    /// for tag columns that are already interned in the `DictEncoded` dictionary.
    pub fn ingest_row_refs(&mut self, values: &[IngestValue<'_>]) -> Result<(), ColumnarError> {
        if values.len() != self.schema.columns.len() {
            return Err(ColumnarError::SchemaMismatch {
                expected: self.schema.columns.len(),
                got: values.len(),
            });
        }

        for (i, (col_def, value)) in self.schema.columns.iter().zip(values.iter()).enumerate() {
            if matches!(value, IngestValue::Null) && !col_def.nullable {
                return Err(ColumnarError::NullViolation(col_def.name.clone()));
            }
            self.columns[i].push_ref(value, &col_def.name)?;
        }

        self.row_count += 1;
        Ok(())
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
        self.schema.columns.push(ColumnDef {
            name,
            column_type,
            nullable,
            default: None,
            primary_key: false,
            modifiers: Vec::new(),
            generated_expr: None,
            generated_deps: Vec::new(),
            added_at_version: 1,
        });
    }
}

/// Borrowed value for zero-copy ingest into the columnar memtable.
#[derive(Debug, Clone, Copy)]
pub enum IngestValue<'a> {
    Null,
    Int64(i64),
    Float64(f64),
    Bool(bool),
    Timestamp(i64),
    /// Borrowed string — for `String` or `DictEncoded` columns.
    Str(&'a str),
}

/// Row iterator over a columnar memtable.
pub struct MemtableRowIter<'a> {
    columns: &'a [ColumnData],
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
        for col in self.columns {
            row.push(col.get_value(self.current));
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
    use nodedb_types::columnar::{ColumnDef, ColumnType, ColumnarSchema};

    use super::*;

    fn test_schema() -> ColumnarSchema {
        ColumnarSchema::new(vec![
            ColumnDef::required("id", ColumnType::Int64).with_primary_key(),
            ColumnDef::required("name", ColumnType::String),
            ColumnDef::nullable("score", ColumnType::Float64),
        ])
        .expect("valid schema")
    }

    #[test]
    fn append_and_count() {
        let schema = test_schema();
        let mut mt = ColumnarMemtable::new(&schema);

        mt.append_row(&[
            Value::Integer(1),
            Value::String("Alice".into()),
            Value::Float(0.75),
        ])
        .expect("append");

        mt.append_row(&[Value::Integer(2), Value::String("Bob".into()), Value::Null])
            .expect("append");

        assert_eq!(mt.row_count(), 2);
        assert!(!mt.is_empty());
    }

    #[test]
    fn null_violation_rejected() {
        let schema = test_schema();
        let mut mt = ColumnarMemtable::new(&schema);

        let err = mt
            .append_row(&[Value::Null, Value::String("x".into()), Value::Null])
            .unwrap_err();
        assert!(matches!(err, ColumnarError::NullViolation(ref s) if s == "id"));
    }

    #[test]
    fn schema_mismatch_rejected() {
        let schema = test_schema();
        let mut mt = ColumnarMemtable::new(&schema);

        let err = mt.append_row(&[Value::Integer(1)]).unwrap_err();
        assert!(matches!(err, ColumnarError::SchemaMismatch { .. }));
    }

    #[test]
    fn flush_threshold() {
        let schema = test_schema();
        let mut mt = ColumnarMemtable::with_threshold(&schema, 3);

        for i in 0..2 {
            mt.append_row(&[
                Value::Integer(i),
                Value::String(format!("u{i}")),
                Value::Null,
            ])
            .expect("append");
        }
        assert!(!mt.should_flush());

        mt.append_row(&[Value::Integer(2), Value::String("u2".into()), Value::Null])
            .expect("append");
        assert!(mt.should_flush());
    }

    #[test]
    fn drain_resets() {
        let schema = test_schema();
        let mut mt = ColumnarMemtable::new(&schema);

        mt.append_row(&[
            Value::Integer(1),
            Value::String("x".into()),
            Value::Float(0.5),
        ])
        .expect("append");

        let (_schema, columns, row_count) = mt.drain();
        assert_eq!(row_count, 1);
        assert_eq!(columns.len(), 3);
        assert_eq!(mt.row_count(), 0);
        assert!(mt.is_empty());

        match &columns[0] {
            ColumnData::Int64 { values, valid } => {
                assert_eq!(values, &[1]);
                assert!(valid.is_none());
            }
            _ => panic!("expected Int64"),
        }
        match &columns[1] {
            ColumnData::String {
                data,
                offsets,
                valid,
            } => {
                assert_eq!(std::str::from_utf8(data).unwrap(), "x");
                assert_eq!(offsets, &[0, 1]);
                assert!(valid.is_none());
            }
            _ => panic!("expected String"),
        }
    }

    #[test]
    fn all_types() {
        let schema = ColumnarSchema::new(vec![
            ColumnDef::required("i", ColumnType::Int64),
            ColumnDef::required("f", ColumnType::Float64),
            ColumnDef::required("b", ColumnType::Bool),
            ColumnDef::required("ts", ColumnType::Timestamp),
            ColumnDef::required("s", ColumnType::String),
            ColumnDef::required("raw", ColumnType::Bytes),
            ColumnDef::required("vec", ColumnType::Vector(3)),
        ])
        .expect("valid");

        let mut mt = ColumnarMemtable::new(&schema);
        mt.append_row(&[
            Value::Integer(42),
            Value::Float(0.25),
            Value::Bool(true),
            Value::Integer(1_700_000_000),
            Value::String("hello".into()),
            Value::Bytes(vec![0xDE, 0xAD]),
            Value::Array(vec![
                Value::Float(1.0),
                Value::Float(2.0),
                Value::Float(3.0),
            ]),
        ])
        .expect("append all types");

        assert_eq!(mt.row_count(), 1);
    }

    #[test]
    fn dict_encode_low_cardinality() {
        let schema = ColumnarSchema::new(vec![ColumnDef::required("qtype", ColumnType::String)])
            .expect("valid");

        let mut mt = ColumnarMemtable::new(&schema);
        let qtypes = ["A", "B", "AAAA", "NS", "MX", "SOA", "CNAME", "PTR"];
        for _ in 0..10 {
            for &q in &qtypes {
                mt.append_row(&[Value::String(q.into())]).expect("append");
            }
        }
        assert_eq!(mt.row_count(), 80);

        mt.try_dict_encode_columns(DICT_ENCODE_MAX_CARDINALITY);

        let (_schema, columns, _row_count) = mt.drain();
        match &columns[0] {
            ColumnData::DictEncoded {
                ids,
                dictionary,
                valid,
                ..
            } => {
                assert_eq!(ids.len(), 80);
                assert!(valid.is_none());
                assert_eq!(dictionary.len(), 8);
                for &id in ids {
                    assert!((id as usize) < dictionary.len());
                }
                for (i, &q) in qtypes.iter().enumerate().take(8) {
                    let expected_id = dictionary.iter().position(|s| s == q).expect("in dict");
                    assert_eq!(ids[i], expected_id as u32);
                }
            }
            _ => panic!("expected DictEncoded after try_dict_encode_columns"),
        }
    }

    #[test]
    fn dict_encode_exceeds_cardinality_stays_string() {
        let schema = ColumnarSchema::new(vec![ColumnDef::required("name", ColumnType::String)])
            .expect("valid");

        let mut mt = ColumnarMemtable::new(&schema);
        let max: u32 = 4;
        for i in 0..=max {
            mt.append_row(&[Value::String(format!("val_{i}"))])
                .expect("append");
        }

        mt.try_dict_encode_columns(max);

        let (_schema, columns, _row_count) = mt.drain();
        assert!(matches!(columns[0], ColumnData::String { .. }));
    }

    #[test]
    fn dict_encode_with_nulls() {
        let schema = ColumnarSchema::new(vec![ColumnDef::nullable("tag", ColumnType::String)])
            .expect("valid");

        let mut mt = ColumnarMemtable::new(&schema);
        mt.append_row(&[Value::String("foo".into())])
            .expect("append");
        mt.append_row(&[Value::Null]).expect("append null");
        mt.append_row(&[Value::String("bar".into())])
            .expect("append");
        mt.append_row(&[Value::Null]).expect("append null");

        mt.try_dict_encode_columns(DICT_ENCODE_MAX_CARDINALITY);

        let (_schema, columns, _row_count) = mt.drain();
        match &columns[0] {
            ColumnData::DictEncoded {
                ids,
                valid,
                dictionary,
                ..
            } => {
                assert_eq!(ids.len(), 4);
                let v = valid.as_ref().expect("nullable column has validity bitmap");
                assert_eq!(v.len(), 4);
                assert!(v[0]);
                assert!(!v[1]);
                assert!(v[2]);
                assert!(!v[3]);
                assert_eq!(dictionary.len(), 2);
            }
            _ => panic!("expected DictEncoded"),
        }
    }
}
