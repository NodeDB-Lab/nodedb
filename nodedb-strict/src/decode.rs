// SPDX-License-Identifier: Apache-2.0

//! Binary Tuple decoder: O(1) field extraction from tuple bytes.
//!
//! Given a schema and a column index, computes the byte offset and extracts
//! the field value without parsing any other column. This is the core
//! performance advantage over self-describing formats like MessagePack/BSON.

use nodedb_types::columnar::{ColumnType, SchemaOps, StrictSchema};

use crate::encode::{FORMAT_VERSION, MAGIC};
use nodedb_types::value::Value;

use crate::error::StrictError;

#[path = "decode/value.rs"]
mod value_decode;
use value_decode::{decode_fixed_value, decode_variable_value};

/// Decodes fields from Binary Tuples according to a fixed schema.
///
/// Reusable: create once per schema, decode many tuples. Precomputes
/// byte offsets for O(1) field access.
pub struct TupleDecoder {
    schema: StrictSchema,
    /// Byte offset of each fixed-size column within the fixed section.
    /// Variable-length columns get `None`.
    fixed_offsets: Vec<Option<usize>>,
    /// Total size of the fixed-fields section.
    fixed_section_size: usize,
    /// For each schema column: if it's variable-length, its index in the
    /// offset table (0-based among variable columns). Otherwise `None`.
    var_table_index: Vec<Option<usize>>,
    /// Number of variable-length columns.
    var_count: usize,
    /// Size of the tuple header: 4 (version) + null_bitmap_size.
    header_size: usize,
    /// Whether schema-derived layout arithmetic fit in `usize`.
    layout_valid: bool,
}

impl TupleDecoder {
    /// Create a decoder for the given schema.
    pub fn new(schema: &StrictSchema) -> Self {
        let mut fixed_offsets = Vec::with_capacity(schema.columns.len());
        let mut var_table_index = Vec::with_capacity(schema.columns.len());
        let mut fixed_offset = 0usize;
        let mut var_idx = 0usize;
        let mut layout_valid = true;

        for col in &schema.columns {
            if let Some(size) = col.column_type.fixed_size() {
                fixed_offsets.push(Some(fixed_offset));
                var_table_index.push(None);
                match fixed_offset.checked_add(size) {
                    Some(next) => fixed_offset = next,
                    None => layout_valid = false,
                }
            } else {
                fixed_offsets.push(None);
                var_table_index.push(Some(var_idx));
                match var_idx.checked_add(1) {
                    Some(next) => var_idx = next,
                    None => layout_valid = false,
                }
            }
        }

        // Header: magic(4) + format_version(1) + schema_version(4) + null_bitmap.
        let header_size = match 9usize.checked_add(schema.null_bitmap_size()) {
            Some(size) => size,
            None => {
                layout_valid = false;
                0
            }
        };

        Self {
            schema: schema.clone(),
            fixed_offsets,
            fixed_section_size: fixed_offset,
            var_table_index,
            var_count: var_idx,
            header_size,
            layout_valid,
        }
    }

    /// Read and validate the header, then return the schema version.
    ///
    /// Validates magic bytes at [0..4] and format version at [4] before
    /// returning the schema version at [5..9].
    pub fn schema_version(&self, tuple: &[u8]) -> Result<u32, StrictError> {
        if tuple.len() < 9 {
            return Err(StrictError::TruncatedTuple {
                expected: 9,
                got: tuple.len(),
            });
        }
        let got_magic = u32::from_le_bytes([tuple[0], tuple[1], tuple[2], tuple[3]]);
        if got_magic != MAGIC {
            return Err(StrictError::InvalidMagic {
                expected: MAGIC,
                got: got_magic,
            });
        }
        let got_version = tuple[4];
        if got_version != FORMAT_VERSION {
            return Err(StrictError::InvalidFormatVersion {
                expected: FORMAT_VERSION,
                got: got_version,
            });
        }
        Ok(u32::from_le_bytes([tuple[5], tuple[6], tuple[7], tuple[8]]))
    }

    /// Check whether column `col_idx` is null in the given tuple.
    pub fn is_null(&self, tuple: &[u8], col_idx: usize) -> Result<bool, StrictError> {
        self.check_bounds(col_idx)?;
        self.check_min_size(tuple)?;

        let bitmap_offset = 9usize
            .checked_add(col_idx / 8)
            .ok_or_else(|| self.corrupt_layout())?;
        let bitmap_byte = tuple
            .get(bitmap_offset)
            .copied()
            .ok_or(StrictError::TruncatedTuple {
                expected: bitmap_offset
                    .checked_add(1)
                    .ok_or_else(|| self.corrupt_layout())?,
                got: tuple.len(),
            })?;
        Ok(bitmap_byte & (1 << (col_idx % 8)) != 0)
    }

    /// Extract raw bytes for a fixed-size column. Returns `None` if null.
    ///
    /// This is the O(1) fast path: a single bounds check + pointer slice.
    pub fn extract_fixed_raw<'a>(
        &self,
        tuple: &'a [u8],
        col_idx: usize,
    ) -> Result<Option<&'a [u8]>, StrictError> {
        self.check_bounds(col_idx)?;
        self.check_min_size(tuple)?;

        if self.is_null_unchecked(tuple, col_idx) {
            return Ok(None);
        }

        let offset = self.fixed_offsets[col_idx].ok_or(StrictError::TypeMismatch {
            column: self.schema.columns[col_idx].name.clone(),
            expected: self.schema.columns[col_idx].column_type,
        })?;

        let size = self.schema.columns[col_idx]
            .column_type
            .fixed_size()
            .ok_or(StrictError::TypeMismatch {
                column: self.schema.columns[col_idx].name.clone(),
                expected: self.schema.columns[col_idx].column_type,
            })?;
        let start = self.checked_layout_add(self.header_size, offset)?;
        let end = self.checked_layout_add(start, size)?;
        let raw = tuple.get(start..end).ok_or(StrictError::TruncatedTuple {
            expected: end,
            got: tuple.len(),
        })?;

        Ok(Some(raw))
    }

    /// Extract raw bytes for a variable-length column. Returns `None` if null.
    ///
    /// Reads two entries from the offset table to determine start and length.
    pub fn extract_variable_raw<'a>(
        &self,
        tuple: &'a [u8],
        col_idx: usize,
    ) -> Result<Option<&'a [u8]>, StrictError> {
        self.check_bounds(col_idx)?;
        self.check_min_size(tuple)?;

        if self.is_null_unchecked(tuple, col_idx) {
            return Ok(None);
        }

        let var_idx = self.var_table_index[col_idx].ok_or(StrictError::TypeMismatch {
            column: self.schema.columns[col_idx].name.clone(),
            expected: self.schema.columns[col_idx].column_type,
        })?;

        let table_start = self.checked_layout_add(self.header_size, self.fixed_section_size)?;
        let entry_offset = var_idx
            .checked_mul(4)
            .ok_or_else(|| self.corrupt_layout())?;
        let entry_pos = self.checked_layout_add(table_start, entry_offset)?;
        let next_pos = self.checked_layout_add(entry_pos, 4)?;
        let table_end = self.checked_layout_add(next_pos, 4)?;

        let entry = tuple
            .get(entry_pos..next_pos)
            .ok_or(StrictError::TruncatedTuple {
                expected: table_end,
                got: tuple.len(),
            })?;
        let next_entry = tuple
            .get(next_pos..table_end)
            .ok_or(StrictError::TruncatedTuple {
                expected: table_end,
                got: tuple.len(),
            })?;
        let offset = u32::from_le_bytes(entry.try_into().map_err(|_| self.corrupt_layout())?);
        let next_offset =
            u32::from_le_bytes(next_entry.try_into().map_err(|_| self.corrupt_layout())?);

        let table_entries = self
            .var_count
            .checked_add(1)
            .and_then(|count| count.checked_mul(4))
            .ok_or_else(|| self.corrupt_layout())?;
        let var_data_start = self.checked_layout_add(table_start, table_entries)?;
        let abs_start = self.checked_layout_add(
            var_data_start,
            usize::try_from(offset).map_err(|_| self.corrupt_offset(offset, tuple.len()))?,
        )?;
        let abs_end = self.checked_layout_add(
            var_data_start,
            usize::try_from(next_offset)
                .map_err(|_| self.corrupt_offset(next_offset, tuple.len()))?,
        )?;

        if abs_start > abs_end || abs_end > tuple.len() {
            return Err(self.corrupt_offset(next_offset, tuple.len()));
        }

        tuple
            .get(abs_start..abs_end)
            .map(Some)
            .ok_or_else(|| self.corrupt_offset(next_offset, tuple.len()))
    }

    /// Extract a column value as a `Value`, performing type-aware decoding.
    ///
    /// This is the general-purpose extraction path. For hot paths, prefer
    /// `extract_fixed_raw` / `extract_variable_raw` to avoid `Value` allocation.
    pub fn extract_value(&self, tuple: &[u8], col_idx: usize) -> Result<Value, StrictError> {
        self.check_bounds(col_idx)?;

        if self.is_null(tuple, col_idx)? {
            return Ok(Value::Null);
        }

        let col = &self.schema.columns[col_idx];

        if col.column_type.fixed_size().is_some() {
            let raw = self
                .extract_fixed_raw(tuple, col_idx)?
                .ok_or(StrictError::TypeMismatch {
                    column: col.name.clone(),
                    expected: col.column_type,
                })?;
            Ok(decode_fixed_value(&col.column_type, raw))
        } else {
            let raw =
                self.extract_variable_raw(tuple, col_idx)?
                    .ok_or(StrictError::TypeMismatch {
                        column: col.name.clone(),
                        expected: col.column_type,
                    })?;
            decode_variable_value(&col.column_type, raw)
        }
    }

    /// Extract all columns from a tuple into a Vec<Value>.
    pub fn extract_all(&self, tuple: &[u8]) -> Result<Vec<Value>, StrictError> {
        let mut values = Vec::with_capacity(self.schema.columns.len());
        for i in 0..self.schema.columns.len() {
            values.push(self.extract_value(tuple, i)?);
        }
        Ok(values)
    }

    /// Extract a column by name.
    pub fn extract_by_name(&self, tuple: &[u8], name: &str) -> Result<Value, StrictError> {
        let idx = self
            .schema
            .column_index(name)
            .ok_or(StrictError::ColumnOutOfRange {
                index: usize::MAX,
                count: self.schema.columns.len(),
            })?;
        self.extract_value(tuple, idx)
    }

    /// Decode a tuple written with an older schema version.
    ///
    /// Columns present in the old schema are extracted normally. Columns added
    /// in newer schema versions return their declared default value, or
    /// `Value::Null` for nullable columns without a default.
    ///
    /// `old_col_count` is the number of columns in the schema version that
    /// wrote this tuple.
    ///
    /// Correctness invariant: a non-nullable column added via
    /// `ALTER ADD COLUMN ... NOT NULL DEFAULT <expr>` will always return the
    /// materialized default, never `Value::Null`. The ALTER path rejects
    /// `NOT NULL` without a `DEFAULT`, so every non-nullable column in the
    /// schema must have `col.default` populated.
    pub fn extract_value_versioned(
        &self,
        tuple: &[u8],
        col_idx: usize,
        old_col_count: usize,
    ) -> Result<Value, StrictError> {
        self.check_bounds(col_idx)?;

        if col_idx >= old_col_count {
            // Column was added after this tuple was written: materialize the
            // declared default, or null for nullable columns without a default.
            let col = &self.schema.columns[col_idx];
            let value = col
                .default
                .as_deref()
                .map(nodedb_types::columnar::StrictSchema::parse_default_literal)
                .unwrap_or(Value::Null);
            return Ok(value);
        }

        self.extract_value(tuple, col_idx)
    }

    /// Access the schema this decoder was built for.
    pub fn schema(&self) -> &StrictSchema {
        &self.schema
    }

    /// Extract the three bitemporal timestamps from a tuple:
    /// `(system_from, valid_from_ms, valid_until_ms)`. Only valid for
    /// schemas constructed with `StrictSchema::new_bitemporal`.
    pub fn extract_bitemporal_timestamps(
        &self,
        tuple: &[u8],
    ) -> Result<(i64, i64, i64), StrictError> {
        if !self.schema.bitemporal {
            return Err(StrictError::ColumnOutOfRange {
                index: 0,
                count: self.schema.columns.len(),
            });
        }
        let sys = extract_i64(self, tuple, 0)?;
        let vf = extract_i64(self, tuple, 1)?;
        let vu = extract_i64(self, tuple, 2)?;
        Ok((sys, vf, vu))
    }

    /// Byte offset where fixed-field section starts.
    pub fn fixed_section_start(&self) -> usize {
        self.header_size
    }

    /// Byte offset where the variable offset table starts.
    pub fn offset_table_start(&self) -> Result<usize, StrictError> {
        self.checked_layout_add(self.header_size, self.fixed_section_size)
    }

    /// Byte offset where variable data starts.
    pub fn var_data_start(&self) -> Result<usize, StrictError> {
        let entry_bytes = self
            .var_count
            .checked_add(1)
            .and_then(|count| count.checked_mul(4))
            .ok_or_else(|| self.corrupt_layout())?;
        self.checked_layout_add(self.offset_table_start()?, entry_bytes)
    }

    /// Number of variable-length columns in the schema.
    pub fn var_count(&self) -> usize {
        self.var_count
    }

    /// Byte offset and size for a fixed column (relative to tuple start).
    /// Returns `None` if the column is variable-length.
    pub fn fixed_field_location(&self, col_idx: usize) -> Option<(usize, usize)> {
        let offset = self.fixed_offsets.get(col_idx).copied().flatten()?;
        let size = self.schema.columns[col_idx].column_type.fixed_size()?;
        self.header_size
            .checked_add(offset)
            .map(|start| (start, size))
    }

    /// Index in the variable offset table for a column.
    /// Returns `None` if the column is fixed-size.
    pub fn var_field_index(&self, col_idx: usize) -> Option<usize> {
        self.var_table_index.get(col_idx).copied().flatten()
    }

    // -- Internal helpers --

    fn check_bounds(&self, col_idx: usize) -> Result<(), StrictError> {
        if col_idx >= self.schema.columns.len() {
            Err(StrictError::ColumnOutOfRange {
                index: col_idx,
                count: self.schema.columns.len(),
            })
        } else {
            Ok(())
        }
    }

    fn check_min_size(&self, tuple: &[u8]) -> Result<(), StrictError> {
        if !self.layout_valid {
            return Err(self.corrupt_layout());
        }
        let min = self.header_size;
        if tuple.len() < min {
            Err(StrictError::TruncatedTuple {
                expected: min,
                got: tuple.len(),
            })
        } else {
            Ok(())
        }
    }

    fn is_null_unchecked(&self, tuple: &[u8], col_idx: usize) -> bool {
        let bitmap_offset = 9usize.checked_add(col_idx / 8);
        let bitmap_byte = bitmap_offset
            .and_then(|offset| tuple.get(offset))
            .copied()
            .unwrap_or(0);
        bitmap_byte & (1 << (col_idx % 8)) != 0
    }

    fn checked_layout_add(&self, start: usize, len: usize) -> Result<usize, StrictError> {
        start.checked_add(len).ok_or_else(|| self.corrupt_layout())
    }

    fn corrupt_layout(&self) -> StrictError {
        StrictError::CorruptOffset {
            offset: u32::MAX,
            len: 0,
        }
    }

    fn corrupt_offset(&self, offset: u32, tuple_len: usize) -> StrictError {
        StrictError::CorruptOffset {
            offset,
            len: tuple_len,
        }
    }
}

/// Extract a fixed Int64 column as a raw i64.
fn extract_i64(decoder: &TupleDecoder, tuple: &[u8], col_idx: usize) -> Result<i64, StrictError> {
    let raw = decoder
        .extract_fixed_raw(tuple, col_idx)?
        .ok_or(StrictError::TypeMismatch {
            column: decoder.schema.columns[col_idx].name.clone(),
            expected: ColumnType::Int64,
        })?;
    Ok(i64::from_le_bytes([
        raw[0], raw[1], raw[2], raw[3], raw[4], raw[5], raw[6], raw[7],
    ]))
}

#[cfg(test)]
mod tests {
    use nodedb_types::columnar::ColumnDef;
    use nodedb_types::datetime::NdbDateTime;

    use super::*;
    use crate::encode::TupleEncoder;

    fn crm_schema() -> StrictSchema {
        StrictSchema::new(vec![
            ColumnDef::required("id", ColumnType::Int64).with_primary_key(),
            ColumnDef::required("name", ColumnType::String),
            ColumnDef::nullable("email", ColumnType::String),
            ColumnDef::required(
                "balance",
                ColumnType::Decimal(Some(
                    nodedb_types::columnar::DecimalTypmod::new(18, 4).expect("valid typmod"),
                )),
            ),
            ColumnDef::nullable("active", ColumnType::Bool),
        ])
        .unwrap()
    }

    fn encode_crm_row(values: &[Value]) -> Vec<u8> {
        let schema = crm_schema();
        TupleEncoder::new(&schema).encode(values).unwrap()
    }

    #[test]
    fn roundtrip_all_fields() {
        let schema = crm_schema();
        let encoder = TupleEncoder::new(&schema);
        let decoder = TupleDecoder::new(&schema);

        let values = vec![
            Value::Integer(42),
            Value::String("Alice".into()),
            Value::String("alice@example.com".into()),
            Value::Decimal(rust_decimal::Decimal::new(5000, 2)),
            Value::Bool(true),
        ];

        let tuple = encoder.encode(&values).unwrap();
        let decoded = decoder.extract_all(&tuple).unwrap();

        assert_eq!(decoded[0], Value::Integer(42));
        assert_eq!(decoded[1], Value::String("Alice".into()));
        assert_eq!(decoded[2], Value::String("alice@example.com".into()));
        assert_eq!(
            decoded[3],
            Value::Decimal(rust_decimal::Decimal::new(5000, 2))
        );
        assert_eq!(decoded[4], Value::Bool(true));
    }

    #[test]
    fn roundtrip_with_nulls() {
        let schema = crm_schema();
        let encoder = TupleEncoder::new(&schema);
        let decoder = TupleDecoder::new(&schema);

        let values = vec![
            Value::Integer(1),
            Value::String("Bob".into()),
            Value::Null,
            Value::Decimal(rust_decimal::Decimal::ZERO),
            Value::Null,
        ];

        let tuple = encoder.encode(&values).unwrap();
        let decoded = decoder.extract_all(&tuple).unwrap();

        assert_eq!(decoded[0], Value::Integer(1));
        assert_eq!(decoded[1], Value::String("Bob".into()));
        assert_eq!(decoded[2], Value::Null);
        assert_eq!(decoded[3], Value::Decimal(rust_decimal::Decimal::ZERO));
        assert_eq!(decoded[4], Value::Null);
    }

    #[test]
    fn o1_extraction_single_field() {
        let schema = crm_schema();
        let decoder = TupleDecoder::new(&schema);

        let tuple = encode_crm_row(&[
            Value::Integer(99),
            Value::String("Charlie".into()),
            Value::String("charlie@co.com".into()),
            Value::Decimal(rust_decimal::Decimal::new(12345, 0)),
            Value::Bool(false),
        ]);

        // Extract just the balance (column 3) without touching other columns.
        let balance = decoder.extract_value(&tuple, 3).unwrap();
        assert_eq!(
            balance,
            Value::Decimal(rust_decimal::Decimal::new(12345, 0))
        );

        // Extract just the name (column 1) — variable-length.
        let name = decoder.extract_value(&tuple, 1).unwrap();
        assert_eq!(name, Value::String("Charlie".into()));
    }

    #[test]
    fn extract_by_name() {
        let schema = crm_schema();
        let decoder = TupleDecoder::new(&schema);

        let tuple = encode_crm_row(&[
            Value::Integer(7),
            Value::String("Dana".into()),
            Value::Null,
            Value::Decimal(rust_decimal::Decimal::new(999, 1)),
            Value::Bool(true),
        ]);

        assert_eq!(
            decoder.extract_by_name(&tuple, "name").unwrap(),
            Value::String("Dana".into())
        );
        assert_eq!(
            decoder.extract_by_name(&tuple, "email").unwrap(),
            Value::Null
        );
    }

    #[test]
    fn null_bitmap_check() {
        let schema = crm_schema();
        let decoder = TupleDecoder::new(&schema);

        let tuple = encode_crm_row(&[
            Value::Integer(1),
            Value::String("x".into()),
            Value::Null,
            Value::Decimal(rust_decimal::Decimal::ZERO),
            Value::Null,
        ]);

        assert!(!decoder.is_null(&tuple, 0).unwrap()); // id
        assert!(!decoder.is_null(&tuple, 1).unwrap()); // name
        assert!(decoder.is_null(&tuple, 2).unwrap()); // email
        assert!(!decoder.is_null(&tuple, 3).unwrap()); // balance
        assert!(decoder.is_null(&tuple, 4).unwrap()); // active
    }

    #[test]
    fn column_out_of_range() {
        let schema = crm_schema();
        let decoder = TupleDecoder::new(&schema);
        let tuple = encode_crm_row(&[
            Value::Integer(1),
            Value::String("x".into()),
            Value::Null,
            Value::Decimal(rust_decimal::Decimal::ZERO),
            Value::Null,
        ]);

        let err = decoder.extract_value(&tuple, 99).unwrap_err();
        assert!(matches!(
            err,
            StrictError::ColumnOutOfRange { index: 99, .. }
        ));
    }

    #[test]
    fn schema_version_read() {
        let schema = crm_schema();
        let decoder = TupleDecoder::new(&schema);
        let tuple = encode_crm_row(&[
            Value::Integer(1),
            Value::String("x".into()),
            Value::Null,
            Value::Decimal(rust_decimal::Decimal::ZERO),
            Value::Null,
        ]);

        assert_eq!(decoder.schema_version(&tuple).unwrap(), 1);
    }

    #[test]
    fn schema_version_u32_no_truncation() {
        // Verify that a schema version above u16::MAX (0x0001_0000 = 65536) encodes
        // and decodes without truncation — the u16 ceiling bug this test guards against.
        let mut schema = crm_schema();
        schema.version = 0x0001_0000;
        let encoder = TupleEncoder::new(&schema);
        let decoder = TupleDecoder::new(&schema);

        let tuple = encoder
            .encode(&[
                Value::Integer(1),
                Value::String("test".into()),
                Value::Null,
                Value::Decimal(rust_decimal::Decimal::ZERO),
                Value::Null,
            ])
            .unwrap();

        let decoded_version = decoder.schema_version(&tuple).unwrap();
        assert_eq!(
            decoded_version, 0x0001_0000u32,
            "schema_version must not truncate to u16"
        );
    }

    #[test]
    fn versioned_extraction_new_column_returns_null() {
        let schema = crm_schema();
        let decoder = TupleDecoder::new(&schema);

        // Tuple was written with only 3 columns (older schema).
        let old_schema = StrictSchema::new(vec![
            ColumnDef::required("id", ColumnType::Int64).with_primary_key(),
            ColumnDef::required("name", ColumnType::String),
            ColumnDef::nullable("email", ColumnType::String),
        ])
        .unwrap();
        let old_encoder = TupleEncoder::new(&old_schema);
        let tuple = old_encoder
            .encode(&[Value::Integer(1), Value::String("x".into()), Value::Null])
            .unwrap();

        // Reading column 3 (balance) and 4 (active) with old_col_count=3:
        let balance = decoder.extract_value_versioned(&tuple, 3, 3).unwrap();
        assert_eq!(balance, Value::Null);

        let active = decoder.extract_value_versioned(&tuple, 4, 3).unwrap();
        assert_eq!(active, Value::Null);

        // But column 0 (id) still works:
        let id = decoder.extract_value_versioned(&tuple, 0, 3).unwrap();
        assert_eq!(id, Value::Integer(1));
    }

    #[test]
    fn raw_fixed_extraction() {
        let schema = StrictSchema::new(vec![
            ColumnDef::required("a", ColumnType::Int64),
            ColumnDef::required("b", ColumnType::Float64),
            ColumnDef::required("c", ColumnType::Bool),
        ])
        .unwrap();
        let encoder = TupleEncoder::new(&schema);
        let decoder = TupleDecoder::new(&schema);

        let tuple = encoder
            .encode(&[Value::Integer(42), Value::Float(0.75), Value::Bool(true)])
            .unwrap();

        let a_raw = decoder.extract_fixed_raw(&tuple, 0).unwrap().unwrap();
        assert_eq!(i64::from_le_bytes(a_raw.try_into().unwrap()), 42);

        let b_raw = decoder.extract_fixed_raw(&tuple, 1).unwrap().unwrap();
        assert_eq!(f64::from_le_bytes(b_raw.try_into().unwrap()), 0.75);

        let c_raw = decoder.extract_fixed_raw(&tuple, 2).unwrap().unwrap();
        assert_eq!(c_raw[0], 1);
    }

    #[test]
    fn raw_variable_extraction() {
        let schema = StrictSchema::new(vec![
            ColumnDef::required("id", ColumnType::Int64),
            ColumnDef::required("name", ColumnType::String),
            ColumnDef::nullable("bio", ColumnType::String),
        ])
        .unwrap();
        let encoder = TupleEncoder::new(&schema);
        let decoder = TupleDecoder::new(&schema);

        let tuple = encoder
            .encode(&[
                Value::Integer(1),
                Value::String("hello".into()),
                Value::String("world".into()),
            ])
            .unwrap();

        let name_raw = decoder.extract_variable_raw(&tuple, 1).unwrap().unwrap();
        assert_eq!(std::str::from_utf8(name_raw).unwrap(), "hello");

        let bio_raw = decoder.extract_variable_raw(&tuple, 2).unwrap().unwrap();
        assert_eq!(std::str::from_utf8(bio_raw).unwrap(), "world");
    }

    #[test]
    fn all_types_roundtrip() {
        let schema = StrictSchema::new(vec![
            ColumnDef::required("i", ColumnType::Int64),
            ColumnDef::required("f", ColumnType::Float64),
            ColumnDef::required("s", ColumnType::String),
            ColumnDef::required("b", ColumnType::Bool),
            ColumnDef::required("raw", ColumnType::Bytes),
            ColumnDef::required("ts", ColumnType::Timestamp),
            ColumnDef::required("tstz", ColumnType::Timestamptz),
            ColumnDef::required(
                "dec",
                ColumnType::Decimal(Some(
                    nodedb_types::columnar::DecimalTypmod::new(18, 4).expect("valid typmod"),
                )),
            ),
            ColumnDef::required("uid", ColumnType::Uuid),
            ColumnDef::required("vec", ColumnType::Vector(2)),
        ])
        .unwrap();
        let encoder = TupleEncoder::new(&schema);
        let decoder = TupleDecoder::new(&schema);

        let uuid_str = "550e8400-e29b-41d4-a716-446655440000";
        let values = vec![
            Value::Integer(-100),
            Value::Float(0.5),
            Value::String("test string".into()),
            Value::Bool(false),
            Value::Bytes(vec![0xDE, 0xAD, 0xBE, 0xEF]),
            Value::NaiveDateTime(NdbDateTime::from_micros(1_000_000)),
            Value::DateTime(NdbDateTime::from_micros(2_000_000)),
            Value::Decimal(rust_decimal::Decimal::new(314159, 5)),
            Value::Uuid(uuid_str.into()),
            Value::Array(vec![Value::Float(1.5), Value::Float(2.5)]),
        ];

        let tuple = encoder.encode(&values).unwrap();
        let decoded = decoder.extract_all(&tuple).unwrap();

        assert_eq!(decoded[0], Value::Integer(-100));
        assert_eq!(decoded[1], Value::Float(0.5));
        assert_eq!(decoded[2], Value::String("test string".into()));
        assert_eq!(decoded[3], Value::Bool(false));
        assert_eq!(decoded[4], Value::Bytes(vec![0xDE, 0xAD, 0xBE, 0xEF]));
        assert_eq!(
            decoded[5],
            Value::NaiveDateTime(NdbDateTime::from_micros(1_000_000))
        );
        assert_eq!(
            decoded[6],
            Value::DateTime(NdbDateTime::from_micros(2_000_000))
        );
        assert_eq!(
            decoded[7],
            Value::Decimal(rust_decimal::Decimal::new(314159, 5))
        );
        assert_eq!(decoded[8], Value::Uuid(uuid_str.into()));
        // Vector goes through f64→f32→f64 roundtrip, check approximate.
        if let Value::Array(ref arr) = decoded[9] {
            assert_eq!(arr.len(), 2);
            if let Value::Float(v) = arr[0] {
                assert!((v - 1.5).abs() < 0.001);
            }
        } else {
            panic!("expected array");
        }
    }

    /// Build a two-column "base" schema (id INT64, name TEXT) encoded as a
    /// tuple, then decode it against a four-column "current" schema that added
    /// two columns via ALTER.
    fn base_schema() -> StrictSchema {
        StrictSchema::new(vec![
            ColumnDef::required("id", ColumnType::Int64).with_primary_key(),
            ColumnDef::required("name", ColumnType::String),
        ])
        .unwrap()
    }

    fn base_tuple() -> Vec<u8> {
        TupleEncoder::new(&base_schema())
            .encode(&[Value::Integer(7), Value::String("Alice".into())])
            .unwrap()
    }

    #[test]
    fn versioned_int_default_zero_not_null() {
        // Schema gained a NOT NULL column with DEFAULT 0.
        let mut schema = base_schema();
        let mut col = ColumnDef::required("score", ColumnType::Int64).with_default("0");
        col.added_at_version = 2;
        schema.columns.push(col);
        schema.version = 2;

        let decoder = TupleDecoder::new(&schema);
        let tuple = base_tuple();

        // Column 2 (score) did not exist when the tuple was written (old_col_count=2).
        let val = decoder.extract_value_versioned(&tuple, 2, 2).unwrap();
        assert_eq!(val, Value::Integer(0), "expected default 0, not null");
    }

    #[test]
    fn versioned_text_default_pending_not_null() {
        // Schema gained a NOT NULL TEXT column with DEFAULT 'pending'.
        let mut schema = base_schema();
        let mut col = ColumnDef::required("status", ColumnType::String).with_default("'pending'");
        col.added_at_version = 2;
        schema.columns.push(col);
        schema.version = 2;

        let decoder = TupleDecoder::new(&schema);
        let tuple = base_tuple();

        let val = decoder.extract_value_versioned(&tuple, 2, 2).unwrap();
        assert_eq!(
            val,
            Value::String("pending".into()),
            "expected default 'pending', not null"
        );
    }

    #[test]
    fn versioned_new_row_written_at_new_schema_no_double_default() {
        // A tuple written under the new schema already has the column encoded;
        // extract_value_versioned must read the real encoded value, not the default.
        let mut schema = base_schema();
        let mut col = ColumnDef::required("score", ColumnType::Int64).with_default("0");
        col.added_at_version = 2;
        schema.columns.push(col);
        schema.version = 2;

        let encoder = TupleEncoder::new(&schema);
        let tuple = encoder
            .encode(&[
                Value::Integer(42),
                Value::String("Bob".into()),
                Value::Integer(99),
            ])
            .unwrap();

        let decoder = TupleDecoder::new(&schema);
        // All three columns were present when this tuple was written.
        let val = decoder.extract_value_versioned(&tuple, 2, 3).unwrap();
        assert_eq!(
            val,
            Value::Integer(99),
            "must read encoded value, not default"
        );
    }

    #[test]
    fn versioned_multiple_alters_accumulate() {
        // V0 (2 cols) → V1 adds `a INT64 DEFAULT 10` → V2 adds `b TEXT DEFAULT 'x'`.
        // A V0 tuple must read defaults for both `a` and `b`.
        let mut schema = base_schema();

        let mut col_a = ColumnDef::required("a", ColumnType::Int64).with_default("10");
        col_a.added_at_version = 2;
        schema.columns.push(col_a);
        schema.version = 2;

        let mut col_b = ColumnDef::required("b", ColumnType::String).with_default("'x'");
        col_b.added_at_version = 3;
        schema.columns.push(col_b);
        schema.version = 3;

        let decoder = TupleDecoder::new(&schema);
        let tuple = base_tuple(); // written at V1 (2 cols)

        let a = decoder.extract_value_versioned(&tuple, 2, 2).unwrap();
        assert_eq!(a, Value::Integer(10), "a default must be 10");

        let b = decoder.extract_value_versioned(&tuple, 3, 2).unwrap();
        assert_eq!(b, Value::String("x".into()), "b default must be 'x'");

        // Original columns still decode correctly.
        let id = decoder.extract_value_versioned(&tuple, 0, 2).unwrap();
        assert_eq!(id, Value::Integer(7));
    }

    #[test]
    fn variable_offsets_reject_reversed_and_truncated_ranges() {
        let schema = StrictSchema::new(vec![ColumnDef::required("text", ColumnType::String)])
            .expect("schema");
        let decoder = TupleDecoder::new(&schema);

        // Header (10 bytes), then the two-entry variable offset table. The
        // second offset precedes the first, which must never reach slicing.
        let mut reversed = vec![0u8; 18];
        reversed[..4].copy_from_slice(&MAGIC.to_le_bytes());
        reversed[4] = FORMAT_VERSION;
        reversed[10..14].copy_from_slice(&5u32.to_le_bytes());
        reversed[14..18].copy_from_slice(&4u32.to_le_bytes());
        assert!(matches!(
            decoder.extract_variable_raw(&reversed, 0),
            Err(StrictError::CorruptOffset { offset: 4, len: 18 })
        ));

        let truncated = &reversed[..14];
        assert!(matches!(
            decoder.extract_variable_raw(truncated, 0),
            Err(StrictError::TruncatedTuple { .. })
        ));
    }

    #[test]
    fn versioned_nullable_column_no_default_returns_null() {
        // A nullable column with no default must return null (not an error).
        let mut schema = base_schema();
        let mut col = ColumnDef::nullable("note", ColumnType::String);
        col.added_at_version = 2;
        schema.columns.push(col);
        schema.version = 2;

        let decoder = TupleDecoder::new(&schema);
        let tuple = base_tuple();

        let val = decoder.extract_value_versioned(&tuple, 2, 2).unwrap();
        assert_eq!(
            val,
            Value::Null,
            "nullable column without default must be null"
        );
    }
}
