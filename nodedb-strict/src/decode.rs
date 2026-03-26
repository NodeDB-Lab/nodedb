//! Binary Tuple decoder: O(1) field extraction from tuple bytes.
//!
//! Given a schema and a column index, computes the byte offset and extracts
//! the field value without parsing any other column. This is the core
//! performance advantage over self-describing formats like MessagePack/BSON.

use nodedb_types::columnar::{ColumnType, SchemaOps, StrictSchema};
use nodedb_types::datetime::NdbDateTime;
use nodedb_types::value::Value;

use crate::error::StrictError;

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
    /// Size of the tuple header: 2 (version) + null_bitmap_size.
    header_size: usize,
}

impl TupleDecoder {
    /// Create a decoder for the given schema.
    pub fn new(schema: &StrictSchema) -> Self {
        let mut fixed_offsets = Vec::with_capacity(schema.columns.len());
        let mut var_table_index = Vec::with_capacity(schema.columns.len());
        let mut fixed_offset = 0usize;
        let mut var_idx = 0usize;

        for col in &schema.columns {
            if let Some(size) = col.column_type.fixed_size() {
                fixed_offsets.push(Some(fixed_offset));
                var_table_index.push(None);
                fixed_offset += size;
            } else {
                fixed_offsets.push(None);
                var_table_index.push(Some(var_idx));
                var_idx += 1;
            }
        }

        let header_size = 2 + schema.null_bitmap_size();

        Self {
            schema: schema.clone(),
            fixed_offsets,
            fixed_section_size: fixed_offset,
            var_table_index,
            var_count: var_idx,
            header_size,
        }
    }

    /// Read the schema version from a tuple's header.
    pub fn schema_version(&self, tuple: &[u8]) -> Result<u16, StrictError> {
        if tuple.len() < 2 {
            return Err(StrictError::TruncatedTuple {
                expected: 2,
                got: tuple.len(),
            });
        }
        Ok(u16::from_le_bytes([tuple[0], tuple[1]]))
    }

    /// Check whether column `col_idx` is null in the given tuple.
    pub fn is_null(&self, tuple: &[u8], col_idx: usize) -> Result<bool, StrictError> {
        self.check_bounds(col_idx)?;
        self.check_min_size(tuple)?;

        let bitmap_byte = tuple[2 + col_idx / 8];
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
            expected: self.schema.columns[col_idx].column_type.clone(),
        })?;

        let size = self.schema.columns[col_idx]
            .column_type
            .fixed_size()
            .unwrap();
        let start = self.header_size + offset;
        let end = start + size;

        if end > tuple.len() {
            return Err(StrictError::TruncatedTuple {
                expected: end,
                got: tuple.len(),
            });
        }

        Ok(Some(&tuple[start..end]))
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
            expected: self.schema.columns[col_idx].column_type.clone(),
        })?;

        let table_start = self.header_size + self.fixed_section_size;
        let entry_pos = table_start + var_idx * 4;
        let next_pos = entry_pos + 4;

        if next_pos + 4 > tuple.len() {
            return Err(StrictError::TruncatedTuple {
                expected: next_pos + 4,
                got: tuple.len(),
            });
        }

        // Safety: bounds checked above — entry_pos..+4 and next_pos..+4 are within tuple.
        let offset = u32::from_le_bytes(
            tuple[entry_pos..entry_pos + 4]
                .try_into()
                .expect("4-byte slice from bounds-checked range"),
        );
        let next_offset = u32::from_le_bytes(
            tuple[next_pos..next_pos + 4]
                .try_into()
                .expect("4-byte slice from bounds-checked range"),
        );

        let var_data_start = table_start + (self.var_count + 1) * 4;
        let abs_start = var_data_start + offset as usize;
        let abs_end = var_data_start + next_offset as usize;

        if abs_end > tuple.len() {
            return Err(StrictError::CorruptOffset {
                offset: next_offset,
                len: tuple.len(),
            });
        }

        Ok(Some(&tuple[abs_start..abs_end]))
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
            let raw = self.extract_fixed_raw(tuple, col_idx)?.unwrap();
            Ok(decode_fixed_value(&col.column_type, raw))
        } else {
            let raw = self.extract_variable_raw(tuple, col_idx)?.unwrap();
            Ok(decode_variable_value(&col.column_type, raw))
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
    /// in newer schema versions return their default value or null.
    ///
    /// `old_col_count` is the number of columns in the schema version that
    /// wrote this tuple.
    pub fn extract_value_versioned(
        &self,
        tuple: &[u8],
        col_idx: usize,
        old_col_count: usize,
    ) -> Result<Value, StrictError> {
        self.check_bounds(col_idx)?;

        if col_idx >= old_col_count {
            // Column was added after this tuple was written.
            // Return default or null.
            let col = &self.schema.columns[col_idx];
            return if col.nullable {
                Ok(Value::Null)
            } else {
                // Non-nullable column added later must have a default.
                // Return null as a sentinel — the write path enforces defaults.
                Ok(Value::Null)
            };
        }

        self.extract_value(tuple, col_idx)
    }

    /// Access the schema this decoder was built for.
    pub fn schema(&self) -> &StrictSchema {
        &self.schema
    }

    /// Byte offset where fixed-field section starts.
    pub fn fixed_section_start(&self) -> usize {
        self.header_size
    }

    /// Byte offset where the variable offset table starts.
    pub fn offset_table_start(&self) -> usize {
        self.header_size + self.fixed_section_size
    }

    /// Byte offset where variable data starts.
    pub fn var_data_start(&self) -> usize {
        self.offset_table_start() + (self.var_count + 1) * 4
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
        Some((self.header_size + offset, size))
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
        let bitmap_byte = tuple[2 + col_idx / 8];
        bitmap_byte & (1 << (col_idx % 8)) != 0
    }
}

/// Decode a fixed-size raw byte slice into a Value.
fn decode_fixed_value(col_type: &ColumnType, raw: &[u8]) -> Value {
    match col_type {
        ColumnType::Int64 => Value::Integer(i64::from_le_bytes(raw[..8].try_into().unwrap())),
        ColumnType::Float64 => Value::Float(f64::from_le_bytes(raw[..8].try_into().unwrap())),
        ColumnType::Bool => Value::Bool(raw[0] != 0),
        ColumnType::Timestamp => {
            let micros = i64::from_le_bytes(raw[..8].try_into().unwrap());
            Value::DateTime(NdbDateTime::from_micros(micros))
        }
        ColumnType::Decimal => {
            let mut bytes = [0u8; 16];
            bytes.copy_from_slice(&raw[..16]);
            Value::Decimal(rust_decimal::Decimal::deserialize(bytes))
        }
        ColumnType::Uuid => {
            let parsed = uuid::Uuid::from_bytes(raw[..16].try_into().unwrap());
            Value::Uuid(parsed.to_string())
        }
        ColumnType::Vector(dim) => {
            let d = *dim as usize;
            let mut floats = Vec::with_capacity(d);
            for i in 0..d {
                let bytes: [u8; 4] = raw[i * 4..(i + 1) * 4].try_into().unwrap();
                let f = f32::from_le_bytes(bytes);
                floats.push(Value::Float(f as f64));
            }
            Value::Array(floats)
        }
        _ => Value::Null, // Unreachable for fixed types.
    }
}

/// Decode a variable-length raw byte slice into a Value.
fn decode_variable_value(col_type: &ColumnType, raw: &[u8]) -> Value {
    match col_type {
        ColumnType::String => {
            Value::String(std::str::from_utf8(raw).unwrap_or_default().to_string())
        }
        ColumnType::Bytes => Value::Bytes(raw.to_vec()),
        ColumnType::Geometry => {
            // Try JSON (native Geometry encoding), fall back to string (WKT passthrough).
            if let Ok(geom) = serde_json::from_slice::<nodedb_types::geometry::Geometry>(raw) {
                Value::Geometry(geom)
            } else {
                Value::String(std::str::from_utf8(raw).unwrap_or_default().to_string())
            }
        }
        _ => Value::Null,
    }
}

#[cfg(test)]
mod tests {
    use nodedb_types::columnar::ColumnDef;

    use super::*;
    use crate::encode::TupleEncoder;

    fn crm_schema() -> StrictSchema {
        StrictSchema::new(vec![
            ColumnDef::required("id", ColumnType::Int64).with_primary_key(),
            ColumnDef::required("name", ColumnType::String),
            ColumnDef::nullable("email", ColumnType::String),
            ColumnDef::required("balance", ColumnType::Decimal),
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
            ColumnDef::required("dec", ColumnType::Decimal),
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
            Value::DateTime(NdbDateTime::from_micros(1_000_000)),
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
            Value::DateTime(NdbDateTime::from_micros(1_000_000))
        );
        assert_eq!(
            decoded[6],
            Value::Decimal(rust_decimal::Decimal::new(314159, 5))
        );
        assert_eq!(decoded[7], Value::Uuid(uuid_str.into()));
        // Vector goes through f64→f32→f64 roundtrip, check approximate.
        if let Value::Array(ref arr) = decoded[8] {
            assert_eq!(arr.len(), 2);
            if let Value::Float(v) = arr[0] {
                assert!((v - 1.5).abs() < 0.001);
            }
        } else {
            panic!("expected array");
        }
    }
}
