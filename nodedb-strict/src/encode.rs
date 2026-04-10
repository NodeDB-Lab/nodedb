//! Binary Tuple encoder: schema + values → compact byte representation.
//!
//! Layout:
//! ```text
//! [schema_version: u16 LE]
//! [null_bitmap: ceil(N/8) bytes, bit=1 means NULL]
//! [fixed_fields: concatenated, zeroed when null]
//! [offset_table: (N_var + 1) × u32 LE]
//! [variable_data: concatenated variable-length bytes]
//! ```

use nodedb_types::columnar::{ColumnType, StrictSchema};
use nodedb_types::value::Value;

use crate::error::StrictError;

/// Encodes rows into Binary Tuples according to a fixed schema.
///
/// Reusable: create once per schema, encode many rows. Internal buffers
/// are reused across calls to minimize allocation.
pub struct TupleEncoder {
    schema: StrictSchema,
    /// Precomputed: byte offset of each fixed-size column within the fixed section.
    /// Variable-length columns get `None`.
    fixed_offsets: Vec<Option<usize>>,
    /// Total size of the fixed-fields section.
    fixed_section_size: usize,
    /// Indices of variable-length columns in schema order.
    var_indices: Vec<usize>,
    /// Size of the tuple header: 2 (version) + null_bitmap_size.
    header_size: usize,
}

impl TupleEncoder {
    /// Create an encoder for the given schema.
    pub fn new(schema: &StrictSchema) -> Self {
        let mut fixed_offsets = Vec::with_capacity(schema.columns.len());
        let mut var_indices = Vec::new();
        let mut fixed_offset = 0usize;

        for (i, col) in schema.columns.iter().enumerate() {
            if let Some(size) = col.column_type.fixed_size() {
                fixed_offsets.push(Some(fixed_offset));
                fixed_offset += size;
            } else {
                fixed_offsets.push(None);
                var_indices.push(i);
            }
        }

        let header_size = 2 + schema.null_bitmap_size();

        Self {
            schema: schema.clone(),
            fixed_offsets,
            fixed_section_size: fixed_offset,
            var_indices,
            header_size,
        }
    }

    /// Encode a row of values into a Binary Tuple.
    ///
    /// `values` must have exactly `schema.len()` entries. A `Value::Null` is
    /// allowed only if the corresponding column is nullable.
    pub fn encode(&self, values: &[Value]) -> Result<Vec<u8>, StrictError> {
        let n_cols = self.schema.columns.len();
        if values.len() != n_cols {
            return Err(StrictError::ValueCountMismatch {
                expected: n_cols,
                got: values.len(),
            });
        }

        // Pre-size: header + fixed + offset_table. Variable data appended later.
        let offset_table_size = (self.var_indices.len() + 1) * 4;
        let base_size = self.header_size + self.fixed_section_size + offset_table_size;
        let mut buf = vec![0u8; base_size];

        // 1. Schema version.
        buf[0..2].copy_from_slice(&self.schema.version.to_le_bytes());

        // 2. Null bitmap + fixed fields + type validation.
        let bitmap_start = 2;
        let fixed_start = self.header_size;

        for (i, (col, val)) in self.schema.columns.iter().zip(values.iter()).enumerate() {
            let is_null = matches!(val, Value::Null);

            if is_null {
                if !col.nullable {
                    return Err(StrictError::NullViolation(col.name.clone()));
                }
                // Set null bit: byte = i / 8, bit = i % 8.
                buf[bitmap_start + i / 8] |= 1 << (i % 8);
                // Fixed fields remain zeroed; no variable data emitted.
                continue;
            }

            // Type check (with coercion).
            if !col.column_type.accepts(val) {
                return Err(StrictError::TypeMismatch {
                    column: col.name.clone(),
                    expected: col.column_type.clone(),
                });
            }

            // Write fixed-size value.
            if let Some(offset) = self.fixed_offsets[i] {
                let dst = fixed_start + offset;
                encode_fixed(&mut buf[dst..], &col.column_type, val);
            }
            // Variable-length values are handled in the offset table pass below.
        }

        // 3. Variable-length fields: build offset table + variable data.
        let offset_table_start = self.header_size + self.fixed_section_size;
        let mut var_data: Vec<u8> = Vec::new();

        for (var_idx, &col_idx) in self.var_indices.iter().enumerate() {
            // Write current offset.
            let offset = var_data.len() as u32;
            let table_pos = offset_table_start + var_idx * 4;
            buf[table_pos..table_pos + 4].copy_from_slice(&offset.to_le_bytes());

            let val = &values[col_idx];
            if !matches!(val, Value::Null) {
                encode_variable(
                    &mut var_data,
                    &self.schema.columns[col_idx].column_type,
                    val,
                );
            }
            // If null: offset stays the same as next entry → zero length.
        }

        // Final sentinel offset (marks end of last variable field).
        let sentinel = var_data.len() as u32;
        let sentinel_pos = offset_table_start + self.var_indices.len() * 4;
        buf[sentinel_pos..sentinel_pos + 4].copy_from_slice(&sentinel.to_le_bytes());

        // 4. Append variable data.
        buf.extend_from_slice(&var_data);

        Ok(buf)
    }

    /// Access the schema this encoder was built for.
    pub fn schema(&self) -> &StrictSchema {
        &self.schema
    }
}

/// Encode a fixed-size value into the buffer at the given position.
///
/// Handles both native Value types and SQL coercion sources.
fn encode_fixed(dst: &mut [u8], col_type: &ColumnType, value: &Value) {
    match (col_type, value) {
        // Int64: native.
        (ColumnType::Int64, Value::Integer(v)) => {
            dst[..8].copy_from_slice(&v.to_le_bytes());
        }
        // Float64: native + Int64→Float64 coercion.
        (ColumnType::Float64, Value::Float(v)) => {
            dst[..8].copy_from_slice(&v.to_le_bytes());
        }
        (ColumnType::Float64, Value::Integer(v)) => {
            dst[..8].copy_from_slice(&(*v as f64).to_le_bytes());
        }
        // Bool: native.
        (ColumnType::Bool, Value::Bool(v)) => {
            dst[0] = *v as u8;
        }
        // Timestamp: native DateTime + Integer (micros) + String (ISO 8601 parse).
        (ColumnType::Timestamp, Value::DateTime(dt)) => {
            dst[..8].copy_from_slice(&dt.micros.to_le_bytes());
        }
        (ColumnType::Timestamp, Value::Integer(micros)) => {
            dst[..8].copy_from_slice(&micros.to_le_bytes());
        }
        (ColumnType::Timestamp, Value::String(s)) => {
            let micros = nodedb_types::NdbDateTime::parse(s)
                .map(|dt| dt.micros)
                .unwrap_or(0);
            dst[..8].copy_from_slice(&micros.to_le_bytes());
        }
        // Decimal: native Decimal + String/Float/Integer coercion.
        (ColumnType::Decimal, Value::Decimal(d)) => {
            dst[..16].copy_from_slice(&d.serialize());
        }
        (ColumnType::Decimal, Value::String(s)) => {
            let d: rust_decimal::Decimal = s.parse().unwrap_or_default();
            dst[..16].copy_from_slice(&d.serialize());
        }
        (ColumnType::Decimal, Value::Float(f)) => {
            let d = rust_decimal::Decimal::try_from(*f).unwrap_or_default();
            dst[..16].copy_from_slice(&d.serialize());
        }
        (ColumnType::Decimal, Value::Integer(i)) => {
            let d = rust_decimal::Decimal::from(*i);
            dst[..16].copy_from_slice(&d.serialize());
        }
        // Uuid: native Uuid string + String coercion.
        (ColumnType::Uuid, Value::Uuid(s) | Value::String(s)) => {
            if let Ok(parsed) = uuid::Uuid::parse_str(s) {
                dst[..16].copy_from_slice(parsed.as_bytes());
            }
        }
        // Vector: Array of floats + Bytes (packed f32).
        (ColumnType::Vector(dim), Value::Array(arr)) => {
            let d = *dim as usize;
            for (i, v) in arr.iter().take(d).enumerate() {
                let f = match v {
                    Value::Float(f) => *f as f32,
                    Value::Integer(n) => *n as f32,
                    _ => 0.0,
                };
                dst[i * 4..(i + 1) * 4].copy_from_slice(&f.to_le_bytes());
            }
        }
        (ColumnType::Vector(dim), Value::Bytes(b)) => {
            let byte_len = (*dim as usize) * 4;
            let copy_len = b.len().min(byte_len);
            dst[..copy_len].copy_from_slice(&b[..copy_len]);
        }
        _ => {} // Type mismatch caught earlier by accepts().
    }
}

/// Encode a variable-length value, appending to the data buffer.
///
/// Handles both native Value types and SQL coercion sources.
fn encode_variable(var_data: &mut Vec<u8>, col_type: &ColumnType, value: &Value) {
    match (col_type, value) {
        (ColumnType::String, Value::String(s)) => {
            var_data.extend_from_slice(s.as_bytes());
        }
        (ColumnType::Bytes, Value::Bytes(b)) => {
            var_data.extend_from_slice(b);
        }
        // Geometry: native Geometry (JSON-serialized) + String (WKT/GeoJSON passthrough).
        (ColumnType::Geometry, Value::Geometry(g)) => {
            if let Ok(json) = sonic_rs::to_vec(g) {
                var_data.extend_from_slice(&json);
            }
        }
        (ColumnType::Geometry, Value::String(s)) => {
            var_data.extend_from_slice(s.as_bytes());
        }
        (ColumnType::Json, Value::String(s)) => {
            // String input for JSON column: parse as JSON, then serialize as MessagePack.
            // This handles VALUES ('{"key":"val"}') where the SQL planner passes a string literal.
            let parsed = sonic_rs::from_str::<serde_json::Value>(s)
                .ok()
                .map(nodedb_types::Value::from);
            let to_encode = parsed.as_ref().unwrap_or(value);
            if let Ok(bytes) = nodedb_types::value_to_msgpack(to_encode) {
                var_data.extend_from_slice(&bytes);
            }
        }
        (ColumnType::Json, value) => {
            // Non-string input (Object, Array, etc.): serialize directly as MessagePack.
            if let Ok(bytes) = nodedb_types::value_to_msgpack(value) {
                var_data.extend_from_slice(&bytes);
            }
        }
        _ => {}
    }
}

#[cfg(test)]
mod tests {
    use nodedb_types::columnar::ColumnDef;
    use nodedb_types::datetime::NdbDateTime;

    use super::*;

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

    #[test]
    fn encode_basic_row() {
        let schema = crm_schema();
        let encoder = TupleEncoder::new(&schema);

        let values = vec![
            Value::Integer(42),
            Value::String("Alice".into()),
            Value::String("alice@example.com".into()),
            Value::Decimal(rust_decimal::Decimal::new(5000, 2)),
            Value::Bool(true),
        ];

        let tuple = encoder.encode(&values).unwrap();

        // Header: 2 (version) + 1 (null bitmap for 5 cols) = 3 bytes
        assert_eq!(tuple[0], 1); // schema version 1
        assert_eq!(tuple[1], 0); // version high byte
        assert_eq!(tuple[2], 0); // null bitmap: no nulls

        // Fixed section: Int64(8) + Decimal(16) + Bool(1) = 25 bytes
        // Starting at offset 3
        let id_bytes = &tuple[3..11];
        assert_eq!(i64::from_le_bytes(id_bytes.try_into().unwrap()), 42);
    }

    #[test]
    fn encode_with_nulls() {
        let schema = crm_schema();
        let encoder = TupleEncoder::new(&schema);

        let values = vec![
            Value::Integer(1),
            Value::String("Bob".into()),
            Value::Null, // email is nullable
            Value::Decimal(rust_decimal::Decimal::ZERO),
            Value::Null, // active is nullable
        ];

        let tuple = encoder.encode(&values).unwrap();

        // Null bitmap: bit 2 (email) and bit 4 (active) set.
        // Bit 2 = 0b00000100 = 4, bit 4 = 0b00010000 = 16. Combined = 20.
        assert_eq!(tuple[2], 0b00010100);
    }

    #[test]
    fn encode_null_violation() {
        let schema = crm_schema();
        let encoder = TupleEncoder::new(&schema);

        let values = vec![
            Value::Null, // id is NOT NULL
            Value::String("x".into()),
            Value::Null,
            Value::Decimal(rust_decimal::Decimal::ZERO),
            Value::Null,
        ];

        let err = encoder.encode(&values).unwrap_err();
        assert!(matches!(err, StrictError::NullViolation(ref s) if s == "id"));
    }

    #[test]
    fn encode_type_mismatch() {
        let schema = crm_schema();
        let encoder = TupleEncoder::new(&schema);

        let values = vec![
            Value::String("not_an_int".into()), // id expects Int64
            Value::String("x".into()),
            Value::Null,
            Value::Decimal(rust_decimal::Decimal::ZERO),
            Value::Null,
        ];

        let err = encoder.encode(&values).unwrap_err();
        assert!(matches!(err, StrictError::TypeMismatch { .. }));
    }

    #[test]
    fn encode_value_count_mismatch() {
        let schema = crm_schema();
        let encoder = TupleEncoder::new(&schema);

        let err = encoder.encode(&[Value::Integer(1)]).unwrap_err();
        assert!(matches!(err, StrictError::ValueCountMismatch { .. }));
    }

    #[test]
    fn encode_int_to_float_coercion() {
        let schema =
            StrictSchema::new(vec![ColumnDef::required("val", ColumnType::Float64)]).unwrap();
        let encoder = TupleEncoder::new(&schema);

        // Int64 → Float64 coercion should work.
        let tuple = encoder.encode(&[Value::Integer(42)]).unwrap();
        // Header: 2 (version) + 1 (bitmap) = 3. Fixed: 8 bytes Float64.
        let f = f64::from_le_bytes(tuple[3..11].try_into().unwrap());
        assert_eq!(f, 42.0);
    }

    #[test]
    fn encode_timestamp() {
        let schema =
            StrictSchema::new(vec![ColumnDef::required("ts", ColumnType::Timestamp)]).unwrap();
        let encoder = TupleEncoder::new(&schema);

        let dt = NdbDateTime::from_micros(1_700_000_000_000_000);
        let tuple = encoder.encode(&[Value::DateTime(dt)]).unwrap();
        let micros = i64::from_le_bytes(tuple[3..11].try_into().unwrap());
        assert_eq!(micros, 1_700_000_000_000_000);
    }

    #[test]
    fn encode_decode_json_column() {
        let schema = StrictSchema::new(vec![
            ColumnDef::required("id", ColumnType::Int64).with_primary_key(),
            ColumnDef::nullable("metadata", ColumnType::Json),
        ])
        .unwrap();
        let encoder = TupleEncoder::new(&schema);

        let metadata = Value::Object(std::collections::HashMap::from([
            ("source".to_string(), Value::String("web".to_string())),
            ("priority".to_string(), Value::Integer(3)),
        ]));
        let values = vec![Value::Integer(1), metadata.clone()];
        let tuple = encoder.encode(&values).unwrap();

        // Tuple must be longer than just the header + fixed section.
        // Header: 2 (version) + 1 (bitmap) = 3. Fixed: 8 (Int64). Offset table: 8 (2 entries × u32).
        // Variable data must be non-empty (MessagePack of the object).
        let min_size = 3 + 8 + 8;
        assert!(tuple.len() > min_size, "tuple should contain variable data");

        // Decode and verify the value roundtrips correctly.
        let decoder = crate::decode::TupleDecoder::new(&schema);
        let decoded = decoder.extract_all(&tuple).unwrap();
        assert_eq!(decoded[0], Value::Integer(1));
        assert_eq!(decoded[1], metadata);
    }

    #[test]
    fn encode_json_null() {
        let schema = StrictSchema::new(vec![
            ColumnDef::required("id", ColumnType::Int64).with_primary_key(),
            ColumnDef::nullable("data", ColumnType::Json),
        ])
        .unwrap();
        let encoder = TupleEncoder::new(&schema);
        let tuple = encoder.encode(&[Value::Integer(1), Value::Null]).unwrap();
        // Null bitmap byte (index 2): bit 1 (column 1) should be set → 0b00000010 = 2.
        assert_eq!(tuple[2] & 0b10, 0b10);
    }

    #[test]
    fn encode_vector() {
        let schema =
            StrictSchema::new(vec![ColumnDef::required("emb", ColumnType::Vector(3))]).unwrap();
        let encoder = TupleEncoder::new(&schema);

        let vals = vec![Value::Array(vec![
            Value::Float(1.0),
            Value::Float(2.0),
            Value::Float(3.0),
        ])];
        let tuple = encoder.encode(&vals).unwrap();
        // Header: 3 bytes. Fixed: 12 bytes (3 × f32).
        let f0 = f32::from_le_bytes(tuple[3..7].try_into().unwrap());
        let f1 = f32::from_le_bytes(tuple[7..11].try_into().unwrap());
        let f2 = f32::from_le_bytes(tuple[11..15].try_into().unwrap());
        assert_eq!((f0, f1, f2), (1.0, 2.0, 3.0));
    }
}
