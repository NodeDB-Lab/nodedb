//! Vectorized Arrow extraction from Binary Tuples.
//!
//! Batch-extracts a single column from N tuples into an Arrow array.
//! This is the DataFusion fast path: scan N tuples from redb, extract
//! one column, hand it to DataFusion as an Arrow RecordBatch.
//!
//! For fixed-size columns: stride-copy at known offsets into contiguous buffer.
//! For variable-length columns: gather offsets, build Arrow offset + data buffer.

use std::sync::Arc;

use arrow::array::{
    Array, ArrayRef, BinaryArray, BooleanArray, Float64Array, Int64Array, StringArray,
    TimestampMicrosecondArray,
};
use arrow::buffer::{BooleanBuffer, NullBuffer};
use nodedb_types::columnar::{ColumnType, StrictSchema};

use crate::decode::TupleDecoder;
use crate::error::StrictError;

/// Extract a single column from a batch of tuples into an Arrow `ArrayRef`.
///
/// `tuples` is a slice of raw tuple byte slices (as stored in redb).
/// `col_idx` is the column index in the schema.
///
/// Returns an appropriately-typed Arrow array (Int64Array, StringArray, etc.)
/// with a null bitmap derived from the tuples' null bitmaps.
pub fn extract_column_to_arrow(
    schema: &StrictSchema,
    decoder: &TupleDecoder,
    tuples: &[&[u8]],
    col_idx: usize,
) -> Result<ArrayRef, StrictError> {
    if col_idx >= schema.columns.len() {
        return Err(StrictError::ColumnOutOfRange {
            index: col_idx,
            count: schema.columns.len(),
        });
    }

    let col_type = &schema.columns[col_idx].column_type;
    let n = tuples.len();

    match col_type {
        ColumnType::Int64 => extract_int64(decoder, tuples, col_idx, n),
        ColumnType::Float64 => extract_float64(decoder, tuples, col_idx, n),
        ColumnType::Bool => extract_bool(decoder, tuples, col_idx, n),
        ColumnType::Timestamp => extract_timestamp(decoder, tuples, col_idx, n),
        ColumnType::String => extract_string(decoder, tuples, col_idx, n),
        ColumnType::Bytes | ColumnType::Geometry => extract_binary(decoder, tuples, col_idx, n),
        ColumnType::Decimal => extract_decimal_as_string(decoder, tuples, col_idx, n),
        ColumnType::Uuid => extract_uuid(decoder, tuples, col_idx, n),
        ColumnType::Vector(dim) => extract_vector(decoder, tuples, col_idx, n, *dim as usize),
    }
}

/// Build a NullBuffer from the null bitmap of N tuples for a given column.
fn build_null_buffer(
    decoder: &TupleDecoder,
    tuples: &[&[u8]],
    col_idx: usize,
) -> Result<(NullBuffer, usize), StrictError> {
    let n = tuples.len();
    let mut validity = Vec::with_capacity(n);
    let mut null_count = 0;

    for tuple in tuples {
        let is_null = decoder.is_null(tuple, col_idx)?;
        validity.push(!is_null);
        if is_null {
            null_count += 1;
        }
    }

    Ok((NullBuffer::new(BooleanBuffer::from(validity)), null_count))
}

fn extract_int64(
    decoder: &TupleDecoder,
    tuples: &[&[u8]],
    col_idx: usize,
    n: usize,
) -> Result<ArrayRef, StrictError> {
    let (null_buf, null_count) = build_null_buffer(decoder, tuples, col_idx)?;
    let mut values = Vec::with_capacity(n);

    for tuple in tuples {
        if let Some(raw) = decoder.extract_fixed_raw(tuple, col_idx)? {
            values.push(i64::from_le_bytes(
                raw[..8]
                    .try_into()
                    .expect("extract_fixed_raw guarantees exact size"),
            ));
        } else {
            values.push(0); // null sentinel, masked by null buffer
        }
    }

    let array = Int64Array::new(
        values.into(),
        if null_count > 0 { Some(null_buf) } else { None },
    );
    Ok(Arc::new(array))
}

fn extract_float64(
    decoder: &TupleDecoder,
    tuples: &[&[u8]],
    col_idx: usize,
    n: usize,
) -> Result<ArrayRef, StrictError> {
    let (null_buf, null_count) = build_null_buffer(decoder, tuples, col_idx)?;
    let mut values = Vec::with_capacity(n);

    for tuple in tuples {
        if let Some(raw) = decoder.extract_fixed_raw(tuple, col_idx)? {
            values.push(f64::from_le_bytes(
                raw[..8]
                    .try_into()
                    .expect("extract_fixed_raw guarantees exact size"),
            ));
        } else {
            values.push(0.0);
        }
    }

    let array = Float64Array::new(
        values.into(),
        if null_count > 0 { Some(null_buf) } else { None },
    );
    Ok(Arc::new(array))
}

fn extract_bool(
    decoder: &TupleDecoder,
    tuples: &[&[u8]],
    col_idx: usize,
    _n: usize,
) -> Result<ArrayRef, StrictError> {
    let (null_buf, null_count) = build_null_buffer(decoder, tuples, col_idx)?;
    let mut bools = Vec::with_capacity(tuples.len());

    for tuple in tuples {
        if let Some(raw) = decoder.extract_fixed_raw(tuple, col_idx)? {
            bools.push(raw[0] != 0);
        } else {
            bools.push(false);
        }
    }

    let bool_buf = BooleanBuffer::from(bools);
    let array = BooleanArray::new(bool_buf, if null_count > 0 { Some(null_buf) } else { None });
    Ok(Arc::new(array))
}

fn extract_timestamp(
    decoder: &TupleDecoder,
    tuples: &[&[u8]],
    col_idx: usize,
    n: usize,
) -> Result<ArrayRef, StrictError> {
    let (null_buf, null_count) = build_null_buffer(decoder, tuples, col_idx)?;
    let mut values = Vec::with_capacity(n);

    for tuple in tuples {
        if let Some(raw) = decoder.extract_fixed_raw(tuple, col_idx)? {
            values.push(i64::from_le_bytes(
                raw[..8]
                    .try_into()
                    .expect("extract_fixed_raw guarantees exact size"),
            ));
        } else {
            values.push(0);
        }
    }

    let array = TimestampMicrosecondArray::new(
        values.into(),
        if null_count > 0 { Some(null_buf) } else { None },
    );
    Ok(Arc::new(array))
}

fn extract_string(
    decoder: &TupleDecoder,
    tuples: &[&[u8]],
    col_idx: usize,
    n: usize,
) -> Result<ArrayRef, StrictError> {
    let (null_buf, null_count) = build_null_buffer(decoder, tuples, col_idx)?;
    let mut strs: Vec<Option<&str>> = Vec::with_capacity(n);

    for tuple in tuples {
        if let Some(raw) = decoder.extract_variable_raw(tuple, col_idx)? {
            strs.push(Some(std::str::from_utf8(raw).unwrap_or("")));
        } else {
            strs.push(None);
        }
    }

    let array = StringArray::from(strs);
    // Reapply our null buffer in case of nulls not captured by Option.
    if null_count > 0 {
        let data = array
            .into_data()
            .into_builder()
            .null_bit_buffer(Some(null_buf.into_inner().into_inner()))
            .build()
            .expect("arrow array builder lengths are consistent");
        Ok(Arc::new(StringArray::from(data)))
    } else {
        Ok(Arc::new(array))
    }
}

fn extract_binary(
    decoder: &TupleDecoder,
    tuples: &[&[u8]],
    col_idx: usize,
    n: usize,
) -> Result<ArrayRef, StrictError> {
    let (null_buf, null_count) = build_null_buffer(decoder, tuples, col_idx)?;
    let mut blobs: Vec<Option<&[u8]>> = Vec::with_capacity(n);

    for tuple in tuples {
        if let Some(raw) = decoder.extract_variable_raw(tuple, col_idx)? {
            blobs.push(Some(raw));
        } else {
            blobs.push(None);
        }
    }

    let array = BinaryArray::from(blobs);
    if null_count > 0 {
        let data = array
            .into_data()
            .into_builder()
            .null_bit_buffer(Some(null_buf.into_inner().into_inner()))
            .build()
            .expect("arrow array builder lengths are consistent");
        Ok(Arc::new(BinaryArray::from(data)))
    } else {
        Ok(Arc::new(array))
    }
}

/// Decimal → String representation for Arrow (DataFusion handles decimal as strings
/// or Decimal128 — we use string for lossless representation).
fn extract_decimal_as_string(
    decoder: &TupleDecoder,
    tuples: &[&[u8]],
    col_idx: usize,
    n: usize,
) -> Result<ArrayRef, StrictError> {
    let (null_buf, null_count) = build_null_buffer(decoder, tuples, col_idx)?;
    let mut strs: Vec<Option<String>> = Vec::with_capacity(n);

    for tuple in tuples {
        if let Some(raw) = decoder.extract_fixed_raw(tuple, col_idx)? {
            let mut bytes = [0u8; 16];
            bytes.copy_from_slice(&raw[..16]);
            let dec = rust_decimal::Decimal::deserialize(bytes);
            strs.push(Some(dec.to_string()));
        } else {
            strs.push(None);
        }
    }

    let array = StringArray::from(strs);
    if null_count > 0 {
        let data = array
            .into_data()
            .into_builder()
            .null_bit_buffer(Some(null_buf.into_inner().into_inner()))
            .build()
            .expect("arrow array builder lengths are consistent");
        Ok(Arc::new(StringArray::from(data)))
    } else {
        Ok(Arc::new(array))
    }
}

fn extract_uuid(
    decoder: &TupleDecoder,
    tuples: &[&[u8]],
    col_idx: usize,
    n: usize,
) -> Result<ArrayRef, StrictError> {
    let (null_buf, null_count) = build_null_buffer(decoder, tuples, col_idx)?;
    let mut strs: Vec<Option<String>> = Vec::with_capacity(n);

    for tuple in tuples {
        if let Some(raw) = decoder.extract_fixed_raw(tuple, col_idx)? {
            let uuid = uuid::Uuid::from_bytes(
                raw[..16]
                    .try_into()
                    .expect("extract_fixed_raw guarantees exact size"),
            );
            strs.push(Some(uuid.to_string()));
        } else {
            strs.push(None);
        }
    }

    let array = StringArray::from(strs);
    if null_count > 0 {
        let data = array
            .into_data()
            .into_builder()
            .null_bit_buffer(Some(null_buf.into_inner().into_inner()))
            .build()
            .expect("arrow array builder lengths are consistent");
        Ok(Arc::new(StringArray::from(data)))
    } else {
        Ok(Arc::new(array))
    }
}

/// Vector columns → BinaryArray of packed f32 bytes.
/// Each entry is `dim * 4` bytes of packed little-endian f32.
fn extract_vector(
    decoder: &TupleDecoder,
    tuples: &[&[u8]],
    col_idx: usize,
    n: usize,
    dim: usize,
) -> Result<ArrayRef, StrictError> {
    let (null_buf, null_count) = build_null_buffer(decoder, tuples, col_idx)?;
    let vec_size = dim * 4;
    let mut blobs: Vec<Option<&[u8]>> = Vec::with_capacity(n);

    for tuple in tuples {
        if let Some(raw) = decoder.extract_fixed_raw(tuple, col_idx)? {
            blobs.push(Some(&raw[..vec_size]));
        } else {
            blobs.push(None);
        }
    }

    let array = BinaryArray::from(blobs);
    if null_count > 0 {
        let data = array
            .into_data()
            .into_builder()
            .null_bit_buffer(Some(null_buf.into_inner().into_inner()))
            .build()
            .expect("arrow array builder lengths are consistent");
        Ok(Arc::new(BinaryArray::from(data)))
    } else {
        Ok(Arc::new(array))
    }
}

#[cfg(test)]
mod tests {
    use arrow::array::{Array, AsArray};
    use arrow::datatypes::{Float64Type, Int64Type};
    use nodedb_types::columnar::ColumnDef;
    use nodedb_types::value::Value;

    use super::*;
    use crate::encode::TupleEncoder;

    fn test_schema() -> StrictSchema {
        StrictSchema::new(vec![
            ColumnDef::required("id", ColumnType::Int64).with_primary_key(),
            ColumnDef::required("name", ColumnType::String),
            ColumnDef::nullable("score", ColumnType::Float64),
            ColumnDef::nullable("active", ColumnType::Bool),
        ])
        .unwrap()
    }

    fn encode_rows(schema: &StrictSchema, rows: &[Vec<Value>]) -> Vec<Vec<u8>> {
        let encoder = TupleEncoder::new(schema);
        rows.iter().map(|r| encoder.encode(r).unwrap()).collect()
    }

    #[test]
    fn extract_int64_column() {
        let schema = test_schema();
        let decoder = TupleDecoder::new(&schema);
        let rows = vec![
            vec![
                Value::Integer(1),
                Value::String("a".into()),
                Value::Float(1.0),
                Value::Bool(true),
            ],
            vec![
                Value::Integer(2),
                Value::String("b".into()),
                Value::Float(2.0),
                Value::Bool(false),
            ],
            vec![
                Value::Integer(3),
                Value::String("c".into()),
                Value::Null,
                Value::Null,
            ],
        ];
        let encoded = encode_rows(&schema, &rows);
        let refs: Vec<&[u8]> = encoded.iter().map(|v| v.as_slice()).collect();

        let arr = extract_column_to_arrow(&schema, &decoder, &refs, 0).unwrap();
        let int_arr = arr.as_primitive::<Int64Type>();
        assert_eq!(int_arr.len(), 3);
        assert_eq!(int_arr.value(0), 1);
        assert_eq!(int_arr.value(1), 2);
        assert_eq!(int_arr.value(2), 3);
        assert_eq!(int_arr.null_count(), 0);
    }

    #[test]
    fn extract_float64_column_with_nulls() {
        let schema = test_schema();
        let decoder = TupleDecoder::new(&schema);
        let rows = vec![
            vec![
                Value::Integer(1),
                Value::String("a".into()),
                Value::Float(0.75),
                Value::Bool(true),
            ],
            vec![
                Value::Integer(2),
                Value::String("b".into()),
                Value::Null,
                Value::Bool(false),
            ],
            vec![
                Value::Integer(3),
                Value::String("c".into()),
                Value::Float(1.25),
                Value::Null,
            ],
        ];
        let encoded = encode_rows(&schema, &rows);
        let refs: Vec<&[u8]> = encoded.iter().map(|v| v.as_slice()).collect();

        let arr = extract_column_to_arrow(&schema, &decoder, &refs, 2).unwrap();
        let float_arr = arr.as_primitive::<Float64Type>();
        assert_eq!(float_arr.len(), 3);
        assert_eq!(float_arr.value(0), 0.75);
        assert!(float_arr.is_null(1));
        assert_eq!(float_arr.value(2), 1.25);
        assert_eq!(float_arr.null_count(), 1);
    }

    #[test]
    fn extract_string_column() {
        let schema = test_schema();
        let decoder = TupleDecoder::new(&schema);
        let rows = vec![
            vec![
                Value::Integer(1),
                Value::String("hello".into()),
                Value::Float(1.0),
                Value::Bool(true),
            ],
            vec![
                Value::Integer(2),
                Value::String("world".into()),
                Value::Null,
                Value::Null,
            ],
        ];
        let encoded = encode_rows(&schema, &rows);
        let refs: Vec<&[u8]> = encoded.iter().map(|v| v.as_slice()).collect();

        let arr = extract_column_to_arrow(&schema, &decoder, &refs, 1).unwrap();
        let str_arr = arr.as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(str_arr.len(), 2);
        assert_eq!(str_arr.value(0), "hello");
        assert_eq!(str_arr.value(1), "world");
    }

    #[test]
    fn extract_bool_column_with_nulls() {
        let schema = test_schema();
        let decoder = TupleDecoder::new(&schema);
        let rows = vec![
            vec![
                Value::Integer(1),
                Value::String("a".into()),
                Value::Float(1.0),
                Value::Bool(true),
            ],
            vec![
                Value::Integer(2),
                Value::String("b".into()),
                Value::Float(2.0),
                Value::Null,
            ],
            vec![
                Value::Integer(3),
                Value::String("c".into()),
                Value::Float(3.0),
                Value::Bool(false),
            ],
        ];
        let encoded = encode_rows(&schema, &rows);
        let refs: Vec<&[u8]> = encoded.iter().map(|v| v.as_slice()).collect();

        let arr = extract_column_to_arrow(&schema, &decoder, &refs, 3).unwrap();
        let bool_arr = arr.as_any().downcast_ref::<BooleanArray>().unwrap();
        assert_eq!(bool_arr.len(), 3);
        assert!(bool_arr.value(0));
        assert!(bool_arr.is_null(1));
        assert!(!bool_arr.value(2));
    }

    #[test]
    fn extract_10000_tuples_int64() {
        let schema = StrictSchema::new(vec![ColumnDef::required("x", ColumnType::Int64)]).unwrap();
        let encoder = TupleEncoder::new(&schema);
        let decoder = TupleDecoder::new(&schema);

        let encoded: Vec<Vec<u8>> = (0..10_000)
            .map(|i| encoder.encode(&[Value::Integer(i)]).unwrap())
            .collect();
        let refs: Vec<&[u8]> = encoded.iter().map(|v| v.as_slice()).collect();

        let arr = extract_column_to_arrow(&schema, &decoder, &refs, 0).unwrap();
        let int_arr = arr.as_primitive::<Int64Type>();
        assert_eq!(int_arr.len(), 10_000);
        assert_eq!(int_arr.value(0), 0);
        assert_eq!(int_arr.value(9999), 9999);
    }
}
