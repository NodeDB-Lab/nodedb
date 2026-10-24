// SPDX-License-Identifier: Apache-2.0

//! One decoded segment cell as a `Value`.
//!
//! The rule is the memtable's read rule (`ColumnData::get_value`): a cell
//! reads as the same `Value` before and after its memtable flushes. The
//! declared column type decides the variant, because a segment block records
//! only its physical layout.

use nodedb_types::columnar::ColumnType;
use nodedb_types::value::Value;
use nodedb_types::value_from_msgpack;

use crate::error::ColumnarError;

use super::types::DecodedColumn;

/// Width of a decimal, UUID or ULID cell.
const ID_WIDTH: usize = 16;

/// Width of one vector element: a little-endian `f32`.
const F32_WIDTH: usize = 4;

/// Row `row` of `col` as the `Value` the memtable reads for a cell of
/// `declared` type.
///
/// A null cell or a row past the column is `Value::Null`. A cell whose bytes
/// do not hold a value of `declared` type is a corrupt segment and an error.
pub fn decoded_cell_value(
    col: &DecodedColumn,
    row: usize,
    declared: &ColumnType,
) -> Result<Value, ColumnarError> {
    let is_valid = |valid: &[bool]| valid.get(row).copied().unwrap_or(false);
    match col {
        // A time column decodes as `Int64`. The declared type types the cell.
        DecodedColumn::Int64 { values, valid } | DecodedColumn::Timestamp { values, valid } => {
            Ok(match values.get(row) {
                Some(&v) if is_valid(valid) => declared.time_cell(v),
                _ => Value::Null,
            })
        }
        DecodedColumn::Float64 { values, valid } => Ok(match values.get(row) {
            Some(&v) if is_valid(valid) => Value::Float(v),
            _ => Value::Null,
        }),
        DecodedColumn::Bool { values, valid } => Ok(match values.get(row) {
            Some(&v) if is_valid(valid) => Value::Bool(v),
            _ => Value::Null,
        }),
        DecodedColumn::DictEncoded {
            ids,
            dictionary,
            valid,
        } => {
            let Some(&id) = ids.get(row).filter(|_| is_valid(valid)) else {
                return Ok(Value::Null);
            };
            let text = usize::try_from(id)
                .ok()
                .and_then(|id| dictionary.get(id))
                .ok_or_else(|| {
                    corruption(format!(
                        "dictionary ID {id} is outside a dictionary of {} entries",
                        dictionary.len()
                    ))
                })?;
            Ok(Value::String(text.clone()))
        }
        DecodedColumn::Binary {
            data,
            offsets,
            valid,
        } => {
            if !is_valid(valid) {
                return Ok(Value::Null);
            }
            let bytes = cell_bytes(data, offsets, row)?;
            binary_cell_value(bytes, declared)
        }
    }
}

/// The bytes of row `row` of a binary column.
fn cell_bytes<'a>(data: &'a [u8], offsets: &[u32], row: usize) -> Result<&'a [u8], ColumnarError> {
    let bound = |i: usize| offsets.get(i).and_then(|&o| usize::try_from(o).ok());
    bound(row)
        .zip(row.checked_add(1).and_then(bound))
        .and_then(|(start, end)| data.get(start..end))
        .ok_or_else(|| {
            corruption(format!(
                "row {row} has no byte range in a binary column of {} offsets and {} bytes",
                offsets.len(),
                data.len()
            ))
        })
}

/// A binary cell as the `Value` the memtable reads for `declared`.
fn binary_cell_value(bytes: &[u8], declared: &ColumnType) -> Result<Value, ColumnarError> {
    Ok(match declared {
        ColumnType::String | ColumnType::Regex | ColumnType::SparseVector => {
            Value::String(utf8(bytes, declared)?.to_owned())
        }
        ColumnType::Geometry => nodedb_spatial::value_of_ewkb(bytes)
            .map_err(|e| corruption(crate::geometry_cell::not_ewkb(e)))?,
        ColumnType::Bytes
        | ColumnType::Array
        | ColumnType::Set
        | ColumnType::Range
        | ColumnType::Record => Value::Bytes(bytes.to_vec()),
        ColumnType::Json if bytes.is_empty() => Value::Null,
        ColumnType::Json => value_from_msgpack(bytes)
            .map_err(|e| corruption(format!("JSON cell is not MessagePack: {e}")))?,
        ColumnType::Decimal { .. } => Value::Decimal(rust_decimal::Decimal::deserialize(id_cell(
            bytes, declared,
        )?)),
        ColumnType::Uuid => {
            Value::Uuid(uuid::Uuid::from_bytes(id_cell(bytes, declared)?).to_string())
        }
        ColumnType::Ulid => {
            Value::Ulid(ulid::Ulid::from_bytes(id_cell(bytes, declared)?).to_string())
        }
        ColumnType::Vector(dim) => vector_cell(bytes, *dim)?,
        ColumnType::Int64
        | ColumnType::Float64
        | ColumnType::Bool
        | ColumnType::Timestamp
        | ColumnType::Timestamptz
        | ColumnType::SystemTimestamp
        | ColumnType::Duration => {
            return Err(corruption(format!(
                "a {declared} column has no binary cells"
            )));
        }
        // `ColumnType` is `#[non_exhaustive]`. The memtable stores a type it
        // does not know as raw bytes.
        _ => Value::Bytes(bytes.to_vec()),
    })
}

fn utf8<'a>(bytes: &'a [u8], declared: &ColumnType) -> Result<&'a str, ColumnarError> {
    std::str::from_utf8(bytes).map_err(|e| corruption(format!("{declared} cell is not UTF-8: {e}")))
}

fn id_cell(bytes: &[u8], declared: &ColumnType) -> Result<[u8; ID_WIDTH], ColumnarError> {
    <[u8; ID_WIDTH]>::try_from(bytes).map_err(|_| {
        corruption(format!(
            "{declared} cell holds {} bytes, not {ID_WIDTH}",
            bytes.len()
        ))
    })
}

/// A packed little-endian `f32` vector cell as an array of floats.
fn vector_cell(bytes: &[u8], dim: u32) -> Result<Value, ColumnarError> {
    let expected = usize::try_from(dim)
        .ok()
        .and_then(|d| d.checked_mul(F32_WIDTH));
    if expected != Some(bytes.len()) {
        return Err(corruption(format!(
            "VECTOR({dim}) cell holds {} bytes",
            bytes.len()
        )));
    }
    let (chunks, _) = bytes.as_chunks::<F32_WIDTH>();
    Ok(Value::Array(
        chunks
            .iter()
            .map(|c| Value::Float(f64::from(f32::from_le_bytes(*c))))
            .collect(),
    ))
}

fn corruption(reason: String) -> ColumnarError {
    ColumnarError::Corruption {
        segment_id: None,
        reason,
        offset: None,
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use nodedb_types::columnar::{ColumnDef, ColumnType, ColumnarSchema};
    use nodedb_types::value::Value;
    use nodedb_types::{NdbDateTime, NdbDuration};

    use super::decoded_cell_value;
    use crate::memtable::ColumnarMemtable;
    use crate::reader::SegmentReader;
    use crate::test_support::test_memory;
    use crate::writer::{PROFILE_PLAIN, SegmentWriter};

    /// Rows past one block, so the second block's offsets are covered.
    const ROWS: usize = 1500;

    const BASE_MICROS: i64 = 1_700_000_000_000_000;

    fn every_type_schema() -> ColumnarSchema {
        ColumnarSchema::new(vec![
            ColumnDef::required("id", ColumnType::Int64).with_primary_key(),
            ColumnDef::nullable("f", ColumnType::Float64),
            ColumnDef::nullable("b", ColumnType::Bool),
            ColumnDef::nullable("s", ColumnType::String),
            ColumnDef::nullable("tag", ColumnType::String),
            ColumnDef::nullable("raw", ColumnType::Bytes),
            ColumnDef::nullable("ts", ColumnType::Timestamp),
            ColumnDef::nullable("tz", ColumnType::Timestamptz),
            ColumnDef::nullable("sys", ColumnType::SystemTimestamp),
            ColumnDef::nullable("dec", ColumnType::Decimal(None)),
            ColumnDef::nullable("u", ColumnType::Uuid),
            ColumnDef::nullable("ul", ColumnType::Ulid),
            ColumnDef::nullable("geo", ColumnType::Geometry),
            ColumnDef::nullable("vec", ColumnType::Vector(3)),
            ColumnDef::nullable("j", ColumnType::Json),
            ColumnDef::nullable("dur", ColumnType::Duration),
            ColumnDef::nullable("re", ColumnType::Regex),
            ColumnDef::nullable("arr", ColumnType::Array),
            ColumnDef::nullable("sparse", ColumnType::SparseVector),
        ])
        .expect("valid schema")
    }

    fn ulid_text(i: usize) -> String {
        let mut bytes = [0u8; 16];
        bytes[..8].copy_from_slice(&(i as u64 + 1).to_be_bytes());
        bytes[15] = 7;
        ulid::Ulid::from_bytes(bytes).to_string()
    }

    fn json_cell(i: usize) -> Value {
        if i.is_multiple_of(5) {
            // JSON text of a string scalar.
            Value::String("\"scalar\"".into())
        } else {
            Value::Object(HashMap::from([
                ("n".to_string(), Value::Integer(i as i64)),
                ("tag".to_string(), Value::String("x".into())),
            ]))
        }
    }

    fn row(i: usize) -> Vec<Value> {
        let mut values = vec![Value::Integer(i as i64)];
        if i % 11 == 4 {
            values.extend(std::iter::repeat_n(Value::Null, 18));
            return values;
        }
        values.extend([
            Value::Float(i as f64 * 0.5),
            Value::Bool(i.is_multiple_of(2)),
            Value::String(format!("row-{i}")),
            Value::String(["alpha", "beta", "gamma"][i % 3].into()),
            Value::Bytes(vec![i as u8, 0xFF, 0x00]),
            Value::NaiveDateTime(NdbDateTime::from_micros(BASE_MICROS + i as i64)),
            Value::DateTime(NdbDateTime::from_micros(BASE_MICROS - i as i64)),
            Value::Integer(i as i64 * 1_000),
            Value::Decimal(rust_decimal::Decimal::new(i as i64 * 25, 2)),
            Value::Uuid(uuid::Uuid::from_u128(i as u128 + 1).to_string()),
            Value::Ulid(ulid_text(i)),
            Value::String(format!("POINT({i} 1)")),
            Value::Array(vec![
                Value::Float(i as f64),
                Value::Float(0.5),
                Value::Float(-1.25),
            ]),
            json_cell(i),
            Value::Duration(NdbDuration::from_micros(i as i64 * 3)),
            Value::Regex(format!("^r{i}$")),
            Value::Array(vec![Value::Integer(i as i64), Value::String("a".into())]),
            Value::String(format!("{{{i}: 0.5}}")),
        ]);
        values
    }

    /// Every column type reads from a flushed segment as the same `Value` the
    /// live memtable reads for the same cell, nulls and the second block
    /// included.
    #[test]
    fn a_flushed_cell_reads_as_its_memtable_value() {
        let schema = every_type_schema();
        let mut mt = ColumnarMemtable::new(&schema);
        for i in 0..ROWS {
            mt.append_row(&row(i)).expect("append");
        }
        let expected: Vec<Vec<Value>> = (0..ROWS)
            .map(|i| mt.get_row(i).expect("read").expect("memtable row"))
            .collect();

        let (schema, columns, row_count) = mt.drain_optimized();
        let segment = SegmentWriter::new(PROFILE_PLAIN, test_memory())
            .write_segment(&schema, &columns, row_count, None)
            .expect("write segment");
        let reader = SegmentReader::open(&segment).expect("open segment");
        let indices: Vec<usize> = (0..schema.columns.len()).collect();
        let decoded = reader.read_columns(&indices, &[]).expect("read columns");

        for (i, memtable_row) in expected.iter().enumerate() {
            for ((col, def), want) in decoded.iter().zip(&schema.columns).zip(memtable_row) {
                let got = decoded_cell_value(col, i, &def.column_type).expect("decodable cell");
                assert_eq!(&got, want, "row {i} column '{}'", def.name);
            }
        }

        // The memtable shapes the comparison relies on.
        let first = &expected[1];
        assert_eq!(first[10], Value::Uuid(uuid::Uuid::from_u128(2).to_string()));
        assert_eq!(first[11], Value::Ulid(ulid_text(1)));
        assert_eq!(
            first[12],
            Value::Geometry(nodedb_types::geometry::SridGeometry::wgs84(
                nodedb_types::geometry::Geometry::point(1.0, 1.0)
            ))
        );
        assert_eq!(first[15], Value::Integer(3));
        assert_eq!(first[16], Value::String("^r1$".into()));
        assert_eq!(expected[4][10], Value::Null);
    }

    /// A cell whose bytes do not hold its declared type is an error, not a
    /// default value.
    #[test]
    fn a_corrupt_binary_cell_is_an_error() {
        let col = crate::reader::DecodedColumn::Binary {
            data: vec![1, 2, 3],
            offsets: vec![0, 3],
            valid: vec![true],
        };
        for ty in [
            ColumnType::Uuid,
            ColumnType::Ulid,
            ColumnType::Decimal(None),
            ColumnType::Vector(2),
            ColumnType::Geometry,
        ] {
            assert!(
                decoded_cell_value(&col, 0, &ty).is_err(),
                "{ty} must reject a 3-byte cell"
            );
        }
        // 0xC1 is the one byte MessagePack never uses.
        let not_msgpack = crate::reader::DecodedColumn::Binary {
            data: vec![0xC1],
            offsets: vec![0, 1],
            valid: vec![true],
        };
        assert!(decoded_cell_value(&not_msgpack, 0, &ColumnType::Json).is_err());
        let not_utf8 = crate::reader::DecodedColumn::Binary {
            data: vec![0xFF],
            offsets: vec![0, 1],
            valid: vec![true],
        };
        assert!(decoded_cell_value(&not_utf8, 0, &ColumnType::String).is_err());
        assert_eq!(
            decoded_cell_value(&not_utf8, 0, &ColumnType::Bytes).expect("bytes"),
            Value::Bytes(vec![0xFF])
        );
    }
}
