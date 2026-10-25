// SPDX-License-Identifier: Apache-2.0

// Attr column format:
//
//   [u8 tag]            the column's attr type, or NULL_ONLY
//   [u32 LE count]      rows, NULL rows included
//   validity section    see `validity.rs`
//   payload             the non-null values only, by tag
//
// A NULL row stores no value, so a NULL never decodes as 0 or 0.0. Every
// non-null value of a column has one type: the schema check at write time
// holds a stored column to its attr's declared type.

use crate::codec::limits::{MAX_COLUMN_ENTRIES, checked_capacity};
use crate::error::{ArrayError, ArrayResult};
use crate::schema::attr_spec::AttrType;
use crate::types::cell_value::value::CellValue;

use super::float::{decode_f32, decode_f64, encode_f32, encode_f64};
use super::int::{decode_ints, encode_ints};
use super::validity::{decode_validity, encode_validity};
use super::varlen::{decode_varlen, encode_varlen};

const TAG_NULL_ONLY: u8 = 0;

fn tag_of(dtype: AttrType) -> u8 {
    match dtype {
        AttrType::Int64 => 1,
        AttrType::Float64 => 2,
        AttrType::String => 3,
        AttrType::Bytes => 4,
        AttrType::UInt8 => 5,
        AttrType::Int16 => 6,
        AttrType::UInt16 => 7,
        AttrType::Int32 => 8,
        AttrType::UInt32 => 9,
        AttrType::Float32 => 10,
        AttrType::Int8 => 11,
    }
}

fn type_of(tag: u8) -> ArrayResult<Option<AttrType>> {
    Ok(Some(match tag {
        TAG_NULL_ONLY => return Ok(None),
        1 => AttrType::Int64,
        2 => AttrType::Float64,
        3 => AttrType::String,
        4 => AttrType::Bytes,
        5 => AttrType::UInt8,
        6 => AttrType::Int16,
        7 => AttrType::UInt16,
        8 => AttrType::Int32,
        9 => AttrType::UInt32,
        10 => AttrType::Float32,
        11 => AttrType::Int8,
        other => {
            return Err(ArrayError::SegmentCorruption {
                detail: format!("attr col: unknown tag {other:#04x}"),
            });
        }
    }))
}

/// The one attr type of the column's non-null values, or `None` when every
/// row is NULL. A column that mixes types is refused.
fn column_type(values: &[CellValue]) -> ArrayResult<Option<AttrType>> {
    let mut found: Option<AttrType> = None;
    for t in values.iter().filter_map(CellValue::attr_type) {
        match found {
            None => found = Some(t),
            Some(f) if f == t => {}
            Some(f) => {
                return Err(ArrayError::InvalidOp {
                    detail: format!(
                        "attr column mixes {} and {} values",
                        f.sql_name(),
                        t.sql_name()
                    ),
                });
            }
        }
    }
    Ok(found)
}

pub fn encode_attr_col(values: &[CellValue]) -> ArrayResult<Vec<u8>> {
    let dtype = column_type(values)?;
    let mut out = Vec::new();
    out.push(dtype.map_or(TAG_NULL_ONLY, tag_of));
    out.extend_from_slice(&(values.len() as u32).to_le_bytes());
    let Some(dtype) = dtype else {
        return Ok(out);
    };
    encode_validity(values, &mut out);
    let present: Vec<&CellValue> = values.iter().filter(|v| !v.is_null()).collect();
    let payload = match dtype {
        AttrType::Int64
        | AttrType::Int8
        | AttrType::UInt8
        | AttrType::Int16
        | AttrType::UInt16
        | AttrType::Int32
        | AttrType::UInt32 => encode_ints(&present)?,
        AttrType::Float64 => encode_f64(&present)?,
        AttrType::Float32 => encode_f32(&present)?,
        AttrType::String | AttrType::Bytes => encode_varlen(&present)?,
    };
    out.extend_from_slice(&payload);
    Ok(out)
}

/// Decode an attr column of `rows` rows. A column that holds another row count
/// is corrupt and is refused before any allocation.
pub fn decode_attr_col(data: &[u8], rows: usize) -> ArrayResult<Vec<CellValue>> {
    let Some((&tag, rest)) = data.split_first() else {
        return Err(ArrayError::SegmentCorruption {
            detail: "attr col: empty payload".into(),
        });
    };
    let Some((count_bytes, _)) = rest.split_first_chunk::<4>() else {
        return Err(ArrayError::SegmentCorruption {
            detail: "attr col: truncated count".into(),
        });
    };
    let stored = u32::from_le_bytes(*count_bytes) as usize;
    if stored != rows {
        return Err(ArrayError::SegmentCorruption {
            detail: format!("attr col holds {stored} rows, its tile holds {rows}"),
        });
    }
    let count = checked_capacity(stored, MAX_COLUMN_ENTRIES, rows, 0, "attr_col count")?;
    let mut pos = 5;
    let Some(dtype) = type_of(tag)? else {
        if data.len() != pos {
            return Err(ArrayError::SegmentCorruption {
                detail: "attr col: a NULL-only column carries a payload".into(),
            });
        }
        return Ok(vec![CellValue::Null; count]);
    };
    let validity = decode_validity(data, &mut pos, count)?;
    let present = (0..count).filter(|&row| validity.is_present(row)).count();
    if present == 0 {
        return Err(ArrayError::SegmentCorruption {
            detail: format!("attr col {}: no value under a typed tag", dtype.sql_name()),
        });
    }
    let body = &data[pos..];
    let values = match dtype {
        AttrType::Int64
        | AttrType::Int8
        | AttrType::UInt8
        | AttrType::Int16
        | AttrType::UInt16
        | AttrType::Int32
        | AttrType::UInt32 => decode_ints(body, dtype, present)?,
        AttrType::Float64 => decode_f64(body, present)?,
        AttrType::Float32 => decode_f32(body, present)?,
        AttrType::String | AttrType::Bytes => decode_varlen(body, dtype, present)?,
    };
    let mut values = values.into_iter();
    let mut out = Vec::with_capacity(count);
    for row in 0..count {
        if validity.is_present(row) {
            out.push(values.next().ok_or_else(|| ArrayError::SegmentCorruption {
                detail: "attr col: fewer values than the validity holds".into(),
            })?);
        } else {
            out.push(CellValue::Null);
        }
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn round_trip(values: &[CellValue]) -> Vec<CellValue> {
        decode_attr_col(&encode_attr_col(values).expect("encode"), values.len()).expect("decode")
    }

    fn with_nulls(values: Vec<CellValue>) -> Vec<CellValue> {
        values
            .into_iter()
            .enumerate()
            .map(|(i, v)| if i % 4 == 1 { CellValue::Null } else { v })
            .collect()
    }

    #[test]
    fn every_type_round_trips_with_nulls() {
        let columns: Vec<Vec<CellValue>> = vec![
            (0..40).map(CellValue::Int64).collect(),
            (0..40)
                .map(|i| CellValue::Float64(i as f64 * 0.5))
                .collect(),
            (0..40)
                .map(|i| CellValue::String(format!("s{i}")))
                .collect(),
            (0..40)
                .map(|i| CellValue::Bytes(vec![i as u8; 3]))
                .collect(),
            (0..40).map(|i| CellValue::UInt8(i as u8 * 6)).collect(),
            (0..40).map(|i| CellValue::Int8(i as i8 * -3)).collect(),
            (0..40).map(|i| CellValue::Int16(i as i16 * -800)).collect(),
            (0..40)
                .map(|i| CellValue::UInt16(i as u16 * 1600))
                .collect(),
            (0..40).map(|i| CellValue::Int32(i * -50_000_000)).collect(),
            (0..40)
                .map(|i| CellValue::UInt32(i as u32 * 100_000_000))
                .collect(),
            (0..40)
                .map(|i| CellValue::Float32(i as f32 * 0.1))
                .collect(),
        ];
        for col in columns {
            let col = with_nulls(col);
            assert_eq!(round_trip(&col), col);
        }
    }

    /// A NULL keeps its NULL through the codec, in a numeric column too.
    #[test]
    fn numeric_null_stays_null() {
        let ints = vec![CellValue::Int64(5), CellValue::Null, CellValue::Int64(0)];
        assert_eq!(round_trip(&ints), ints);
        let floats = vec![CellValue::Null, CellValue::Float64(0.0), CellValue::Null];
        assert_eq!(round_trip(&floats), floats);
        let narrow = vec![CellValue::UInt16(0), CellValue::Null];
        assert_eq!(round_trip(&narrow), narrow);
    }

    #[test]
    fn all_null_and_empty_round_trip() {
        let nulls = vec![CellValue::Null; 17];
        let encoded = encode_attr_col(&nulls).unwrap();
        assert_eq!(encoded.len(), 5);
        assert_eq!(round_trip(&nulls), nulls);
        assert!(round_trip(&[]).is_empty());
    }

    #[test]
    fn a_mixed_column_is_refused() {
        let mixed = vec![CellValue::Int64(1), CellValue::Float64(1.0)];
        assert!(matches!(
            encode_attr_col(&mixed),
            Err(ArrayError::InvalidOp { .. })
        ));
    }

    /// FastLanes packs by value range, so a narrow integer column costs the
    /// bytes of the same values in an `INT64` column.
    #[test]
    fn uint16_bytes_equal_int64_bytes() {
        let narrow: Vec<CellValue> = (0..1000u16).map(|i| CellValue::UInt16(i * 37)).collect();
        let wide: Vec<CellValue> = (0..1000i64).map(|i| CellValue::Int64(i * 37)).collect();
        assert_eq!(
            encode_attr_col(&narrow).unwrap().len(),
            encode_attr_col(&wide).unwrap().len()
        );
    }

    /// A FLOAT32 column of reflectance-like values is smaller than the same
    /// values stored as FLOAT64.
    #[test]
    fn float32_is_smaller_than_float64() {
        let vals: Vec<f32> = (0..1000)
            .map(|i| ((i * 7919) % 10_000) as f32 / 10_000.0)
            .collect();
        let narrow: Vec<CellValue> = vals.iter().map(|v| CellValue::Float32(*v)).collect();
        let wide: Vec<CellValue> = vals
            .iter()
            .map(|v| CellValue::Float64(f64::from(*v)))
            .collect();
        let narrow_len = encode_attr_col(&narrow).unwrap().len();
        let wide_len = encode_attr_col(&wide).unwrap().len();
        assert!(narrow_len < wide_len, "{narrow_len} >= {wide_len}");
        assert!(narrow_len <= 5 + 1 + 1 + 4000);
    }

    #[test]
    fn a_huge_count_in_a_tiny_payload_is_refused() {
        let mut data = vec![tag_of(AttrType::String)];
        data.extend_from_slice(&(MAX_COLUMN_ENTRIES as u32).to_le_bytes());
        data.push(0);
        assert!(matches!(
            decode_attr_col(&data, MAX_COLUMN_ENTRIES),
            Err(ArrayError::SegmentCorruption { .. })
        ));
    }

    #[test]
    fn an_out_of_width_stored_value_is_corruption() {
        let mut data = vec![tag_of(AttrType::UInt8)];
        data.extend_from_slice(&1u32.to_le_bytes());
        data.push(0);
        data.extend_from_slice(&nodedb_codec::fastlanes::encode(&[300]).unwrap());
        assert!(matches!(
            decode_attr_col(&data, 1),
            Err(ArrayError::SegmentCorruption { .. })
        ));
    }

    /// INT8 keeps its full signed range through the codec, and a stored
    /// value past it is corruption.
    #[test]
    fn int8_round_trips_its_full_range() {
        let col = vec![
            CellValue::Int8(i8::MIN),
            CellValue::Null,
            CellValue::Int8(-1),
            CellValue::Int8(0),
            CellValue::Int8(i8::MAX),
        ];
        let encoded = encode_attr_col(&col).unwrap();
        assert_eq!(encoded[0], tag_of(AttrType::Int8));
        assert_eq!(round_trip(&col), col);

        let mut data = vec![tag_of(AttrType::Int8)];
        data.extend_from_slice(&1u32.to_le_bytes());
        data.push(0);
        data.extend_from_slice(&nodedb_codec::fastlanes::encode(&[-129]).unwrap());
        assert!(matches!(
            decode_attr_col(&data, 1),
            Err(ArrayError::SegmentCorruption { .. })
        ));
    }

    #[test]
    fn an_unknown_tag_is_corruption() {
        assert!(decode_attr_col(&[0xEE, 0, 0, 0, 0], 0).is_err());
    }

    #[test]
    fn a_row_count_that_differs_from_the_tile_is_refused() {
        let mut data = vec![TAG_NULL_ONLY];
        data.extend_from_slice(&8_000_000u32.to_le_bytes());
        assert!(matches!(
            decode_attr_col(&data, 3),
            Err(ArrayError::SegmentCorruption { .. })
        ));
    }
}
