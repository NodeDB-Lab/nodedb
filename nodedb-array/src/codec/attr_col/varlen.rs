// SPDX-License-Identifier: Apache-2.0

// String and Bytes attr payload: per non-null value, [u32 LE len][bytes].

use crate::codec::frame::{take_bytes, take_len};
use crate::codec::limits::{MAX_COLUMN_ENTRIES, checked_capacity};
use crate::error::{ArrayError, ArrayResult};
use crate::schema::attr_spec::AttrType;
use crate::types::cell_value::value::CellValue;

pub(super) fn encode_varlen(present: &[&CellValue]) -> ArrayResult<Vec<u8>> {
    let mut out = Vec::new();
    for v in present {
        let bytes = match v {
            CellValue::String(s) => s.as_bytes(),
            CellValue::Bytes(b) => b.as_slice(),
            other => {
                return Err(ArrayError::InvalidOp {
                    detail: format!("variable-length attr column holds {other:?}"),
                });
            }
        };
        out.extend_from_slice(&(bytes.len() as u32).to_le_bytes());
        out.extend_from_slice(bytes);
    }
    Ok(out)
}

/// Decode `expected` values of `dtype`, which is `String` or `Bytes`.
pub(super) fn decode_varlen(
    body: &[u8],
    dtype: AttrType,
    expected: usize,
) -> ArrayResult<Vec<CellValue>> {
    let capacity = checked_capacity(
        expected,
        MAX_COLUMN_ENTRIES,
        body.len(),
        4,
        "attr_col varlen count",
    )?;
    let mut values = Vec::with_capacity(capacity);
    let mut pos = 0usize;
    for _ in 0..expected {
        let len = take_len(body, &mut pos, "attr col varlen: truncated entry len")?;
        let bytes = take_bytes(
            body,
            &mut pos,
            len,
            "attr col varlen: truncated entry bytes",
        )?;
        let value = match dtype {
            AttrType::String => {
                let s = std::str::from_utf8(bytes).map_err(|e| ArrayError::SegmentCorruption {
                    detail: format!("attr col STRING: {e}"),
                })?;
                CellValue::String(s.to_string())
            }
            AttrType::Bytes => CellValue::Bytes(bytes.to_vec()),
            AttrType::Int64
            | AttrType::Float64
            | AttrType::Int8
            | AttrType::UInt8
            | AttrType::Int16
            | AttrType::UInt16
            | AttrType::Int32
            | AttrType::UInt32
            | AttrType::Float32 => {
                return Err(ArrayError::SegmentCorruption {
                    detail: format!("attr col varlen: tag names {}", dtype.sql_name()),
                });
            }
        };
        values.push(value);
    }
    if pos != body.len() {
        return Err(ArrayError::SegmentCorruption {
            detail: format!("attr col varlen: {} trailing bytes", body.len() - pos),
        });
    }
    Ok(values)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn assert_corrupt(body: &[u8], expected: usize) {
        assert!(matches!(
            decode_varlen(body, AttrType::Bytes, expected),
            Err(ArrayError::SegmentCorruption { .. })
        ));
    }

    #[test]
    fn a_hostile_entry_length_is_corruption() {
        assert_corrupt(&u32::MAX.to_le_bytes(), 1);
    }

    #[test]
    fn a_hostile_length_after_a_good_entry_is_corruption() {
        let mut body = 1u32.to_le_bytes().to_vec();
        body.push(b'a');
        body.extend_from_slice(&u32::MAX.to_le_bytes());
        assert_corrupt(&body, 2);
    }

    #[test]
    fn a_truncated_entry_length_is_corruption() {
        let mut body = 1u32.to_le_bytes().to_vec();
        body.push(b'a');
        body.extend_from_slice(&[0, 0, 0]);
        assert_corrupt(&body, 2);
    }

    #[test]
    fn entries_decode_in_order() {
        let mut body = 1u32.to_le_bytes().to_vec();
        body.push(b'a');
        body.extend_from_slice(&0u32.to_le_bytes());
        let values = decode_varlen(&body, AttrType::Bytes, 2).unwrap();
        assert_eq!(
            values,
            vec![CellValue::Bytes(vec![b'a']), CellValue::Bytes(vec![])]
        );
    }
}
