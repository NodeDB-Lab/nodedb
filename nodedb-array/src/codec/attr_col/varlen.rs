// SPDX-License-Identifier: Apache-2.0

// String and Bytes attr payload: per non-null value, [u32 LE len][bytes].

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
        let len_bytes = body
            .get(pos..pos + 4)
            .ok_or_else(|| ArrayError::SegmentCorruption {
                detail: "attr col varlen: truncated entry len".into(),
            })?;
        let len =
            u32::from_le_bytes([len_bytes[0], len_bytes[1], len_bytes[2], len_bytes[3]]) as usize;
        pos += 4;
        let bytes = body
            .get(pos..pos + len)
            .ok_or_else(|| ArrayError::SegmentCorruption {
                detail: "attr col varlen: truncated entry bytes".into(),
            })?;
        pos += len;
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
