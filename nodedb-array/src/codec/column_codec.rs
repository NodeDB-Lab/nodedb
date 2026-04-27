// Per-column codecs for SparseTile scalar columns.
//
// surrogates  (Vec<Surrogate>)  → fastlanes (u32 as i64)
// row_kinds   (Vec<u8>)         → raw bytes (sentinel-dominated, not compressible)
// *_ms cols   (Vec<i64>)        → gorilla timestamp encoding
// attr cols   (Vec<CellValue>)  → type-dispatch: fastlanes for Int64/Float64,
//                                  raw zerompk for String/Bytes/Null

use nodedb_codec::error::CodecError;
use nodedb_types::Surrogate;

use crate::codec::limits::{MAX_COLUMN_ENTRIES, check_decoded_size};
use crate::error::{ArrayError, ArrayResult};
use crate::types::cell_value::value::CellValue;

// ---------------------------------------------------------------------------
// Error conversion
// ---------------------------------------------------------------------------

fn codec_err(e: CodecError) -> ArrayError {
    ArrayError::SegmentCorruption {
        detail: format!("codec error: {e}"),
    }
}

// ---------------------------------------------------------------------------
// Surrogates: Vec<Surrogate> via fastlanes (u32 widened to i64)
// ---------------------------------------------------------------------------

pub fn encode_surrogates(surrogates: &[Surrogate]) -> Vec<u8> {
    let as_i64: Vec<i64> = surrogates.iter().map(|s| s.as_u32() as i64).collect();
    nodedb_codec::fastlanes::encode(&as_i64)
}

pub fn decode_surrogates(data: &[u8]) -> ArrayResult<Vec<Surrogate>> {
    let as_i64 = nodedb_codec::fastlanes::decode(data).map_err(codec_err)?;
    Ok(as_i64
        .into_iter()
        .map(|v| Surrogate::new(v as u32))
        .collect())
}

// ---------------------------------------------------------------------------
// Row kinds: raw u8 bytes
// ---------------------------------------------------------------------------

pub fn encode_row_kinds(row_kinds: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(4 + row_kinds.len());
    out.extend_from_slice(&(row_kinds.len() as u32).to_le_bytes());
    out.extend_from_slice(row_kinds);
    out
}

pub fn decode_row_kinds(data: &[u8]) -> ArrayResult<Vec<u8>> {
    if data.len() < 4 {
        return Err(ArrayError::SegmentCorruption {
            detail: "row_kinds: truncated count".into(),
        });
    }
    let count = u32::from_le_bytes(data[0..4].try_into().unwrap()) as usize;
    if data.len() < 4 + count {
        return Err(ArrayError::SegmentCorruption {
            detail: "row_kinds: truncated body".into(),
        });
    }
    Ok(data[4..4 + count].to_vec())
}

// ---------------------------------------------------------------------------
// Timestamp columns: Vec<i64> via gorilla
// ---------------------------------------------------------------------------

pub fn encode_timestamps_col(timestamps: &[i64]) -> Vec<u8> {
    nodedb_codec::gorilla::encode_timestamps(timestamps)
}

pub fn decode_timestamps_col(data: &[u8]) -> ArrayResult<Vec<i64>> {
    nodedb_codec::gorilla::decode_timestamps(data).map_err(codec_err)
}

// ---------------------------------------------------------------------------
// Attribute columns: type-dispatched
// ---------------------------------------------------------------------------

/// Tag byte for each attr column format variant.
const ATTR_TAG_INT64: u8 = 0;
const ATTR_TAG_FLOAT64: u8 = 1;
const ATTR_TAG_MSGPACK: u8 = 2; // String, Bytes, Null — zerompk per-value

pub fn encode_attr_col(values: &[CellValue]) -> ArrayResult<Vec<u8>> {
    if values.is_empty() {
        let mut out = vec![ATTR_TAG_MSGPACK];
        out.extend_from_slice(&0u32.to_le_bytes());
        return Ok(out);
    }

    // Check if all values are Int64 or Float64 — only then use numeric codec.
    let all_int = values
        .iter()
        .all(|v| matches!(v, CellValue::Int64(_) | CellValue::Null));
    let all_float = values
        .iter()
        .all(|v| matches!(v, CellValue::Float64(_) | CellValue::Null));

    if all_int {
        let ints: Vec<i64> = values
            .iter()
            .map(|v| match v {
                CellValue::Int64(i) => *i,
                _ => 0,
            })
            .collect();
        let encoded = nodedb_codec::fastlanes::encode(&ints);
        let mut out = vec![ATTR_TAG_INT64];
        out.extend_from_slice(&encoded);
        return Ok(out);
    }

    if all_float {
        // Gorilla XOR-encodes f64 series — exploits common-prefix bits across
        // adjacent values, which dominates the size of monotonic / smooth
        // numeric columns. fastlanes-on-bits used to live here but treats
        // each f64 as an independent i64, missing the inter-value redundancy.
        let floats: Vec<f64> = values
            .iter()
            .map(|v| match v {
                CellValue::Float64(f) => *f,
                _ => 0.0,
            })
            .collect();
        let encoded = nodedb_codec::gorilla::encode_f64(&floats);
        let mut out = vec![ATTR_TAG_FLOAT64];
        out.extend_from_slice(&encoded);
        return Ok(out);
    }

    // Generic zerompk fallback for String / Bytes / mixed / Null.
    let mut out = vec![ATTR_TAG_MSGPACK];
    out.extend_from_slice(&(values.len() as u32).to_le_bytes());
    for v in values {
        let bytes = zerompk::to_msgpack_vec(v).map_err(|e| ArrayError::SegmentCorruption {
            detail: format!("attr col encode: {e}"),
        })?;
        out.extend_from_slice(&(bytes.len() as u32).to_le_bytes());
        out.extend_from_slice(&bytes);
    }
    Ok(out)
}

pub fn decode_attr_col(data: &[u8]) -> ArrayResult<Vec<CellValue>> {
    if data.is_empty() {
        return Err(ArrayError::SegmentCorruption {
            detail: "attr col: empty payload".into(),
        });
    }
    let tag = data[0];
    let body = &data[1..];

    match tag {
        ATTR_TAG_INT64 => {
            let ints = nodedb_codec::fastlanes::decode(body).map_err(codec_err)?;
            Ok(ints.into_iter().map(CellValue::Int64).collect())
        }
        ATTR_TAG_FLOAT64 => {
            let floats = nodedb_codec::gorilla::decode_f64(body).map_err(codec_err)?;
            Ok(floats.into_iter().map(CellValue::Float64).collect())
        }
        ATTR_TAG_MSGPACK => {
            if body.len() < 4 {
                return Err(ArrayError::SegmentCorruption {
                    detail: "attr col msgpack: truncated count".into(),
                });
            }
            let count = u32::from_le_bytes(body[0..4].try_into().unwrap()) as usize;
            check_decoded_size(count, MAX_COLUMN_ENTRIES, "attr_col_msgpack count")?;
            let mut pos = 4;
            let mut values = Vec::with_capacity(count);
            for _ in 0..count {
                if pos + 4 > body.len() {
                    return Err(ArrayError::SegmentCorruption {
                        detail: "attr col msgpack: truncated entry len".into(),
                    });
                }
                let len = u32::from_le_bytes(body[pos..pos + 4].try_into().unwrap()) as usize;
                pos += 4;
                if pos + len > body.len() {
                    return Err(ArrayError::SegmentCorruption {
                        detail: "attr col msgpack: truncated entry bytes".into(),
                    });
                }
                let v: CellValue = zerompk::from_msgpack(&body[pos..pos + len]).map_err(|e| {
                    ArrayError::SegmentCorruption {
                        detail: format!("attr col decode: {e}"),
                    }
                })?;
                pos += len;
                values.push(v);
            }
            Ok(values)
        }
        other => Err(ArrayError::SegmentCorruption {
            detail: format!("attr col: unknown tag {other:#04x}"),
        }),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn surrogates_empty_roundtrip() {
        let data = encode_surrogates(&[]);
        let out = decode_surrogates(&data).unwrap();
        assert!(out.is_empty());
    }

    #[test]
    fn surrogates_roundtrip() {
        let vals = vec![
            Surrogate::new(0),
            Surrogate::new(1),
            Surrogate::new(1000),
            Surrogate::new(9999),
        ];
        let data = encode_surrogates(&vals);
        let out = decode_surrogates(&data).unwrap();
        assert_eq!(out, vals);
    }

    #[test]
    fn row_kinds_roundtrip() {
        let kinds = vec![0u8, 1, 2, 0, 0, 1];
        let data = encode_row_kinds(&kinds);
        let out = decode_row_kinds(&data).unwrap();
        assert_eq!(out, kinds);
    }

    #[test]
    fn row_kinds_empty_roundtrip() {
        let data = encode_row_kinds(&[]);
        let out = decode_row_kinds(&data).unwrap();
        assert!(out.is_empty());
    }

    #[test]
    fn timestamps_roundtrip() {
        let ts = vec![1_000_000i64, 1_001_000, 1_002_000, 1_100_000];
        let data = encode_timestamps_col(&ts);
        let out = decode_timestamps_col(&data).unwrap();
        assert_eq!(out, ts);
    }

    #[test]
    fn attr_col_int64_roundtrip() {
        let vals = vec![
            CellValue::Int64(10),
            CellValue::Int64(-5),
            CellValue::Int64(0),
        ];
        let data = encode_attr_col(&vals).unwrap();
        let out = decode_attr_col(&data).unwrap();
        assert_eq!(out, vals);
    }

    #[test]
    fn attr_col_float64_roundtrip() {
        let vals = vec![CellValue::Float64(1.5), CellValue::Float64(-2.5)];
        let data = encode_attr_col(&vals).unwrap();
        let out = decode_attr_col(&data).unwrap();
        assert_eq!(out, vals);
    }

    #[test]
    fn attr_col_string_roundtrip() {
        let vals = vec![
            CellValue::String("hello".into()),
            CellValue::String("world".into()),
        ];
        let data = encode_attr_col(&vals).unwrap();
        let out = decode_attr_col(&data).unwrap();
        assert_eq!(out, vals);
    }

    #[test]
    fn attr_col_empty_roundtrip() {
        let data = encode_attr_col(&[]).unwrap();
        let out = decode_attr_col(&data).unwrap();
        assert!(out.is_empty());
    }

    #[test]
    fn attr_col_mixed_types_roundtrip() {
        let vals = vec![
            CellValue::String("x".into()),
            CellValue::Null,
            CellValue::Bytes(vec![1, 2, 3]),
        ];
        let data = encode_attr_col(&vals).unwrap();
        let out = decode_attr_col(&data).unwrap();
        assert_eq!(out, vals);
    }

    #[test]
    fn surrogates_large_roundtrip() {
        let vals: Vec<Surrogate> = (0u32..1000).map(|i| Surrogate::new(i * 7)).collect();
        let data = encode_surrogates(&vals);
        let out = decode_surrogates(&data).unwrap();
        assert_eq!(out, vals);
    }
}
