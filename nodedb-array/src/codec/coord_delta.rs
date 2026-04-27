// Per-axis zigzag-varint delta encode/decode over a `DimDict`'s index stream.
//
// The DimDict stores (dict_values, indices). For each cell, we record the
// dict index. Delta-encoding the index stream compresses monotonic coordinate
// access patterns (common in range scans) from 4 bytes/cell to 1-2 bytes.
// Non-monotonic and repeated-value patterns are handled correctly — the
// zigzag mapping keeps backward steps small too.
//
// Wire format per axis:
//   [u32 LE] dict entry count (= cardinality of distinct values)
//   [N * msgpack bytes] serialized CoordValue dict entries (zerompk)
//   [u32 LE] index count (= cell count)
//   [zigzag-varint deltas ...] index deltas (first value as-is, then deltas)

use crate::codec::limits::{MAX_CELLS_PER_TILE, MAX_DICT_CARDINALITY, check_decoded_size};
use crate::error::{ArrayError, ArrayResult};
use crate::tile::sparse_tile::DimDict;
use crate::types::coord::value::CoordValue;

// ---------------------------------------------------------------------------
// Zigzag varint helpers
// ---------------------------------------------------------------------------

fn zigzag_encode(v: i64) -> u64 {
    ((v << 1) ^ (v >> 63)) as u64
}

fn zigzag_decode(v: u64) -> i64 {
    ((v >> 1) as i64) ^ (-((v & 1) as i64))
}

fn write_varint(out: &mut Vec<u8>, mut v: u64) {
    loop {
        let b = (v & 0x7F) as u8;
        v >>= 7;
        if v == 0 {
            out.push(b);
            break;
        } else {
            out.push(b | 0x80);
        }
    }
}

/// Returns (value, bytes_consumed). Returns None on truncated input.
fn read_varint(data: &[u8]) -> Option<(u64, usize)> {
    let mut val: u64 = 0;
    let mut shift = 0u32;
    for (i, &b) in data.iter().enumerate() {
        val |= ((b & 0x7F) as u64) << shift;
        if b & 0x80 == 0 {
            return Some((val, i + 1));
        }
        shift += 7;
        if shift >= 70 {
            return None;
        }
    }
    None
}

// ---------------------------------------------------------------------------
// Public API
// ---------------------------------------------------------------------------

/// Tag byte preceding the dict-values block. Selects the encoding format
/// for the distinct coordinate values stored by the `DimDict`.
const DICT_TAG_GENERIC: u8 = 0; // legacy: per-value zerompk msgpack
const DICT_TAG_INT64_FASTLANES: u8 = 1; // homogeneous Int64 dict via fastlanes

fn try_encode_int64_dict(values: &[CoordValue]) -> Option<Vec<i64>> {
    let mut out = Vec::with_capacity(values.len());
    for v in values {
        match v {
            CoordValue::Int64(n) => out.push(*n),
            _ => return None,
        }
    }
    Some(out)
}

/// Encode one axis of a `DimDict` into a byte vector.
pub fn encode_coord_axis(dict: &DimDict, out: &mut Vec<u8>) -> ArrayResult<()> {
    // Dict entries: tag + count + values. Per-axis homogeneous-Int64 dicts
    // are batch-encoded with fastlanes; mixed/string dicts fall back to
    // per-value zerompk msgpack.
    if let Some(ints) = try_encode_int64_dict(&dict.values) {
        out.push(DICT_TAG_INT64_FASTLANES);
        out.extend_from_slice(&(ints.len() as u32).to_le_bytes());
        let encoded = nodedb_codec::fastlanes::encode(&ints);
        out.extend_from_slice(&(encoded.len() as u32).to_le_bytes());
        out.extend_from_slice(&encoded);
    } else {
        out.push(DICT_TAG_GENERIC);
        let dict_count = dict.values.len() as u32;
        out.extend_from_slice(&dict_count.to_le_bytes());
        for cv in &dict.values {
            let bytes = zerompk::to_msgpack_vec(cv).map_err(|e| ArrayError::SegmentCorruption {
                detail: format!("coord dict encode: {e}"),
            })?;
            let len = bytes.len() as u32;
            out.extend_from_slice(&len.to_le_bytes());
            out.extend_from_slice(&bytes);
        }
    }

    // Index stream: count + zigzag-varint deltas.
    let idx_count = dict.indices.len() as u32;
    out.extend_from_slice(&idx_count.to_le_bytes());
    let mut prev: i64 = 0;
    for &idx in &dict.indices {
        let cur = idx as i64;
        let delta = cur - prev;
        write_varint(out, zigzag_encode(delta));
        prev = cur;
    }
    Ok(())
}

/// Decode one axis from `data[pos..]`, advancing `pos` past consumed bytes.
pub fn decode_coord_axis(data: &[u8], pos: &mut usize) -> ArrayResult<DimDict> {
    // Dict entries — tag-dispatched.
    if *pos >= data.len() {
        return Err(ArrayError::SegmentCorruption {
            detail: "coord axis: truncated dict tag".into(),
        });
    }
    let dict_tag = data[*pos];
    *pos += 1;

    let dict_values: Vec<CoordValue> = match dict_tag {
        DICT_TAG_GENERIC => {
            if *pos + 4 > data.len() {
                return Err(ArrayError::SegmentCorruption {
                    detail: "coord axis: truncated dict count".into(),
                });
            }
            let dict_count = u32::from_le_bytes(data[*pos..*pos + 4].try_into().unwrap()) as usize;
            *pos += 4;
            check_decoded_size(dict_count, MAX_DICT_CARDINALITY, "coord_delta dict_count")?;

            let mut out: Vec<CoordValue> = Vec::with_capacity(dict_count);
            for _ in 0..dict_count {
                if *pos + 4 > data.len() {
                    return Err(ArrayError::SegmentCorruption {
                        detail: "coord axis: truncated dict entry len".into(),
                    });
                }
                let len = u32::from_le_bytes(data[*pos..*pos + 4].try_into().unwrap()) as usize;
                *pos += 4;
                if *pos + len > data.len() {
                    return Err(ArrayError::SegmentCorruption {
                        detail: "coord axis: truncated dict entry bytes".into(),
                    });
                }
                let cv: CoordValue =
                    zerompk::from_msgpack(&data[*pos..*pos + len]).map_err(|e| {
                        ArrayError::SegmentCorruption {
                            detail: format!("coord dict decode: {e}"),
                        }
                    })?;
                *pos += len;
                out.push(cv);
            }
            out
        }
        DICT_TAG_INT64_FASTLANES => {
            if *pos + 8 > data.len() {
                return Err(ArrayError::SegmentCorruption {
                    detail: "coord axis: truncated int64-dict header".into(),
                });
            }
            let dict_count = u32::from_le_bytes(data[*pos..*pos + 4].try_into().unwrap()) as usize;
            *pos += 4;
            check_decoded_size(
                dict_count,
                MAX_DICT_CARDINALITY,
                "coord_delta int64-dict count",
            )?;
            let payload_len = u32::from_le_bytes(data[*pos..*pos + 4].try_into().unwrap()) as usize;
            *pos += 4;
            if *pos + payload_len > data.len() {
                return Err(ArrayError::SegmentCorruption {
                    detail: "coord axis: truncated int64-dict payload".into(),
                });
            }
            let ints =
                nodedb_codec::fastlanes::decode(&data[*pos..*pos + payload_len]).map_err(|e| {
                    ArrayError::SegmentCorruption {
                        detail: format!("coord dict fastlanes decode: {e}"),
                    }
                })?;
            *pos += payload_len;
            if ints.len() != dict_count {
                return Err(ArrayError::SegmentCorruption {
                    detail: format!(
                        "coord dict count mismatch: declared {dict_count}, decoded {}",
                        ints.len()
                    ),
                });
            }
            ints.into_iter().map(CoordValue::Int64).collect()
        }
        other => {
            return Err(ArrayError::SegmentCorruption {
                detail: format!("coord axis: unknown dict tag {other:#04x}"),
            });
        }
    };

    // Index stream.
    if *pos + 4 > data.len() {
        return Err(ArrayError::SegmentCorruption {
            detail: "coord axis: truncated index count".into(),
        });
    }
    let idx_count = u32::from_le_bytes(data[*pos..*pos + 4].try_into().unwrap()) as usize;
    *pos += 4;
    check_decoded_size(idx_count, MAX_CELLS_PER_TILE, "coord_delta idx_count")?;

    let mut indices: Vec<u32> = Vec::with_capacity(idx_count);
    let mut prev: i64 = 0;
    for _ in 0..idx_count {
        let (zz, consumed) =
            read_varint(&data[*pos..]).ok_or_else(|| ArrayError::SegmentCorruption {
                detail: "coord axis: truncated index varint".into(),
            })?;
        *pos += consumed;
        let delta = zigzag_decode(zz);
        let cur = prev + delta;
        indices.push(cur as u32);
        prev = cur;
    }

    Ok(DimDict {
        values: dict_values,
        indices,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::coord::value::CoordValue;

    fn roundtrip(dict: &DimDict) -> DimDict {
        let mut buf = Vec::new();
        encode_coord_axis(dict, &mut buf).unwrap();
        let mut pos = 0;
        decode_coord_axis(&buf, &mut pos).unwrap()
    }

    fn make_dict(coords: &[i64]) -> DimDict {
        let mut d = DimDict {
            values: vec![],
            indices: vec![],
        };
        for &c in coords {
            let cv = CoordValue::Int64(c);
            if let Some(idx) = d.values.iter().position(|x| x == &cv) {
                d.indices.push(idx as u32);
            } else {
                d.indices.push(d.values.len() as u32);
                d.values.push(cv);
            }
        }
        d
    }

    #[test]
    fn empty_dict_roundtrip() {
        let d = DimDict {
            values: vec![],
            indices: vec![],
        };
        let out = roundtrip(&d);
        assert_eq!(out.values, d.values);
        assert_eq!(out.indices, d.indices);
    }

    #[test]
    fn single_entry_roundtrip() {
        let d = make_dict(&[42]);
        let out = roundtrip(&d);
        assert_eq!(out.values, d.values);
        assert_eq!(out.indices, d.indices);
    }

    #[test]
    fn monotonic_coords_roundtrip() {
        let coords: Vec<i64> = (0..1000).collect();
        let d = make_dict(&coords);
        let out = roundtrip(&d);
        assert_eq!(out.indices, d.indices);
        assert_eq!(out.values.len(), d.values.len());
    }

    #[test]
    fn non_monotonic_coords_roundtrip() {
        let coords = vec![100i64, 5, 200, 5, 100, 300];
        let d = make_dict(&coords);
        let out = roundtrip(&d);
        assert_eq!(out.indices, d.indices);
        assert_eq!(out.values, d.values);
    }

    #[test]
    fn repeated_coords_roundtrip() {
        let coords = vec![7i64; 100];
        let d = make_dict(&coords);
        let out = roundtrip(&d);
        assert_eq!(out.indices, d.indices);
        assert_eq!(out.values.len(), 1);
    }

    #[test]
    fn large_coord_roundtrip() {
        let coords: Vec<i64> = (0..100_000).step_by(7).collect();
        let d = make_dict(&coords);
        let out = roundtrip(&d);
        assert_eq!(out.indices, d.indices);
    }

    #[test]
    fn string_coord_roundtrip() {
        let mut d = DimDict {
            values: vec![],
            indices: vec![],
        };
        for s in &["ALT", "REF", "ALT", "DEL"] {
            let cv = CoordValue::String(s.to_string());
            if let Some(idx) = d.values.iter().position(|x| x == &cv) {
                d.indices.push(idx as u32);
            } else {
                d.indices.push(d.values.len() as u32);
                d.values.push(cv);
            }
        }
        let out = roundtrip(&d);
        assert_eq!(out.indices, d.indices);
        assert_eq!(out.values, d.values);
    }
}
