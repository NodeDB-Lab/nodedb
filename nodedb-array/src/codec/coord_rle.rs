// Leading-axis run-length compression for DimDict index streams.
//
// When the leading axis has long runs of the same dict index (common in
// genomic tiles where chrom is constant across thousands of positions),
// RLE saves ~75 % vs raw indices. For tiles with no runs longer than 2,
// we fall back to the delta codec so overhead is bounded.
//
// Wire format:
//   RLE mode:   [u32 LE = 0xFFFF_FFFF marker][u32 LE run_count] ...
//   Delta mode: delegates to coord_delta::encode_coord_axis / decode_coord_axis
//
// The RLE marker 0xFFFF_FFFF is safe because DimDict cardinality fits in
// a u32 but realistic tile dicts never exceed ~10M distinct values; an
// all-ones 4-byte value is reserved as the RLE sentinel.

use crate::codec::coord_delta::{decode_coord_axis, encode_coord_axis};
use crate::codec::limits::{
    MAX_CELLS_PER_TILE, MAX_DICT_CARDINALITY, MAX_RLE_RUN_LEN, MAX_RLE_RUNS, check_decoded_size,
};
use crate::error::{ArrayError, ArrayResult};
use crate::tile::sparse_tile::DimDict;
use crate::types::coord::value::CoordValue;

/// Minimum ratio of run savings to total indices before RLE is preferred.
/// Using integer arithmetic: savings_cells / total_cells >= 1/2.
const RLE_BENEFIT_NUMERATOR: usize = 1;
const RLE_BENEFIT_DENOMINATOR: usize = 2;

fn should_use_rle(indices: &[u32]) -> bool {
    if indices.len() < 4 {
        return false;
    }
    let mut run_count = 1usize;
    for i in 1..indices.len() {
        if indices[i] != indices[i - 1] {
            run_count += 1;
        }
    }
    // RLE cells needed: run_count * 8 bytes (value + length pairs)
    // Delta cells approx: indices.len() * 1.5 bytes average
    // Use simpler heuristic: worth it if run_count < indices.len() / 2
    run_count * RLE_BENEFIT_DENOMINATOR < indices.len() * RLE_BENEFIT_NUMERATOR
}

/// RLE mode sentinel: u32::MAX. This value can never appear as a dict count
/// because a DimDict with u32::MAX distinct values would require ~16 GB of
/// coordinate storage in the tile before it reaches this codec.
const RLE_MARKER: u32 = u32::MAX;

/// Encode a DimDict using RLE if beneficial, falling back to delta otherwise.
pub fn encode_coord_axis_rle(dict: &DimDict, out: &mut Vec<u8>) -> ArrayResult<()> {
    if should_use_rle(&dict.indices) {
        encode_rle(dict, out)
    } else {
        encode_coord_axis(dict, out)
    }
}

fn encode_rle(dict: &DimDict, out: &mut Vec<u8>) -> ArrayResult<()> {
    out.extend_from_slice(&RLE_MARKER.to_le_bytes());

    // Compute runs over index stream.
    let mut runs: Vec<(u32, u32)> = Vec::new(); // (value, length)
    let mut i = 0;
    while i < dict.indices.len() {
        let val = dict.indices[i];
        let mut len = 1u32;
        while i + (len as usize) < dict.indices.len() && dict.indices[i + (len as usize)] == val {
            len += 1;
        }
        runs.push((val, len));
        i += len as usize;
    }

    out.extend_from_slice(&(runs.len() as u32).to_le_bytes());
    for (val, len) in &runs {
        out.extend_from_slice(&val.to_le_bytes());
        out.extend_from_slice(&len.to_le_bytes());
    }

    // Dict values.
    out.extend_from_slice(&(dict.values.len() as u32).to_le_bytes());
    for cv in &dict.values {
        let bytes = zerompk::to_msgpack_vec(cv).map_err(|e| ArrayError::SegmentCorruption {
            detail: format!("rle coord dict encode: {e}"),
        })?;
        out.extend_from_slice(&(bytes.len() as u32).to_le_bytes());
        out.extend_from_slice(&bytes);
    }

    Ok(())
}

/// Decode a DimDict from bytes previously encoded by `encode_coord_axis_rle`.
pub fn decode_coord_axis_rle(data: &[u8], pos: &mut usize) -> ArrayResult<DimDict> {
    if *pos + 4 > data.len() {
        return Err(ArrayError::SegmentCorruption {
            detail: "rle coord: truncated mode marker".into(),
        });
    }
    let first_u32 = u32::from_le_bytes(data[*pos..*pos + 4].try_into().unwrap());

    if first_u32 == RLE_MARKER {
        *pos += 4;
        decode_rle(data, pos)
    } else {
        // Delta mode — don't advance pos; coord_delta reads dict_count first.
        decode_coord_axis(data, pos)
    }
}

fn decode_rle(data: &[u8], pos: &mut usize) -> ArrayResult<DimDict> {
    if *pos + 4 > data.len() {
        return Err(ArrayError::SegmentCorruption {
            detail: "rle coord: truncated run count".into(),
        });
    }
    let run_count = u32::from_le_bytes(data[*pos..*pos + 4].try_into().unwrap()) as usize;
    *pos += 4;
    check_decoded_size(run_count, MAX_RLE_RUNS, "rle run_count")?;

    let mut indices: Vec<u32> = Vec::new();
    let mut total_len: usize = 0;
    for _ in 0..run_count {
        if *pos + 8 > data.len() {
            return Err(ArrayError::SegmentCorruption {
                detail: "rle coord: truncated run entry".into(),
            });
        }
        let val = u32::from_le_bytes(data[*pos..*pos + 4].try_into().unwrap());
        *pos += 4;
        let len = u32::from_le_bytes(data[*pos..*pos + 4].try_into().unwrap()) as usize;
        *pos += 4;
        check_decoded_size(len, MAX_RLE_RUN_LEN, "rle run len")?;
        total_len = total_len.saturating_add(len);
        check_decoded_size(total_len, MAX_CELLS_PER_TILE, "rle indices total_len")?;
        for _ in 0..len {
            indices.push(val);
        }
    }

    // Dict values.
    if *pos + 4 > data.len() {
        return Err(ArrayError::SegmentCorruption {
            detail: "rle coord: truncated dict count".into(),
        });
    }
    let dict_count = u32::from_le_bytes(data[*pos..*pos + 4].try_into().unwrap()) as usize;
    *pos += 4;
    check_decoded_size(dict_count, MAX_DICT_CARDINALITY, "rle dict_count")?;

    let mut values: Vec<CoordValue> = Vec::with_capacity(dict_count);
    for _ in 0..dict_count {
        if *pos + 4 > data.len() {
            return Err(ArrayError::SegmentCorruption {
                detail: "rle coord: truncated dict entry len".into(),
            });
        }
        let len = u32::from_le_bytes(data[*pos..*pos + 4].try_into().unwrap()) as usize;
        *pos += 4;
        if *pos + len > data.len() {
            return Err(ArrayError::SegmentCorruption {
                detail: "rle coord: truncated dict entry bytes".into(),
            });
        }
        let cv: CoordValue = zerompk::from_msgpack(&data[*pos..*pos + len]).map_err(|e| {
            ArrayError::SegmentCorruption {
                detail: format!("rle coord dict decode: {e}"),
            }
        })?;
        *pos += len;
        values.push(cv);
    }

    Ok(DimDict { values, indices })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::coord::value::CoordValue;

    fn make_dict(indices: Vec<u32>, values: Vec<CoordValue>) -> DimDict {
        DimDict { values, indices }
    }

    fn roundtrip(dict: &DimDict) -> DimDict {
        let mut buf = Vec::new();
        encode_coord_axis_rle(dict, &mut buf).unwrap();
        let mut pos = 0;
        decode_coord_axis_rle(&buf, &mut pos).unwrap()
    }

    #[test]
    fn empty_dict_roundtrip() {
        let d = make_dict(vec![], vec![]);
        let out = roundtrip(&d);
        assert_eq!(out.indices, d.indices);
        assert_eq!(out.values, d.values);
    }

    #[test]
    fn single_entry_roundtrip() {
        let d = make_dict(vec![0], vec![CoordValue::Int64(5)]);
        let out = roundtrip(&d);
        assert_eq!(out.indices, d.indices);
        assert_eq!(out.values, d.values);
    }

    #[test]
    fn long_run_uses_rle() {
        // 1000 cells all pointing at index 0 — should trigger RLE.
        let d = make_dict(vec![0u32; 1000], vec![CoordValue::Int64(42)]);
        let mut buf = Vec::new();
        encode_coord_axis_rle(&d, &mut buf).unwrap();
        // RLE mode: first 4 bytes are the RLE_MARKER sentinel.
        let first = u32::from_le_bytes(buf[0..4].try_into().unwrap());
        assert_eq!(first, u32::MAX, "expected RLE mode marker");

        let out = roundtrip(&d);
        assert_eq!(out.indices, d.indices);
    }

    #[test]
    fn no_runs_falls_back_to_delta() {
        // All distinct coords — no benefit from RLE.
        let values: Vec<CoordValue> = (0..10).map(CoordValue::Int64).collect();
        let indices: Vec<u32> = (0..10).collect();
        let d = make_dict(indices, values);
        let mut buf = Vec::new();
        encode_coord_axis_rle(&d, &mut buf).unwrap();
        // Delta mode: first 4 bytes are dict_count (10), not the RLE marker.
        let first = u32::from_le_bytes(buf[0..4].try_into().unwrap());
        assert_ne!(first, u32::MAX, "should be delta mode");

        let out = roundtrip(&d);
        assert_eq!(out.indices, d.indices);
    }

    #[test]
    fn mixed_runs_roundtrip() {
        // Two chromosomes, each repeated 500 times.
        let mut indices = vec![0u32; 500];
        indices.extend(vec![1u32; 500]);
        let d = make_dict(indices, vec![CoordValue::Int64(1), CoordValue::Int64(2)]);
        let out = roundtrip(&d);
        assert_eq!(out.indices, d.indices);
        assert_eq!(out.values, d.values);
    }

    #[test]
    fn large_rle_roundtrip() {
        let d = make_dict(vec![0u32; 100_000], vec![CoordValue::Int64(99)]);
        let out = roundtrip(&d);
        assert_eq!(out.indices.len(), 100_000);
        assert!(out.indices.iter().all(|&i| i == 0));
    }

    #[test]
    fn timestamp_coord_roundtrip() {
        let values = vec![
            CoordValue::TimestampMs(1_000_000),
            CoordValue::TimestampMs(2_000_000),
        ];
        let indices: Vec<u32> = (0..10).map(|i| i % 2).collect();
        let d = make_dict(indices, values);
        let out = roundtrip(&d);
        assert_eq!(out.indices, d.indices);
        assert_eq!(out.values, d.values);
    }
}
