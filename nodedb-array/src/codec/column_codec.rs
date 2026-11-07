// SPDX-License-Identifier: Apache-2.0

// Per-column codecs for SparseTile structural columns.
//
// surrogates  (Vec<Surrogate>)  → fastlanes (u32 as i64)
// row_kinds   (Vec<u8>)         → raw bytes (sentinel-dominated, not compressible)
// *_ms cols   (Vec<i64>)        → gorilla timestamp encoding
//
// Attr columns live in `attr_col`.

use nodedb_codec::error::CodecError;
use nodedb_types::Surrogate;

use crate::codec::frame::{take_bytes, take_len};
use crate::error::{ArrayError, ArrayResult};

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

pub fn encode_surrogates(surrogates: &[Surrogate]) -> ArrayResult<Vec<u8>> {
    let as_i64: Vec<i64> = surrogates.iter().map(|s| s.as_u32() as i64).collect();
    nodedb_codec::fastlanes::encode(&as_i64).map_err(codec_err)
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
    let mut pos = 0;
    let count = take_len(data, &mut pos, "row_kinds: truncated count")?;
    let kinds = take_bytes(data, &mut pos, count, "row_kinds: truncated body")?;
    Ok(kinds.to_vec())
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

#[cfg(test)]
mod tests {
    use super::*;

    fn encode_surrogates(values: &[Surrogate]) -> Vec<u8> {
        super::encode_surrogates(values).expect("test surrogate encode")
    }

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
    fn a_hostile_row_kind_count_is_corruption() {
        let mut data = u32::MAX.to_le_bytes().to_vec();
        data.extend_from_slice(&[0, 0, 0]);
        assert!(matches!(
            decode_row_kinds(&data),
            Err(ArrayError::SegmentCorruption { .. })
        ));
    }

    #[test]
    fn a_short_row_kind_header_is_corruption() {
        assert!(matches!(
            decode_row_kinds(&[1, 0]),
            Err(ArrayError::SegmentCorruption { .. })
        ));
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
    fn surrogates_large_roundtrip() {
        let vals: Vec<Surrogate> = (0u32..1000).map(|i| Surrogate::new(i * 7)).collect();
        let data = encode_surrogates(&vals);
        let out = decode_surrogates(&data).unwrap();
        assert_eq!(out, vals);
    }
}
