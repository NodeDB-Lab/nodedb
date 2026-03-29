//! Cascading codec pipeline: chains type-aware encoding → terminal compressor.
//!
//! Each `ColumnCodec` variant maps to a fixed pipeline. The `encode_pipeline()`
//! and `decode_pipeline()` functions dispatch to the appropriate chain.
//!
//! Cascading chains:
//! - `AlpFastLanesLz4`:    f64 → ALP → FastLanes bit-pack → lz4
//! - `DeltaFastLanesLz4`:  i64 → Delta → FastLanes bit-pack → lz4
//! - `FastLanesLz4`:       i64 → FastLanes bit-pack → lz4
//!
//! The pipeline writes a 1-byte codec ID header so the decoder knows which
//! chain to reverse. This header is read by `decode_pipeline()`.

use crate::ColumnCodec;
use crate::error::CodecError;

// ---------------------------------------------------------------------------
// Pipeline encode
// ---------------------------------------------------------------------------

/// Encode i64 values through a cascading pipeline.
///
/// For cascading codecs, chains the appropriate stages. For legacy codecs,
/// delegates to the single-step encoder.
pub fn encode_i64_pipeline(values: &[i64], codec: ColumnCodec) -> Result<Vec<u8>, CodecError> {
    match codec {
        // Cascading chains — lz4 terminal.
        ColumnCodec::DeltaFastLanesLz4 => encode_delta_fastlanes_lz4(values),
        ColumnCodec::FastLanesLz4 => encode_fastlanes_lz4_i64(values),
        ColumnCodec::PcodecLz4 => {
            let pco_bytes = crate::pcodec::encode_i64(values)?;
            Ok(crate::lz4::encode(&pco_bytes))
        }
        // Cascading chains — rANS terminal (cold tier).
        ColumnCodec::DeltaFastLanesRans => encode_delta_fastlanes_rans(values),
        // Single-step codecs (small partitions / non-ALP-encodable data).
        ColumnCodec::DoubleDelta => Ok(crate::double_delta::encode(values)),
        ColumnCodec::Delta => Ok(crate::delta::encode(values)),
        ColumnCodec::Gorilla => Ok(crate::gorilla::encode_timestamps(values)),
        ColumnCodec::Raw => {
            let raw: Vec<u8> = values.iter().flat_map(|v| v.to_le_bytes()).collect();
            Ok(crate::raw::encode(&raw))
        }
        ColumnCodec::Lz4 => {
            let raw: Vec<u8> = values.iter().flat_map(|v| v.to_le_bytes()).collect();
            Ok(crate::lz4::encode(&raw))
        }
        ColumnCodec::Zstd => {
            let raw: Vec<u8> = values.iter().flat_map(|v| v.to_le_bytes()).collect();
            crate::zstd_codec::encode(&raw)
        }
        _ => Ok(crate::delta::encode(values)),
    }
}

/// Encode f64 values through a cascading pipeline.
pub fn encode_f64_pipeline(values: &[f64], codec: ColumnCodec) -> Result<Vec<u8>, CodecError> {
    match codec {
        // Cascading chains — lz4 terminal.
        ColumnCodec::AlpFastLanesLz4 => encode_alp_fastlanes_lz4(values),
        ColumnCodec::AlpRdLz4 => {
            let alp_rd_bytes = crate::alp_rd::encode(values)?;
            Ok(crate::lz4::encode(&alp_rd_bytes))
        }
        ColumnCodec::PcodecLz4 => {
            let pco_bytes = crate::pcodec::encode_f64(values)?;
            Ok(crate::lz4::encode(&pco_bytes))
        }
        // Cascading chains — rANS terminal (cold tier).
        ColumnCodec::AlpFastLanesRans => encode_alp_fastlanes_rans(values),
        // Single-step codecs (small partitions / non-ALP-encodable data).
        ColumnCodec::Gorilla => Ok(crate::gorilla::encode_f64(values)),
        ColumnCodec::Raw => {
            let raw: Vec<u8> = values.iter().flat_map(|v| v.to_le_bytes()).collect();
            Ok(crate::raw::encode(&raw))
        }
        ColumnCodec::Lz4 => {
            let raw: Vec<u8> = values.iter().flat_map(|v| v.to_le_bytes()).collect();
            Ok(crate::lz4::encode(&raw))
        }
        ColumnCodec::Zstd => {
            let raw: Vec<u8> = values.iter().flat_map(|v| v.to_le_bytes()).collect();
            crate::zstd_codec::encode(&raw)
        }
        _ => Ok(crate::gorilla::encode_f64(values)),
    }
}

/// Encode raw bytes (symbol columns or string data) through a pipeline.
pub fn encode_bytes_pipeline(raw: &[u8], codec: ColumnCodec) -> Result<Vec<u8>, CodecError> {
    match codec {
        ColumnCodec::FsstLz4 => {
            // FSST expects an array of strings. Treat raw as a single blob.
            let fsst_bytes = crate::fsst::encode(&[raw]);
            Ok(crate::lz4::encode(&fsst_bytes))
        }
        ColumnCodec::FsstRans => {
            let fsst_bytes = crate::fsst::encode(&[raw]);
            Ok(crate::rans::encode(&fsst_bytes))
        }
        ColumnCodec::Raw => Ok(crate::raw::encode(raw)),
        ColumnCodec::Lz4 => Ok(crate::lz4::encode(raw)),
        ColumnCodec::Zstd => crate::zstd_codec::encode(raw),
        ColumnCodec::FastLanesLz4 => {
            // Symbol IDs are u32 — convert to i64, FastLanes pack, lz4.
            if raw.len().is_multiple_of(4) {
                let i64_vals: Vec<i64> = raw
                    .chunks_exact(4)
                    .map(|c| u32::from_le_bytes([c[0], c[1], c[2], c[3]]) as i64)
                    .collect();
                encode_fastlanes_lz4_i64(&i64_vals)
            } else {
                Ok(crate::raw::encode(raw))
            }
        }
        _ => Ok(crate::raw::encode(raw)),
    }
}

// ---------------------------------------------------------------------------
// Pipeline decode
// ---------------------------------------------------------------------------

/// Decode i64 values from a cascading pipeline.
pub fn decode_i64_pipeline(data: &[u8], codec: ColumnCodec) -> Result<Vec<i64>, CodecError> {
    match codec {
        // Cascading — lz4 terminal.
        ColumnCodec::DeltaFastLanesLz4 => decode_delta_fastlanes_lz4(data),
        ColumnCodec::FastLanesLz4 => decode_fastlanes_lz4_i64(data),
        ColumnCodec::PcodecLz4 => {
            let pco_bytes = crate::lz4::decode(data)?;
            crate::pcodec::decode_i64(&pco_bytes)
        }
        // Cascading — rANS terminal.
        ColumnCodec::DeltaFastLanesRans => decode_delta_fastlanes_rans(data),
        // Single-step codecs (small partitions / non-ALP-encodable data).
        ColumnCodec::DoubleDelta => crate::double_delta::decode(data),
        ColumnCodec::Delta => crate::delta::decode(data),
        ColumnCodec::Gorilla => crate::gorilla::decode_timestamps(data),
        ColumnCodec::Raw => {
            let raw = crate::raw::decode(data)?;
            raw_to_i64(&raw)
        }
        ColumnCodec::Lz4 => {
            let raw = crate::lz4::decode(data)?;
            raw_to_i64(&raw)
        }
        ColumnCodec::Zstd => {
            let raw = crate::zstd_codec::decode(data)?;
            raw_to_i64(&raw)
        }
        _ => crate::delta::decode(data),
    }
}

/// Decode f64 values from a cascading pipeline.
pub fn decode_f64_pipeline(data: &[u8], codec: ColumnCodec) -> Result<Vec<f64>, CodecError> {
    match codec {
        // Cascading — lz4 terminal.
        ColumnCodec::AlpFastLanesLz4 => decode_alp_fastlanes_lz4(data),
        ColumnCodec::AlpRdLz4 => {
            let alp_rd_bytes = crate::lz4::decode(data)?;
            crate::alp_rd::decode(&alp_rd_bytes)
        }
        ColumnCodec::PcodecLz4 => {
            let pco_bytes = crate::lz4::decode(data)?;
            crate::pcodec::decode_f64(&pco_bytes)
        }
        // Cascading — rANS terminal.
        ColumnCodec::AlpFastLanesRans => decode_alp_fastlanes_rans(data),
        // Single-step codecs (small partitions / non-ALP-encodable data).
        ColumnCodec::Gorilla => crate::gorilla::decode_f64(data),
        ColumnCodec::Raw => {
            let raw = crate::raw::decode(data)?;
            raw_to_f64(&raw)
        }
        ColumnCodec::Lz4 => {
            let raw = crate::lz4::decode(data)?;
            raw_to_f64(&raw)
        }
        ColumnCodec::Zstd => {
            let raw = crate::zstd_codec::decode(data)?;
            raw_to_f64(&raw)
        }
        _ => crate::gorilla::decode_f64(data),
    }
}

/// Decode raw bytes (symbol columns or string data) from a pipeline.
pub fn decode_bytes_pipeline(data: &[u8], codec: ColumnCodec) -> Result<Vec<u8>, CodecError> {
    match codec {
        ColumnCodec::FsstLz4 => {
            let fsst_bytes = crate::lz4::decode(data)?;
            let strings = crate::fsst::decode(&fsst_bytes)?;
            strings.into_iter().next().ok_or(CodecError::Corrupt {
                detail: "FSST decode produced no output".into(),
            })
        }
        ColumnCodec::FsstRans => {
            let fsst_bytes = crate::rans::decode(data)?;
            let strings = crate::fsst::decode(&fsst_bytes)?;
            strings.into_iter().next().ok_or(CodecError::Corrupt {
                detail: "FSST decode produced no output".into(),
            })
        }
        ColumnCodec::Raw => crate::raw::decode(data),
        ColumnCodec::Lz4 => crate::lz4::decode(data),
        ColumnCodec::Zstd => crate::zstd_codec::decode(data),
        ColumnCodec::FastLanesLz4 => {
            let i64_vals = decode_fastlanes_lz4_i64(data)?;
            Ok(i64_vals
                .iter()
                .flat_map(|&v| (v as u32).to_le_bytes())
                .collect())
        }
        _ => crate::raw::decode(data).or_else(|_| Ok(data.to_vec())),
    }
}

// ---------------------------------------------------------------------------
// Cascading chain implementations
// ---------------------------------------------------------------------------

/// ALP → FastLanes → LZ4: f64 metrics (the big win).
fn encode_alp_fastlanes_lz4(values: &[f64]) -> Result<Vec<u8>, CodecError> {
    // Stage 1: ALP encodes f64 → FastLanes-packed i64 bytes.
    let alp_encoded = crate::alp::encode(values);
    // Stage 2: LZ4 terminal compression on the ALP output.
    Ok(crate::lz4::encode(&alp_encoded))
}

fn decode_alp_fastlanes_lz4(data: &[u8]) -> Result<Vec<f64>, CodecError> {
    // Reverse: LZ4 decompress → ALP decode (which internally FastLanes-decodes).
    let alp_bytes = crate::lz4::decode(data)?;
    crate::alp::decode(&alp_bytes)
}

/// Delta → FastLanes → LZ4: i64 timestamps and counters.
fn encode_delta_fastlanes_lz4(values: &[i64]) -> Result<Vec<u8>, CodecError> {
    if values.is_empty() {
        return Ok(crate::lz4::encode(&crate::fastlanes::encode(&[])));
    }

    // Stage 1a: Compute deltas.
    let mut deltas = Vec::with_capacity(values.len());
    deltas.push(values[0]); // First value stored raw.
    for i in 1..values.len() {
        deltas.push(values[i].wrapping_sub(values[i - 1]));
    }

    // Stage 1b: FastLanes bit-pack the deltas.
    let packed = crate::fastlanes::encode(&deltas);

    // Stage 2: LZ4 terminal.
    Ok(crate::lz4::encode(&packed))
}

fn decode_delta_fastlanes_lz4(data: &[u8]) -> Result<Vec<i64>, CodecError> {
    // Reverse: LZ4 → FastLanes unpack → reconstruct from deltas.
    let packed = crate::lz4::decode(data)?;
    let deltas = crate::fastlanes::decode(&packed)?;

    if deltas.is_empty() {
        return Ok(Vec::new());
    }

    // Reconstruct values from deltas.
    let mut values = Vec::with_capacity(deltas.len());
    values.push(deltas[0]); // First value is raw.
    for &d in &deltas[1..] {
        let prev = values[values.len() - 1];
        values.push(prev.wrapping_add(d));
    }

    Ok(values)
}

/// FastLanes → LZ4: raw integers (symbol IDs, non-delta columns).
fn encode_fastlanes_lz4_i64(values: &[i64]) -> Result<Vec<u8>, CodecError> {
    let packed = crate::fastlanes::encode(values);
    Ok(crate::lz4::encode(&packed))
}

fn decode_fastlanes_lz4_i64(data: &[u8]) -> Result<Vec<i64>, CodecError> {
    let packed = crate::lz4::decode(data)?;
    crate::fastlanes::decode(&packed)
}

/// ALP → FastLanes → rANS: f64 metrics cold tier.
fn encode_alp_fastlanes_rans(values: &[f64]) -> Result<Vec<u8>, CodecError> {
    let alp_encoded = crate::alp::encode(values);
    Ok(crate::rans::encode(&alp_encoded))
}

fn decode_alp_fastlanes_rans(data: &[u8]) -> Result<Vec<f64>, CodecError> {
    let alp_bytes = crate::rans::decode(data)?;
    crate::alp::decode(&alp_bytes)
}

/// Delta → FastLanes → rANS: i64 cold tier.
fn encode_delta_fastlanes_rans(values: &[i64]) -> Result<Vec<u8>, CodecError> {
    if values.is_empty() {
        return Ok(crate::rans::encode(&crate::fastlanes::encode(&[])));
    }

    let mut deltas = Vec::with_capacity(values.len());
    deltas.push(values[0]);
    for i in 1..values.len() {
        deltas.push(values[i].wrapping_sub(values[i - 1]));
    }

    let packed = crate::fastlanes::encode(&deltas);
    Ok(crate::rans::encode(&packed))
}

fn decode_delta_fastlanes_rans(data: &[u8]) -> Result<Vec<i64>, CodecError> {
    let packed = crate::rans::decode(data)?;
    let deltas = crate::fastlanes::decode(&packed)?;

    if deltas.is_empty() {
        return Ok(Vec::new());
    }

    let mut values = Vec::with_capacity(deltas.len());
    values.push(deltas[0]);
    for &d in &deltas[1..] {
        let prev = values[values.len() - 1];
        values.push(prev.wrapping_add(d));
    }

    Ok(values)
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn raw_to_i64(data: &[u8]) -> Result<Vec<i64>, CodecError> {
    if !data.len().is_multiple_of(8) {
        return Err(CodecError::Corrupt {
            detail: "i64 data not aligned to 8 bytes".into(),
        });
    }
    Ok(data
        .chunks_exact(8)
        .map(|c| i64::from_le_bytes([c[0], c[1], c[2], c[3], c[4], c[5], c[6], c[7]]))
        .collect())
}

fn raw_to_f64(data: &[u8]) -> Result<Vec<f64>, CodecError> {
    if !data.len().is_multiple_of(8) {
        return Err(CodecError::Corrupt {
            detail: "f64 data not aligned to 8 bytes".into(),
        });
    }
    Ok(data
        .chunks_exact(8)
        .map(|c| f64::from_le_bytes([c[0], c[1], c[2], c[3], c[4], c[5], c[6], c[7]]))
        .collect())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn alp_fastlanes_lz4_decimal_metrics() {
        let values: Vec<f64> = (0..10_000).map(|i| i as f64 * 0.1).collect();
        let encoded = encode_f64_pipeline(&values, ColumnCodec::AlpFastLanesLz4).unwrap();
        let decoded = decode_f64_pipeline(&encoded, ColumnCodec::AlpFastLanesLz4).unwrap();

        for (i, (a, b)) in values.iter().zip(decoded.iter()).enumerate() {
            assert_eq!(a.to_bits(), b.to_bits(), "mismatch at {i}");
        }

        let raw_size = values.len() * 8;
        let ratio = raw_size as f64 / encoded.len() as f64;
        assert!(
            ratio > 3.0,
            "ALP+FL+LZ4 should compress decimals >3x, got {ratio:.1}x"
        );
    }

    #[test]
    fn alp_beats_gorilla_on_decimals() {
        let mut values = Vec::with_capacity(10_000);
        let mut rng: u64 = 42;
        for _ in 0..10_000 {
            rng = rng.wrapping_mul(6364136223846793005).wrapping_add(1);
            let cpu = ((rng >> 33) as f64 / (u32::MAX as f64)) * 100.0;
            values.push((cpu * 10.0).round() / 10.0);
        }

        let alp_size = encode_f64_pipeline(&values, ColumnCodec::AlpFastLanesLz4)
            .unwrap()
            .len();
        let gorilla_size = encode_f64_pipeline(&values, ColumnCodec::Gorilla)
            .unwrap()
            .len();

        assert!(
            alp_size < gorilla_size,
            "ALP ({alp_size}) should beat Gorilla ({gorilla_size}) on decimal metrics"
        );
    }

    #[test]
    fn delta_fastlanes_lz4_timestamps() {
        let values: Vec<i64> = (0..10_000)
            .map(|i| 1_700_000_000_000 + i * 10_000)
            .collect();
        let encoded = encode_i64_pipeline(&values, ColumnCodec::DeltaFastLanesLz4).unwrap();
        let decoded = decode_i64_pipeline(&encoded, ColumnCodec::DeltaFastLanesLz4).unwrap();
        assert_eq!(decoded, values);

        let raw_size = values.len() * 8;
        let ratio = raw_size as f64 / encoded.len() as f64;
        assert!(
            ratio > 5.0,
            "Delta+FL+LZ4 should compress timestamps >5x, got {ratio:.1}x"
        );
    }

    #[test]
    fn delta_fastlanes_lz4_jittered_timestamps() {
        let mut values = Vec::with_capacity(10_000);
        let mut ts = 1_700_000_000_000i64;
        let mut rng: u64 = 42;
        for _ in 0..10_000 {
            values.push(ts);
            rng = rng.wrapping_mul(6364136223846793005).wrapping_add(1);
            let jitter = ((rng >> 33) as i64 % 101) - 50;
            ts += 10_000 + jitter;
        }
        let encoded = encode_i64_pipeline(&values, ColumnCodec::DeltaFastLanesLz4).unwrap();
        let decoded = decode_i64_pipeline(&encoded, ColumnCodec::DeltaFastLanesLz4).unwrap();
        assert_eq!(decoded, values);
    }

    #[test]
    fn delta_fastlanes_lz4_counters() {
        let values: Vec<i64> = (0..10_000).map(|i| i * 1000).collect();
        let encoded = encode_i64_pipeline(&values, ColumnCodec::DeltaFastLanesLz4).unwrap();
        let decoded = decode_i64_pipeline(&encoded, ColumnCodec::DeltaFastLanesLz4).unwrap();
        assert_eq!(decoded, values);
    }

    #[test]
    fn fastlanes_lz4_symbol_ids() {
        let values: Vec<i64> = (0..5000).map(|i| i % 150).collect();
        let encoded = encode_i64_pipeline(&values, ColumnCodec::FastLanesLz4).unwrap();
        let decoded = decode_i64_pipeline(&encoded, ColumnCodec::FastLanesLz4).unwrap();
        assert_eq!(decoded, values);
    }

    #[test]
    fn legacy_codecs_still_work() {
        let i64_vals: Vec<i64> = (0..1000).collect();
        for codec in [
            ColumnCodec::DoubleDelta,
            ColumnCodec::Delta,
            ColumnCodec::Gorilla,
        ] {
            let encoded = encode_i64_pipeline(&i64_vals, codec).unwrap();
            let decoded = decode_i64_pipeline(&encoded, codec).unwrap();
            assert_eq!(decoded, i64_vals, "legacy i64 codec {codec} failed");
        }

        let f64_vals: Vec<f64> = (0..1000).map(|i| i as f64 * 0.5).collect();
        let encoded = encode_f64_pipeline(&f64_vals, ColumnCodec::Gorilla).unwrap();
        let decoded = decode_f64_pipeline(&encoded, ColumnCodec::Gorilla).unwrap();
        for (a, b) in f64_vals.iter().zip(decoded.iter()) {
            assert_eq!(a.to_bits(), b.to_bits());
        }
    }

    #[test]
    fn empty_values() {
        let empty_i64: Vec<i64> = vec![];
        let empty_f64: Vec<f64> = vec![];

        for codec in [
            ColumnCodec::DeltaFastLanesLz4,
            ColumnCodec::FastLanesLz4,
            ColumnCodec::AlpFastLanesLz4,
        ] {
            if matches!(codec, ColumnCodec::AlpFastLanesLz4) {
                let enc = encode_f64_pipeline(&empty_f64, codec).unwrap();
                let dec = decode_f64_pipeline(&enc, codec).unwrap();
                assert!(dec.is_empty());
            } else {
                let enc = encode_i64_pipeline(&empty_i64, codec).unwrap();
                let dec = decode_i64_pipeline(&enc, codec).unwrap();
                assert!(dec.is_empty());
            }
        }
    }

    #[test]
    fn bytes_pipeline_roundtrip() {
        let raw: Vec<u8> = (0..1000u32).flat_map(|i| i.to_le_bytes()).collect();
        for codec in [ColumnCodec::Raw, ColumnCodec::Lz4] {
            let encoded = encode_bytes_pipeline(&raw, codec).unwrap();
            let decoded = decode_bytes_pipeline(&encoded, codec).unwrap();
            assert_eq!(decoded, raw, "bytes pipeline {codec} failed");
        }
    }
}
