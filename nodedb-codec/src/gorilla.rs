// SPDX-License-Identifier: Apache-2.0

//! Gorilla XOR encoding for floating-point timeseries metrics.
//!
//! Implements the Facebook Gorilla paper's XOR-based compression for
//! double-precision floating-point values. Achieves ~1.5 bytes per
//! 16-byte (timestamp + value) sample by exploiting temporal locality
//! in metric streams.
//!
//! Also usable for non-monotonic i64 values by casting through f64 bits.
//!
//! Wire format:
//! ```text
//! [4 bytes] sample count (LE u32)
//! [N bytes] bitstream: first sample raw (64+64 bits), then delta-of-delta
//!           timestamps + XOR-compressed values
//! ```
//!
//! Reference: "Gorilla: A Fast, Scalable, In-Memory Time Series Database"
//! (Pelkonen et al., VLDB 2015)

use crate::double_delta::{BitReader, BitWriter};
use crate::error::CodecError;

// ---------------------------------------------------------------------------
// Encoder
// ---------------------------------------------------------------------------

/// Gorilla XOR encoder for (timestamp, f64) sample streams.
///
/// Timestamps use delta-of-delta encoding. Values use XOR with
/// leading/trailing zero compression.
#[derive(Debug)]
pub struct GorillaEncoder {
    buf: BitWriter,
    prev_ts: i64,
    prev_delta: i64,
    prev_value: u64,
    prev_leading: u8,
    prev_trailing: u8,
    count: u64,
}

impl GorillaEncoder {
    pub fn new() -> Self {
        Self {
            buf: BitWriter::new(),
            prev_ts: 0,
            prev_delta: 0,
            prev_value: 0,
            prev_leading: u8::MAX,
            prev_trailing: 0,
            count: 0,
        }
    }

    /// Encode a (timestamp_ms, value) sample.
    pub fn encode(&mut self, timestamp_ms: i64, value: f64) {
        let value_bits = value.to_bits();

        if self.count == 0 {
            self.buf.write_bits(timestamp_ms as u64, 64);
            self.buf.write_bits(value_bits, 64);
            self.prev_ts = timestamp_ms;
            self.prev_value = value_bits;
            self.count = 1;
            return;
        }

        // Timestamp: delta-of-delta.
        let delta = timestamp_ms.wrapping_sub(self.prev_ts);
        let dod = delta.wrapping_sub(self.prev_delta);
        self.encode_timestamp_dod(dod);
        self.prev_ts = timestamp_ms;
        self.prev_delta = delta;

        // Value: XOR.
        let xor = self.prev_value ^ value_bits;
        self.encode_value_xor(xor);
        self.prev_value = value_bits;

        self.count += 1;
    }

    fn encode_timestamp_dod(&mut self, dod: i64) {
        if dod == 0 {
            self.buf.write_bit(false);
        } else if (-64..=63).contains(&dod) {
            self.buf.write_bits(0b10, 2);
            self.buf.write_bits((dod as u64) & 0x7F, 7);
        } else if (-256..=255).contains(&dod) {
            self.buf.write_bits(0b110, 3);
            self.buf.write_bits((dod as u64) & 0x1FF, 9);
        } else if (-2048..=2047).contains(&dod) {
            self.buf.write_bits(0b1110, 4);
            self.buf.write_bits((dod as u64) & 0xFFF, 12);
        } else {
            self.buf.write_bits(0b1111, 4);
            self.buf.write_bits(dod as u64, 64);
        }
    }

    fn encode_value_xor(&mut self, xor: u64) {
        if xor == 0 {
            self.buf.write_bit(false);
            return;
        }

        self.buf.write_bit(true);

        let leading = xor.leading_zeros() as u8;
        let trailing = xor.trailing_zeros() as u8;

        if self.prev_leading != u8::MAX
            && leading >= self.prev_leading
            && trailing >= self.prev_trailing
        {
            // Fits within previous window.
            self.buf.write_bit(false);
            let meaningful_bits = 64 - self.prev_leading - self.prev_trailing;
            self.buf
                .write_bits(xor >> self.prev_trailing, meaningful_bits as usize);
        } else {
            // New window.
            self.buf.write_bit(true);
            self.buf.write_bits(leading as u64, 6);
            let meaningful_bits = 64 - leading - trailing;
            self.buf.write_bits((meaningful_bits - 1) as u64, 6);
            self.buf
                .write_bits(xor >> trailing, meaningful_bits as usize);
            self.prev_leading = leading;
            self.prev_trailing = trailing;
        }
    }

    /// Finish encoding and return compressed bytes.
    ///
    /// Prepends a 4-byte LE sample count header.
    pub fn finish(self) -> Vec<u8> {
        let count_bytes = (self.count as u32).to_le_bytes();
        let bitstream = self.buf.as_bytes();
        let mut out = Vec::with_capacity(4 + bitstream.len());
        out.extend_from_slice(&count_bytes);
        out.extend_from_slice(bitstream);
        out
    }

    pub fn count(&self) -> u64 {
        self.count
    }

    pub fn compressed_size(&self) -> usize {
        self.buf.bit_len().div_ceil(8)
    }
}

impl Default for GorillaEncoder {
    fn default() -> Self {
        Self::new()
    }
}

// ---------------------------------------------------------------------------
// Decoder
// ---------------------------------------------------------------------------

/// Gorilla XOR decoder for (timestamp, f64) sample streams.
pub struct GorillaDecoder<'a> {
    reader: BitReader<'a>,
    prev_ts: i64,
    prev_delta: i64,
    prev_value: u64,
    prev_leading: u8,
    prev_trailing: u8,
    count: u64,
    total: u64,
    first: bool,
}

impl<'a> GorillaDecoder<'a> {
    /// Create a decoder from compressed bytes.
    ///
    /// Expects a 4-byte LE sample count header followed by the bitstream.
    pub fn new(buf: &'a [u8]) -> Self {
        if buf.len() < 4 {
            return Self {
                reader: BitReader::new(&[]),
                prev_ts: 0,
                prev_delta: 0,
                prev_value: 0,
                prev_leading: 0,
                prev_trailing: 0,
                count: 0,
                total: 0,
                first: true,
            };
        }
        let total = u32::from_le_bytes([buf[0], buf[1], buf[2], buf[3]]) as u64;
        Self {
            reader: BitReader::new(&buf[4..]),
            prev_ts: 0,
            prev_delta: 0,
            prev_value: 0,
            prev_leading: 0,
            prev_trailing: 0,
            count: 0,
            total,
            first: true,
        }
    }

    /// Decode the next sample, or None if all samples decoded.
    pub fn next_sample(&mut self) -> Option<(i64, f64)> {
        if self.count >= self.total {
            return None;
        }

        if self.first {
            self.first = false;
            let ts = self.reader.read_bits(64).ok()? as i64;
            let val = self.reader.read_bits(64).ok()?;
            self.prev_ts = ts;
            self.prev_value = val;
            self.count = 1;
            return Some((ts, f64::from_bits(val)));
        }

        let ts = self.decode_timestamp().ok()?;
        let val = self.decode_value().ok()?;
        self.count += 1;
        Some((ts, f64::from_bits(val)))
    }

    fn decode_timestamp(&mut self) -> Result<i64, CodecError> {
        let bit = self.reader.read_bit()?;
        let dod = if !bit {
            0i64
        } else {
            let bit2 = self.reader.read_bit()?;
            if !bit2 {
                let raw = self.reader.read_bits(7)? as i64;
                sign_extend(raw, 7)
            } else {
                let bit3 = self.reader.read_bit()?;
                if !bit3 {
                    let raw = self.reader.read_bits(9)? as i64;
                    sign_extend(raw, 9)
                } else {
                    let bit4 = self.reader.read_bit()?;
                    if !bit4 {
                        let raw = self.reader.read_bits(12)? as i64;
                        sign_extend(raw, 12)
                    } else {
                        self.reader.read_bits(64)? as i64
                    }
                }
            }
        };

        let delta = self.prev_delta.wrapping_add(dod);
        let ts = self.prev_ts.wrapping_add(delta);
        self.prev_ts = ts;
        self.prev_delta = delta;
        Ok(ts)
    }

    fn decode_value(&mut self) -> Result<u64, CodecError> {
        let bit = self.reader.read_bit()?;
        if !bit {
            return Ok(self.prev_value);
        }

        let bit2 = self.reader.read_bit()?;
        let xor = if !bit2 {
            let meaningful_bits = 64 - self.prev_leading - self.prev_trailing;
            let bits = self.reader.read_bits(meaningful_bits as usize)?;
            bits << self.prev_trailing
        } else {
            let leading = self.reader.read_bits(6)? as u8;
            let meaningful_bits = self.reader.read_bits(6)? as u8 + 1;
            let window_bits = leading + meaningful_bits;
            if window_bits > 64 {
                return Err(CodecError::Corrupt {
                    detail: format!(
                        "invalid Gorilla value window: {leading} leading + {meaningful_bits} meaningful bits"
                    ),
                });
            }
            let trailing = 64 - window_bits;
            let bits = self.reader.read_bits(meaningful_bits as usize)?;
            self.prev_leading = leading;
            self.prev_trailing = trailing;
            bits << trailing
        };

        let val = self.prev_value ^ xor;
        self.prev_value = val;
        Ok(val)
    }

    /// Decode all remaining samples.
    pub fn decode_all(&mut self) -> Vec<(i64, f64)> {
        let mut samples = Vec::new();
        while let Some(s) = self.next_sample() {
            samples.push(s);
        }
        samples
    }
}

fn sign_extend(value: i64, bits: u32) -> i64 {
    let shift = 64 - bits;
    (value << shift) >> shift
}

// ---------------------------------------------------------------------------
// Convenience functions for pure-value encoding (no timestamps)
// ---------------------------------------------------------------------------

/// Encode a slice of f64 values using Gorilla XOR compression.
///
/// Uses synthetic sequential timestamps (0, 1, 2, ...) so the timestamp
/// channel compresses to near-zero overhead.
pub fn encode_f64(values: &[f64]) -> Vec<u8> {
    let mut enc = GorillaEncoder::new();
    for (i, &v) in values.iter().enumerate() {
        enc.encode(i as i64, v);
    }
    enc.finish()
}

/// Decode Gorilla-compressed f64 values (encoded with `encode_f64`).
pub fn decode_f64(data: &[u8]) -> Result<Vec<f64>, CodecError> {
    let mut dec = GorillaDecoder::new(data);
    let samples = dec.decode_all();
    if samples.len() != dec.total as usize {
        return Err(CodecError::Truncated {
            expected: dec.total as usize,
            actual: samples.len(),
        });
    }
    Ok(samples.into_iter().map(|(_, v)| v).collect())
}

/// Encode a slice of i64 timestamps using Gorilla (value channel unused).
///
/// For timestamps, prefer `DoubleDelta` codec — it compresses ~4x better.
/// The array tile encoder writes its timestamp columns with this function.
pub fn encode_timestamps(timestamps: &[i64]) -> Vec<u8> {
    let mut enc = GorillaEncoder::new();
    for &ts in timestamps {
        enc.encode(ts, 0.0);
    }
    enc.finish()
}

/// Decode Gorilla-encoded timestamps.
pub fn decode_timestamps(data: &[u8]) -> Result<Vec<i64>, CodecError> {
    let mut dec = GorillaDecoder::new(data);
    let samples = dec.decode_all();
    if samples.len() != dec.total as usize {
        return Err(CodecError::Truncated {
            expected: dec.total as usize,
            actual: samples.len(),
        });
    }
    Ok(samples.into_iter().map(|(ts, _)| ts).collect())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_encoder() {
        let enc = GorillaEncoder::new();
        assert_eq!(enc.count(), 0);
        let data = enc.finish();
        assert_eq!(data.len(), 4);
        assert_eq!(u32::from_le_bytes(data[0..4].try_into().unwrap()), 0);
    }

    #[test]
    fn single_sample_roundtrip() {
        let mut enc = GorillaEncoder::new();
        enc.encode(1000, 42.5);
        let data = enc.finish();

        let mut dec = GorillaDecoder::new(&data);
        let (ts, val) = dec.next_sample().unwrap();
        assert_eq!(ts, 1000);
        assert!((val - 42.5).abs() < f64::EPSILON);
        assert!(dec.next_sample().is_none());
    }

    #[test]
    fn monotonic_timestamps_compress_well() {
        let mut enc = GorillaEncoder::new();
        for i in 0..1000 {
            enc.encode(1_000_000 + i * 10_000, 100.0 + (i as f64) * 0.001);
        }
        let data = enc.finish();

        assert!(
            data.len() < 8000,
            "expected good compression, got {} bytes for 1000 samples",
            data.len()
        );

        let mut dec = GorillaDecoder::new(&data);
        let samples = dec.decode_all();
        assert_eq!(samples.len(), 1000);
        assert_eq!(samples[0].0, 1_000_000);
    }

    #[test]
    fn identical_values_compress_minimally() {
        let mut enc = GorillaEncoder::new();
        for i in 0..100 {
            enc.encode(1000 + i * 1000, 42.0);
        }
        let data = enc.finish();

        assert!(
            data.len() < 100,
            "identical values should compress well, got {} bytes",
            data.len()
        );

        let mut dec = GorillaDecoder::new(&data);
        let samples = dec.decode_all();
        assert_eq!(samples.len(), 100);
        for s in &samples {
            assert!((s.1 - 42.0).abs() < f64::EPSILON);
        }
    }

    #[test]
    fn f64_batch_roundtrip() {
        let values: Vec<f64> = (0..500).map(|i| 42.0 + i as f64 * 0.1).collect();
        let encoded = encode_f64(&values);
        let decoded = decode_f64(&encoded).unwrap();
        assert_eq!(values.len(), decoded.len());
        for (a, b) in values.iter().zip(decoded.iter()) {
            assert_eq!(a.to_bits(), b.to_bits());
        }
    }

    #[test]
    fn timestamp_batch_roundtrip() {
        let timestamps: Vec<i64> = (0..1000).map(|i| 1_700_000_000_000 + i * 10_000).collect();
        let encoded = encode_timestamps(&timestamps);
        let decoded = decode_timestamps(&encoded).unwrap();
        assert_eq!(timestamps, decoded);
    }

    #[test]
    fn timestamp_arithmetic_wraps_across_i64_bounds() {
        let timestamps = [i64::MIN, i64::MAX, 0, i64::MIN, i64::MAX];
        let encoded = encode_timestamps(&timestamps);
        let decoded = decode_timestamps(&encoded).expect("extreme timestamps must decode");
        assert_eq!(decoded, timestamps);
    }

    #[test]
    fn decoded_timestamp_arithmetic_wraps_without_panicking() {
        let mut decoder = GorillaDecoder {
            reader: BitReader::new(&[0]),
            prev_ts: i64::MAX,
            prev_delta: 1,
            prev_value: 0,
            prev_leading: 0,
            prev_trailing: 0,
            count: 0,
            total: 1,
            first: false,
        };

        let timestamp = decoder
            .decode_timestamp()
            .expect("zero delta-of-delta must decode");
        assert_eq!(timestamp, i64::MIN);
    }

    #[test]
    fn invalid_value_window_returns_corrupt_error() {
        let mut decoder = GorillaDecoder {
            reader: BitReader::new(&[0xff, 0xfc]),
            prev_ts: 0,
            prev_delta: 0,
            prev_value: 0,
            prev_leading: 0,
            prev_trailing: 0,
            count: 0,
            total: 1,
            first: false,
        };

        let error = decoder
            .decode_value()
            .expect_err("an oversized Gorilla value window must fail");
        assert!(matches!(error, CodecError::Corrupt { .. }));
    }

    #[test]
    fn varying_values_roundtrip() {
        let mut enc = GorillaEncoder::new();
        let test_values = [
            0.0,
            1.0,
            -1.0,
            f64::MAX,
            f64::MIN,
            std::f64::consts::PI,
            1e-300,
            1e300,
        ];
        for (i, &val) in test_values.iter().enumerate() {
            enc.encode(i as i64 * 1000, val);
        }
        let data = enc.finish();

        let mut dec = GorillaDecoder::new(&data);
        let samples = dec.decode_all();
        assert_eq!(samples.len(), test_values.len());
        for (i, &expected) in test_values.iter().enumerate() {
            assert_eq!(samples[i].1.to_bits(), expected.to_bits());
        }
    }

    #[test]
    fn compression_ratio() {
        let mut enc = GorillaEncoder::new();
        let mut rng_state: u64 = 12345;
        for i in 0..10_000 {
            rng_state = rng_state.wrapping_mul(6364136223846793005).wrapping_add(1);
            let jitter = ((rng_state >> 33) as f64) / (u32::MAX as f64) * 2.0 - 1.0;
            let value = 50.0 + jitter * 5.0;
            enc.encode(1_700_000_000_000 + i * 10_000, value);
        }
        let data = enc.finish();

        let raw_size = 10_000 * 16;
        let ratio = raw_size as f64 / data.len() as f64;
        assert!(
            ratio > 2.0,
            "compression ratio {ratio:.1}:1 too low (expected >2:1)"
        );
    }
}
