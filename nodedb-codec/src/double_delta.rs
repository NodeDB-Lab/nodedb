// SPDX-License-Identifier: Apache-2.0

//! DoubleDelta codec for timestamp columns.
//!
//! Timestamps are monotonically increasing with near-constant intervals
//! (e.g., every 10s). DoubleDelta encodes the difference-of-differences:
//!
//! ```text
//! value[0]              → stored raw (8 bytes)
//! delta[0] = v[1]-v[0]  → stored raw (8 bytes)
//! dod[i]   = delta[i] - delta[i-1]  → bit-packed (usually 0 → 1 bit)
//! ```
//!
//! For constant-rate timestamps, all delta-of-deltas are 0, achieving ~1 bit
//! per sample after the header. 4x better than Gorilla for timestamp columns.
//!
//! Wire format:
//! ```text
//! [4 bytes] sample count (LE u32)
//! [8 bytes] first value (LE i64)
//! [8 bytes] first delta (LE i64)  — only present if count >= 2
//! [N bytes] bitstream of delta-of-deltas — only present if count >= 3
//! ```
//!
//! DoD bit-packing uses the same bucket scheme as Gorilla timestamps:
//! - `0`              → dod == 0
//! - `10` + 7 bits    → dod in [-63, 64]
//! - `110` + 9 bits   → dod in [-255, 256]
//! - `1110` + 12 bits → dod in [-2047, 2048]
//! - `1111` + 64 bits → arbitrary dod

use std::mem::size_of;

use crate::bounds::{checked_add, checked_capacity, encode_input_len, u32_to_usize};
use crate::error::CodecError;

// ---------------------------------------------------------------------------
// Bit I/O helpers
// ---------------------------------------------------------------------------

#[derive(Debug)]
pub(crate) struct BitWriter {
    buf: Vec<u8>,
    bit_pos: usize,
}

impl BitWriter {
    pub(crate) fn new() -> Self {
        Self {
            buf: Vec::with_capacity(1024),
            bit_pos: 0,
        }
    }

    pub(crate) fn write_bit(&mut self, bit: bool) {
        let byte_idx = self.bit_pos / 8;
        let bit_idx = 7 - (self.bit_pos % 8);
        if byte_idx >= self.buf.len() {
            self.buf.push(0);
        }
        if bit {
            self.buf[byte_idx] |= 1 << bit_idx;
        }
        self.bit_pos += 1;
    }

    pub(crate) fn write_bits(&mut self, value: u64, num_bits: usize) {
        for i in (0..num_bits).rev() {
            self.write_bit((value >> i) & 1 == 1);
        }
    }

    fn try_write_bit(&mut self, bit: bool) -> Result<(), CodecError> {
        checked_add(self.bit_pos, 1, "DoubleDelta bit offset")?;
        self.write_bit(bit);
        Ok(())
    }

    fn try_write_bits(&mut self, value: u64, num_bits: usize) -> Result<(), CodecError> {
        checked_add(self.bit_pos, num_bits, "DoubleDelta bit offset")?;
        for i in (0..num_bits).rev() {
            self.write_bit((value >> i) & 1 == 1);
        }
        Ok(())
    }

    pub(crate) fn as_bytes(&self) -> &[u8] {
        &self.buf
    }

    pub(crate) fn bit_len(&self) -> usize {
        self.bit_pos
    }
}

pub(crate) struct BitReader<'a> {
    buf: &'a [u8],
    bit_pos: usize,
}

impl<'a> BitReader<'a> {
    pub(crate) fn new(buf: &'a [u8]) -> Self {
        Self { buf, bit_pos: 0 }
    }

    pub(crate) fn read_bit(&mut self) -> Result<bool, CodecError> {
        let byte_idx = self.bit_pos / 8;
        if byte_idx >= self.buf.len() {
            return Err(CodecError::Truncated {
                expected: checked_add(byte_idx, 1, "DoubleDelta bit byte index")?,
                actual: self.buf.len(),
            });
        }
        let bit_idx = 7 - (self.bit_pos % 8);
        let bit = (self.buf[byte_idx] >> bit_idx) & 1 == 1;
        self.bit_pos = checked_add(self.bit_pos, 1, "DoubleDelta bit offset")?;
        Ok(bit)
    }

    pub(crate) fn read_bits(&mut self, num_bits: usize) -> Result<u64, CodecError> {
        let mut value = 0u64;
        for _ in 0..num_bits {
            value = (value << 1) | u64::from(self.read_bit()?);
        }
        Ok(value)
    }
}

// ---------------------------------------------------------------------------
// Public encode / decode API
// ---------------------------------------------------------------------------

/// Encode a slice of i64 values using DoubleDelta compression.
pub fn encode(values: &[i64]) -> Result<Vec<u8>, CodecError> {
    let count = encode_value_count(values.len(), "DoubleDelta")?;
    let estimated = checked_add(20, values.len() / 4, "DoubleDelta output estimate")?;
    let mut out = Vec::with_capacity(estimated);

    out.extend_from_slice(&count.to_le_bytes());

    if values.is_empty() {
        return Ok(out);
    }

    out.extend_from_slice(&values[0].to_le_bytes());

    if values.len() == 1 {
        return Ok(out);
    }

    let first_delta = values[1].wrapping_sub(values[0]);
    out.extend_from_slice(&first_delta.to_le_bytes());

    if values.len() == 2 {
        return Ok(out);
    }

    let mut bs = BitWriter::new();
    let mut prev_delta = first_delta;

    for i in 2..values.len() {
        let delta = values[i].wrapping_sub(values[i - 1]);
        let dod = delta.wrapping_sub(prev_delta);
        encode_dod(&mut bs, dod)?;
        prev_delta = delta;
    }

    out.extend_from_slice(bs.as_bytes());
    Ok(out)
}

/// Decode DoubleDelta-compressed bytes back to i64 values.
pub fn decode(data: &[u8]) -> Result<Vec<i64>, CodecError> {
    if data.len() < 4 {
        return Err(CodecError::Truncated {
            expected: 4,
            actual: data.len(),
        });
    }

    let count = u32_to_usize(
        u32::from_le_bytes(data[0..4].try_into().map_err(|_| CodecError::Corrupt {
            detail: "invalid header".into(),
        })?),
        "DoubleDelta value count",
    )?;
    validate_value_count(count, "DoubleDelta")?;

    if count == 0 {
        if data.len() != 4 {
            return Err(CodecError::Corrupt {
                detail: "trailing bytes after empty DoubleDelta frame".into(),
            });
        }
        return Ok(Vec::new());
    }

    if data.len() < 12 {
        return Err(CodecError::Truncated {
            expected: 12,
            actual: data.len(),
        });
    }

    let first_value =
        i64::from_le_bytes(data[4..12].try_into().map_err(|_| CodecError::Corrupt {
            detail: "invalid first value".into(),
        })?);

    let value_capacity = checked_capacity(count, size_of::<i64>(), "DoubleDelta values")?;
    let mut values = Vec::with_capacity(value_capacity);
    values.push(first_value);

    if count == 1 {
        if data.len() != 12 {
            return Err(CodecError::Corrupt {
                detail: "trailing bytes after single-value DoubleDelta frame".into(),
            });
        }
        return Ok(values);
    }

    if data.len() < 20 {
        return Err(CodecError::Truncated {
            expected: 20,
            actual: data.len(),
        });
    }

    let first_delta =
        i64::from_le_bytes(data[12..20].try_into().map_err(|_| CodecError::Corrupt {
            detail: "invalid first delta".into(),
        })?);
    values.push(first_value.wrapping_add(first_delta));

    if count == 2 {
        if data.len() != 20 {
            return Err(CodecError::Corrupt {
                detail: "trailing bytes after two-value DoubleDelta frame".into(),
            });
        }
        return Ok(values);
    }

    let mut reader = BitReader::new(&data[20..]);
    let mut prev_delta = first_delta;

    for _ in 2..count {
        let dod = decode_dod(&mut reader)?;
        let delta = prev_delta.wrapping_add(dod);
        let value = values[values.len() - 1].wrapping_add(delta);
        values.push(value);
        prev_delta = delta;
    }
    let used_bytes = reader.bit_pos.div_ceil(8);
    if used_bytes != reader.buf.len() {
        return Err(CodecError::Corrupt {
            detail: "trailing bytes after DoubleDelta frame".into(),
        });
    }
    if !reader.bit_pos.is_multiple_of(8) {
        let unused_bits = 8 - reader.bit_pos % 8;
        let padding_mask = (1u8 << unused_bits) - 1;
        if reader.buf[used_bytes - 1] & padding_mask != 0 {
            return Err(CodecError::Corrupt {
                detail: "nonzero DoubleDelta padding bits".into(),
            });
        }
    }

    Ok(values)
}

// ---------------------------------------------------------------------------
// DoD bit encoding / decoding
// ---------------------------------------------------------------------------

fn encode_dod(bs: &mut BitWriter, dod: i64) -> Result<(), CodecError> {
    if dod == 0 {
        bs.try_write_bit(false)
    } else if (-64..=63).contains(&dod) {
        bs.try_write_bits(0b10, 2)?;
        bs.try_write_bits((dod as u64) & 0x7F, 7)
    } else if (-256..=255).contains(&dod) {
        bs.try_write_bits(0b110, 3)?;
        bs.try_write_bits((dod as u64) & 0x1FF, 9)
    } else if (-2048..=2047).contains(&dod) {
        bs.try_write_bits(0b1110, 4)?;
        bs.try_write_bits((dod as u64) & 0xFFF, 12)
    } else {
        bs.try_write_bits(0b1111, 4)?;
        bs.try_write_bits(dod as u64, 64)
    }
}

fn decode_dod(reader: &mut BitReader<'_>) -> Result<i64, CodecError> {
    let bit = reader.read_bit()?;
    if !bit {
        return Ok(0);
    }

    let bit2 = reader.read_bit()?;
    if !bit2 {
        let raw = reader.read_bits(7)? as i64;
        return Ok(sign_extend(raw, 7));
    }

    let bit3 = reader.read_bit()?;
    if !bit3 {
        let raw = reader.read_bits(9)? as i64;
        return Ok(sign_extend(raw, 9));
    }

    let bit4 = reader.read_bit()?;
    if !bit4 {
        let raw = reader.read_bits(12)? as i64;
        return Ok(sign_extend(raw, 12));
    }

    let raw = reader.read_bits(64)?;
    Ok(raw as i64)
}

fn sign_extend(value: i64, bits: u32) -> i64 {
    let shift = 64 - bits;
    (value << shift) >> shift
}

fn encode_value_count(len: usize, codec: &str) -> Result<u32, CodecError> {
    validate_value_count(len, codec)?;
    encode_input_len(len, codec)
}

fn validate_value_count(count: usize, codec: &str) -> Result<(), CodecError> {
    // See `checked_capacity`: the byte count is scaled in 64-bit arithmetic so
    // a 32-bit target reports the resource limit rather than a `usize`
    // overflow for a count it simply cannot express.
    checked_capacity(count, size_of::<i64>(), codec)?;
    Ok(())
}

// ---------------------------------------------------------------------------
// Re-export types for lib.rs consistency
// ---------------------------------------------------------------------------

/// Streaming DoubleDelta encoder. Accumulates values and produces
/// compressed bytes on `finish()`.
pub struct DoubleDeltaEncoder {
    values: Vec<i64>,
}

impl DoubleDeltaEncoder {
    pub fn new() -> Self {
        Self {
            values: Vec::with_capacity(4096),
        }
    }

    /// Append a single i64 value.
    pub fn push(&mut self, value: i64) -> Result<(), CodecError> {
        let next = checked_add(self.values.len(), 1, "DoubleDelta streaming count")?;
        validate_value_count(next, "DoubleDelta streaming input")?;
        self.values.push(value);
        Ok(())
    }

    /// Append a batch of i64 values.
    pub fn push_batch(&mut self, values: &[i64]) -> Result<(), CodecError> {
        let next = checked_add(
            self.values.len(),
            values.len(),
            "DoubleDelta streaming count",
        )?;
        validate_value_count(next, "DoubleDelta streaming input")?;
        self.values.extend_from_slice(values);
        Ok(())
    }

    /// Number of values encoded so far.
    pub fn count(&self) -> usize {
        self.values.len()
    }

    /// Finish encoding and return compressed bytes.
    pub fn finish(self) -> Result<Vec<u8>, CodecError> {
        encode(&self.values)
    }
}

impl Default for DoubleDeltaEncoder {
    fn default() -> Self {
        Self::new()
    }
}

/// Streaming DoubleDelta decoder. Wraps the batch `decode()` function.
pub struct DoubleDeltaDecoder {
    values: Vec<i64>,
    pos: usize,
}

impl DoubleDeltaDecoder {
    /// Create a decoder from compressed bytes.
    pub fn new(data: &[u8]) -> Result<Self, CodecError> {
        let values = decode(data)?;
        Ok(Self { values, pos: 0 })
    }

    /// Decode all values at once.
    pub fn decode_all(data: &[u8]) -> Result<Vec<i64>, CodecError> {
        decode(data)
    }

    /// Next value, or None if exhausted.
    pub fn next_value(&mut self) -> Option<i64> {
        if self.pos < self.values.len() {
            let v = self.values[self.pos];
            self.pos += 1;
            Some(v)
        } else {
            None
        }
    }

    /// Remaining value count.
    pub fn remaining(&self) -> usize {
        self.values.len() - self.pos
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn encode(values: &[i64]) -> Vec<u8> {
        super::encode(values).expect("test DoubleDelta encode")
    }

    #[test]
    fn empty_roundtrip() {
        let encoded = encode(&[]);
        let decoded = decode(&encoded).unwrap();
        assert!(decoded.is_empty());
    }

    #[test]
    fn single_value() {
        let encoded = encode(&[1_700_000_000_000i64]);
        let decoded = decode(&encoded).unwrap();
        assert_eq!(decoded, vec![1_700_000_000_000i64]);
        assert_eq!(encoded.len(), 12);
    }

    #[test]
    fn two_values() {
        let values = vec![1000i64, 2000];
        let encoded = encode(&values);
        let decoded = decode(&encoded).unwrap();
        assert_eq!(decoded, values);
        assert_eq!(encoded.len(), 20);
    }

    #[test]
    fn constant_rate_timestamps() {
        let values: Vec<i64> = (0..10_000)
            .map(|i| 1_700_000_000_000 + i * 10_000)
            .collect();
        let encoded = encode(&values);
        let decoded = decode(&encoded).unwrap();
        assert_eq!(decoded, values);

        let bits_per_sample = (encoded.len() as f64 * 8.0) / values.len() as f64;
        assert!(
            bits_per_sample < 2.0,
            "constant-rate should compress to ~1 bit/sample, got {bits_per_sample:.1}"
        );
    }

    #[test]
    fn monotonic_with_jitter() {
        let mut values = Vec::with_capacity(10_000);
        let mut ts = 1_700_000_000_000i64;
        let mut rng: u64 = 42;
        for _ in 0..10_000 {
            values.push(ts);
            rng = rng.wrapping_mul(6364136223846793005).wrapping_add(1);
            let jitter = ((rng >> 33) as i64 % 101) - 50;
            ts += 10_000 + jitter;
        }
        let encoded = encode(&values);
        let decoded = decode(&encoded).unwrap();
        assert_eq!(decoded, values);

        let bytes_per_sample = encoded.len() as f64 / values.len() as f64;
        assert!(
            bytes_per_sample < 2.0,
            "jittered timestamps should compress to <2 bytes/sample, got {bytes_per_sample:.2}"
        );
    }

    #[test]
    fn non_monotonic_values() {
        let values: Vec<i64> = vec![100, 50, 200, 10, 300, 5, 1000, -500, 0, 42];
        let encoded = encode(&values);
        let decoded = decode(&encoded).unwrap();
        assert_eq!(decoded, values);
    }

    #[test]
    fn negative_values() {
        let values: Vec<i64> = vec![-1000, -999, -998, -997, -996];
        let encoded = encode(&values);
        let decoded = decode(&encoded).unwrap();
        assert_eq!(decoded, values);
    }

    #[test]
    fn large_deltas() {
        let values: Vec<i64> = vec![0, i64::MAX / 2, i64::MIN / 2, i64::MAX / 4, 0];
        let encoded = encode(&values);
        let decoded = decode(&encoded).unwrap();
        assert_eq!(decoded, values);
    }

    #[test]
    fn boundary_values() {
        let values: Vec<i64> = vec![i64::MIN, 0, i64::MAX, 0, i64::MIN];
        let encoded = encode(&values);
        let decoded = decode(&encoded).unwrap();
        assert_eq!(decoded, values);
    }

    #[test]
    fn compression_better_than_raw_for_constant_rate() {
        let values: Vec<i64> = (0..100_000)
            .map(|i| 1_700_000_000_000 + i * 10_000)
            .collect();
        let encoded = encode(&values);
        let raw_size = values.len() * 8;
        let ratio = raw_size as f64 / encoded.len() as f64;
        assert!(
            ratio > 5.0,
            "expected >5x compression for constant-rate, got {ratio:.1}x"
        );
    }

    #[test]
    fn streaming_encoder_matches_batch() {
        let values: Vec<i64> = (0..1000).map(|i| 1_000_000 + i * 100).collect();
        let batch_encoded = encode(&values);

        let mut enc = DoubleDeltaEncoder::new();
        for &v in &values {
            enc.push(v).expect("push");
        }
        let stream_encoded = enc.finish().expect("finish");

        assert_eq!(batch_encoded, stream_encoded);
    }

    #[test]
    fn streaming_decoder() {
        let values: Vec<i64> = (0..100).map(|i| 5000 + i * 10).collect();
        let encoded = encode(&values);
        let mut dec = DoubleDeltaDecoder::new(&encoded).unwrap();

        for &expected in &values {
            assert_eq!(dec.next_value(), Some(expected));
        }
        assert_eq!(dec.next_value(), None);
        assert_eq!(dec.remaining(), 0);
    }

    #[test]
    fn huge_count_and_noncanonical_tail_are_rejected_before_decode_work() {
        let mut huge = u32::MAX.to_le_bytes().to_vec();
        huge.extend_from_slice(&[0; 16]);
        assert!(matches!(
            decode(&huge),
            Err(CodecError::ResourceLimit { .. })
        ));

        let mut one = encode(&[7]);
        one.push(1);
        assert!(matches!(decode(&one), Err(CodecError::Corrupt { .. })));

        let mut padded = encode(&[0, 1, 2]);
        let last = padded.len() - 1;
        padded[last] |= 1;
        assert!(matches!(decode(&padded), Err(CodecError::Corrupt { .. })));
    }

    #[test]
    fn truncated_input_errors() {
        assert!(decode(&[]).is_err());
        assert!(decode(&[1, 0, 0, 0]).is_err());
        assert!(decode(&[2, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]).is_err());
    }
}
