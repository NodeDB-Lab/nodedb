//! SmallFloat: 1-byte quantized document lengths for BM25 fieldnorms.
//!
//! Maps u32 doc lengths to a monotonic 0..255 byte scale.
//! - Byte 0 → length 0
//! - Byte 255 → length ~16M+
//! - Monotonic: larger length → larger (or equal) byte
//! - Max relative error dampened by BM25's `b=0.75` length normalization
//! - 4x space reduction vs raw u32

/// Encode a u32 value into a single byte.
///
/// Exact for 0. For values 1+, uses `floor(log2(value)) * 8 + top_3_fractional_bits`.
/// Clamped to 255.
pub fn encode(value: u32) -> u8 {
    if value == 0 {
        return 0;
    }
    // Position of the highest set bit (0-indexed): 0 for value=1, 1 for value=2..3, etc.
    let msb = 31 - value.leading_zeros(); // 0..=31
    // Extract 3 fractional bits below the leading 1-bit.
    let frac = if msb >= 3 {
        (value >> (msb - 3)) & 0x07
    } else {
        // For very small values (1..=7), shift up to fill 3 bits.
        (value << (3 - msb)) & 0x07
    };
    let byte = msb * 8 + frac + 1; // +1 so byte 0 is reserved for value 0
    byte.min(255) as u8
}

/// Precomputed decode table: byte → approximate u32 value.
static DECODE: [u32; 256] = build_decode_table();

const fn build_decode_table() -> [u32; 256] {
    let mut table = [0u32; 256];
    // byte 0 → 0
    let mut b = 1u32;
    while b < 256 {
        let adjusted = b - 1; // Remove the +1 offset from encode.
        let msb = adjusted / 8;
        let frac = adjusted % 8;
        // Reconstruct: leading 1-bit at position `msb`, then frac bits below it.
        if msb >= 3 {
            table[b as usize] = (1 << msb) | (frac << (msb - 3));
        } else {
            // Small msb: value = frac >> (3 - msb) with the leading 1-bit.
            table[b as usize] = (1 << msb) | (frac >> (3 - msb));
        }
        b += 1;
    }
    table
}

/// Decode a byte back to an approximate u32 value.
///
/// Direct table lookup — O(1).
pub fn decode(byte: u8) -> u32 {
    DECODE[byte as usize]
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn zero_roundtrips() {
        assert_eq!(encode(0), 0);
        assert_eq!(decode(0), 0);
    }

    #[test]
    fn small_values_exact() {
        // Values 1..=8 should be exact or very close.
        for v in 1..=8u32 {
            let e = encode(v);
            let d = decode(e);
            assert_eq!(d, v, "value {v}: encoded={e}, decoded={d}");
        }
    }

    #[test]
    fn decode_table_monotonic() {
        for i in 1..256 {
            assert!(
                DECODE[i] >= DECODE[i - 1],
                "DECODE[{i}]={} < DECODE[{}]={}",
                DECODE[i],
                i - 1,
                DECODE[i - 1]
            );
        }
    }

    #[test]
    fn encode_monotonic() {
        let mut prev = 0u8;
        for length in 0..200_000u32 {
            let encoded = encode(length);
            assert!(
                encoded >= prev,
                "encode({length})={encoded} < prev encode({})={prev}",
                length - 1
            );
            prev = encoded;
        }
    }

    #[test]
    fn roundtrip_within_error() {
        for length in [
            10, 20, 50, 100, 200, 500, 1000, 5000, 10_000, 50_000, 100_000,
        ] {
            let encoded = encode(length);
            let decoded = decode(encoded);
            assert!(decoded <= length, "decoded {decoded} > original {length}");
            let error = (length - decoded) as f64 / length as f64;
            assert!(
                error < 0.25,
                "length={length}, decoded={decoded}, error={error:.4}"
            );
        }
    }

    #[test]
    fn max_value() {
        let encoded = encode(u32::MAX);
        assert_eq!(encoded, 255);
        assert!(decode(255) > 1_000_000);
    }

    #[test]
    fn full_range_encode_decode_monotonic() {
        // Verify encode→decode is reasonable for sampled values across the range.
        let mut prev_byte = 0u8;
        let mut prev_decoded = 0u32;
        for &v in &[
            0, 1, 2, 5, 10, 50, 100, 500, 1000, 10_000, 100_000, 1_000_000,
        ] {
            let b = encode(v);
            let d = decode(b);
            assert!(b >= prev_byte);
            assert!(d >= prev_decoded);
            prev_byte = b;
            prev_decoded = d;
        }
    }
}
