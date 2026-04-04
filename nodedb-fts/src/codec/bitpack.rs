//! Variable-width bitpacking for `u32` arrays.
//!
//! Packs an array of u32 values using the minimum number of bits per value.
//! 2-byte block header stores `num_values` (u16) followed by 1-byte `bit_width`.
//! Each value is stored in exactly `bit_width` bits, packed contiguously.
//!
//! Used for both delta-encoded doc IDs and term frequencies.

/// Compute the minimum number of bits needed to represent `max_val`.
/// Returns 0 for max_val == 0, 1 for max_val == 1, etc.
pub fn bits_needed(max_val: u32) -> u8 {
    if max_val == 0 {
        return 0;
    }
    32 - max_val.leading_zeros() as u8
}

/// Pack a slice of u32 values into a compact byte buffer.
///
/// Layout: `[num_values: u16 LE][bit_width: u8][packed bits...]`
///
/// Total bytes = 3 + ceil(num_values * bit_width / 8).
pub fn pack(values: &[u32]) -> Vec<u8> {
    if values.is_empty() {
        let mut buf = Vec::with_capacity(3);
        buf.extend_from_slice(&0u16.to_le_bytes());
        buf.push(0);
        return buf;
    }

    let max_val = values.iter().copied().max().unwrap_or(0);
    let bit_width = bits_needed(max_val);

    let total_bits = values.len() as u64 * bit_width as u64;
    let data_bytes = total_bits.div_ceil(8) as usize;
    let mut buf = Vec::with_capacity(3 + data_bytes);

    // Header.
    buf.extend_from_slice(&(values.len() as u16).to_le_bytes());
    buf.push(bit_width);

    if bit_width == 0 {
        // All values are 0 — no data bytes needed.
        return buf;
    }

    // Pack bits.
    buf.resize(3 + data_bytes, 0);
    let data = &mut buf[3..];
    let mut bit_pos = 0u64;

    for &val in values {
        let byte_idx = (bit_pos / 8) as usize;
        let bit_offset = (bit_pos % 8) as u32;

        // Write value starting at bit_offset within data[byte_idx..].
        // A single value may span up to 5 bytes (32 bits + 7 bit offset).
        let wide = (val as u64) << bit_offset;
        let bytes = wide.to_le_bytes();
        let bytes_to_write = (bit_offset + bit_width as u32).div_ceil(8) as usize;
        for i in 0..bytes_to_write.min(data.len() - byte_idx) {
            data[byte_idx + i] |= bytes[i];
        }

        bit_pos += bit_width as u64;
    }

    buf
}

/// Unpack values from a bitpacked buffer.
///
/// Returns the unpacked `Vec<u32>`. Uses the scalar path.
/// For SIMD-accelerated unpacking, use `super::simd_unpack`.
pub fn unpack(buf: &[u8]) -> Vec<u32> {
    if buf.len() < 3 {
        return Vec::new();
    }

    let num_values = u16::from_le_bytes([buf[0], buf[1]]) as usize;
    let bit_width = buf[2];

    if num_values == 0 || bit_width == 0 {
        return vec![0; num_values];
    }

    let mask = if bit_width >= 32 {
        u32::MAX
    } else {
        (1u32 << bit_width) - 1
    };

    let data = &buf[3..];
    let mut values = Vec::with_capacity(num_values);
    let mut bit_pos = 0u64;

    for _ in 0..num_values {
        let byte_idx = (bit_pos / 8) as usize;
        let bit_offset = (bit_pos % 8) as u32;

        // Read up to 8 bytes starting at byte_idx (handles spanning).
        let mut wide_bytes = [0u8; 8];
        let avail = data.len().saturating_sub(byte_idx).min(8);
        wide_bytes[..avail].copy_from_slice(&data[byte_idx..byte_idx + avail]);
        let wide = u64::from_le_bytes(wide_bytes);

        let val = ((wide >> bit_offset) as u32) & mask;
        values.push(val);

        bit_pos += bit_width as u64;
    }

    values
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bits_needed_cases() {
        assert_eq!(bits_needed(0), 0);
        assert_eq!(bits_needed(1), 1);
        assert_eq!(bits_needed(2), 2);
        assert_eq!(bits_needed(3), 2);
        assert_eq!(bits_needed(4), 3);
        assert_eq!(bits_needed(255), 8);
        assert_eq!(bits_needed(256), 9);
        assert_eq!(bits_needed(u32::MAX), 32);
    }

    #[test]
    fn roundtrip_basic() {
        let values = vec![1, 3, 5, 7, 15];
        let packed = pack(&values);
        let unpacked = unpack(&packed);
        assert_eq!(unpacked, values);
    }

    #[test]
    fn roundtrip_zeros() {
        let values = vec![0, 0, 0, 0];
        let packed = pack(&values);
        assert_eq!(packed.len(), 3); // Header only, no data.
        assert_eq!(unpack(&packed), values);
    }

    #[test]
    fn roundtrip_empty() {
        let packed = pack(&[]);
        assert_eq!(unpack(&packed), Vec::<u32>::new());
    }

    #[test]
    fn roundtrip_single() {
        let values = vec![42];
        assert_eq!(unpack(&pack(&values)), values);
    }

    #[test]
    fn roundtrip_large_values() {
        let values = vec![0, 1_000_000, 2_000_000, u32::MAX];
        assert_eq!(unpack(&pack(&values)), values);
    }

    #[test]
    fn roundtrip_128_consecutive() {
        let values: Vec<u32> = (0..128).collect();
        assert_eq!(unpack(&pack(&values)), values);
    }

    #[test]
    fn roundtrip_deltas() {
        // Typical delta-encoded posting IDs: small values.
        let deltas = vec![5, 1, 1, 3, 1, 2, 1, 1, 7, 1];
        let packed = pack(&deltas);
        // bit_width for max=7 is 3; 10 values × 3 bits = 30 bits = 4 bytes data.
        assert_eq!(packed.len(), 3 + 4);
        assert_eq!(unpack(&packed), deltas);
    }

    #[test]
    fn compact_size() {
        // 128 values with max delta 15 → 4 bits each → 64 bytes data + 3 header.
        let values = vec![15u32; 128];
        let packed = pack(&values);
        assert_eq!(packed.len(), 3 + 64);
    }
}
