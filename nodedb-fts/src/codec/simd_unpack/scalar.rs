//! Scalar (non-SIMD) bitpack unpacking. Always available on all platforms.

/// Unpack `num_values` from bitpacked `data` at `bit_width` bits per value.
///
/// Uses branchless `u64` read + shift + mask for each value.
pub fn unpack_scalar(data: &[u8], bit_width: u8, num_values: usize, out: &mut Vec<u32>) {
    let mask = if bit_width >= 32 {
        u32::MAX
    } else {
        (1u32 << bit_width) - 1
    };

    let mut bit_pos = 0u64;

    for _ in 0..num_values {
        let byte_idx = (bit_pos / 8) as usize;
        let bit_offset = (bit_pos % 8) as u32;

        // Read up to 8 bytes (handles values spanning byte boundaries).
        let mut wide_bytes = [0u8; 8];
        let avail = data.len().saturating_sub(byte_idx).min(8);
        wide_bytes[..avail].copy_from_slice(&data[byte_idx..byte_idx + avail]);
        let wide = u64::from_le_bytes(wide_bytes);

        let val = ((wide >> bit_offset) as u32) & mask;
        out.push(val);

        bit_pos += bit_width as u64;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn unpack_3bit_values() {
        // Pack [1, 3, 5, 7] at 3 bits each manually:
        // 001 011 101 111 = 0b_111_101_011_001 = 0xFB1 but LE bit-packed.
        let values = vec![1u32, 3, 5, 7];
        let packed = crate::codec::bitpack::pack(&values);
        let mut out = Vec::new();
        unpack_scalar(&packed[3..], 3, 4, &mut out);
        assert_eq!(out, values);
    }
}
