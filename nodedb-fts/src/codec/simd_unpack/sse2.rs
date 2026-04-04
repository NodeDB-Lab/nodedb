//! SSE2-accelerated bitpack unpacking for x86_64.
//!
//! Processes 4 values at a time using 128-bit SSE2 registers.
//! Falls back to scalar for the tail (< 4 remaining values).

#[cfg(target_arch = "x86_64")]
use std::arch::x86_64::*;

/// SSE2 unpack: processes 4 values per iteration using 128-bit SIMD.
///
/// # Safety
/// Caller must ensure SSE2 is available (checked at dispatch in `mod.rs`).
#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "sse2")]
pub unsafe fn unpack_sse2(data: &[u8], bit_width: u8, num_values: usize, out: &mut Vec<u32>) {
    let mask_val = if bit_width >= 32 {
        u32::MAX
    } else {
        (1u32 << bit_width) - 1
    };

    // SAFETY: SSE2 is guaranteed by target_feature attribute.
    unsafe {
        let mask = _mm_set1_epi32(mask_val as i32);
        let bw = bit_width as u64;
        let mut bit_pos = 0u64;

        // Process 4 values at a time.
        let chunks = num_values / 4;
        let remainder = num_values % 4;

        for _ in 0..chunks {
            let mut vals = [0u32; 4];
            for (j, val) in vals.iter_mut().enumerate() {
                let bp = bit_pos + j as u64 * bw;
                let byte_idx = (bp / 8) as usize;
                let bit_offset = (bp % 8) as u32;

                let mut wide_bytes = [0u8; 8];
                let avail = data.len().saturating_sub(byte_idx).min(8);
                wide_bytes[..avail].copy_from_slice(&data[byte_idx..byte_idx + avail]);
                let wide = u64::from_le_bytes(wide_bytes);
                *val = ((wide >> bit_offset) as u32) & mask_val;
            }

            // Load 4 values into SSE2 register and apply mask.
            let v = _mm_loadu_si128(vals.as_ptr() as *const __m128i);
            let masked = _mm_and_si128(v, mask);

            // Store to output.
            let mut store_buf = [0u32; 4];
            _mm_storeu_si128(store_buf.as_mut_ptr() as *mut __m128i, masked);
            out.extend_from_slice(&store_buf);

            bit_pos += 4 * bw;
        }

        // Scalar tail.
        for _ in 0..remainder {
            let byte_idx = (bit_pos / 8) as usize;
            let bit_offset = (bit_pos % 8) as u32;

            let mut wide_bytes = [0u8; 8];
            let avail = data.len().saturating_sub(byte_idx).min(8);
            wide_bytes[..avail].copy_from_slice(&data[byte_idx..byte_idx + avail]);
            let wide = u64::from_le_bytes(wide_bytes);
            let val = ((wide >> bit_offset) as u32) & mask_val;
            out.push(val);

            bit_pos += bw;
        }
    }
}

#[cfg(test)]
#[cfg(target_arch = "x86_64")]
mod tests {
    use super::*;

    #[test]
    fn sse2_matches_scalar() {
        if !std::is_x86_feature_detected!("sse2") {
            return;
        }
        let values: Vec<u32> = (0..128).map(|i| i * 7 + 3).collect();
        let packed = crate::codec::bitpack::pack(&values);
        let bit_width = packed[2];

        let mut sse2_out = Vec::new();
        // SAFETY: checked above.
        // SAFETY: SSE2 availability checked above.
        unsafe {
            unpack_sse2(&packed[3..], bit_width, values.len(), &mut sse2_out);
        }
        assert_eq!(sse2_out, values);
    }

    #[test]
    fn sse2_non_multiple_of_4() {
        if !std::is_x86_feature_detected!("sse2") {
            return;
        }
        let values: Vec<u32> = (0..13).map(|i| i * 2).collect();
        let packed = crate::codec::bitpack::pack(&values);
        let bit_width = packed[2];

        let mut sse2_out = Vec::new();
        // SAFETY: SSE2 availability checked above.
        unsafe {
            unpack_sse2(&packed[3..], bit_width, values.len(), &mut sse2_out);
        }
        assert_eq!(sse2_out, values);
    }
}
