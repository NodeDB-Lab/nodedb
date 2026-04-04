//! SIMD-accelerated bitpack unpacking with runtime dispatch.
//!
//! Dispatch order:
//! - x86_64: SSE2 (runtime detected) → scalar
//! - aarch64: NEON (compile-time baseline) → scalar
//! - wasm32/other: scalar

mod scalar;

#[cfg(target_arch = "x86_64")]
mod sse2;

#[cfg(target_arch = "aarch64")]
mod neon;

/// Unpack bitpacked values from raw data (after the 3-byte header).
///
/// Uses the best available SIMD implementation detected at runtime.
pub fn unpack(data: &[u8], bit_width: u8, num_values: usize) -> Vec<u32> {
    if num_values == 0 || bit_width == 0 {
        return vec![0; num_values];
    }
    let mut out = Vec::with_capacity(num_values);

    #[cfg(target_arch = "x86_64")]
    {
        if std::is_x86_feature_detected!("sse2") {
            // SAFETY: SSE2 availability confirmed by runtime check.
            unsafe {
                sse2::unpack_sse2(data, bit_width, num_values, &mut out);
            }
            return out;
        }
    }

    #[cfg(target_arch = "aarch64")]
    {
        neon::unpack_neon(data, bit_width, num_values, &mut out);
        return out;
    }

    #[allow(unreachable_code)]
    {
        scalar::unpack_scalar(data, bit_width, num_values, &mut out);
        out
    }
}

/// Unpack a full bitpacked buffer (with 3-byte header) using SIMD.
pub fn unpack_simd(buf: &[u8]) -> Vec<u32> {
    if buf.len() < 3 {
        return Vec::new();
    }
    let num_values = u16::from_le_bytes([buf[0], buf[1]]) as usize;
    let bit_width = buf[2];
    unpack(&buf[3..], bit_width, num_values)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn simd_unpack_matches_scalar() {
        let values: Vec<u32> = (0..128).map(|i| i * 3 + 1).collect();
        let packed = crate::codec::bitpack::pack(&values);

        let scalar_result = crate::codec::bitpack::unpack(&packed);
        let simd_result = unpack_simd(&packed);

        assert_eq!(scalar_result, values);
        assert_eq!(simd_result, values);
    }

    #[test]
    fn simd_unpack_empty() {
        let packed = crate::codec::bitpack::pack(&[]);
        assert!(unpack_simd(&packed).is_empty());
    }

    #[test]
    fn simd_unpack_zeros() {
        let values = vec![0u32; 64];
        let packed = crate::codec::bitpack::pack(&values);
        assert_eq!(unpack_simd(&packed), values);
    }
}
