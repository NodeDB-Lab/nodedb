//! NEON-accelerated bitpack unpacking for AArch64.
//!
//! Processes 4 values at a time using 128-bit NEON registers.
//! Falls back to scalar for the tail (< 4 remaining values).

#[cfg(target_arch = "aarch64")]
use std::arch::aarch64::*;

/// NEON unpack: processes 4 values per iteration using 128-bit NEON.
///
/// NEON is always available on AArch64 (compile-time baseline).
#[cfg(target_arch = "aarch64")]
pub fn unpack_neon(data: &[u8], bit_width: u8, num_values: usize, out: &mut Vec<u32>) {
    let mask_val = if bit_width >= 32 {
        u32::MAX
    } else {
        (1u32 << bit_width) - 1
    };

    // SAFETY: NEON is baseline on AArch64.
    unsafe {
        let mask = vdupq_n_u32(mask_val);
        let bw = bit_width as u64;
        let mut bit_pos = 0u64;

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

            // Load 4 values into NEON register, apply mask, store.
            let v = vld1q_u32(vals.as_ptr());
            let masked = vandq_u32(v, mask);
            vst1q_u32(out.as_mut_ptr().add(out.len()), masked);
            // SAFETY: we reserved capacity in the caller.
            out.set_len(out.len() + 4);

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

// Provide a fallback for non-aarch64 compilation (never called at runtime).
#[cfg(not(target_arch = "aarch64"))]
pub fn unpack_neon(_data: &[u8], _bit_width: u8, _num_values: usize, _out: &mut Vec<u32>) {
    unreachable!("NEON unpack called on non-AArch64 platform");
}
