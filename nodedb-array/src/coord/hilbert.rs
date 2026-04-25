//! ND Hilbert curve encode/decode (Skilling 2004 transposed-form).
//!
//! Operates on `n` per-dim integer coordinates of `bits` bits each,
//! interleaving the result into a single Hilbert index. Caller-side
//! [`super::normalize`] maps typed coords into this integer space.

use super::normalize::MAX_DIMS;
use crate::error::{ArrayError, ArrayResult};

/// Encode per-dim integer coordinates into a single Hilbert index.
///
/// `coords.len()` must be `<=` [`MAX_DIMS`]; `bits * coords.len()` must
/// be `<= 64`.
pub fn encode(coords: &[u64], bits: u32) -> ArrayResult<u64> {
    check_shape(coords.len(), bits)?;
    let n = coords.len();
    if n == 0 {
        return Ok(0);
    }
    let mut x: [u64; MAX_DIMS] = [0; MAX_DIMS];
    x[..n].copy_from_slice(coords);

    // Skilling: axes → transposed Hilbert.
    let m: u64 = 1u64 << (bits - 1);
    let mut q = m;
    while q > 0 {
        let p = q - 1;
        for i in 0..n {
            if x[i] & q != 0 {
                x[0] ^= p;
            } else {
                let t = (x[0] ^ x[i]) & p;
                x[0] ^= t;
                x[i] ^= t;
            }
        }
        q >>= 1;
    }
    // Gray encode.
    for i in 1..n {
        x[i] ^= x[i - 1];
    }
    let mut t: u64 = 0;
    let mut q = m;
    while q > 1 {
        if x[n - 1] & q != 0 {
            t ^= q - 1;
        }
        q >>= 1;
    }
    for xi in x.iter_mut().take(n) {
        *xi ^= t;
    }

    // Interleave transposed Hilbert into single index, MSB first.
    let mut idx: u64 = 0;
    for b in (0..bits).rev() {
        for xi in x.iter().take(n) {
            let bit = (*xi >> b) & 1;
            idx = (idx << 1) | bit;
        }
    }
    Ok(idx)
}

/// Inverse of [`encode`]. Recovers per-dim integer coordinates from a
/// Hilbert index.
pub fn decode(idx: u64, n: usize, bits: u32) -> ArrayResult<Vec<u64>> {
    check_shape(n, bits)?;
    if n == 0 {
        return Ok(Vec::new());
    }
    // De-interleave bits.
    let mut x: [u64; MAX_DIMS] = [0; MAX_DIMS];
    for b in (0..bits).rev() {
        for (i, slot) in x.iter_mut().enumerate().take(n) {
            let shift = b * n as u32 + (n as u32 - 1 - i as u32);
            let bit = (idx >> shift) & 1;
            *slot = (*slot << 1) | bit;
        }
    }
    // Skilling TransposetoAxes: Gray decode (single-shot), then undo.
    let t = x[n - 1] >> 1;
    for i in (1..n).rev() {
        x[i] ^= x[i - 1];
    }
    x[0] ^= t;
    // Undo excess work: Q = 2 .. 2^bits.
    let n_lim: u128 = 1u128 << bits;
    let mut q: u128 = 2;
    while q != n_lim {
        let p = (q - 1) as u64;
        let qb = q as u64;
        for i in (0..n).rev() {
            if x[i] & qb != 0 {
                x[0] ^= p;
            } else {
                let t = (x[0] ^ x[i]) & p;
                x[0] ^= t;
                x[i] ^= t;
            }
        }
        q <<= 1;
    }
    Ok(x[..n].to_vec())
}

fn check_shape(n: usize, bits: u32) -> ArrayResult<()> {
    if n > MAX_DIMS {
        return Err(ArrayError::InvalidSchema {
            array: String::new(),
            detail: format!("hilbert: arity {n} exceeds MAX_DIMS={MAX_DIMS}"),
        });
    }
    if bits == 0 || (n as u32) * bits > 64 {
        return Err(ArrayError::InvalidSchema {
            array: String::new(),
            detail: format!("hilbert: {n} dims × {bits} bits exceeds 64-bit prefix"),
        });
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hilbert_round_trip_2d_4bit() {
        for x in 0..16u64 {
            for y in 0..16u64 {
                let idx = encode(&[x, y], 4).unwrap();
                let back = decode(idx, 2, 4).unwrap();
                assert_eq!(back, vec![x, y], "mismatch at ({x},{y}) idx={idx}");
            }
        }
    }

    #[test]
    fn hilbert_round_trip_3d_4bit() {
        for x in 0..16u64 {
            for y in 0..16u64 {
                for z in 0..16u64 {
                    let idx = encode(&[x, y, z], 4).unwrap();
                    let back = decode(idx, 3, 4).unwrap();
                    assert_eq!(back, vec![x, y, z]);
                }
            }
        }
    }

    #[test]
    fn hilbert_indices_are_unique_2d() {
        use std::collections::HashSet;
        let mut seen = HashSet::new();
        for x in 0..8u64 {
            for y in 0..8u64 {
                assert!(seen.insert(encode(&[x, y], 3).unwrap()));
            }
        }
        assert_eq!(seen.len(), 64);
    }

    #[test]
    fn hilbert_index_adjacent_cells_are_spatial_neighbors() {
        // The Hilbert guarantee: cells at consecutive Hilbert indices
        // differ by at most 1 in every dimension (Chebyshev distance =
        // 1). The reverse — "spatially-adjacent cells have small index
        // gap" — is NOT a property of any space-filling curve.
        let mut by_idx = vec![[0u64; 2]; 64];
        for x in 0..8u64 {
            for y in 0..8u64 {
                let i = encode(&[x, y], 3).unwrap() as usize;
                by_idx[i] = [x, y];
            }
        }
        for w in by_idx.windows(2) {
            let dx = (w[0][0] as i64 - w[1][0] as i64).abs();
            let dy = (w[0][1] as i64 - w[1][1] as i64).abs();
            assert!(
                dx.max(dy) == 1,
                "index-adjacent cells {:?}, {:?} not spatial neighbors",
                w[0],
                w[1]
            );
        }
    }

    #[test]
    fn hilbert_rejects_too_many_dims() {
        let coords = vec![0u64; MAX_DIMS + 1];
        assert!(encode(&coords, 1).is_err());
    }

    #[test]
    fn hilbert_rejects_overflowing_prefix() {
        // 8 dims × 9 bits = 72 > 64.
        let coords = vec![0u64; 8];
        assert!(encode(&coords, 9).is_err());
    }
}
