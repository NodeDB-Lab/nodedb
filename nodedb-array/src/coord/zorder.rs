//! ND Z-order (Morton) encode/decode.
//!
//! Bit-interleaving fallback for non-uniform dim sizes — cheap to
//! compute, less locality than Hilbert but tolerates dims of differing
//! granularity. Same `n × bits <= 64` constraint as
//! [`super::hilbert`].

use super::normalize::MAX_DIMS;
use crate::error::{ArrayError, ArrayResult};

/// Interleave per-dim bits into a single Z-order index, MSB first.
pub fn encode(coords: &[u64], bits: u32) -> ArrayResult<u64> {
    check_shape(coords.len(), bits)?;
    let n = coords.len();
    let mut idx: u64 = 0;
    for b in (0..bits).rev() {
        for c in coords.iter().take(n) {
            let bit = (*c >> b) & 1;
            idx = (idx << 1) | bit;
        }
    }
    Ok(idx)
}

/// Inverse of [`encode`].
pub fn decode(idx: u64, n: usize, bits: u32) -> ArrayResult<Vec<u64>> {
    check_shape(n, bits)?;
    let mut out = vec![0u64; n];
    for b in (0..bits).rev() {
        for (i, slot) in out.iter_mut().enumerate().take(n) {
            let shift = b * n as u32 + (n as u32 - 1 - i as u32);
            let bit = (idx >> shift) & 1;
            *slot = (*slot << 1) | bit;
        }
    }
    Ok(out)
}

fn check_shape(n: usize, bits: u32) -> ArrayResult<()> {
    if n > MAX_DIMS {
        return Err(ArrayError::InvalidSchema {
            array: String::new(),
            detail: format!("zorder: arity {n} exceeds MAX_DIMS={MAX_DIMS}"),
        });
    }
    if bits == 0 || (n as u32) * bits > 64 {
        return Err(ArrayError::InvalidSchema {
            array: String::new(),
            detail: format!("zorder: {n} dims × {bits} bits exceeds 64-bit prefix"),
        });
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn zorder_round_trip_2d() {
        for x in 0..16u64 {
            for y in 0..16u64 {
                let idx = encode(&[x, y], 4).unwrap();
                assert_eq!(decode(idx, 2, 4).unwrap(), vec![x, y]);
            }
        }
    }

    #[test]
    fn zorder_round_trip_3d() {
        for x in 0..8u64 {
            for y in 0..8u64 {
                for z in 0..8u64 {
                    let idx = encode(&[x, y, z], 3).unwrap();
                    assert_eq!(decode(idx, 3, 3).unwrap(), vec![x, y, z]);
                }
            }
        }
    }

    #[test]
    fn zorder_unique_2d() {
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
    fn zorder_rejects_overflow() {
        let coords = vec![0u64; 9];
        assert!(encode(&coords, 8).is_err());
    }
}
