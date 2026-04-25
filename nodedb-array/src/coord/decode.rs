//! High-level prefix decoding.
//!
//! Decoding is informational — recovers per-dim integer coordinates
//! from a prefix. Inverse normalization (back into typed
//! [`crate::types::CoordValue`]) is intentionally not provided: the
//! prefix is lossy for `Float64` and `String` dims by design.

use super::{hilbert, zorder};
use crate::error::ArrayResult;

pub fn decode_hilbert_prefix(idx: u64, n_dims: usize, bits: u32) -> ArrayResult<Vec<u64>> {
    hilbert::decode(idx, n_dims, bits)
}

pub fn decode_zorder_prefix(idx: u64, n_dims: usize, bits: u32) -> ArrayResult<Vec<u64>> {
    zorder::decode(idx, n_dims, bits)
}

#[cfg(test)]
mod tests {
    use super::super::{hilbert, zorder};
    use super::*;

    #[test]
    fn hilbert_decode_matches_module() {
        let idx = hilbert::encode(&[3, 5], 4).unwrap();
        assert_eq!(decode_hilbert_prefix(idx, 2, 4).unwrap(), vec![3, 5]);
    }

    #[test]
    fn zorder_decode_matches_module() {
        let idx = zorder::encode(&[3, 5], 4).unwrap();
        assert_eq!(decode_zorder_prefix(idx, 2, 4).unwrap(), vec![3, 5]);
    }
}
