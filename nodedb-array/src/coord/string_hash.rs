//! String-dim hashing helper shared between curve normalization and
//! tile-index bucketing.
//!
//! Both [`super::normalize`] (when projecting `String` coords onto a
//! space-filling curve) and [`crate::tile::layout`] (when bucketing
//! `String` coords into tile indices) need the same per-process
//! mapping from a string to an integer. Centralising it here keeps
//! their assignments consistent within a process and gives a single
//! site to swap for sort-preserving codes later.
//!
//! `RandomState` is per-process; bucket assignments are NOT stable
//! across restarts. Tier 1 accepts the loss; future tiers may upgrade
//! to a deterministic dictionary code.

use std::hash::{BuildHasher, Hasher};

/// Hash `s` into the range `[0, bound]` (inclusive) by masking with
/// `bound`. Caller passes `bound = (1 << bits) - 1` for power-of-two
/// curve buckets, or `extent - 1` style values for tile bucketing
/// (where `bound + 1` is a power of two); for arbitrary `bound`,
/// callers should use [`hash_string_modulo`] instead.
pub fn hash_string_masked(s: &str, bound: u64) -> u64 {
    let mut h = std::collections::hash_map::RandomState::new().build_hasher();
    h.write(s.as_bytes());
    if bound == 0 { 0 } else { h.finish() & bound }
}

/// Hash `s` into `[0, modulus)` via modulo. Used by tile-index
/// bucketing where the extent isn't necessarily a power of two.
pub fn hash_string_modulo(s: &str, modulus: u64) -> u64 {
    let mut h = std::collections::hash_map::RandomState::new().build_hasher();
    h.write(s.as_bytes());
    if modulus <= 1 {
        0
    } else {
        h.finish() % modulus
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn masked_hash_within_bound() {
        let bound = 0xFFu64;
        for i in 0..32 {
            let s = format!("k{i}");
            assert!(hash_string_masked(&s, bound) <= bound);
        }
    }

    #[test]
    fn modulo_hash_within_modulus() {
        let m = 7u64;
        for i in 0..32 {
            let s = format!("k{i}");
            assert!(hash_string_modulo(&s, m) < m);
        }
    }

    #[test]
    fn zero_modulus_returns_zero() {
        assert_eq!(hash_string_modulo("x", 0), 0);
        assert_eq!(hash_string_modulo("x", 1), 0);
    }
}
