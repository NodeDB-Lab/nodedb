// SPDX-License-Identifier: BUSL-1.1

//! Shared utility functions.

pub mod bounded_json;
pub mod bounded_msgpack;
pub mod rmpv_value;
pub mod wire_json;

/// FNV-1a 64-bit hash of a byte slice.
///
/// Deterministic, non-cryptographic hash suitable for in-process IDs
/// (R-tree entries, HLL sketches, deduplication keys). NOT suitable
/// for security-sensitive hashing — use SHA-256 for that.
pub fn fnv1a_hash(bytes: &[u8]) -> u64 {
    let mut hash: u64 = 0xcbf29ce484222325; // FNV-1a offset basis
    for &b in bytes {
        hash ^= b as u64;
        hash = hash.wrapping_mul(0x100000001b3); // FNV prime
    }
    hash
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn deterministic() {
        assert_eq!(fnv1a_hash(b"hello"), fnv1a_hash(b"hello"));
    }

    #[test]
    fn distinct() {
        assert_ne!(fnv1a_hash(b"hello"), fnv1a_hash(b"world"));
    }

    #[test]
    fn empty_input() {
        // Should return the offset basis for empty input.
        assert_eq!(fnv1a_hash(b""), 0xcbf29ce484222325);
    }
}
