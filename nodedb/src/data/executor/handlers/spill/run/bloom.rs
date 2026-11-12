// SPDX-License-Identifier: BUSL-1.1

//! Per-run key bloom filter. A lookup reads a run's blocks only when the
//! bloom says the run may hold the key.

use std::hash::{DefaultHasher, Hash, Hasher};

/// Bloom bits per key: about a 1% false-positive rate with
/// [`PROBES`] probes.
const BITS_PER_KEY: usize = 10;

/// Probes per key.
const PROBES: u64 = 7;

/// A bloom filter over encoded key bytes.
#[derive(Debug, Clone)]
pub(in crate::data::executor) struct KeyBloom {
    bits: Vec<u64>,
    nbits: u64,
}

impl KeyBloom {
    /// A filter sized for `keys` distinct keys.
    pub(in crate::data::executor) fn with_capacity(keys: usize) -> Self {
        let nbits = (keys.max(1) * BITS_PER_KEY).next_power_of_two() as u64;
        let words = (nbits / 64).max(1) as usize;
        Self {
            bits: vec![0u64; words],
            nbits: (words as u64) * 64,
        }
    }

    fn hashes(key: &[u8]) -> (u64, u64) {
        let mut first = DefaultHasher::new();
        0u8.hash(&mut first);
        key.hash(&mut first);
        let mut second = DefaultHasher::new();
        1u8.hash(&mut second);
        key.hash(&mut second);
        (first.finish(), second.finish() | 1)
    }

    /// Add `key`.
    pub(in crate::data::executor) fn insert(&mut self, key: &[u8]) {
        let (h1, h2) = Self::hashes(key);
        for probe in 0..PROBES {
            let bit = h1.wrapping_add(probe.wrapping_mul(h2)) % self.nbits;
            if let Some(word) = self.bits.get_mut((bit / 64) as usize) {
                *word |= 1u64 << (bit % 64);
            }
        }
    }

    /// `false` when `key` is not in the filter. `true` when it may be.
    pub(in crate::data::executor) fn may_contain(&self, key: &[u8]) -> bool {
        let (h1, h2) = Self::hashes(key);
        (0..PROBES).all(|probe| {
            let bit = h1.wrapping_add(probe.wrapping_mul(h2)) % self.nbits;
            self.bits
                .get((bit / 64) as usize)
                .is_some_and(|word| word & (1u64 << (bit % 64)) != 0)
        })
    }

    /// Heap bytes the filter holds.
    pub(in crate::data::executor) fn heap_bytes(&self) -> usize {
        self.bits.len() * 8
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn inserted_keys_are_found_and_most_others_are_not() {
        let mut bloom = KeyBloom::with_capacity(1_000);
        for i in 0..1_000u32 {
            bloom.insert(&i.to_le_bytes());
        }
        for i in 0..1_000u32 {
            assert!(bloom.may_contain(&i.to_le_bytes()));
        }
        let false_hits = (1_000..11_000u32)
            .filter(|i| bloom.may_contain(&i.to_le_bytes()))
            .count();
        assert!(false_hits < 500, "false hits {false_hits}");
    }
}
