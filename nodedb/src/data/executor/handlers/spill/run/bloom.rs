// SPDX-License-Identifier: BUSL-1.1

//! Per-block key bloom filter. The run writer builds one filter over the
//! keys of each data block and writes it as the block's trailer frame. A
//! point lookup reads the block, then decodes its records only when the
//! filter admits the key. No filter stays in RAM.

use std::hash::{DefaultHasher, Hash, Hasher};

/// Bloom bits per key: about a 1% false-positive rate with
/// [`PROBES`] probes.
const BITS_PER_KEY: usize = 10;

/// Probes per key.
const PROBES: u64 = 7;

/// The two base hashes of one key.
pub(in crate::data::executor) type KeyHash = (u64, u64);

/// The base hashes of `key`.
pub(in crate::data::executor) fn key_hash(key: &[u8]) -> KeyHash {
    let mut first = DefaultHasher::new();
    0u8.hash(&mut first);
    key.hash(&mut first);
    let mut second = DefaultHasher::new();
    1u8.hash(&mut second);
    key.hash(&mut second);
    (first.finish(), second.finish() | 1)
}

/// The bit positions `hash` sets in a filter of `nbits` bits.
fn probes((h1, h2): KeyHash, nbits: u64) -> impl Iterator<Item = u64> {
    (0..PROBES).map(move |probe| h1.wrapping_add(probe.wrapping_mul(h2)) % nbits)
}

/// A bloom filter under construction.
#[derive(Debug, Clone)]
pub(in crate::data::executor) struct KeyBloom {
    words: Vec<u64>,
}

impl KeyBloom {
    /// A filter over `hashes`, sized for their count.
    pub(in crate::data::executor) fn from_hashes(hashes: &[KeyHash]) -> Self {
        let nbits = (hashes.len().max(1) * BITS_PER_KEY).next_power_of_two();
        let mut words = vec![0u64; (nbits / 64).max(1)];
        let nbits = (words.len() as u64) * 64;
        for hash in hashes {
            for bit in probes(*hash, nbits) {
                if let Some(word) = words.get_mut((bit / 64) as usize) {
                    *word |= 1u64 << (bit % 64);
                }
            }
        }
        Self { words }
    }

    /// The filter's words.
    pub(in crate::data::executor) fn words(&self) -> &[u64] {
        &self.words
    }
}

/// A filter read in place from its little-endian words.
pub(in crate::data::executor) struct BloomView<'a> {
    words: &'a [u8],
}

impl<'a> BloomView<'a> {
    /// A view over `words`: whole little-endian `u64`s, at least one.
    pub(in crate::data::executor) fn new(words: &'a [u8]) -> Option<Self> {
        if words.is_empty() || !words.len().is_multiple_of(8) {
            return None;
        }
        Some(Self { words })
    }

    fn word(&self, index: usize) -> u64 {
        let mut bytes = [0u8; 8];
        if let Some(src) = self.words.get(index * 8..index * 8 + 8) {
            bytes.copy_from_slice(src);
        }
        u64::from_le_bytes(bytes)
    }

    /// `false` when `key` is not in the filter. `true` when it can be.
    pub(in crate::data::executor) fn may_contain(&self, key: &[u8]) -> bool {
        let nbits = (self.words.len() as u64) * 8;
        probes(key_hash(key), nbits)
            .all(|bit| self.word((bit / 64) as usize) & (1u64 << (bit % 64)) != 0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn inserted_keys_are_found_and_most_others_are_not() {
        let hashes: Vec<KeyHash> = (0..1_000u32).map(|i| key_hash(&i.to_le_bytes())).collect();
        let bloom = KeyBloom::from_hashes(&hashes);
        let bytes: Vec<u8> = bloom.words().iter().flat_map(|w| w.to_le_bytes()).collect();
        let view = BloomView::new(&bytes).unwrap();
        for i in 0..1_000u32 {
            assert!(view.may_contain(&i.to_le_bytes()));
        }
        let false_hits = (1_000..11_000u32)
            .filter(|i| view.may_contain(&i.to_le_bytes()))
            .count();
        assert!(false_hits < 500, "false hits {false_hits}");
    }

    #[test]
    fn a_view_refuses_partial_words() {
        assert!(BloomView::new(&[]).is_none());
        assert!(BloomView::new(&[0; 7]).is_none());
        assert!(BloomView::new(&[0; 8]).is_some());
    }
}
