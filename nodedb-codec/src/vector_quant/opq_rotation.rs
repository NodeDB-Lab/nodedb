//! Shared utilities for OPQ codebook training.

/// Minimal deterministic RNG (Xorshift64) — avoids external deps in lib code.
pub(super) struct Xorshift64(u64);

impl Xorshift64 {
    pub(super) fn new(seed: u64) -> Self {
        Self(if seed == 0 {
            0xDEAD_BEEF_CAFE_1234
        } else {
            seed
        })
    }

    #[inline]
    pub(super) fn next_u64(&mut self) -> u64 {
        let mut x = self.0;
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        self.0 = x;
        x
    }
}
