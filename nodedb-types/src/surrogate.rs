//! Surrogate — a global, stable, monotonically-allocated u32 row identity.
//!
//! Every logical row in every engine carries a surrogate. The surrogate is
//! allocated from a WAL-durable, Raft-replicated monotonic counter at insert
//! time and never reused. Cross-engine prefilter and join therefore reduce
//! to roaring-bitmap intersections — no per-query translation between
//! engine-local internal IDs.
//!
//! This file defines only the value type and its derives. Allocation,
//! persistence, and bootstrap live in `nodedb::control::surrogate`.

use std::fmt;

use serde::{Deserialize, Serialize};

/// Global surrogate identifier.
///
/// A `u32` newtype carrying the same comparison + hashing semantics as the
/// underlying integer. The `Display` impl renders as `sur:N` for diagnostics
/// (mirrors the `Lsn` convention).
#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Hash,
    Serialize,
    Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
pub struct Surrogate(pub u32);

impl Surrogate {
    /// The zero surrogate. Reserved as a sentinel — real allocation starts at 1.
    pub const ZERO: Surrogate = Surrogate(0);

    /// Construct a surrogate from a raw `u32`.
    pub const fn new(value: u32) -> Self {
        Self(value)
    }

    /// Raw `u32` value.
    pub const fn as_u32(self) -> u32 {
        self.0
    }
}

impl fmt::Display for Surrogate {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "sur:{}", self.0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ordering_matches_u32() {
        let a = Surrogate::new(1);
        let b = Surrogate::new(2);
        assert!(a < b);
        assert_eq!(a.as_u32(), 1);
    }

    #[test]
    fn display_renders_sur_prefix() {
        assert_eq!(Surrogate::new(42).to_string(), "sur:42");
    }

    #[test]
    fn msgpack_roundtrip() {
        let s = Surrogate::new(0xDEAD_BEEF);
        let bytes = zerompk::to_msgpack_vec(&s).unwrap();
        let back: Surrogate = zerompk::from_msgpack(&bytes).unwrap();
        assert_eq!(s, back);
    }

    #[test]
    fn zero_sentinel() {
        assert_eq!(Surrogate::ZERO.as_u32(), 0);
    }
}
