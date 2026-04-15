//! Incarnation numbers — monotonic epoch counters per node.
//!
//! SWIM resolves conflicting state updates by comparing `(incarnation, state)`
//! lexicographically. Each node owns its own incarnation and is the only
//! writer that may bump it (via refutation of a `Suspect` rumour). Remote
//! observers can only propagate the value they learned; they never mint new
//! incarnations for peers.
//!
//! Wrap-around is handled by saturation: the incarnation is a `u64` and will
//! not overflow in any realistic deployment lifetime (2^64 ticks at 1 Hz ≈
//! 5.8 × 10^11 years). Still, [`Incarnation::bump`] uses `saturating_add` so
//! a hypothetical overflow degrades to "no further refutation possible"
//! rather than wrapping silently to zero.

use std::fmt;

use serde::{Deserialize, Serialize};

/// A monotonic epoch counter owned by a single node.
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
pub struct Incarnation(u64);

impl Incarnation {
    /// The bottom incarnation, assigned to a freshly-joined node before it
    /// has ever been suspected.
    pub const ZERO: Incarnation = Incarnation(0);

    /// Construct an incarnation from its raw `u64` representation. Exposed
    /// for deserialization and deterministic tests.
    pub const fn new(v: u64) -> Self {
        Self(v)
    }

    /// The raw value. Exposed for wire serialization.
    pub const fn get(self) -> u64 {
        self.0
    }

    /// Return a new incarnation strictly greater than both `self` and
    /// `rumour`. This is the refutation rule: when the local node receives
    /// a `Suspect(i)` rumour about itself, it must broadcast an `Alive(j)`
    /// with `j > i` — and `j` must also be strictly greater than whatever
    /// the local node last advertised, so the new value dominates both.
    ///
    /// Saturating: at `u64::MAX` the value stays pinned.
    pub fn refute(self, rumour: Incarnation) -> Self {
        let hi = self.0.max(rumour.0);
        Incarnation(hi.saturating_add(1))
    }

    /// Bump by one. Used when the local node voluntarily increments its
    /// incarnation (e.g. on rejoin after a suspected restart).
    pub fn bump(self) -> Self {
        Incarnation(self.0.saturating_add(1))
    }
}

impl fmt::Display for Incarnation {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn zero_is_minimum() {
        assert!(Incarnation::ZERO <= Incarnation::new(1));
        assert_eq!(Incarnation::ZERO.get(), 0);
    }

    #[test]
    fn refute_dominates_both_inputs() {
        let local = Incarnation::new(3);
        let rumour = Incarnation::new(5);
        let new = local.refute(rumour);
        assert!(new > local);
        assert!(new > rumour);
        assert_eq!(new, Incarnation::new(6));
    }

    #[test]
    fn refute_local_greater() {
        let local = Incarnation::new(10);
        let rumour = Incarnation::new(4);
        assert_eq!(local.refute(rumour), Incarnation::new(11));
    }

    #[test]
    fn bump_is_monotonic() {
        let i = Incarnation::new(7);
        assert_eq!(i.bump(), Incarnation::new(8));
    }

    #[test]
    fn saturates_at_u64_max() {
        let max = Incarnation::new(u64::MAX);
        assert_eq!(max.bump(), max);
        assert_eq!(max.refute(Incarnation::ZERO), max);
    }

    #[test]
    fn total_ordering() {
        let mut xs = [
            Incarnation::new(5),
            Incarnation::ZERO,
            Incarnation::new(2),
            Incarnation::new(9),
        ];
        xs.sort();
        assert_eq!(
            xs,
            [
                Incarnation::ZERO,
                Incarnation::new(2),
                Incarnation::new(5),
                Incarnation::new(9),
            ]
        );
    }

    #[test]
    fn display_matches_raw() {
        assert_eq!(Incarnation::new(42).to_string(), "42");
    }
}
