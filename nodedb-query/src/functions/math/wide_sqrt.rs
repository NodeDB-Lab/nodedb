// SPDX-License-Identifier: Apache-2.0

//! Exact integer square root over 256 bits, for numeric `sqrt`.

use std::cmp::Ordering;

/// An unsigned 256-bit integer: `hi * 2^128 + lo`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct U256 {
    hi: u128,
    lo: u128,
}

impl U256 {
    const ZERO: Self = Self { hi: 0, lo: 0 };

    fn from_u128(lo: u128) -> Self {
        Self { hi: 0, lo }
    }

    /// `self * 10`, `None` past 256 bits.
    fn times_ten(self) -> Option<Self> {
        let lo_lo = (self.lo & u128::from(u64::MAX)) * 10;
        let lo_hi = (self.lo >> 64) * 10 + (lo_lo >> 64);
        let lo = (lo_hi << 64) | (lo_lo & u128::from(u64::MAX));
        let hi = self.hi.checked_mul(10)?.checked_add(lo_hi >> 64)?;
        Some(Self { hi, lo })
    }

    fn add(self, other: Self) -> Self {
        let (lo, carry) = self.lo.overflowing_add(other.lo);
        Self {
            hi: self
                .hi
                .wrapping_add(other.hi)
                .wrapping_add(u128::from(carry)),
            lo,
        }
    }

    fn sub(self, other: Self) -> Self {
        let (lo, borrow) = self.lo.overflowing_sub(other.lo);
        Self {
            hi: self
                .hi
                .wrapping_sub(other.hi)
                .wrapping_sub(u128::from(borrow)),
            lo,
        }
    }

    fn shr(self, n: u32) -> Self {
        match n {
            0 => self,
            1..=127 => Self {
                hi: self.hi >> n,
                lo: (self.lo >> n) | (self.hi << (128 - n)),
            },
            _ => Self {
                hi: 0,
                lo: self.hi >> (n - 128),
            },
        }
    }

    /// `2^n` for `n < 256`.
    fn bit(n: u32) -> Self {
        if n < 128 {
            Self { hi: 0, lo: 1 << n }
        } else {
            Self {
                hi: 1 << (n - 128),
                lo: 0,
            }
        }
    }

    fn bits(self) -> u32 {
        if self.hi != 0 {
            256 - self.hi.leading_zeros()
        } else {
            128 - self.lo.leading_zeros()
        }
    }
}

impl PartialOrd for U256 {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for U256 {
    fn cmp(&self, other: &Self) -> Ordering {
        (self.hi, self.lo).cmp(&(other.hi, other.lo))
    }
}

/// `floor(sqrt(n))` and the remainder `n - root^2`, by the binary
/// digit-by-digit method.
fn isqrt(n: U256) -> (U256, U256) {
    let mut rest = n;
    let mut root = U256::ZERO;
    let top = n.bits();
    if top == 0 {
        return (U256::ZERO, U256::ZERO);
    }
    // The highest power of four at or below `n`.
    let mut one = U256::bit((top - 1) & !1);
    while one != U256::ZERO {
        let trial = root.add(one);
        if rest >= trial {
            rest = rest.sub(trial);
            root = root.shr(1).add(one);
        } else {
            root = root.shr(1);
        }
        one = one.shr(2);
    }
    (root, rest)
}

/// `sqrt(m * 10^-s)` at `r` decimal places, rounded half away from zero, as
/// the integer `round(sqrt(m * 10^(2r - s)))`. `None` when `2r < s` or the
/// radicand passes 256 bits, or the root passes 128 bits.
pub(super) fn rounded_sqrt(m: u128, s: u32, r: u32) -> Option<u128> {
    let shift = (2 * r).checked_sub(s)?;
    let mut n = U256::from_u128(m);
    for _ in 0..shift {
        n = n.times_ten()?;
    }
    let (root, rest) = isqrt(n);
    if root.hi != 0 {
        return None;
    }
    // `(root + 1/2)^2 = root^2 + root + 1/4`: an integer radicand rounds up
    // exactly when its remainder passes `root`, and never ties.
    let up = rest > root;
    root.lo.checked_add(u128::from(up))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn integer_square_roots_are_exact() {
        for n in [0u128, 1, 2, 3, 4, 15, 16, 17, 1 << 100, u128::MAX] {
            let (root, rest) = isqrt(U256::from_u128(n));
            let r = root.lo;
            assert_eq!(root.hi, 0);
            assert!(r.checked_mul(r).is_none_or(|sq| sq <= n), "{n}");
            assert_eq!(rest, U256::from_u128(n - r * r), "{n}");
            assert!((r + 1).checked_mul(r + 1).is_none_or(|sq| sq > n), "{n}");
        }
    }

    #[test]
    fn rounded_roots_at_a_scale() {
        // sqrt(16.0) at 15 places.
        assert_eq!(rounded_sqrt(160, 1, 15), Some(4_000_000_000_000_000));
        // sqrt(2) at 16 places: 1.4142135623730950|488...
        assert_eq!(rounded_sqrt(2, 0, 16), Some(14_142_135_623_730_950));
        // sqrt(3) at 16 places: 1.7320508075688772|935... rounds up.
        assert_eq!(rounded_sqrt(3, 0, 16), Some(17_320_508_075_688_773));
        // sqrt(0.0002) at 2 places: 0.0141... is 0.01.
        assert_eq!(rounded_sqrt(2, 4, 2), Some(1));
        assert_eq!(rounded_sqrt(2, 4, 1), None);
    }
}
