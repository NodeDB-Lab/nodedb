// SPDX-License-Identifier: Apache-2.0

//! A `Decimal` against an `f64`: exact order, and the nearest `f64`.
//!
//! - [`cmp_decimal_float_exact`] orders the two values with no rounding of
//!   either. It needs no wide arithmetic past `u128`.
//! - [`decimal_to_f64`] rounds a decimal to the nearest `f64`, ties to even.
//!   `Decimal::as_f64` sums two rounded parts, so it can land one `f64`
//!   away from the nearest.

use std::cmp::Ordering;
use std::fmt::Write as _;

use rust_decimal::Decimal;

/// `2^96` as `f64`: the first float above every `Decimal` magnitude.
pub const TWO_POW_96: f64 = 79_228_162_514_264_337_593_543_950_336.0;

/// A `Decimal` split into its integral part and its fraction numerator over
/// `10^scale`. Both carry the decimal's sign. Exact: the mantissa is below
/// `2^96`, and `10^28` fits an `i128`.
fn decimal_parts(d: Decimal) -> (i128, i128, u32) {
    let scale = d.scale();
    let unit = 10_i128.pow(scale);
    let mantissa = d.mantissa();
    (mantissa / unit, mantissa % unit, scale)
}

/// A positive finite `f64` below 1 as `(m, k)` with value `m * 2^-k`.
fn fraction_bits(f: f64) -> (u128, u32) {
    let bits = f.to_bits();
    let exponent = ((bits >> 52) & 0x7ff) as u32;
    let fraction = bits & ((1_u64 << 52) - 1);
    if exponent == 0 {
        (u128::from(fraction), 1074)
    } else {
        // `f < 1` puts `exponent` below 1023, so `k = 1075 - exponent > 52`.
        (u128::from(fraction | (1_u64 << 52)), 1075 - exponent)
    }
}

/// Exact order of `numerator / 10^scale` against `f`. Both are positive and
/// below 1.
fn cmp_fraction_magnitudes(numerator: u128, scale: u32, f: f64) -> Ordering {
    // `numerator / 10^scale` against `m / 2^k` is `numerator * 2^k` against
    // `m * 5^scale * 2^scale`. `m < 2^53` and `5^28 < 2^66`, so `scaled`
    // fits a `u128`.
    let (m, k) = fraction_bits(f);
    let scaled = m * 5_u128.pow(scale);
    if k >= scale {
        let shift = k - scale;
        let (whole, remainder) = if shift >= 128 {
            (0, scaled != 0)
        } else {
            (scaled >> shift, scaled & ((1_u128 << shift) - 1) != 0)
        };
        let tail = if remainder {
            Ordering::Less
        } else {
            Ordering::Equal
        };
        numerator.cmp(&whole).then(tail)
    } else {
        let shift = scale - k;
        if scaled.leading_zeros() < shift {
            // `scaled << shift` reaches `2^128`, above every numerator.
            return Ordering::Less;
        }
        numerator.cmp(&(scaled << shift))
    }
}

/// The sign of `f` as `-1`, `0` or `1`. `f` is not NaN.
fn float_sign(f: f64) -> i128 {
    if f > 0.0 {
        1
    } else if f < 0.0 {
        -1
    } else {
        0
    }
}

/// Exact order of a `Decimal` against an `f64`, with no rounding of either.
/// NaN is above every decimal. `-0.0` equals zero.
pub fn cmp_decimal_float_exact(d: Decimal, f: f64) -> Ordering {
    if f.is_nan() || f >= TWO_POW_96 {
        return Ordering::Less;
    }
    if f <= -TWO_POW_96 {
        return Ordering::Greater;
    }
    let (whole, fraction, scale) = decimal_parts(d);
    // `|f| < 2^96`, so its integral part is an exact `i128`.
    let f_whole = f.trunc();
    let by_whole = whole.cmp(&(f_whole as i128));
    if by_whole != Ordering::Equal {
        return by_whole;
    }
    let f_fraction = f - f_whole;
    let by_sign = fraction.signum().cmp(&float_sign(f_fraction));
    if by_sign != Ordering::Equal || fraction == 0 {
        return by_sign;
    }
    let magnitude = cmp_fraction_magnitudes(fraction.unsigned_abs(), scale, f_fraction.abs());
    if fraction < 0 {
        magnitude.reverse()
    } else {
        magnitude
    }
}

/// Room for any `Decimal` in plain notation: 29 digits, a sign, a point and
/// a leading zero.
const DECIMAL_TEXT_CAPACITY: usize = 40;

/// A fixed buffer a `Decimal` formats into without a heap allocation.
struct DecimalText {
    bytes: [u8; DECIMAL_TEXT_CAPACITY],
    len: usize,
}

impl std::fmt::Write for DecimalText {
    fn write_str(&mut self, text: &str) -> std::fmt::Result {
        let end = self
            .len
            .checked_add(text.len())
            .filter(|end| *end <= DECIMAL_TEXT_CAPACITY)
            .ok_or(std::fmt::Error)?;
        self.bytes
            .get_mut(self.len..end)
            .ok_or(std::fmt::Error)?
            .copy_from_slice(text.as_bytes());
        self.len = end;
        Ok(())
    }
}

/// `d` rounded to the nearest `f64`, ties to even. The standard float
/// parser rounds decimal text correctly, so `d` reads through its text.
pub fn decimal_to_f64(d: Decimal) -> f64 {
    let mut text = DecimalText {
        bytes: [0; DECIMAL_TEXT_CAPACITY],
        len: 0,
    };
    if write!(text, "{d}").is_ok()
        && let Some(bytes) = text.bytes.get(..text.len)
        && let Ok(text) = std::str::from_utf8(bytes)
        && let Ok(rounded) = text.parse::<f64>()
    {
        return rounded;
    }
    // Plain decimal text always fits the buffer and parses. This keeps the
    // function total without a panic path.
    d.as_f64()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn dec(text: &str) -> Decimal {
        Decimal::from_str_exact(text).unwrap()
    }

    #[test]
    fn integral_parts_decide_first() {
        let float = 9_007_199_254_740_992.0;
        assert_eq!(
            cmp_decimal_float_exact(dec("9007199254740993.0001"), float),
            Ordering::Greater
        );
        assert_eq!(
            cmp_decimal_float_exact(dec("9007199254740991.9999"), float),
            Ordering::Less
        );
        assert_eq!(cmp_decimal_float_exact(dec("-1.5"), 0.25), Ordering::Less);
    }

    #[test]
    fn fractions_compare_exactly() {
        // 0.1 as f64 is 0.1000000000000000055511151231257827...
        assert_eq!(cmp_decimal_float_exact(dec("0.1"), 0.1), Ordering::Less);
        assert_eq!(
            cmp_decimal_float_exact(dec("0.1000000000000000055511151232"), 0.1),
            Ordering::Greater
        );
        assert_eq!(cmp_decimal_float_exact(dec("0.5"), 0.5), Ordering::Equal);
        assert_eq!(
            cmp_decimal_float_exact(dec("-0.25"), -0.25),
            Ordering::Equal
        );
        assert_eq!(cmp_decimal_float_exact(dec("-0.25"), -0.2), Ordering::Less);
        assert_eq!(cmp_decimal_float_exact(dec("0"), -0.0), Ordering::Equal);
        assert_eq!(
            cmp_decimal_float_exact(dec("0.0000001"), f64::from_bits(1)),
            Ordering::Greater
        );
    }

    #[test]
    fn out_of_range_floats_bound_every_decimal() {
        assert_eq!(
            cmp_decimal_float_exact(Decimal::MAX, f64::INFINITY),
            Ordering::Less
        );
        assert_eq!(
            cmp_decimal_float_exact(Decimal::MAX, f64::NAN),
            Ordering::Less
        );
        assert_eq!(
            cmp_decimal_float_exact(Decimal::MIN, -1e30),
            Ordering::Greater
        );
    }

    #[test]
    fn decimals_round_to_the_nearest_float() {
        assert_eq!(decimal_to_f64(dec("0.1")), 0.1);
        assert_eq!(
            decimal_to_f64(dec("9007199254740993.0001")),
            9_007_199_254_740_994.0
        );
        assert_eq!(decimal_to_f64(dec("-2.5")), -2.5);
        // `Decimal::MAX` is `2^96 - 1`, nearest to `2^96`.
        assert_eq!(decimal_to_f64(Decimal::MAX), TWO_POW_96);
        assert_eq!(decimal_to_f64(Decimal::MIN), -TWO_POW_96);
    }
}
