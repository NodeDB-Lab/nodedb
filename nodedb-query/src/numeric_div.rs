// SPDX-License-Identifier: Apache-2.0

//! Numeric division and modulo at PostgreSQL's result scale.
//!
//! PostgreSQL picks the scale of `numeric / numeric` in `select_div_scale`.
//! The quotient keeps at least 16 significant digits. It never shows fewer
//! fractional digits than either operand, and its scale lies between 0 and
//! 1000. The quotient rounds half away from zero at that scale.
//!
//! A `Decimal` holds at most 28 fractional digits in a 96-bit mantissa. The
//! scale is capped at 28, and lowered further while the quotient does not fit.

use rust_decimal::Decimal;

/// PostgreSQL's `NUMERIC_MIN_SIG_DIGITS`.
const MIN_SIG_DIGITS: i64 = 16;
/// Decimal digits in one PostgreSQL numeric digit (`DEC_DIGITS`, base 10000).
const DEC_DIGITS: i64 = 4;
/// PostgreSQL's `NUMERIC_MAX_DISPLAY_SCALE`.
const MAX_DISPLAY_SCALE: i64 = 1000;
/// The largest `Decimal` mantissa, `2^96 - 1`.
const MAX_MANTISSA: u128 = (1u128 << 96) - 1;

/// `a / b` at PostgreSQL's result scale.
///
/// `None` for a zero divisor, and for a quotient whose integer part does not
/// fit a `Decimal`.
pub fn numeric_div(a: Decimal, b: Decimal) -> Option<Decimal> {
    if b.is_zero() {
        return None;
    }
    let numerator = a.mantissa().unsigned_abs();
    let divisor = b.mantissa().unsigned_abs();
    let negative = a.is_sign_negative() != b.is_sign_negative();
    let mut scale = div_scale(a, b).min(i64::from(Decimal::MAX_SCALE));
    loop {
        // a / b = numerator / divisor * 10^(b.scale - a.scale). At `scale`
        // fractional digits the quotient mantissa carries `shift` more
        // decimal places than numerator / divisor.
        let shift = i64::from(b.scale()) - i64::from(a.scale()) + scale;
        if let Some(mantissa) = quotient_at(numerator, divisor, shift)
            && mantissa <= MAX_MANTISSA
        {
            let magnitude = i128::try_from(mantissa).ok()?;
            // A zero quotient carries no sign.
            let signed = if negative && magnitude != 0 {
                -magnitude
            } else {
                magnitude
            };
            return Decimal::try_from_i128_with_scale(signed, u32::try_from(scale).ok()?).ok();
        }
        if scale == 0 {
            return None;
        }
        scale -= 1;
    }
}

/// `a % b` as PostgreSQL's numeric `mod`: `a - trunc(a / b) * b`. The sign
/// follows the dividend, and the scale is the larger operand scale. `None`
/// for a zero divisor.
pub fn numeric_rem(a: Decimal, b: Decimal) -> Option<Decimal> {
    let mut remainder = a.checked_rem(b)?;
    // A zero remainder carries no sign.
    if remainder.is_zero() {
        remainder.set_sign_positive(true);
    }
    // The remainder is below the divisor in magnitude, so it holds at the
    // larger scale wherever the divisor does.
    remainder.rescale(a.scale().max(b.scale()));
    Some(remainder)
}

/// PostgreSQL's `select_div_scale` for `a / b`.
fn div_scale(a: Decimal, b: Decimal) -> i64 {
    let (weight1, first1) = weight_and_first_digit(a);
    let (weight2, first2) = weight_and_first_digit(b);
    // The quotient's weight. Equal leading digits leave it unsure, so the
    // dividend is taken as the smaller.
    let mut quotient_weight = weight1 - weight2;
    if first1 <= first2 {
        quotient_weight -= 1;
    }
    (MIN_SIG_DIGITS - quotient_weight * DEC_DIGITS)
        .max(i64::from(a.scale()))
        .max(i64::from(b.scale()))
        .clamp(0, MAX_DISPLAY_SCALE)
}

/// The base-10000 weight of `value` and its leading base-10000 digit, as
/// PostgreSQL stores a numeric. Zero has weight 0 and leading digit 0.
fn weight_and_first_digit(value: Decimal) -> (i64, u128) {
    let mantissa = value.mantissa().unsigned_abs();
    if mantissa == 0 {
        return (0, 0);
    }
    let scale = i64::from(value.scale());
    // The power of ten of the leading decimal digit.
    let exponent = i64::from(mantissa.ilog10()) - scale;
    let weight = exponent.div_euclid(DEC_DIGITS);
    // value / 10000^weight = mantissa / 10^shift. The leading base-10000
    // digit holds the leading decimal digit, so shift lies in -3..=28.
    let shift = scale + DEC_DIGITS * weight;
    let power = 10u128.pow(shift.unsigned_abs() as u32);
    let first = if shift >= 0 {
        mantissa / power
    } else {
        mantissa * power
    };
    (weight, first)
}

/// `numerator / divisor * 10^shift`, rounded half away from zero. `None`
/// once the result passes the largest `Decimal` mantissa.
fn quotient_at(numerator: u128, divisor: u128, shift: i64) -> Option<u128> {
    let whole = numerator / divisor;
    if shift < 0 {
        let power = 10u128.checked_pow(u32::try_from(shift.unsigned_abs()).ok()?)?;
        let dropped = whole % power;
        // A dropped part of exactly half rounds up, and any remainder below
        // `whole` only adds to it.
        let round_up = dropped.checked_mul(2)? >= power;
        return (whole / power).checked_add(u128::from(round_up));
    }
    let mut quotient = whole;
    let mut remainder = numerator % divisor;
    for _ in 0..shift {
        // remainder < divisor < 2^96, so ten times it fits.
        remainder *= 10;
        quotient = quotient.checked_mul(10)?.checked_add(remainder / divisor)?;
        if quotient > MAX_MANTISSA {
            return None;
        }
        remainder %= divisor;
    }
    let round_up = remainder * 2 >= divisor;
    quotient.checked_add(u128::from(round_up))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn dec(text: &str) -> Decimal {
        Decimal::from_str_exact(text).unwrap()
    }

    fn div(a: &str, b: &str) -> String {
        numeric_div(dec(a), dec(b)).unwrap().to_string()
    }

    #[test]
    fn equal_leading_digits_keep_twenty_fractional_digits() {
        assert_eq!(div("2", "2.0"), "1.00000000000000000000");
    }

    #[test]
    fn a_repeating_quotient_rounds_at_twenty_digits() {
        assert_eq!(div("1", "3.0"), "0.33333333333333333333");
        assert_eq!(div("2", "3"), "0.66666666666666666667");
    }

    #[test]
    fn a_larger_leading_digit_keeps_sixteen_fractional_digits() {
        assert_eq!(div("10.5", "2"), "5.2500000000000000");
    }

    #[test]
    fn the_operand_display_scale_is_a_floor() {
        assert_eq!(
            div("1.0000000000000000000000", "1"),
            "1.0000000000000000000000"
        );
    }

    #[test]
    fn signs_and_zero_follow_the_operands() {
        assert_eq!(div("-1", "4"), "-0.25000000000000000000");
        assert_eq!(div("0", "2.0"), "0.00000000000000000000");
    }

    #[test]
    fn a_large_quotient_lowers_the_scale_until_it_fits() {
        let quotient = numeric_div(dec("79228162514264337593543950335"), dec("1")).unwrap();
        assert_eq!(quotient, dec("79228162514264337593543950335"));
    }

    #[test]
    fn a_zero_divisor_has_no_quotient() {
        assert!(numeric_div(dec("1"), dec("0")).is_none());
        assert!(numeric_rem(dec("1"), dec("0")).is_none());
    }

    fn rem(a: &str, b: &str) -> String {
        numeric_rem(dec(a), dec(b)).unwrap().to_string()
    }

    /// PostgreSQL: `5.5 % 2` = 1.5, `-5.5 % 2` = -1.5, `5.5 % -2` = 1.5,
    /// `7 % 2.00` = 1.00, `1 % 2.00` = 1.00, `6.0 % 2` = 0.0,
    /// `6.00 % 6.0` = 0.00, `-6 % 3` = 0.
    #[test]
    fn modulo_follows_the_dividend_at_the_larger_scale() {
        assert_eq!(rem("5.5", "2"), "1.5");
        assert_eq!(rem("-5.5", "2"), "-1.5");
        assert_eq!(rem("5.5", "-2"), "1.5");
        assert_eq!(rem("7", "2.00"), "1.00");
        assert_eq!(rem("1", "2.00"), "1.00");
        assert_eq!(rem("6.0", "2"), "0.0");
        assert_eq!(rem("6.00", "6.0"), "0.00");
        assert_eq!(rem("-6", "3"), "0");
    }
}
