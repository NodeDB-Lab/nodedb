// SPDX-License-Identifier: Apache-2.0

//! Numeric `sqrt`, `exp`, `ln`, `log` and `power`, at PostgreSQL's result
//! scale.
//!
//! `sqrt` is exact: the integer root of the scaled radicand, rounded half
//! away from zero. `exp`, `ln` and `power` compute at the 28 significant
//! digits `Decimal` holds, through 192-bit intermediates, and round to the
//! result scale half away from zero. A result scale past what `Decimal`
//! holds (28 places, 96-bit mantissa) is reduced to what it holds.

use rust_decimal::prelude::ToPrimitive;
use rust_decimal::{Decimal, MathematicalOps, RoundingStrategy};

use super::numeric_scale::{
    LOG10_E, MAX_DISPLAY_SCALE, MAX_RESULT_SCALE, MIN_SIG_DIGITS, clamp, exp_scale,
    int_power_weight, ln_scale, log_scale, sqrt_scale,
};
use super::wide_sqrt::rounded_sqrt;
use crate::expr::{EvalError, MathDomain, OutOfRange};

/// The largest `Decimal` mantissa, `2^96 - 1`.
const MAX_MANTISSA: u128 = (1u128 << 96) - 1;

fn overflow() -> EvalError {
    EvalError::OutOfRange(OutOfRange::Numeric)
}

/// `value` rounded half away from zero to `rscale` places, written at that
/// scale. A scale past what `Decimal` holds is reduced to what it holds.
fn at_scale(value: Decimal, rscale: i64) -> Decimal {
    let target = rscale.clamp(0, i64::from(Decimal::MAX_SCALE)) as u32;
    let mut rounded = value.round_dp_with_strategy(target, RoundingStrategy::MidpointAwayFromZero);
    rounded.rescale(target);
    if rounded.is_zero() {
        rounded.set_sign_positive(true);
    }
    rounded
}

/// Zero at `rscale` places.
fn zero_at(rscale: i64) -> Decimal {
    at_scale(Decimal::ZERO, rscale)
}

/// `sqrt(x)`. A negative `x` is [`MathDomain::SqrtOfNegative`].
pub(super) fn sqrt(x: Decimal) -> Result<Decimal, EvalError> {
    if x.is_sign_negative() && !x.is_zero() {
        return Err(EvalError::MathDomain(MathDomain::SqrtOfNegative));
    }
    let mantissa = x.mantissa().unsigned_abs();
    let scale = x.scale();
    let mut places = sqrt_scale(x).min(i64::from(Decimal::MAX_SCALE)) as u32;
    loop {
        if let Some(root) = rounded_sqrt(mantissa, scale, places)
            && root <= MAX_MANTISSA
        {
            return Decimal::try_from_i128_with_scale(root as i128, places).map_err(|_| overflow());
        }
        // The root at this scale does not fit: one place fewer.
        places = places.checked_sub(1).ok_or_else(overflow)?;
    }
}

/// `exp(x)`. A result past the `Decimal` range is out of range; one below
/// its smallest step is zero.
pub(super) fn exp(x: Decimal) -> Result<Decimal, EvalError> {
    let rscale = exp_scale(x);
    match x.checked_exp() {
        Some(value) => Ok(at_scale(value, rscale)),
        None if x.is_sign_negative() => Ok(zero_at(rscale)),
        None => Err(overflow()),
    }
}

/// `ln(x)`, positive `x` only.
fn ln_positive(x: Decimal) -> Result<Decimal, EvalError> {
    if x.is_zero() {
        return Err(EvalError::MathDomain(MathDomain::LogOfZero));
    }
    if x.is_sign_negative() {
        return Err(EvalError::MathDomain(MathDomain::LogOfNegative));
    }
    x.checked_ln().ok_or_else(overflow)
}

/// `ln(x)`. Zero is [`MathDomain::LogOfZero`], a negative `x`
/// [`MathDomain::LogOfNegative`].
pub(super) fn ln(x: Decimal) -> Result<Decimal, EvalError> {
    Ok(at_scale(ln_positive(x)?, ln_scale(x)))
}

/// `log(base, x)`, the logarithm of `x` to `base`. A base of 1 divides by
/// `ln(1) = 0`: division by zero.
pub(super) fn log(base: Decimal, x: Decimal) -> Result<Decimal, EvalError> {
    let rscale = log_scale(base, x);
    let ln_base = ln_positive(base)?;
    let ln_x = ln_positive(x)?;
    if ln_base.is_zero() {
        return Err(EvalError::DivisionByZero);
    }
    let value = ln_x.checked_div(ln_base).ok_or_else(overflow)?;
    Ok(at_scale(value, rscale))
}

/// `base ^ exponent`, as PostgreSQL's `numeric_power`.
pub(super) fn power(base: Decimal, exponent: Decimal) -> Result<Decimal, EvalError> {
    let integral = exponent.fract().is_zero();
    if base.is_zero() && exponent.is_sign_negative() && !exponent.is_zero() {
        return Err(EvalError::MathDomain(MathDomain::ZeroToNegativePower));
    }
    if base.is_sign_negative() && !base.is_zero() && !integral {
        return Err(EvalError::MathDomain(MathDomain::NegativeToFractionalPower));
    }
    if integral && let Some(exp) = exponent.to_i32() {
        return int_power(base, i64::from(exp), exponent.scale());
    }
    // 0 ^ a positive non-integer.
    if base.is_zero() {
        return Ok(zero_at(MIN_SIG_DIGITS));
    }
    let ln_num = ln_positive(base)?
        .checked_mul(exponent)
        .ok_or_else(overflow)?;
    let val = ln_num.to_f64().unwrap_or(0.0);
    if val.abs() > MAX_RESULT_SCALE as f64 * 3.01 {
        return if val > 0.0 {
            Err(overflow())
        } else {
            Ok(zero_at(MAX_DISPLAY_SCALE))
        };
    }
    let rscale = clamp(MIN_SIG_DIGITS - (val * LOG10_E) as i64, &[base, exponent]);
    match ln_num.checked_exp() {
        Some(value) => Ok(at_scale(value, rscale)),
        None if ln_num.is_sign_negative() => Ok(zero_at(rscale)),
        None => Err(overflow()),
    }
}

/// `base ^ exp` for an integer `exp`, as PostgreSQL's `power_var_int`.
/// `exp_scale` is the exponent's own scale, which bounds the result scale.
fn int_power(base: Decimal, exp: i64, exp_scale: u32) -> Result<Decimal, EvalError> {
    let weight = int_power_weight(base, exp);
    if weight > f64::from((i32::from(i16::MAX) + 1) * 4) {
        return Err(overflow());
    }
    if weight + 1.0 < -(MAX_DISPLAY_SCALE as f64) {
        return Ok(zero_at(MAX_DISPLAY_SCALE));
    }
    let exp_scale = Decimal::new(0, exp_scale);
    let rscale = clamp(MIN_SIG_DIGITS - weight as i64, &[base, exp_scale]);
    let value = match exp {
        0 => Decimal::ONE,
        1 => base,
        -1 => Decimal::ONE
            .checked_div(base)
            .ok_or(EvalError::DivisionByZero)?,
        2 => base.checked_mul(base).ok_or_else(overflow)?,
        _ if base.is_zero() => Decimal::ZERO,
        _ => match base.checked_powi(exp) {
            Some(value) => value,
            None if exp < 0 => Decimal::ZERO,
            None => return Err(overflow()),
        },
    };
    Ok(at_scale(value, rscale))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn dec(text: &str) -> Decimal {
        Decimal::from_str_exact(text).expect("decimal")
    }

    fn text(result: Result<Decimal, EvalError>) -> String {
        result.expect("numeric math").to_string()
    }

    /// The text PostgreSQL 15 gives for each call.
    #[test]
    fn results_match_postgres() {
        assert_eq!(text(sqrt(dec("16.0"))), "4.000000000000000");
        assert_eq!(text(sqrt(dec("2"))), "1.414213562373095");
        assert_eq!(text(sqrt(dec("0"))), "0.000000000000000");
        assert_eq!(text(sqrt(dec("1000000"))), "1000.0000000000000");
        assert_eq!(text(exp(dec("1.0"))), "2.7182818284590452");
        assert_eq!(text(exp(dec("0"))), "1.0000000000000000");
        assert_eq!(text(ln(dec("2.0"))), "0.6931471805599453");
        assert_eq!(text(ln(dec("1"))), "0.0000000000000000");
        assert_eq!(text(log(dec("10"), dec("100"))), "2.0000000000000000");
        assert_eq!(text(log(dec("2"), dec("8"))), "3.0000000000000000");
        assert_eq!(text(power(dec("2"), dec("3"))), "8.0000000000000000");
        assert_eq!(text(power(dec("2.5"), dec("2"))), "6.2500000000000000");
        assert_eq!(text(power(dec("2"), dec("0.5"))), "1.4142135623730950");
        assert_eq!(text(power(dec("-2"), dec("3"))), "-8.0000000000000000");
        assert_eq!(text(power(dec("0"), dec("0"))), "1.0000000000000000");
    }

    /// Arguments outside a function's domain fail with PostgreSQL's
    /// messages.
    #[test]
    fn domain_faults() {
        let message = |r: Result<Decimal, EvalError>| r.unwrap_err().to_string();
        assert_eq!(
            message(sqrt(dec("-1"))),
            "cannot take square root of a negative number"
        );
        assert_eq!(message(ln(dec("0"))), "cannot take logarithm of zero");
        assert_eq!(
            message(ln(dec("-2"))),
            "cannot take logarithm of a negative number"
        );
        assert_eq!(log(dec("1"), dec("5")), Err(EvalError::DivisionByZero));
        assert_eq!(
            message(power(dec("0"), dec("-1"))),
            "zero raised to a negative power is undefined"
        );
        assert_eq!(
            message(power(dec("-2"), dec("0.5"))),
            "a negative number raised to a non-integer power yields a complex result"
        );
        assert_eq!(
            exp(dec("1000")),
            Err(EvalError::OutOfRange(OutOfRange::Numeric))
        );
        assert!(exp(dec("-1000")).expect("exp").is_zero());
    }
}
