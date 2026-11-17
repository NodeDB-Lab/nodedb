// SPDX-License-Identifier: Apache-2.0

//! float8 `sqrt`, `exp`, logarithms and `power`, with PostgreSQL's domain
//! and range checks. NaN passes through.

use crate::expr::{EvalError, MathDomain, OutOfRange};

fn domain(fault: MathDomain) -> EvalError {
    EvalError::MathDomain(fault)
}

/// `sqrt(x)`. A negative `x` is [`MathDomain::SqrtOfNegative`].
pub(super) fn sqrt(x: f64) -> Result<f64, EvalError> {
    if x < 0.0 {
        return Err(domain(MathDomain::SqrtOfNegative));
    }
    Ok(x.sqrt())
}

/// `log(x)` through `f`. Zero and negative `x` are domain faults.
pub(super) fn log_with(x: f64, f: fn(f64) -> f64) -> Result<f64, EvalError> {
    if x == 0.0 {
        return Err(domain(MathDomain::LogOfZero));
    }
    if x < 0.0 {
        return Err(domain(MathDomain::LogOfNegative));
    }
    Ok(f(x))
}

/// `exp(x)`. A finite `x` whose result overflows or underflows to zero is
/// out of range; an infinite `x` gives infinity or zero.
pub(super) fn exp(x: f64) -> Result<f64, EvalError> {
    let result = x.exp();
    if x.is_finite() {
        if result.is_infinite() {
            return Err(EvalError::OutOfRange(OutOfRange::FloatOverflow));
        }
        if result == 0.0 {
            return Err(EvalError::OutOfRange(OutOfRange::FloatUnderflow));
        }
    }
    Ok(result)
}

/// `base ^ exponent`, as PostgreSQL's `dpow`.
pub(super) fn power(base: f64, exponent: f64) -> Result<f64, EvalError> {
    if base == 0.0 && exponent < 0.0 {
        return Err(domain(MathDomain::ZeroToNegativePower));
    }
    if base < 0.0 && exponent.floor() != exponent {
        return Err(domain(MathDomain::NegativeToFractionalPower));
    }
    let result = base.powf(exponent);
    let finite_args = base.is_finite() && exponent.is_finite();
    if result.is_infinite() && finite_args {
        return Err(EvalError::OutOfRange(OutOfRange::FloatOverflow));
    }
    if result == 0.0 && base != 0.0 && finite_args {
        return Err(EvalError::OutOfRange(OutOfRange::FloatUnderflow));
    }
    Ok(result)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn float_math_follows_postgres() {
        assert_eq!(sqrt(4.0), Ok(2.0));
        assert!(sqrt(f64::NAN).is_ok_and(f64::is_nan));
        assert_eq!(
            sqrt(-1.0).unwrap_err().to_string(),
            "cannot take square root of a negative number"
        );
        assert_eq!(log_with(100.0, f64::log10), Ok(2.0));
        assert_eq!(
            log_with(0.0, f64::ln).unwrap_err().to_string(),
            "cannot take logarithm of zero"
        );
        assert_eq!(exp(0.0), Ok(1.0));
        assert_eq!(exp(f64::NEG_INFINITY), Ok(0.0));
        assert_eq!(
            exp(1000.0).unwrap_err().to_string(),
            "value out of range: overflow"
        );
        assert_eq!(
            exp(-1000.0).unwrap_err().to_string(),
            "value out of range: underflow"
        );
        assert_eq!(power(2.0, 10.0), Ok(1024.0));
        assert_eq!(power(-8.0, 3.0), Ok(-512.0));
        assert_eq!(
            power(0.0, -1.0).unwrap_err().to_string(),
            "zero raised to a negative power is undefined"
        );
        assert_eq!(
            power(-8.0, 0.5).unwrap_err().to_string(),
            "a negative number raised to a non-integer power yields a complex result"
        );
        assert_eq!(
            power(10.0, 400.0).unwrap_err().to_string(),
            "value out of range: overflow"
        );
    }
}
