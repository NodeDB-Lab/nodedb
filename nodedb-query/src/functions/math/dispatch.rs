// SPDX-License-Identifier: Apache-2.0

//! Math scalar functions, typed as PostgreSQL types them.
//!
//! - `abs` keeps its argument's type: `bigint`, `numeric` or `float8`.
//! - `ceil`, `floor`, `sign` and `round` are exact `numeric` over a
//!   decimal, and `float8` over an integer or a float. `round(x, n)` over an
//!   integer is `numeric`, as `round(numeric, int)` is the one overload.
//! - `power`, `sqrt`, `ln`, `log`, `log10`, `log2` and `exp` are `numeric`
//!   at PostgreSQL's result scale when an argument is a decimal and none is
//!   a float, else `float8`. `log(b, x)` is `numeric` over integers too, as
//!   `log(numeric, numeric)` is its one overload. An argument outside the
//!   function's domain fails with PostgreSQL's message.

use rust_decimal::prelude::ToPrimitive;
use rust_decimal::{Decimal, RoundingStrategy};

use super::super::shared::num_arg;
use super::{float, numeric};
use crate::expr::{EvalError, OutOfRange};
use nodedb_types::Value;

/// Dispatch a math scalar function call.
///
/// Every function here returns `Ok(Value::Null)` on invalid/missing
/// arguments, matching the crate-wide NULL-propagation convention. `mod`
/// fails with `22012` for a zero modulus, `abs(i64::MIN)` with `22003`, and
/// the transcendental functions on an argument outside their domain.
pub(in crate::functions) fn try_eval(
    name: &str,
    args: &[Value],
) -> Option<Result<Value, EvalError>> {
    let v = match name {
        "abs" => match args.first() {
            Some(Value::Integer(i)) => {
                return Some(
                    i.checked_abs()
                        .map(Value::Integer)
                        .ok_or(EvalError::OutOfRange(OutOfRange::Bigint)),
                );
            }
            Some(Value::Decimal(d)) => Value::Decimal(d.abs()),
            _ => num_arg(args, 0).map_or(Value::Null, |n| Value::Float(n.abs())),
        },
        "round" => round(args),
        "ceil" | "ceiling" => exact_or_float(args, Decimal::ceil, f64::ceil),
        "floor" => exact_or_float(args, Decimal::floor, f64::floor),
        "sign" => exact_or_float(args, decimal_sign, float_sign),
        "power" | "pow" => {
            return Some(numeric_or_float(
                args,
                2,
                false,
                |d| numeric::power(d[0], d[1]),
                |f| float::power(f[0], f[1]),
            ));
        }
        "sqrt" => {
            return Some(numeric_or_float(
                args,
                1,
                false,
                |d| numeric::sqrt(d[0]),
                |f| float::sqrt(f[0]),
            ));
        }
        // `mod(a, b)` is `a % b`: integer for integers, numeric for numerics,
        // and SQLSTATE 22012 for a zero modulus.
        "mod" => {
            let (Some(a), Some(b)) = (args.first(), args.get(1)) else {
                return Some(Ok(Value::Null));
            };
            return Some(crate::expr::binary::eval_binary_op(
                a,
                crate::expr::BinaryOp::Mod,
                b,
            ));
        }
        "ln" => {
            return Some(numeric_or_float(
                args,
                1,
                false,
                |d| numeric::ln(d[0]),
                |f| float::log_with(f[0], f64::ln),
            ));
        }
        // `log(b, x)` is the base-`b` logarithm, `log(x)` the base-10 one.
        "log" if args.len() >= 2 => {
            return Some(numeric_or_float(
                args,
                2,
                true,
                |d| numeric::log(d[0], d[1]),
                |f| Ok(float::log_with(f[1], f64::ln)? / float::log_with(f[0], f64::ln)?),
            ));
        }
        "log" | "log10" => {
            return Some(numeric_or_float(
                args,
                1,
                false,
                |d| numeric::log(Decimal::TEN, d[0]),
                |f| float::log_with(f[0], f64::log10),
            ));
        }
        "log2" => {
            return Some(numeric_or_float(
                args,
                1,
                false,
                |d| numeric::log(Decimal::TWO, d[0]),
                |f| float::log_with(f[0], f64::log2),
            ));
        }
        "exp" => {
            return Some(numeric_or_float(
                args,
                1,
                false,
                |d| numeric::exp(d[0]),
                |f| float::exp(f[0]),
            ));
        }
        _ => return None,
    };
    Some(Ok(v))
}

/// The first `arity` arguments as decimals, when the call computes as
/// `numeric`: no argument is a float, and every one is a decimal or an
/// integer, at least one a decimal unless `integers_are_numeric`. `None`
/// otherwise.
fn decimal_args(args: &[Value], arity: usize, integers_are_numeric: bool) -> Option<Vec<Decimal>> {
    let picked = args.get(..arity)?;
    let any_decimal = picked.iter().any(|v| matches!(v, Value::Decimal(_)));
    if !any_decimal && !integers_are_numeric {
        return None;
    }
    picked
        .iter()
        .map(|v| match v {
            Value::Decimal(d) => Some(*d),
            Value::Integer(i) => Some(Decimal::from(*i)),
            _ => None,
        })
        .collect()
}

/// `numeric(args)` when the call computes as `numeric`, else
/// `float(args)` over the arguments read as floats. A missing or
/// non-numeric argument is `NULL`.
fn numeric_or_float(
    args: &[Value],
    arity: usize,
    integers_are_numeric: bool,
    numeric: impl Fn(&[Decimal]) -> Result<Decimal, EvalError>,
    float: impl Fn(&[f64]) -> Result<f64, EvalError>,
) -> Result<Value, EvalError> {
    if let Some(decimals) = decimal_args(args, arity, integers_are_numeric) {
        return numeric(&decimals).map(Value::Decimal);
    }
    let mut floats = Vec::with_capacity(arity);
    for i in 0..arity {
        match num_arg(args, i) {
            Some(f) => floats.push(f),
            None => return Ok(Value::Null),
        }
    }
    float(&floats).map(Value::Float)
}

/// `exact(d)` over a decimal argument, `float(n)` as `float8` over any other
/// numeric one.
fn exact_or_float(args: &[Value], exact: fn(&Decimal) -> Decimal, float: fn(f64) -> f64) -> Value {
    match args.first() {
        Some(Value::Decimal(d)) => Value::Decimal(exact(d)),
        _ => num_arg(args, 0).map_or(Value::Null, |n| Value::Float(float(n))),
    }
}

/// PostgreSQL's numeric `sign`: `-1`, `0` or `1`.
fn decimal_sign(d: &Decimal) -> Decimal {
    if d.is_zero() {
        Decimal::ZERO
    } else if d.is_sign_negative() {
        Decimal::NEGATIVE_ONE
    } else {
        Decimal::ONE
    }
}

/// PostgreSQL's float8 `sign`: `1` above zero, `-1` below, else `0`.
fn float_sign(n: f64) -> f64 {
    if n > 0.0 {
        1.0
    } else if n < 0.0 {
        -1.0
    } else {
        0.0
    }
}

/// `round(x [, n [, mode]])`. A decimal, or an integer with `n` given,
/// rounds exactly as `numeric`, a tie away from zero. A float, or an
/// integer alone, rounds as `float8`, a tie to even (`rint`). An explicit
/// `mode` names the tie rule.
fn round(args: &[Value]) -> Value {
    let places = num_arg(args, 1);
    let decimals = places.unwrap_or(0.0).max(0.0) as u32;
    let mode = super::super::shared::str_arg(args, 2).map(|m| m.to_uppercase());
    let strategy = |default| match mode.as_deref() {
        Some("HALF_UP") => RoundingStrategy::MidpointAwayFromZero,
        Some("HALF_DOWN") => RoundingStrategy::MidpointTowardZero,
        Some("TRUNCATE" | "TRUNC") => RoundingStrategy::ToZero,
        Some("CEILING" | "CEIL") => RoundingStrategy::AwayFromZero,
        Some("FLOOR") => RoundingStrategy::ToNegativeInfinity,
        Some("HALF_EVEN") => RoundingStrategy::MidpointNearestEven,
        _ => default,
    };
    let exact = match args.first() {
        Some(Value::Decimal(d)) => Some(*d),
        Some(Value::Integer(i)) if places.is_some() => Some(Decimal::from(*i)),
        _ => None,
    };
    if let Some(d) = exact {
        return Value::Decimal(
            d.round_dp_with_strategy(decimals, strategy(RoundingStrategy::MidpointAwayFromZero)),
        );
    }
    let Some(n) = num_arg(args, 0) else {
        return Value::Null;
    };
    if !n.is_finite() || (places.is_none() && mode.is_none()) {
        return Value::Float(n.round_ties_even());
    }
    // Places or a mode round the float through its decimal expansion.
    match Decimal::try_from(n).ok() {
        Some(d) => d
            .round_dp_with_strategy(decimals, strategy(RoundingStrategy::MidpointNearestEven))
            .to_f64()
            .map_or(Value::Null, Value::Float),
        None => Value::Float(n),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn call(name: &str, args: Vec<Value>) -> Value {
        try_eval(name, &args)
            .expect("math function")
            .expect("no fault")
    }

    fn dec(text: &str) -> Value {
        Value::Decimal(Decimal::from_str_exact(text).expect("decimal"))
    }

    /// A whole float8 result stays a float.
    #[test]
    fn float_results_stay_float() {
        assert_eq!(call("sqrt", vec![Value::Integer(4)]), Value::Float(2.0));
        assert_eq!(
            call("power", vec![Value::Integer(2), Value::Integer(3)]),
            Value::Float(8.0)
        );
        assert_eq!(call("ceil", vec![Value::Float(1.2)]), Value::Float(2.0));
        assert_eq!(call("floor", vec![Value::Integer(3)]), Value::Float(3.0));
        assert_eq!(call("sign", vec![Value::Float(-2.5)]), Value::Float(-1.0));
        assert_eq!(call("exp", vec![Value::Integer(0)]), Value::Float(1.0));
        assert_eq!(
            try_eval("sqrt", &[Value::Integer(-1)]).expect("sqrt"),
            Err(EvalError::MathDomain(
                crate::expr::MathDomain::SqrtOfNegative
            ))
        );
    }

    /// `abs` keeps its argument's type.
    #[test]
    fn abs_keeps_the_argument_type() {
        assert_eq!(call("abs", vec![Value::Integer(-5)]), Value::Integer(5));
        assert_eq!(call("abs", vec![dec("-1.50")]), dec("1.50"));
        assert_eq!(call("abs", vec![Value::Float(-2.0)]), Value::Float(2.0));
        assert_eq!(
            try_eval("abs", &[Value::Integer(i64::MIN)]).expect("abs"),
            Err(EvalError::OutOfRange(OutOfRange::Bigint))
        );
    }

    /// Over a numeric, ceil, floor, sign and round are exact numerics.
    #[test]
    fn numeric_math_is_exact() {
        assert_eq!(call("ceil", vec![dec("1.2")]), dec("2"));
        assert_eq!(call("floor", vec![dec("-1.2")]), dec("-2"));
        assert_eq!(call("sign", vec![dec("-0.5")]), dec("-1"));
        // round(numeric) ties away from zero; round(float8) ties to even.
        assert_eq!(call("round", vec![dec("2.5")]), dec("3"));
        assert_eq!(call("round", vec![Value::Float(2.5)]), Value::Float(2.0));
        let Value::Decimal(r) = call("round", vec![dec("1.005"), Value::Integer(2)]) else {
            panic!("round(numeric, int) is numeric");
        };
        assert_eq!(r.to_string(), "1.01");
        assert_eq!(
            call("round", vec![Value::Integer(7), Value::Integer(1)]),
            dec("7.0")
        );
    }

    /// `log(x)` is base 10, `log(b, x)` base `b` and numeric, `ln` base e.
    #[test]
    fn logarithms() {
        assert_eq!(call("log", vec![Value::Integer(100)]), Value::Float(2.0));
        assert_eq!(
            call("log", vec![Value::Integer(2), Value::Integer(8)]),
            dec("3.0000000000000000")
        );
        assert_eq!(call("ln", vec![Value::Integer(1)]), Value::Float(0.0));
    }

    /// A decimal argument makes the call numeric, at PostgreSQL's scale.
    #[test]
    fn numeric_arguments_compute_as_numeric() {
        let text = |name: &str, args: Vec<Value>| match call(name, args) {
            Value::Decimal(d) => d.to_string(),
            other => panic!("{name} over a numeric is numeric, got {other:?}"),
        };
        assert_eq!(text("sqrt", vec![dec("16.0")]), "4.000000000000000");
        assert_eq!(text("exp", vec![dec("1.0")]), "2.7182818284590452");
        assert_eq!(text("ln", vec![dec("2.0")]), "0.6931471805599453");
        assert_eq!(text("log", vec![dec("100")]), "2.0000000000000000");
        assert_eq!(text("log10", vec![dec("1000")]), "3.0000000000000000");
        assert_eq!(
            text("power", vec![dec("2"), Value::Integer(3)]),
            "8.0000000000000000"
        );
        // A float argument makes it float8.
        assert_eq!(
            call("power", vec![dec("2"), Value::Float(3.0)]),
            Value::Float(8.0)
        );
    }
}
