// SPDX-License-Identifier: Apache-2.0

//! Binary-operator evaluation on `Value` operands.

use rust_decimal::Decimal;

use nodedb_types::Value;

use crate::value_ops::{
    coerced_eq, is_truthy, partial_compare_values, value_to_display_string, value_to_f64,
};

use super::eval::EvalError;
use super::out_of_range::OutOfRange;
use super::types::BinaryOp;

/// Coerce a `Value` to `Decimal` for exact numeric arithmetic: a `Decimal`
/// as is, an `Integer` exactly. Any other type is `None`. A `Float` operand
/// never reaches here: it makes the arithmetic float8.
fn value_to_decimal(v: &Value) -> Option<Decimal> {
    match v {
        Value::Decimal(d) => Some(*d),
        Value::Integer(i) => Some(Decimal::from(*i)),
        _ => None,
    }
}

/// Exact numeric `a op b`. A zero divisor for `/` or `%` is `22012`. A
/// result past the `Decimal` range is [`OutOfRange::Numeric`].
fn decimal_arith(a: Decimal, op: BinaryOp, b: Decimal) -> Result<Value, EvalError> {
    if matches!(op, BinaryOp::Div | BinaryOp::Mod) && b.is_zero() {
        return Err(EvalError::DivisionByZero);
    }
    let result = match op {
        BinaryOp::Add => a.checked_add(b),
        BinaryOp::Sub => a.checked_sub(b),
        BinaryOp::Mul => a.checked_mul(b),
        // PostgreSQL's numeric result scale.
        BinaryOp::Div => crate::numeric_div::numeric_div(a, b),
        // PostgreSQL's numeric mod: dividend sign, larger operand scale.
        BinaryOp::Mod => crate::numeric_div::numeric_rem(a, b),
        _ => return Ok(Value::Null),
    };
    result
        .map(Value::Decimal)
        .ok_or(EvalError::OutOfRange(OutOfRange::Numeric))
}

/// `bigint` `a op b` in checked `i64` arithmetic. `/` and `%` truncate
/// toward zero, and a zero divisor is `22012`. A result past the `i64` range,
/// `i64::MIN / -1` included, is [`OutOfRange::Bigint`]. `i64::MIN % -1` is
/// `0`, as in PostgreSQL.
fn integer_arith(a: i64, op: BinaryOp, b: i64) -> Result<Value, EvalError> {
    if matches!(op, BinaryOp::Div | BinaryOp::Mod) && b == 0 {
        return Err(EvalError::DivisionByZero);
    }
    let result = match op {
        BinaryOp::Add => a.checked_add(b),
        BinaryOp::Sub => a.checked_sub(b),
        BinaryOp::Mul => a.checked_mul(b),
        BinaryOp::Div => a.checked_div(b),
        // The only `checked_rem` overflow is `i64::MIN % -1`, whose
        // remainder is 0.
        BinaryOp::Mod => Some(a.checked_rem(b).unwrap_or(0)),
        _ => return Ok(Value::Null),
    };
    result
        .map(Value::Integer)
        .ok_or(EvalError::OutOfRange(OutOfRange::Bigint))
}

/// `f64` `a op b` with PostgreSQL's float8 range checks: an infinite result
/// from finite operands is [`OutOfRange::FloatOverflow`], and a zero product
/// or quotient of nonzero operands is [`OutOfRange::FloatUnderflow`].
fn float_arith(a: f64, op: BinaryOp, b: f64) -> Result<Value, EvalError> {
    if matches!(op, BinaryOp::Div | BinaryOp::Mod) && b == 0.0 {
        return Err(EvalError::DivisionByZero);
    }
    let result = match op {
        BinaryOp::Add => a + b,
        BinaryOp::Sub => a - b,
        BinaryOp::Mul => a * b,
        BinaryOp::Div => a / b,
        BinaryOp::Mod => a % b,
        _ => return Ok(Value::Null),
    };
    let overflowed = result.is_infinite() && a.is_finite() && b.is_finite();
    if overflowed && !matches!(op, BinaryOp::Mod) {
        return Err(EvalError::OutOfRange(OutOfRange::FloatOverflow));
    }
    let underflowed = result == 0.0
        && a != 0.0
        && match op {
            BinaryOp::Mul => b != 0.0,
            BinaryOp::Div => !b.is_infinite(),
            _ => false,
        };
    if underflowed {
        return Err(EvalError::OutOfRange(OutOfRange::FloatUnderflow));
    }
    Ok(Value::Float(result))
}

pub(crate) fn eval_binary_op(
    left: &Value,
    op: BinaryOp,
    right: &Value,
) -> Result<Value, EvalError> {
    match op {
        // Arithmetic: a Decimal operand keeps exact numeric arithmetic, and a
        // Float operand makes it float8, as PostgreSQL casts numeric to
        // float8 for a mixed pair. Two integers are checked `i64`
        // arithmetic. The planner casts a `smallint` or `integer` result to
        // its width, which checks it there.
        BinaryOp::Add | BinaryOp::Sub | BinaryOp::Mul | BinaryOp::Div | BinaryOp::Mod => {
            let any_decimal =
                matches!(left, Value::Decimal(_)) || matches!(right, Value::Decimal(_));
            let any_float = matches!(left, Value::Float(_)) || matches!(right, Value::Float(_));
            if any_decimal && !any_float {
                return match (value_to_decimal(left), value_to_decimal(right)) {
                    (Some(a), Some(b)) => decimal_arith(a, op, b),
                    _ => Ok(Value::Null),
                };
            }
            if let (Value::Integer(a), Value::Integer(b)) = (left, right) {
                return integer_arith(*a, op, *b);
            }
            match (value_to_f64(left, true), value_to_f64(right, true)) {
                (Some(a), Some(b)) => float_arith(a, op, b),
                _ => Ok(Value::Null),
            }
        }
        BinaryOp::Concat => {
            let ls = value_to_display_string(left);
            let rs = value_to_display_string(right);
            Ok(Value::String(format!("{ls}{rs}")))
        }
        BinaryOp::Eq => Ok(Value::Bool(coerced_eq(left, right))),
        BinaryOp::NotEq => Ok(Value::Bool(!coerced_eq(left, right))),
        BinaryOp::Gt => Ok(Value::Bool(matches!(
            partial_compare_values(left, right),
            Some(std::cmp::Ordering::Greater)
        ))),
        BinaryOp::GtEq => Ok(Value::Bool(matches!(
            partial_compare_values(left, right),
            Some(std::cmp::Ordering::Greater | std::cmp::Ordering::Equal)
        ))),
        BinaryOp::Lt => Ok(Value::Bool(matches!(
            partial_compare_values(left, right),
            Some(std::cmp::Ordering::Less)
        ))),
        BinaryOp::LtEq => Ok(Value::Bool(matches!(
            partial_compare_values(left, right),
            Some(std::cmp::Ordering::Less | std::cmp::Ordering::Equal)
        ))),
        BinaryOp::And => Ok(Value::Bool(is_truthy(left) && is_truthy(right))),
        BinaryOp::Or => Ok(Value::Bool(is_truthy(left) || is_truthy(right))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn dec(text: &str) -> Value {
        Value::Decimal(Decimal::from_str_exact(text).unwrap())
    }

    fn divide(left: Value, right: Value) -> Value {
        eval_binary_op(&left, BinaryOp::Div, &right).unwrap()
    }

    /// Numeric division takes PostgreSQL's result scale.
    #[test]
    fn decimal_division_takes_the_postgres_scale() {
        assert_eq!(
            divide(Value::Integer(2), dec("2.0")),
            dec("1.00000000000000000000")
        );
        assert_eq!(
            divide(Value::Integer(1), dec("3.0")),
            dec("0.33333333333333333333")
        );
        assert_eq!(
            divide(dec("10.5"), Value::Integer(2)),
            dec("5.2500000000000000")
        );
        let Value::Decimal(one) = divide(Value::Integer(2), dec("2.0")) else {
            panic!("a numeric quotient is a Decimal");
        };
        assert_eq!(one.to_string(), "1.00000000000000000000");
    }

    /// A float against a numeric is float8 arithmetic, in either order, as
    /// PostgreSQL casts the numeric to float8.
    #[test]
    fn float_with_decimal_is_float_arithmetic() {
        let ops = [
            (BinaryOp::Add, "0.25", 2.75),
            (BinaryOp::Sub, "0.25", 2.25),
            (BinaryOp::Mul, "0.25", 0.625),
            (BinaryOp::Div, "2", 1.25),
        ];
        for (op, right, want) in ops {
            assert_eq!(
                eval_binary_op(&Value::Float(2.5), op, &dec(right)).unwrap(),
                Value::Float(want),
                "{op:?}"
            );
        }
        assert_eq!(
            eval_binary_op(&dec("0.25"), BinaryOp::Sub, &Value::Float(2.5)).unwrap(),
            Value::Float(-2.25)
        );
        assert_eq!(
            eval_binary_op(&dec("5.5"), BinaryOp::Mod, &Value::Float(2.0)).unwrap(),
            Value::Float(1.5)
        );
        // A whole float8 result stays a float, as PostgreSQL's
        // `2.5::float8 * 2` is float8 5.
        assert_eq!(
            eval_binary_op(&dec("2.5"), BinaryOp::Mul, &Value::Float(2.0)).unwrap(),
            Value::Float(5.0)
        );
        assert_eq!(
            eval_binary_op(&Value::Float(2.5), BinaryOp::Mul, &Value::Integer(2)).unwrap(),
            Value::Float(5.0)
        );
    }

    /// Numeric modulo keeps the dividend's sign at the larger scale.
    #[test]
    fn decimal_modulo_takes_the_postgres_scale() {
        let Value::Decimal(r) =
            eval_binary_op(&Value::Integer(7), BinaryOp::Mod, &dec("2.00")).unwrap()
        else {
            panic!("a numeric remainder is a Decimal");
        };
        assert_eq!(r.to_string(), "1.00");
        let Value::Decimal(r) =
            eval_binary_op(&dec("-5.5"), BinaryOp::Mod, &Value::Integer(2)).unwrap()
        else {
            panic!("a numeric remainder is a Decimal");
        };
        assert_eq!(r.to_string(), "-1.5");
    }

    /// Integer division and modulo stay integer and truncate toward zero.
    #[test]
    fn integer_division_stays_integer() {
        assert_eq!(
            divide(Value::Integer(1), Value::Integer(3)),
            Value::Integer(0)
        );
        assert_eq!(
            divide(Value::Integer(-7), Value::Integer(2)),
            Value::Integer(-3)
        );
        assert_eq!(
            eval_binary_op(&Value::Integer(-7), BinaryOp::Mod, &Value::Integer(2)).unwrap(),
            Value::Integer(-1)
        );
        assert_eq!(
            eval_binary_op(
                &Value::Integer(i64::MIN),
                BinaryOp::Div,
                &Value::Integer(-1)
            ),
            Err(EvalError::OutOfRange(OutOfRange::Bigint))
        );
        assert_eq!(
            eval_binary_op(
                &Value::Integer(i64::MIN),
                BinaryOp::Mod,
                &Value::Integer(-1)
            ),
            Ok(Value::Integer(0))
        );
        assert!(matches!(
            eval_binary_op(&Value::Integer(1), BinaryOp::Div, &Value::Integer(0)),
            Err(EvalError::DivisionByZero)
        ));
    }

    /// `bigint` `+`, `-` and `*` are exact `i64` arithmetic. A result past
    /// the range is `22003` "bigint out of range".
    #[test]
    fn integer_arithmetic_is_checked() {
        let big = 9_007_199_254_740_993;
        assert_eq!(
            eval_binary_op(&Value::Integer(big), BinaryOp::Add, &Value::Integer(2)),
            Ok(Value::Integer(9_007_199_254_740_995))
        );
        assert_eq!(
            eval_binary_op(&Value::Integer(big), BinaryOp::Mul, &Value::Integer(1)),
            Ok(Value::Integer(big))
        );
        for (a, op, b) in [
            (i64::MAX, BinaryOp::Add, 1),
            (i64::MIN, BinaryOp::Sub, 1),
            (i64::MAX, BinaryOp::Mul, 2),
            (i64::MIN, BinaryOp::Mul, -1),
        ] {
            let err = eval_binary_op(&Value::Integer(a), op, &Value::Integer(b)).unwrap_err();
            assert_eq!(err, EvalError::OutOfRange(OutOfRange::Bigint), "{op:?}");
            assert_eq!(err.to_string(), "bigint out of range");
        }
    }

    /// A numeric result past the `Decimal` range is `22003`.
    #[test]
    fn decimal_overflow_is_out_of_range() {
        let err =
            eval_binary_op(&Value::Decimal(Decimal::MAX), BinaryOp::Add, &dec("1")).unwrap_err();
        assert_eq!(err, EvalError::OutOfRange(OutOfRange::Numeric));
        assert_eq!(err.to_string(), "value overflows numeric format");
    }

    /// float8 overflow and underflow are `22003`. A non-finite operand
    /// gives a non-finite result.
    #[test]
    fn float_range_checks_follow_postgres() {
        assert_eq!(
            eval_binary_op(&Value::Float(1e308), BinaryOp::Mul, &Value::Float(10.0)),
            Err(EvalError::OutOfRange(OutOfRange::FloatOverflow))
        );
        assert_eq!(
            eval_binary_op(&Value::Float(1e-308), BinaryOp::Mul, &Value::Float(1e-308)),
            Err(EvalError::OutOfRange(OutOfRange::FloatUnderflow))
        );
        assert_eq!(
            eval_binary_op(
                &Value::Float(f64::INFINITY),
                BinaryOp::Add,
                &Value::Float(1.5)
            ),
            Ok(Value::Float(f64::INFINITY))
        );
        assert!(matches!(
            eval_binary_op(
                &Value::Float(f64::INFINITY),
                BinaryOp::Sub,
                &Value::Float(f64::INFINITY)
            ),
            Ok(Value::Float(f)) if f.is_nan()
        ));
    }
}
