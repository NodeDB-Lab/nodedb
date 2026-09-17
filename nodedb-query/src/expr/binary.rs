// SPDX-License-Identifier: Apache-2.0

//! Binary-operator evaluation on `Value` operands.

use rust_decimal::Decimal;

use nodedb_types::Value;

use crate::value_ops::{
    coerced_eq, is_truthy, partial_compare_values, to_value_number, value_to_display_string,
    value_to_f64,
};

use super::eval::EvalError;
use super::types::BinaryOp;

/// Coerce a `Value` to `Decimal` for precise arithmetic.
///
/// - `Decimal`: identity
/// - `Integer`: exact conversion via `Decimal::from`
/// - `Float`: best-effort via `Decimal::try_from` (preserves fractional digits
///   rather than compounding f64 ULP error through further f64 arithmetic)
/// - Other types: `None`
fn value_to_decimal(v: &Value) -> Option<Decimal> {
    match v {
        Value::Decimal(d) => Some(*d),
        Value::Integer(i) => Some(Decimal::from(*i)),
        Value::Float(f) => Decimal::try_from(*f).ok(),
        _ => None,
    }
}

/// Apply a Decimal arithmetic operation, returning `Value::Null` on overflow
/// and `Err(EvalError::DivisionByZero)` when `/` or `%` see a zero divisor.
/// The zero check happens before `checked_div`/
/// `checked_rem` are called because those methods return `None` for both
/// div-by-zero *and* overflow, and only the former is a `22012` error —
/// overflow keeps folding to `Value::Null` exactly as before.
fn decimal_arith(a: Decimal, op: BinaryOp, b: Decimal) -> Result<Value, EvalError> {
    let result = match op {
        BinaryOp::Add => a.checked_add(b),
        BinaryOp::Sub => a.checked_sub(b),
        BinaryOp::Mul => a.checked_mul(b),
        BinaryOp::Div => {
            if b.is_zero() {
                return Err(EvalError::DivisionByZero);
            }
            a.checked_div(b)
        }
        BinaryOp::Mod => {
            if b.is_zero() {
                return Err(EvalError::DivisionByZero);
            }
            a.checked_rem(b)
        }
        _ => return Ok(Value::Null),
    };
    Ok(result.map(Value::Decimal).unwrap_or(Value::Null))
}

pub(super) fn eval_binary_op(
    left: &Value,
    op: BinaryOp,
    right: &Value,
) -> Result<Value, EvalError> {
    match op {
        // Arithmetic: prefer Decimal when either operand is Decimal to avoid f64 drift.
        BinaryOp::Add | BinaryOp::Sub | BinaryOp::Mul | BinaryOp::Div | BinaryOp::Mod => {
            let left_is_decimal = matches!(left, Value::Decimal(_));
            let right_is_decimal = matches!(right, Value::Decimal(_));
            if left_is_decimal || right_is_decimal {
                return match (value_to_decimal(left), value_to_decimal(right)) {
                    (Some(a), Some(b)) => decimal_arith(a, op, b),
                    _ => Ok(Value::Null),
                };
            }
            // Both operands are non-Decimal: use f64 path.
            match op {
                BinaryOp::Add => Ok(
                    match (value_to_f64(left, true), value_to_f64(right, true)) {
                        (Some(a), Some(b)) => to_value_number(a + b),
                        _ => Value::Null,
                    },
                ),
                BinaryOp::Sub => Ok(
                    match (value_to_f64(left, true), value_to_f64(right, true)) {
                        (Some(a), Some(b)) => to_value_number(a - b),
                        _ => Value::Null,
                    },
                ),
                BinaryOp::Mul => Ok(
                    match (value_to_f64(left, true), value_to_f64(right, true)) {
                        (Some(a), Some(b)) => to_value_number(a * b),
                        _ => Value::Null,
                    },
                ),
                BinaryOp::Div => match (value_to_f64(left, true), value_to_f64(right, true)) {
                    (Some(a), Some(b)) => {
                        if b == 0.0 {
                            Err(EvalError::DivisionByZero)
                        } else {
                            Ok(to_value_number(a / b))
                        }
                    }
                    _ => Ok(Value::Null),
                },
                BinaryOp::Mod => match (value_to_f64(left, true), value_to_f64(right, true)) {
                    (Some(a), Some(b)) => {
                        if b == 0.0 {
                            Err(EvalError::DivisionByZero)
                        } else {
                            Ok(to_value_number(a % b))
                        }
                    }
                    _ => Ok(Value::Null),
                },
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
