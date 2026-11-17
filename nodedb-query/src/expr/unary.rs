// SPDX-License-Identifier: Apache-2.0

//! Unary-minus evaluation on a `Value` operand.

use nodedb_types::Value;

use crate::value_ops::value_to_f64;

use super::eval::EvalError;
use super::out_of_range::OutOfRange;

/// `-v`. A boolean negates to its complement. An integer negates exactly,
/// and `-i64::MIN` is [`OutOfRange::Bigint`]. A decimal negates exactly. A
/// float negates as `f64` and stays a float. Any other value is `NULL`.
pub fn eval_negate(v: Value) -> Result<Value, EvalError> {
    match v {
        Value::Bool(b) => Ok(Value::Bool(!b)),
        Value::Integer(i) => i
            .checked_neg()
            .map(Value::Integer)
            .ok_or(EvalError::OutOfRange(OutOfRange::Bigint)),
        Value::Decimal(d) => {
            let mut negated = -d;
            // Numeric zero carries no sign.
            if negated.is_zero() {
                negated.set_sign_positive(true);
            }
            Ok(Value::Decimal(negated))
        }
        other => Ok(match value_to_f64(&other, false) {
            Some(n) => Value::Float(-n),
            None => Value::Null,
        }),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rust_decimal::Decimal;

    #[test]
    fn integer_negation_is_exact_and_refuses_the_minimum() {
        assert_eq!(
            eval_negate(Value::Integer(9_007_199_254_740_993)),
            Ok(Value::Integer(-9_007_199_254_740_993))
        );
        assert_eq!(
            eval_negate(Value::Integer(i64::MAX)),
            Ok(Value::Integer(-i64::MAX))
        );
        assert_eq!(
            eval_negate(Value::Integer(i64::MIN)),
            Err(EvalError::OutOfRange(OutOfRange::Bigint))
        );
    }

    #[test]
    fn decimal_negation_keeps_the_scale() {
        let Ok(Value::Decimal(d)) = eval_negate(Value::Decimal(Decimal::new(1250, 2))) else {
            panic!("a negated numeric is a Decimal");
        };
        assert_eq!(d.to_string(), "-12.50");
        let Ok(Value::Decimal(zero)) = eval_negate(Value::Decimal(Decimal::new(0, 1))) else {
            panic!("a negated numeric is a Decimal");
        };
        assert_eq!(zero.to_string(), "0.0");
    }

    #[test]
    fn float_and_bool_negation() {
        assert_eq!(eval_negate(Value::Float(1.5)), Ok(Value::Float(-1.5)));
        assert_eq!(
            eval_negate(Value::Float(f64::INFINITY)),
            Ok(Value::Float(f64::NEG_INFINITY))
        );
        assert_eq!(eval_negate(Value::Bool(true)), Ok(Value::Bool(false)));
        assert_eq!(eval_negate(Value::Null), Ok(Value::Null));
    }
}
