// SPDX-License-Identifier: Apache-2.0

//! Canonical key values: one representative per exact value.
//!
//! GROUP BY identity, equi-join keys and partition routing compare numbers by
//! exact value, as `cmp_numeric_total` orders them `Equal`. Each number maps
//! to one representative before it is encoded:
//!
//! - An integral value below `2^96` in magnitude reads as `Integer` when it
//!   fits an `i64`, else as a scale-zero `Decimal`. `Float(5.0)`,
//!   `Decimal(5.00)` and `Integer(5)` share `Integer(5)`, and `-0.0` reads
//!   as `Integer(0)`.
//! - A non-integral value that is exactly an `f64` reads as that `Float`, so
//!   `Decimal(1.5)` reads as `Float(1.5)`. Any other non-integral decimal
//!   reads as its normalized `Decimal`: `Decimal(0.1)` stays apart from
//!   `Float(0.1)`, whose exact value differs.
//! - Every NaN reads as one NaN, which equals NaN as in the total order. An
//!   infinity and an integral float past `2^96` stay `Float`.
//!
//! Arrays, sets and object values canonicalize element by element.

use std::cmp::Ordering;

use nodedb_types::Value;
use nodedb_types::decimal_float::{TWO_POW_96, cmp_decimal_float_exact, decimal_to_f64};
use rust_decimal::Decimal;

/// The canonical representative of `value`. Non-numeric scalars are kept.
pub fn canonical_key_value(value: Value) -> Value {
    match value {
        Value::Float(f) => canonical_float(f),
        Value::Decimal(d) => canonical_decimal(d),
        Value::Array(items) => Value::Array(items.into_iter().map(canonical_key_value).collect()),
        Value::Set(items) => Value::Set(items.into_iter().map(canonical_key_value).collect()),
        Value::Object(fields) => Value::Object(
            fields
                .into_iter()
                .map(|(key, value)| (key, canonical_key_value(value)))
                .collect(),
        ),
        other => other,
    }
}

/// An integral value below `2^96` in magnitude: `Integer` when it fits an
/// `i64`, else a scale-zero `Decimal`. `None` past the `Decimal` range.
fn canonical_integer(value: i128) -> Option<Value> {
    match i64::try_from(value) {
        Ok(value) => Some(Value::Integer(value)),
        Err(_) => Decimal::try_from_i128_with_scale(value, 0)
            .ok()
            .map(Value::Decimal),
    }
}

fn canonical_float(f: f64) -> Value {
    if f.is_nan() {
        return Value::Float(f64::NAN);
    }
    if f.is_finite() && f.fract() == 0.0 && f.abs() < TWO_POW_96 {
        // `|f| < 2^96`, so the cast is exact.
        return canonical_integer(f as i128).unwrap_or(Value::Float(f));
    }
    Value::Float(f)
}

fn canonical_decimal(d: Decimal) -> Value {
    let reduced = d.normalize();
    if reduced.scale() == 0 {
        return canonical_integer(reduced.mantissa()).unwrap_or(Value::Decimal(reduced));
    }
    let nearest = decimal_to_f64(reduced);
    if cmp_decimal_float_exact(reduced, nearest) == Ordering::Equal {
        Value::Float(nearest)
    } else {
        Value::Decimal(reduced)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn dec(text: &str) -> Value {
        Value::Decimal(Decimal::from_str_exact(text).unwrap())
    }

    #[test]
    fn equal_numbers_share_one_representative() {
        for value in [Value::Integer(5), Value::Float(5.0), dec("5"), dec("5.00")] {
            assert_eq!(canonical_key_value(value), Value::Integer(5));
        }
        assert_eq!(canonical_key_value(Value::Float(-0.0)), Value::Integer(0));
        assert_eq!(canonical_key_value(dec("1.50")), Value::Float(1.5));
        assert_eq!(canonical_key_value(Value::Float(1.5)), Value::Float(1.5));
        let big = 2f64.powi(70);
        assert_eq!(
            canonical_key_value(Value::Float(big)),
            canonical_key_value(dec("1180591620717411303424"))
        );
    }

    #[test]
    fn distinct_exact_values_stay_apart() {
        assert_eq!(canonical_key_value(dec("0.1")), dec("0.1"));
        assert_eq!(canonical_key_value(Value::Float(0.1)), Value::Float(0.1));
        assert_eq!(canonical_key_value(dec("0.10")), dec("0.1"));
    }

    #[test]
    fn nan_is_one_value_and_nesting_canonicalizes() {
        let other_nan = f64::from_bits(f64::NAN.to_bits() | 1);
        assert_eq!(
            canonical_key_value(Value::Float(other_nan))
                .as_f64()
                .map(f64::to_bits),
            Some(f64::NAN.to_bits())
        );
        assert_eq!(
            canonical_key_value(Value::Array(vec![Value::Float(2.0), dec("3.0")])),
            Value::Array(vec![Value::Integer(2), Value::Integer(3)])
        );
    }
}
