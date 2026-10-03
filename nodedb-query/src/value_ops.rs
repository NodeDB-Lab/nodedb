// SPDX-License-Identifier: Apache-2.0

//! Core value operations on `nodedb_types::Value`: comparison, coercion, truthiness.
//!
//! Equivalent to `json_ops.rs` but operates on the internal `Value` type directly,
//! avoiding the `serde_json::Value` intermediate. Used by expression evaluation,
//! computed projections, and aggregate expression paths.

use std::cmp::Ordering;

use nodedb_types::Value;

use crate::json_ops::{Numeric, cmp_numeric, parse_numeric_str};
use crate::numeric_sum::decimal_reading;

/// Coerce a Value to f64.
///
/// - Integer/Float: direct conversion
/// - String: parse as f64
/// - Bool: `true` → `1.0`, `false` → `0.0` (when `coerce_bool` is true)
/// - Other types: `None`
pub fn value_to_f64(v: &Value, coerce_bool: bool) -> Option<f64> {
    match v {
        Value::Integer(i) => Some(*i as f64),
        Value::Float(f) => Some(*f),
        Value::String(s) => s.parse::<f64>().ok(),
        Value::Bool(b) if coerce_bool => Some(if *b { 1.0 } else { 0.0 }),
        Value::Decimal(d) => {
            use rust_decimal::prelude::ToPrimitive;
            d.to_f64()
        }
        _ => None,
    }
}

/// Whether either side is a typed instant, so the pair compares by epoch
/// microseconds through `Value::partial_cmp_coerced` (an ISO string on the
/// other side is parsed).
pub fn involves_instant(a: &Value, b: &Value) -> bool {
    a.as_instant().is_some() || b.as_instant().is_some()
}

/// `v` read as a number: integers, floats, decimals, numeric strings, and
/// bools (`true` = 1). An integer string and an integral decimal stay exact.
fn numeric_reading(v: &Value) -> Option<Numeric> {
    match v {
        Value::Integer(i) => Some(Numeric::Int(i128::from(*i))),
        Value::Bool(b) => Some(Numeric::Int(i128::from(*b))),
        Value::String(s) => parse_numeric_str(s),
        Value::Float(f) => Some(Numeric::Float(*f)),
        Value::Decimal(d) => decimal_reading(d),
        _ => None,
    }
}

/// Numeric order of `a` and `b` when both have a numeric reading (number,
/// decimal, numeric string, bool). Outer `None`: at least one side is not
/// numeric. Inner `None`: the pair has no order (a NaN).
pub fn numeric_order(a: &Value, b: &Value) -> Option<Option<Ordering>> {
    let (na, nb) = (numeric_reading(a)?, numeric_reading(b)?);
    Some(cmp_numeric(na, nb))
}

/// Compare two Values with type coercion, for a predicate.
///
/// A typed instant compares by epoch microseconds against another instant
/// or an ISO-8601 string, and against anything else has no order: `None`,
/// so `WHERE at >= 5` over an instant matches nothing. Otherwise numeric
/// comparison first (with bool coercion; exact for integers; `None` for a
/// NaN), then string comparison of the display forms.
pub fn partial_compare_values(a: &Value, b: &Value) -> Option<Ordering> {
    if involves_instant(a, b) {
        return a.partial_cmp_coerced(b);
    }
    if let Some(order) = numeric_order(a, b) {
        return order;
    }
    let sa = value_to_display_string(a);
    let sb = value_to_display_string(b);
    Some(sa.cmp(&sb))
}

/// Compare two Values with type coercion, for sorting and extremum
/// selection (ORDER BY, MIN / MAX, GREATEST / LEAST, window frames).
///
/// [`partial_compare_values`] with an unordered pair placed as `Equal`, so a
/// stable sort keeps such rows in their input order. A predicate uses
/// `partial_compare_values` instead.
pub fn compare_values(a: &Value, b: &Value) -> Ordering {
    partial_compare_values(a, b).unwrap_or(Ordering::Equal)
}

/// Check equality with type coercion.
///
/// A typed instant equals another instant or an ISO-8601 string with the
/// same epoch microseconds. Handles `"5" == 5` by reading both sides as
/// numbers when each has a numeric reading. A pair with an integer side
/// compares exactly; a float pair is equal within `f64::EPSILON`.
pub fn coerced_eq(a: &Value, b: &Value) -> bool {
    if a == b {
        return true;
    }
    if involves_instant(a, b) {
        return a.eq_coerced(b);
    }
    match (numeric_reading(a), numeric_reading(b)) {
        (Some(Numeric::Float(af)), Some(Numeric::Float(bf))) => (af - bf).abs() < f64::EPSILON,
        (Some(na), Some(nb)) => cmp_numeric(na, nb) == Some(Ordering::Equal),
        _ => false,
    }
}

/// Check if a Value is truthy (for boolean contexts).
///
/// - `true` → true, `false` → false
/// - `Null` → false
/// - Numbers: non-zero → true
/// - Strings: non-empty → true
/// - Arrays/Objects: always true
pub fn is_truthy(v: &Value) -> bool {
    match v {
        Value::Bool(b) => *b,
        Value::Null => false,
        Value::Integer(i) => *i != 0,
        Value::Float(f) => *f != 0.0,
        Value::String(s) => !s.is_empty(),
        _ => true,
    }
}

/// Convert a Value to a display string.
///
/// - Strings: returned as-is (no quotes)
/// - Null: empty string
/// - Numbers/Bools: `.to_string()`
/// - Objects/Arrays: JSON serialization
pub fn value_to_display_string(v: &Value) -> String {
    match v {
        Value::String(s) => s.clone(),
        Value::Null => String::new(),
        Value::Integer(i) => i.to_string(),
        Value::Float(f) => f.to_string(),
        Value::Bool(b) => b.to_string(),
        Value::Uuid(s) | Value::Ulid(s) | Value::Regex(s) => s.clone(),
        Value::DateTime(dt) | Value::NaiveDateTime(dt) => dt.to_iso8601(),
        Value::Duration(d) => d.to_human(),
        Value::Decimal(d) => d.to_string(),
        other => {
            // Fallback: convert to JSON string representation.
            let json = serde_json::Value::from(other.clone());
            json.to_string()
        }
    }
}

/// Convert an f64 to a Value, preferring integer representation.
///
/// Returns `Null` for NaN/Infinity.
pub fn to_value_number(n: f64) -> Value {
    if n.is_nan() || n.is_infinite() {
        Value::Null
    } else if n.fract() == 0.0 && n.abs() < i64::MAX as f64 {
        Value::Integer(n as i64)
    } else {
        Value::Float(n)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn coerced_eq_mixed_types() {
        assert!(coerced_eq(&Value::Integer(5), &Value::String("5".into())));
        assert!(!coerced_eq(&Value::Integer(5), &Value::String("6".into())));
    }

    #[test]
    fn coerced_eq_bool_numeric() {
        assert!(coerced_eq(&Value::Bool(true), &Value::Integer(1)));
        assert!(coerced_eq(&Value::Bool(false), &Value::Integer(0)));
        assert!(!coerced_eq(&Value::Bool(true), &Value::Integer(0)));
    }

    #[test]
    fn compare_numeric_coercion() {
        assert_eq!(
            compare_values(&Value::Integer(5), &Value::String("4".into())),
            Ordering::Greater
        );
    }

    /// `2^53 + 1` and `2^53` collapse to one `f64`. Integers compare exactly.
    #[test]
    fn integers_past_two_pow_53_compare_exactly() {
        let above = Value::Integer(9_007_199_254_740_993);
        let at = Value::Integer(9_007_199_254_740_992);
        assert!(!coerced_eq(&above, &at));
        assert_eq!(partial_compare_values(&above, &at), Some(Ordering::Greater));
        assert_eq!(partial_compare_values(&at, &above), Some(Ordering::Less));
        // Nanosecond timestamps one tick apart.
        let t0 = Value::Integer(1_700_000_000_000_000_001);
        let t1 = Value::Integer(1_700_000_000_000_000_002);
        assert!(!coerced_eq(&t0, &t1));
        assert_eq!(partial_compare_values(&t0, &t1), Some(Ordering::Less));
        assert_eq!(
            partial_compare_values(&Value::Integer(i64::MAX), &Value::Integer(i64::MAX - 1)),
            Some(Ordering::Greater)
        );
    }

    #[test]
    fn integer_against_float_compares_without_rounding() {
        let above = Value::Integer(9_007_199_254_740_993);
        let float = Value::Float(9_007_199_254_740_992.0);
        assert!(!coerced_eq(&above, &float));
        assert_eq!(partial_compare_values(&above, &float), Some(Ordering::Greater));
        assert_eq!(partial_compare_values(&float, &above), Some(Ordering::Less));
        assert_eq!(
            partial_compare_values(&Value::Integer(2), &Value::Float(2.5)),
            Some(Ordering::Less)
        );
        assert_eq!(
            partial_compare_values(&Value::Integer(-2), &Value::Float(-2.5)),
            Some(Ordering::Greater)
        );
        assert!(coerced_eq(&Value::Integer(3), &Value::Float(3.0)));
        // `i64::MAX` rounds up to `2^63` as an `f64`, so it is below that float.
        const TWO_POW_63: f64 = 9_223_372_036_854_775_808.0;
        assert_eq!(
            partial_compare_values(&Value::Integer(i64::MAX), &Value::Float(TWO_POW_63)),
            Some(Ordering::Less)
        );
        assert_eq!(
            partial_compare_values(&Value::Integer(i64::MIN), &Value::Float(-TWO_POW_63)),
            Some(Ordering::Equal)
        );
        assert_eq!(
            partial_compare_values(&Value::Integer(i64::MIN), &Value::Float(f64::NEG_INFINITY)),
            Some(Ordering::Greater)
        );
        assert_eq!(
            partial_compare_values(&Value::Integer(0), &Value::Float(f64::NAN)),
            None
        );
    }

    /// A `u64` above `i64::MAX` is an integral `Decimal`, which compares
    /// exactly against integers instead of rounding through `f64`.
    #[test]
    fn integral_decimals_compare_exactly() {
        let u_max = Value::from_u64(u64::MAX);
        let u_max_less_one = Value::from_u64(u64::MAX - 1);
        assert_eq!(
            partial_compare_values(&u_max_less_one, &u_max),
            Some(Ordering::Less)
        );
        assert!(!coerced_eq(&u_max_less_one, &u_max));
        assert_eq!(
            partial_compare_values(&u_max, &Value::Integer(i64::MAX)),
            Some(Ordering::Greater)
        );
        let two_pow_63 = Value::from_u64(1 << 63);
        assert_eq!(
            partial_compare_values(&two_pow_63, &Value::Integer(i64::MAX)),
            Some(Ordering::Greater)
        );
        assert_eq!(
            partial_compare_values(
                &Value::Decimal(rust_decimal::Decimal::new(15, 1)),
                &Value::Integer(1)
            ),
            Some(Ordering::Greater)
        );
    }

    #[test]
    fn integer_strings_compare_exactly_against_integers() {
        let text = Value::String("9007199254740993".into());
        assert!(!coerced_eq(&text, &Value::Integer(9_007_199_254_740_992)));
        assert!(coerced_eq(&text, &Value::Integer(9_007_199_254_740_993)));
        assert!(coerced_eq(&Value::String("5.0".into()), &Value::Integer(5)));
    }

    #[test]
    fn instants_compare_by_micros_against_instants_and_iso_strings() {
        let earlier = Value::NaiveDateTime(nodedb_types::NdbDateTime::from_micros(
            1_583_402_400_000_000,
        ));
        let later = Value::DateTime(nodedb_types::NdbDateTime::from_micros(
            1_583_406_000_000_000,
        ));
        assert_eq!(compare_values(&earlier, &later), Ordering::Less);
        assert_eq!(
            compare_values(&later, &Value::String("2020-03-05 10:00:00".into())),
            Ordering::Greater
        );
        assert!(coerced_eq(
            &earlier,
            &Value::String("2020-03-05T10:00:00Z".into())
        ));
        assert!(!coerced_eq(&earlier, &later));
    }

    /// An instant against a bare integer has no order on the predicate path
    /// and sorts as equal on the extremum path.
    #[test]
    fn an_instant_against_an_integer_is_unordered_for_predicates() {
        let at = Value::NaiveDateTime(nodedb_types::NdbDateTime::from_micros(
            1_583_402_400_000_000,
        ));
        assert_eq!(partial_compare_values(&at, &Value::Integer(5)), None);
        assert_eq!(partial_compare_values(&Value::Integer(5), &at), None);
        assert_eq!(compare_values(&at, &Value::Integer(5)), Ordering::Equal);
    }

    #[test]
    fn truthiness() {
        assert!(is_truthy(&Value::Bool(true)));
        assert!(!is_truthy(&Value::Bool(false)));
        assert!(!is_truthy(&Value::Null));
        assert!(is_truthy(&Value::Integer(1)));
        assert!(!is_truthy(&Value::Integer(0)));
        assert!(is_truthy(&Value::String("hello".into())));
        assert!(!is_truthy(&Value::String(String::new())));
    }

    #[test]
    fn to_value_number_nan() {
        assert_eq!(to_value_number(f64::NAN), Value::Null);
    }

    #[test]
    fn to_value_number_integer() {
        assert_eq!(to_value_number(42.0), Value::Integer(42));
    }

    #[test]
    fn to_value_number_float() {
        assert_eq!(to_value_number(3.15), Value::Float(3.15));
    }
}
