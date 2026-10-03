// SPDX-License-Identifier: Apache-2.0

//! Shared JSON value operations: comparison, coercion, truthiness.
//!
//! Used by both expression evaluation (computed projections) and scan filters
//! (WHERE predicate evaluation). Pure functions on `serde_json::Value`.

use std::cmp::Ordering;

/// Coerce a JSON value to f64.
///
/// - Numbers: `as_f64()` directly
/// - Strings: parse as f64 (`"5"` → `5.0`)
/// - Booleans: `true` → `1.0`, `false` → `0.0` (when `coerce_bool` is true)
/// - Other types: `None`
pub fn json_to_f64(v: &serde_json::Value, coerce_bool: bool) -> Option<f64> {
    match v {
        serde_json::Value::Number(n) => n.as_f64(),
        serde_json::Value::String(s) => s.parse::<f64>().ok(),
        serde_json::Value::Bool(b) if coerce_bool => Some(if *b { 1.0 } else { 0.0 }),
        _ => None,
    }
}

/// The numeric reading of a value for comparison, shared by the JSON and
/// `nodedb_types::Value` paths. An integer stays exact: `i128` holds every
/// `i64` and every `u64`.
#[derive(Clone, Copy)]
pub(crate) enum Numeric {
    Int(i128),
    Float(f64),
}

/// A numeric string read as a number. An integer string stays exact.
pub(crate) fn parse_numeric_str(s: &str) -> Option<Numeric> {
    match s.parse::<i128>() {
        Ok(i) => Some(Numeric::Int(i)),
        Err(_) => s.parse::<f64>().ok().map(Numeric::Float),
    }
}

/// A JSON number read without rounding an integer through `f64`.
fn number_reading(n: &serde_json::Number) -> Option<Numeric> {
    if let Some(i) = n.as_i64() {
        return Some(Numeric::Int(i128::from(i)));
    }
    if let Some(u) = n.as_u64() {
        return Some(Numeric::Int(i128::from(u)));
    }
    n.as_f64().map(Numeric::Float)
}

/// `v` read as a number: numbers, numeric strings, and bools (`true` = 1).
/// A string that parses as an integer stays exact.
fn numeric_reading(v: &serde_json::Value) -> Option<Numeric> {
    match v {
        serde_json::Value::Number(n) => number_reading(n),
        serde_json::Value::Bool(b) => Some(Numeric::Int(i128::from(*b))),
        serde_json::Value::String(s) => parse_numeric_str(s),
        _ => None,
    }
}

/// `2^127` as `f64`: the first float above every `i128`.
const TWO_POW_127: f64 = 170_141_183_460_469_231_731_687_303_715_884_105_728.0;

/// Exact order of an `i128` against an `f64`, with no rounding of either.
/// `None` for a NaN.
fn cmp_int_float(i: i128, f: f64) -> Option<Ordering> {
    if f.is_nan() {
        return None;
    }
    if f >= TWO_POW_127 {
        return Some(Ordering::Less);
    }
    if f < -TWO_POW_127 {
        return Some(Ordering::Greater);
    }
    // `f` lies in `[-2^127, 2^127)`, so its integral part is an exact `i128`.
    let whole = f.trunc();
    let by_whole = i.cmp(&(whole as i128));
    if by_whole != Ordering::Equal {
        return Some(by_whole);
    }
    let fraction = f - whole;
    Some(if fraction > 0.0 {
        Ordering::Less
    } else if fraction < 0.0 {
        Ordering::Greater
    } else {
        Ordering::Equal
    })
}

/// Order of two numeric readings. Integer pairs and integer/float pairs
/// compare exactly. `None` for a NaN.
pub(crate) fn cmp_numeric(a: Numeric, b: Numeric) -> Option<Ordering> {
    match (a, b) {
        (Numeric::Int(x), Numeric::Int(y)) => Some(x.cmp(&y)),
        (Numeric::Int(x), Numeric::Float(y)) => cmp_int_float(x, y),
        (Numeric::Float(x), Numeric::Int(y)) => cmp_int_float(y, x).map(Ordering::reverse),
        (Numeric::Float(x), Numeric::Float(y)) => x.partial_cmp(&y),
    }
}

/// Exact order of two JSON numbers. `i64` and `u64` pairs compare exactly,
/// and an integer against a float compares without rounding. `None` for a
/// pair with no order.
pub fn compare_json_numbers(a: &serde_json::Number, b: &serde_json::Number) -> Option<Ordering> {
    cmp_numeric(number_reading(a)?, number_reading(b)?)
}

/// Numeric order of `a` and `b` when both have a numeric reading (number,
/// numeric string, bool). Outer `None`: at least one side is not numeric.
/// Inner `None`: the pair has no order (a NaN).
pub fn numeric_order(
    a: &serde_json::Value,
    b: &serde_json::Value,
) -> Option<Option<Ordering>> {
    let (na, nb) = (numeric_reading(a)?, numeric_reading(b)?);
    Some(cmp_numeric(na, nb))
}

/// Compare two JSON values with type coercion.
///
/// Numeric comparison first (with bool coercion; exact for integers; a NaN
/// pair is `Equal`), then string comparison of the display forms.
pub fn compare_json(a: &serde_json::Value, b: &serde_json::Value) -> Ordering {
    if let Some(order) = numeric_order(a, b) {
        return order.unwrap_or(Ordering::Equal);
    }
    // Fallback: string comparison.
    let sa = json_to_display_string(a);
    let sb = json_to_display_string(b);
    sa.cmp(&sb)
}

/// Compare two optional JSON values (for scan_filter compatibility).
pub fn compare_json_optional(
    a: Option<&serde_json::Value>,
    b: Option<&serde_json::Value>,
) -> Ordering {
    match (a, b) {
        (None, None) => Ordering::Equal,
        (None, Some(_)) => Ordering::Less,
        (Some(_), None) => Ordering::Greater,
        (Some(a), Some(b)) => compare_json(a, b),
    }
}

/// Check equality with type coercion.
///
/// Handles `"5" == 5` by reading both sides as numbers when each has a
/// numeric reading. A pair with an integer side compares exactly. A float
/// pair is equal within `f64::EPSILON`.
pub fn coerced_eq(a: &serde_json::Value, b: &serde_json::Value) -> bool {
    if a == b {
        return true;
    }
    match (numeric_reading(a), numeric_reading(b)) {
        (Some(Numeric::Float(af)), Some(Numeric::Float(bf))) => (af - bf).abs() < f64::EPSILON,
        (Some(na), Some(nb)) => cmp_numeric(na, nb) == Some(Ordering::Equal),
        _ => false,
    }
}

/// Check if a JSON value is truthy (for boolean contexts).
///
/// - `true` → true, `false` → false
/// - `null` → false
/// - Numbers: non-zero → true
/// - Strings: non-empty → true
/// - Arrays/Objects: always true
pub fn is_truthy(v: &serde_json::Value) -> bool {
    match v {
        serde_json::Value::Bool(b) => *b,
        serde_json::Value::Null => false,
        serde_json::Value::Number(n) => n.as_f64().unwrap_or(0.0) != 0.0,
        serde_json::Value::String(s) => !s.is_empty(),
        _ => true,
    }
}

/// Convert a JSON value to a display string.
///
/// - Strings: returned as-is (no quotes)
/// - Null: empty string
/// - Numbers/Bools: `.to_string()`
/// - Objects/Arrays: JSON serialization
pub fn json_to_display_string(v: &serde_json::Value) -> String {
    match v {
        serde_json::Value::String(s) => s.clone(),
        serde_json::Value::Null => String::new(),
        serde_json::Value::Number(n) => n.to_string(),
        serde_json::Value::Bool(b) => b.to_string(),
        other => other.to_string(),
    }
}

/// Convert a f64 to a JSON number, preferring integer representation.
///
/// Returns `Null` for NaN/Infinity.
pub fn to_json_number(n: f64) -> serde_json::Value {
    if n.fract() == 0.0 && n.abs() < i64::MAX as f64 {
        serde_json::Value::Number(serde_json::Number::from(n as i64))
    } else {
        serde_json::Number::from_f64(n)
            .map(serde_json::Value::Number)
            .unwrap_or(serde_json::Value::Null)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn coerced_eq_mixed_types() {
        assert!(coerced_eq(&json!(5), &json!("5")));
        assert!(coerced_eq(&json!(3.15), &json!("3.15")));
        assert!(!coerced_eq(&json!(5), &json!("6")));
    }

    #[test]
    fn coerced_eq_bool_numeric() {
        assert!(coerced_eq(&json!(true), &json!(1)));
        assert!(coerced_eq(&json!(false), &json!(0)));
        assert!(!coerced_eq(&json!(true), &json!(0)));
    }

    #[test]
    fn compare_numeric_coercion() {
        assert_eq!(
            compare_json(&json!(5), &json!("4")),
            std::cmp::Ordering::Greater
        );
        assert_eq!(
            compare_json(&json!("10"), &json!(9)),
            std::cmp::Ordering::Greater
        );
    }

    /// `2^53 + 1` and `2^53` collapse to one `f64`. Integers compare exactly.
    #[test]
    fn integers_past_two_pow_53_compare_exactly() {
        let above = json!(9_007_199_254_740_993_i64);
        let at = json!(9_007_199_254_740_992_i64);
        assert!(!coerced_eq(&above, &at));
        assert_eq!(compare_json(&above, &at), Ordering::Greater);
        assert_eq!(compare_json(&at, &above), Ordering::Less);
        assert!(coerced_eq(&above, &json!(9_007_199_254_740_993_u64)));
    }

    #[test]
    fn nanosecond_timestamps_one_tick_apart() {
        let t0 = json!(1_700_000_000_000_000_001_i64);
        let t1 = json!(1_700_000_000_000_000_002_i64);
        assert!(!coerced_eq(&t0, &t1));
        assert_eq!(compare_json(&t0, &t1), Ordering::Less);
        assert_eq!(compare_json(&t1, &t0), Ordering::Greater);
    }

    #[test]
    fn u64_against_i64_compares_exactly() {
        let u_max = json!(u64::MAX);
        let i_max = json!(i64::MAX);
        assert!(!coerced_eq(&u_max, &i_max));
        assert_eq!(compare_json(&u_max, &i_max), Ordering::Greater);
        assert_eq!(compare_json(&i_max, &u_max), Ordering::Less);
        assert_eq!(compare_json(&json!(u64::MAX - 1), &u_max), Ordering::Less);
        assert_eq!(compare_json(&json!(i64::MIN), &u_max), Ordering::Less);
        // `i64::MAX + 1` as `u64` against `i64::MAX`.
        assert_eq!(
            compare_json(&json!(9_223_372_036_854_775_808_u64), &i_max),
            Ordering::Greater
        );
        assert_eq!(
            compare_json_numbers(
                &serde_json::Number::from(u64::MAX),
                &serde_json::Number::from(i64::MAX)
            ),
            Some(Ordering::Greater)
        );
    }

    #[test]
    fn integer_against_float_compares_without_rounding() {
        let above = json!(9_007_199_254_740_993_i64);
        let float = json!(9_007_199_254_740_992.0_f64);
        assert!(!coerced_eq(&above, &float));
        assert_eq!(compare_json(&above, &float), Ordering::Greater);
        assert_eq!(compare_json(&float, &above), Ordering::Less);
        assert!(coerced_eq(&json!(9_007_199_254_740_992_i64), &float));
        assert_eq!(compare_json(&json!(2), &json!(2.5)), Ordering::Less);
        assert_eq!(compare_json(&json!(-2), &json!(-2.5)), Ordering::Greater);
        assert!(coerced_eq(&json!(3), &json!(3.0)));
        // `u64::MAX` rounds up to `2^64` as an `f64`, so it is below that float.
        assert_eq!(
            compare_json(&json!(u64::MAX), &json!(18_446_744_073_709_551_616.0_f64)),
            Ordering::Less
        );
        assert!(!coerced_eq(
            &json!(u64::MAX),
            &json!(18_446_744_073_709_551_616.0_f64)
        ));
        assert_eq!(
            compare_json(&json!(i64::MIN), &json!(-9_223_372_036_854_775_808.0_f64)),
            Ordering::Equal
        );
    }

    #[test]
    fn integer_strings_compare_exactly_against_integers() {
        let text = json!("9007199254740993");
        assert!(!coerced_eq(&text, &json!(9_007_199_254_740_992_i64)));
        assert!(coerced_eq(&text, &json!(9_007_199_254_740_993_i64)));
        assert_eq!(
            compare_json(&text, &json!(9_007_199_254_740_992_i64)),
            Ordering::Greater
        );
        assert!(coerced_eq(&json!("18446744073709551615"), &json!(u64::MAX)));
        assert!(coerced_eq(&json!("5.0"), &json!(5)));
    }

    #[test]
    fn small_numbers_keep_their_order() {
        assert_eq!(compare_json(&json!(1), &json!(2)), Ordering::Less);
        assert_eq!(compare_json(&json!(1.5), &json!(1.25)), Ordering::Greater);
        assert_eq!(compare_json(&json!(-3), &json!(-3)), Ordering::Equal);
        assert_eq!(compare_json(&json!(true), &json!(0)), Ordering::Greater);
        assert_eq!(compare_json(&json!("abc"), &json!("abd")), Ordering::Less);
        assert_eq!(compare_json(&json!("NaN"), &json!(1)), Ordering::Equal);
        assert!(coerced_eq(&json!(0.1 + 0.2), &json!(0.3)));
        assert!(!coerced_eq(&json!("abc"), &json!(1)));
    }

    #[test]
    fn truthiness() {
        assert!(is_truthy(&json!(true)));
        assert!(!is_truthy(&json!(false)));
        assert!(!is_truthy(&json!(null)));
        assert!(is_truthy(&json!(1)));
        assert!(!is_truthy(&json!(0)));
        assert!(is_truthy(&json!("hello")));
        assert!(!is_truthy(&json!("")));
    }

    #[test]
    fn to_json_number_nan() {
        assert_eq!(to_json_number(f64::NAN), serde_json::Value::Null);
    }

    #[test]
    fn to_json_number_integer() {
        assert_eq!(to_json_number(42.0), json!(42));
    }

    #[test]
    fn to_json_number_float() {
        assert_eq!(to_json_number(3.15), json!(3.15));
    }
}
