// SPDX-License-Identifier: Apache-2.0

//! Extremum selection for MIN / MAX aggregates.
//!
//! MIN / MAX keep the original value and return it unchanged. An integer
//! stays an integer, a float stays a float. A numeric pair compares by its
//! exact numeric reading, so integers above 2^53 never round through `f64`.
//! A NaN extreme yields to any candidate. A NaN candidate never replaces an
//! ordered extreme. A non-numeric pair uses the coerced value order.

use std::cmp::Ordering;

use nodedb_types::Value;

/// Whether a candidate replaces the current extreme.
///
/// `numeric` is the candidate's numeric order against the current extreme:
/// outer `None` when either side has no numeric reading, inner `None` when
/// the pair is unordered (a NaN). `fallback` orders a non-numeric pair.
fn replaces(
    numeric: Option<Option<Ordering>>,
    current_is_nan: bool,
    fallback: impl FnOnce() -> Ordering,
    want_max: bool,
) -> bool {
    let order = match numeric {
        Some(Some(o)) => o,
        Some(None) => return current_is_nan,
        None => fallback(),
    };
    let wanted = if want_max {
        Ordering::Greater
    } else {
        Ordering::Less
    };
    order == wanted
}

/// Whether the JSON `candidate` replaces `current` as the MIN (`want_max`
/// false) or MAX (`want_max` true). An empty `current` is always replaced.
pub fn json_replaces(
    candidate: &serde_json::Value,
    current: Option<&serde_json::Value>,
    want_max: bool,
) -> bool {
    let Some(current) = current else {
        return true;
    };
    replaces(
        crate::json_ops::numeric_order(candidate, current),
        matches!(crate::json_ops::numeric_order(current, current), Some(None)),
        || crate::json_ops::compare_json(candidate, current),
        want_max,
    )
}

/// Whether the `Value` `candidate` replaces `current` as the MIN (`want_max`
/// false) or MAX (`want_max` true). An empty `current` is always replaced.
pub fn value_replaces(candidate: &Value, current: Option<&Value>, want_max: bool) -> bool {
    let Some(current) = current else {
        return true;
    };
    replaces(
        crate::value_ops::numeric_order(candidate, current),
        matches!(
            crate::value_ops::numeric_order(current, current),
            Some(None)
        ),
        || crate::value_ops::compare_values(candidate, current),
        want_max,
    )
}

/// Direction of an extremum function: `Some(false)` for MIN, `Some(true)`
/// for MAX, `None` for any other function.
pub fn extremum_direction(func_name: &str) -> Option<bool> {
    match func_name {
        "min" => Some(false),
        "max" => Some(true),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn json_integers_above_2_pow_53_compare_exactly() {
        let a = json!(9_007_199_254_740_993_i64);
        let b = json!(9_007_199_254_740_992_i64);
        assert!(json_replaces(&b, Some(&a), false));
        assert!(!json_replaces(&b, Some(&a), true));
        assert!(json_replaces(&a, Some(&b), true));
    }

    #[test]
    fn value_nan_never_replaces_ordered_extreme() {
        let one = Value::Integer(1);
        let nan = Value::Float(f64::NAN);
        assert!(!value_replaces(&nan, Some(&one), false));
        assert!(!value_replaces(&nan, Some(&one), true));
        assert!(value_replaces(&one, Some(&nan), false));
        assert!(value_replaces(&one, Some(&nan), true));
    }

    #[test]
    fn value_mixed_int_float_compare_exactly() {
        let big = Value::Integer(9_007_199_254_740_993);
        let float = Value::Float(9_007_199_254_740_992.0);
        assert!(value_replaces(&big, Some(&float), true));
        assert!(!value_replaces(&big, Some(&float), false));
        assert!(value_replaces(&float, Some(&big), false));
    }
}
