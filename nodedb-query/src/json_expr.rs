// SPDX-License-Identifier: Apache-2.0

//! Expression evaluation against a JSON row.
//!
//! Scan handlers materialize rows as `serde_json::Value` before applying
//! ORDER BY and window functions. Both evaluate arbitrary expressions per row
//! and both must surface an evaluation failure — a zero divisor in
//! `ORDER BY 100 / weight` fails the statement with SQLSTATE `22012` rather
//! than sorting the row under NULL.

use crate::expr::{EvalError, SqlExpr};
use crate::json_ops::compare_json_numbers;

/// Evaluate `expr` against a JSON row.
///
/// Column and literal references short-circuit; anything else goes through the
/// shared row evaluator, so arithmetic, function calls, and `CASE` all behave
/// exactly as they do in a projection or WHERE clause.
pub fn eval_expr_on_json(
    expr: &SqlExpr,
    doc: &serde_json::Value,
) -> Result<serde_json::Value, EvalError> {
    match expr {
        SqlExpr::Column(name) => Ok(doc.get(name).cloned().unwrap_or(serde_json::Value::Null)),
        SqlExpr::Literal(v) => Ok(serde_json::Value::from(v.clone())),
        other => {
            let row = nodedb_types::Value::from(doc.clone());
            Ok(serde_json::Value::from(other.eval(&row)?))
        }
    }
}

/// Total order over JSON values for sorting.
///
/// NULLs sort first; numbers compare exactly (integers past `2^53` and
/// integer/float pairs without rounding, an unordered pair as `Equal`);
/// strings lexicographically; mixed or composite values fall back to their
/// rendered form so the ordering stays deterministic.
pub fn compare_json(a: &serde_json::Value, b: &serde_json::Value) -> std::cmp::Ordering {
    use serde_json::Value;
    match (a, b) {
        (Value::Null, Value::Null) => std::cmp::Ordering::Equal,
        (Value::Null, _) => std::cmp::Ordering::Less,
        (_, Value::Null) => std::cmp::Ordering::Greater,
        (Value::Bool(x), Value::Bool(y)) => x.cmp(y),
        (Value::Number(x), Value::Number(y)) => {
            compare_json_numbers(x, y).unwrap_or(std::cmp::Ordering::Equal)
        }
        (Value::String(x), Value::String(y)) => x.cmp(y),
        _ => a.to_string().cmp(&b.to_string()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use std::cmp::Ordering;

    fn sorted(mut values: Vec<serde_json::Value>) -> Vec<serde_json::Value> {
        values.sort_by(compare_json);
        values
    }

    /// `2^53 + 1` and `2^53` collapse to one `f64`. Integers order exactly.
    #[test]
    fn integers_past_two_pow_53_order_exactly() {
        let above = json!(9_007_199_254_740_993_i64);
        let at = json!(9_007_199_254_740_992_i64);
        assert_eq!(compare_json(&above, &at), Ordering::Greater);
        assert_eq!(compare_json(&at, &above), Ordering::Less);
        assert_eq!(sorted(vec![above.clone(), at.clone()]), vec![at, above]);
    }

    #[test]
    fn nanosecond_timestamps_order_exactly() {
        let t0 = json!(1_700_000_000_000_000_001_i64);
        let t1 = json!(1_700_000_000_000_000_002_i64);
        let t2 = json!(1_700_000_000_000_000_003_i64);
        assert_eq!(
            sorted(vec![t2.clone(), t0.clone(), t1.clone()]),
            vec![t0, t1, t2]
        );
    }

    #[test]
    fn u64_against_i64_orders_exactly() {
        let u_max = json!(u64::MAX);
        let i_max = json!(i64::MAX);
        let i_min = json!(i64::MIN);
        assert_eq!(compare_json(&u_max, &i_max), Ordering::Greater);
        assert_eq!(compare_json(&i_max, &u_max), Ordering::Less);
        assert_eq!(
            sorted(vec![u_max.clone(), i_min.clone(), i_max.clone()]),
            vec![i_min, i_max, u_max]
        );
    }

    #[test]
    fn integer_against_float_near_two_pow_53_orders_without_rounding() {
        let above = json!(9_007_199_254_740_993_i64);
        let float = json!(9_007_199_254_740_992.0_f64);
        assert_eq!(compare_json(&above, &float), Ordering::Greater);
        assert_eq!(compare_json(&float, &above), Ordering::Less);
        assert_eq!(
            compare_json(&json!(9_007_199_254_740_992_i64), &float),
            Ordering::Equal
        );
    }

    #[test]
    fn small_values_keep_their_order() {
        assert_eq!(
            sorted(vec![json!(2), json!(null), json!(1.5), json!(-1), json!(1)]),
            vec![json!(null), json!(-1), json!(1), json!(1.5), json!(2)]
        );
        assert_eq!(compare_json(&json!(3), &json!(3.0)), Ordering::Equal);
        assert_eq!(compare_json(&json!("a"), &json!("b")), Ordering::Less);
        assert_eq!(compare_json(&json!(false), &json!(true)), Ordering::Less);
    }

    /// A NaN has no JSON number form: it reaches the sort as NULL.
    #[test]
    fn nan_sorts_as_null() {
        let nan = json!(f64::NAN);
        assert!(nan.is_null());
        assert_eq!(compare_json(&nan, &json!(1)), Ordering::Less);
        assert_eq!(compare_json(&nan, &json!(null)), Ordering::Equal);
    }
}
