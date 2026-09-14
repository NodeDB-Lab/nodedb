// SPDX-License-Identifier: Apache-2.0

//! Shared helpers for window-function evaluation.

use std::collections::HashMap;

use crate::expr::types::SqlExpr;

/// Group row indices by partition key, preserving first-seen partition order.
///
/// A division/modulo-by-zero in a PARTITION BY expression propagates as
/// `Err(EvalError::DivisionByZero)` rather than being folded to NULL.
pub(super) fn build_partitions(
    rows: &[(String, serde_json::Value)],
    partition_by: &[SqlExpr],
) -> Result<Vec<Vec<usize>>, crate::expr::EvalError> {
    if partition_by.is_empty() {
        return Ok(vec![(0..rows.len()).collect()]);
    }

    let mut groups: HashMap<String, Vec<usize>> = HashMap::new();
    let mut order = Vec::new();

    for (i, (_id, doc)) in rows.iter().enumerate() {
        let key: String = partition_by
            .iter()
            .map(|expr| eval_expr_on_json(expr, doc).map(|v| v.to_string()))
            .collect::<Result<Vec<_>, _>>()?
            .join("\x00");
        let entry = groups.entry(key.clone()).or_default();
        if entry.is_empty() {
            order.push(key);
        }
        entry.push(i);
    }

    Ok(order.iter().filter_map(|k| groups.remove(k)).collect())
}

pub(super) fn set_window_col(row: &mut serde_json::Value, alias: &str, val: serde_json::Value) {
    if let serde_json::Value::Object(map) = row {
        map.insert(alias.to_string(), val);
    }
}

/// Evaluate a `SqlExpr` against a serde_json document, returning a serde_json value.
///
/// A division/modulo-by-zero in a PARTITION BY / ORDER BY expression is
/// surfaced as `Err(EvalError::DivisionByZero)` — the same
/// statement-failure treatment WHERE/projection expressions get — rather than
/// being folded to `NULL`. The `Result` is threaded through every window
/// function that reaches this evaluator up to `evaluate_window_functions`.
pub(super) fn eval_expr_on_json(
    expr: &SqlExpr,
    doc: &serde_json::Value,
) -> Result<serde_json::Value, crate::expr::EvalError> {
    crate::json_expr::eval_expr_on_json(expr, doc)
}

pub(super) fn as_f64(v: &serde_json::Value) -> Option<f64> {
    match v {
        serde_json::Value::Number(n) => n.as_f64(),
        serde_json::Value::String(s) => s.parse().ok(),
        _ => None,
    }
}

/// Returns true when row at index `b` has the same ORDER BY key as row at
/// index `a` (used by peer-aware ranking like RANK and PERCENT_RANK).
pub(super) fn order_keys_equal(
    rows: &[(String, serde_json::Value)],
    a: usize,
    b: usize,
    order_by: &[(SqlExpr, bool)],
) -> Result<bool, crate::expr::EvalError> {
    for (expr, _) in order_by {
        let va = eval_expr_on_json(expr, &rows[a].1)?;
        let vb = eval_expr_on_json(expr, &rows[b].1)?;
        if va != vb {
            return Ok(false);
        }
    }
    Ok(true)
}

/// Indices of one partition, ordered by a window spec's ORDER BY keys.
///
/// Window functions receive the sorted result set, but a window's own ORDER
/// BY is a different ordering from the query's — nothing upstream sorts by
/// it. Evaluating a partition in arrival order silently assigned
/// row numbers by arrival and compared peers in the wrong sequence, so every
/// ranking and running frame was wrong whenever the two orders differed.
///
/// Keys are evaluated once per row; a division/modulo-by-zero propagates as
/// `Err(EvalError::DivisionByZero)`. The sort is stable, so rows with equal
/// keys keep their arrival order.
pub(super) fn ordered_partition_indices(
    rows: &[(String, serde_json::Value)],
    indices: &[usize],
    order_by: &[(SqlExpr, bool)],
) -> Result<Vec<usize>, crate::expr::EvalError> {
    if order_by.is_empty() {
        return Ok(indices.to_vec());
    }
    // Keys are evaluated even for a one-row partition: the ordering cannot
    // change the result, but a key that divides by zero is still a statement
    // error in PostgreSQL, and skipping the evaluation would answer where
    // the same expression anywhere else fails.
    let mut keyed: Vec<(usize, Vec<serde_json::Value>)> = Vec::with_capacity(indices.len());
    for &i in indices {
        let mut keys = Vec::with_capacity(order_by.len());
        for (expr, _) in order_by {
            keys.push(eval_expr_on_json(expr, &rows[i].1)?);
        }
        keyed.push((i, keys));
    }
    keyed.sort_by(|(_, a), (_, b)| compare_key_values(a, b, order_by));
    Ok(keyed.into_iter().map(|(i, _)| i).collect())
}

/// Compare two pre-evaluated ORDER BY key vectors under their direction flags.
fn compare_key_values(
    a: &[serde_json::Value],
    b: &[serde_json::Value],
    order_by: &[(SqlExpr, bool)],
) -> std::cmp::Ordering {
    for ((va, vb), (_expr, ascending)) in a.iter().zip(b.iter()).zip(order_by.iter()) {
        let ord = compare_with_direction(va, vb, *ascending);
        if ord != std::cmp::Ordering::Equal {
            return ord;
        }
    }
    std::cmp::Ordering::Equal
}

/// Compare two key values, applying the window ORDER BY null convention:
/// NULLS LAST under ASC, NULLS FIRST under DESC.
fn compare_with_direction(
    a: &serde_json::Value,
    b: &serde_json::Value,
    ascending: bool,
) -> std::cmp::Ordering {
    use serde_json::Value as J;
    use std::cmp::Ordering;

    let base = match (a, b) {
        (J::Null, J::Null) => Ordering::Equal,
        (J::Null, _) => Ordering::Greater,
        (_, J::Null) => Ordering::Less,
        _ => json_rank(a).cmp(&json_rank(b)).then_with(|| {
            if let (Some(x), Some(y)) = (as_f64(a), as_f64(b)) {
                x.partial_cmp(&y).unwrap_or(Ordering::Equal)
            } else {
                a.to_string().cmp(&b.to_string())
            }
        }),
    };
    if ascending { base } else { base.reverse() }
}

/// Rank JSON kinds so mixed-type keys still compare deterministically.
fn json_rank(v: &serde_json::Value) -> u8 {
    use serde_json::Value as J;
    match v {
        J::Null => 0,
        J::Bool(_) => 1,
        J::Number(_) => 2,
        J::String(_) => 3,
        J::Array(_) => 4,
        J::Object(_) => 5,
    }
}
