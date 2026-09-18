// SPDX-License-Identifier: Apache-2.0

//! Shared helpers for window-function evaluation.

use std::collections::HashMap;

use crate::expr::types::SqlExpr;

/// Group row indices by partition key, preserving first-seen partition order,
/// then sort each partition's indices by the spec's ORDER BY.
///
/// The returned index lists are ordered by `order_by`; the `rows` array
/// itself keeps its input order — only the per-partition index lists move.
///
/// A division/modulo-by-zero in a PARTITION BY or ORDER BY expression
/// propagates as `Err(EvalError::DivisionByZero)` rather than being folded to
/// NULL.
pub(super) fn build_partitions(
    rows: &[(String, serde_json::Value)],
    partition_by: &[SqlExpr],
    order_by: &[(SqlExpr, bool)],
) -> Result<Vec<Vec<usize>>, crate::expr::EvalError> {
    let mut partitions = if partition_by.is_empty() {
        vec![(0..rows.len()).collect::<Vec<usize>>()]
    } else {
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

        order.iter().filter_map(|k| groups.remove(k)).collect()
    };

    if !order_by.is_empty() {
        let mut keys: Vec<Vec<serde_json::Value>> = Vec::with_capacity(rows.len());
        for (_id, doc) in rows.iter() {
            keys.push(
                order_by
                    .iter()
                    .map(|(expr, _)| eval_expr_on_json(expr, doc))
                    .collect::<Result<Vec<_>, _>>()?,
            );
        }

        for partition in &mut partitions {
            partition.sort_by(|&a, &b| compare_order_keys(&keys[a], &keys[b], order_by));
        }
    }

    Ok(partitions)
}

/// Decide NULL placement for one ORDER BY column, shared by every window
/// evaluator's `compare_order_keys`.
///
/// NULL placement follows PostgreSQL's default: ASC places NULLs last, DESC
/// places NULLs first. A window spec carries no explicit NULLS FIRST/LAST
/// override, so this default is fixed by direction alone. Returns `None`
/// when neither value is NULL, leaving the non-null comparison to the
/// caller.
pub(super) fn null_order(
    a_null: bool,
    b_null: bool,
    ascending: bool,
) -> Option<std::cmp::Ordering> {
    use std::cmp::Ordering;
    let nulls_first = !ascending;
    match (a_null, b_null) {
        (true, true) => Some(Ordering::Equal),
        (true, false) => Some(if nulls_first {
            Ordering::Less
        } else {
            Ordering::Greater
        }),
        (false, true) => Some(if nulls_first {
            Ordering::Greater
        } else {
            Ordering::Less
        }),
        (false, false) => None,
    }
}

/// Compare two rows' pre-evaluated ORDER BY keys.
fn compare_order_keys(
    a: &[serde_json::Value],
    b: &[serde_json::Value],
    order_by: &[(SqlExpr, bool)],
) -> std::cmp::Ordering {
    use std::cmp::Ordering;
    for (idx, (_, ascending)) in order_by.iter().enumerate() {
        let (Some(va), Some(vb)) = (a.get(idx), b.get(idx)) else {
            continue;
        };
        let ord = null_order(va.is_null(), vb.is_null(), *ascending).unwrap_or_else(|| {
            let c = crate::json_expr::compare_json(va, vb);
            if *ascending { c } else { c.reverse() }
        });
        if ord != Ordering::Equal {
            return ord;
        }
    }
    Ordering::Equal
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
