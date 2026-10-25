// SPDX-License-Identifier: Apache-2.0

//! Shared helpers for window-function evaluation over document rows.
//!
//! A row is `(id, Value::Object)`. `Value` holds every result as computed,
//! NaN and ±Infinity included, which a JSON number cannot.

use std::collections::HashMap;

use nodedb_crs::CrsContext;
use nodedb_types::Value;

use crate::expr::types::SqlExpr;
use crate::value_ops::{compare_sort_values, sort_peers, value_to_display_string};

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
    rows: &[(String, Value)],
    partition_by: &[SqlExpr],
    order_by: &[(SqlExpr, bool)],
    crs: &CrsContext<'_>,
) -> Result<Vec<Vec<usize>>, crate::expr::EvalError> {
    let mut partitions = if partition_by.is_empty() {
        vec![(0..rows.len()).collect::<Vec<usize>>()]
    } else {
        let mut groups: HashMap<String, Vec<usize>> = HashMap::new();
        let mut order = Vec::new();

        for (i, (_id, doc)) in rows.iter().enumerate() {
            let key: String = partition_by
                .iter()
                .map(|expr| expr.eval(doc, crs).map(|v| partition_key_part(&v)))
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
        let mut keys: Vec<Vec<Value>> = Vec::with_capacity(rows.len());
        for (_id, doc) in rows.iter() {
            keys.push(
                order_by
                    .iter()
                    .map(|(expr, _)| expr.eval(doc, crs))
                    .collect::<Result<Vec<_>, _>>()?,
            );
        }

        for partition in &mut partitions {
            partition.sort_by(|&a, &b| compare_order_keys(&keys[a], &keys[b], order_by));
        }
    }

    Ok(partitions)
}

/// One PARTITION BY value as a key fragment. The type name keeps the text
/// `"1"` apart from the integer `1`.
fn partition_key_part(v: &Value) -> String {
    format!("{}:{}", v.type_name(), value_to_display_string(v))
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
    a: &[Value],
    b: &[Value],
    order_by: &[(SqlExpr, bool)],
) -> std::cmp::Ordering {
    use std::cmp::Ordering;
    for (idx, (_, ascending)) in order_by.iter().enumerate() {
        let (Some(va), Some(vb)) = (a.get(idx), b.get(idx)) else {
            continue;
        };
        let ord = null_order(va.is_null(), vb.is_null(), *ascending).unwrap_or_else(|| {
            let c = compare_sort_values(va, vb);
            if *ascending { c } else { c.reverse() }
        });
        if ord != Ordering::Equal {
            return ord;
        }
    }
    Ordering::Equal
}

pub(super) fn set_window_col(row: &mut Value, alias: &str, val: Value) {
    if let Value::Object(map) = row {
        map.insert(alias.to_string(), val);
    }
}

/// Numeric view of a value for RANGE offsets and MIN / MAX filtering: a
/// number or numeric text. `None` for any other value.
pub(super) fn as_f64(v: &Value) -> Option<f64> {
    crate::value_ops::value_to_f64(v, false)
}

/// Returns true when row at index `b` has the same ORDER BY key as row at
/// index `a` (used by peer-aware ranking like RANK and PERCENT_RANK).
pub(super) fn order_keys_equal(
    rows: &[(String, Value)],
    a: usize,
    b: usize,
    order_by: &[(SqlExpr, bool)],
    crs: &CrsContext<'_>,
) -> Result<bool, crate::expr::EvalError> {
    for (expr, _) in order_by {
        let va = expr.eval(&rows[a].1, crs)?;
        let vb = expr.eval(&rows[b].1, crs)?;
        if !sort_peers(&va, &vb) {
            return Ok(false);
        }
    }
    Ok(true)
}
