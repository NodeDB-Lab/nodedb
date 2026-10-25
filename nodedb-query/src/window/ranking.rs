// SPDX-License-Identifier: Apache-2.0

//! Ranking and distribution window functions: row_number, rank, dense_rank,
//! ntile, percent_rank, cume_dist.

use nodedb_crs::CrsContext;
use nodedb_types::Value;

use crate::expr::SqlExpr;

use super::helpers::{order_keys_equal, set_window_col};
use super::spec::WindowFuncSpec;

/// A row count or rank as an integer value.
fn count_value(n: usize) -> Value {
    Value::from_u64(n as u64)
}

pub(super) fn apply_row_number(rows: &mut [(String, Value)], indices: &[usize], alias: &str) {
    for (rank, &i) in indices.iter().enumerate() {
        set_window_col(&mut rows[i].1, alias, count_value(rank + 1));
    }
}

pub(super) fn apply_rank(
    rows: &mut [(String, Value)],
    indices: &[usize],
    alias: &str,
    order_by: &[(SqlExpr, bool)],
    crs: &CrsContext<'_>,
) -> Result<(), crate::expr::EvalError> {
    if indices.is_empty() {
        return Ok(());
    }
    let mut current_rank = 1;
    set_window_col(&mut rows[indices[0]].1, alias, count_value(1));

    for pos in 1..indices.len() {
        if !order_keys_equal(rows, indices[pos - 1], indices[pos], order_by, crs)? {
            current_rank = pos + 1;
        }
        set_window_col(&mut rows[indices[pos]].1, alias, count_value(current_rank));
    }
    Ok(())
}

pub(super) fn apply_dense_rank(
    rows: &mut [(String, Value)],
    indices: &[usize],
    alias: &str,
    order_by: &[(SqlExpr, bool)],
    crs: &CrsContext<'_>,
) -> Result<(), crate::expr::EvalError> {
    if indices.is_empty() {
        return Ok(());
    }
    let mut current_rank = 1;
    set_window_col(&mut rows[indices[0]].1, alias, count_value(1));

    for pos in 1..indices.len() {
        if !order_keys_equal(rows, indices[pos - 1], indices[pos], order_by, crs)? {
            current_rank += 1;
        }
        set_window_col(&mut rows[indices[pos]].1, alias, count_value(current_rank));
    }
    Ok(())
}

pub(super) fn apply_ntile(rows: &mut [(String, Value)], indices: &[usize], spec: &WindowFuncSpec) {
    let n = spec
        .args
        .first()
        .and_then(|e| {
            if let SqlExpr::Literal(v) = e {
                v.as_f64().map(|x| x as usize)
            } else {
                None
            }
        })
        .unwrap_or(1)
        .max(1);
    let total = indices.len();
    if total == 0 {
        return;
    }
    for (pos, &i) in indices.iter().enumerate() {
        // Integer division distributes rows as evenly as possible (PostgreSQL semantics).
        let bucket = (pos * n / total) + 1;
        set_window_col(&mut rows[i].1, &spec.alias, count_value(bucket));
    }
}

/// PostgreSQL `percent_rank()` — `(rank - 1) / (partition_rows - 1)`. Single-
/// row partitions return 0. Peer rows share their leader's value.
pub(super) fn apply_percent_rank(
    rows: &mut [(String, Value)],
    indices: &[usize],
    alias: &str,
    order_by: &[(SqlExpr, bool)],
    crs: &CrsContext<'_>,
) -> Result<(), crate::expr::EvalError> {
    let total = indices.len();
    if total == 0 {
        return Ok(());
    }
    if total == 1 {
        set_window_col(&mut rows[indices[0]].1, alias, Value::Float(0.0));
        return Ok(());
    }
    let denom = (total - 1) as f64;
    let mut current_rank = 1usize;
    set_window_col(&mut rows[indices[0]].1, alias, Value::Float(0.0));

    for pos in 1..total {
        if !order_keys_equal(rows, indices[pos - 1], indices[pos], order_by, crs)? {
            current_rank = pos + 1;
        }
        let pr = (current_rank - 1) as f64 / denom;
        set_window_col(&mut rows[indices[pos]].1, alias, Value::Float(pr));
    }
    Ok(())
}

/// PostgreSQL `cume_dist()` — `rows_at_or_before_current_peer / partition_rows`.
/// Peer rows (equal ORDER BY keys) share the same value, taken from the last
/// peer's position.
pub(super) fn apply_cume_dist(
    rows: &mut [(String, Value)],
    indices: &[usize],
    alias: &str,
    order_by: &[(SqlExpr, bool)],
    crs: &CrsContext<'_>,
) -> Result<(), crate::expr::EvalError> {
    let total = indices.len();
    if total == 0 {
        return Ok(());
    }
    let denom = total as f64;

    let mut group_start = 0;
    while group_start < total {
        let mut group_end = group_start + 1;
        while group_end < total
            && order_keys_equal(
                rows,
                indices[group_start],
                indices[group_end],
                order_by,
                crs,
            )?
        {
            group_end += 1;
        }
        let cd = group_end as f64 / denom;
        for pos in group_start..group_end {
            set_window_col(&mut rows[indices[pos]].1, alias, Value::Float(cd));
        }
        group_start = group_end;
    }
    Ok(())
}
