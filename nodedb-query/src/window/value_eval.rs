// SPDX-License-Identifier: Apache-2.0

//! Value-native window-function evaluator for the Lite embedded engine.
//!
//! Operates on `Vec<Vec<nodedb_types::Value>>` rows directly, without any
//! serde_json dependency. Each spec appends one `Value` per row; the caller
//! appends the returned column names to its `columns` vec.

use std::collections::HashMap;

use nodedb_types::Value;

use super::spec::WindowFuncSpec;
use super::value_agg::apply_v_aggregate;
use super::value_partition::build_value_partitions;
use crate::expr::types::SqlExpr;
use crate::value_ops::compare_values;

/// Error type for Value-mode window evaluation.
#[derive(Debug, thiserror::Error)]
pub enum WindowError {
    #[error("window column '{name}' not found in result columns")]
    ColumnNotFound { name: String },

    #[error("window function argument error: {detail}")]
    ArgEval { detail: String },

    #[error("window frame error: {detail}")]
    BadFrame { detail: String },

    #[error("division by zero in window expression")]
    Eval(#[from] crate::expr::EvalError),
}

/// Evaluate window functions over a `Vec<Vec<Value>>` result set.
///
/// `column_index` maps column name → position in each row slice.
/// For each spec, one `Value` is appended to every row. Returns the list of
/// new column names, one per spec in spec order. `rows` keeps its input
/// order; each spec's partitions are ordered by that spec's own ORDER BY,
/// independent of the row order.
pub fn evaluate_window_functions_value(
    rows: &mut [Vec<Value>],
    column_index: &HashMap<String, usize>,
    specs: &[WindowFuncSpec],
) -> Result<Vec<String>, WindowError> {
    let mut new_cols: Vec<String> = Vec::with_capacity(specs.len());

    for spec in specs {
        let partitions = build_value_partitions(rows, column_index, spec)?;
        let write_col = rows.first().map(|r| r.len()).unwrap_or(0);

        for row in rows.iter_mut() {
            row.push(Value::Null);
        }

        for partition_indices in &partitions {
            match spec.func_name.as_str() {
                "row_number" => apply_v_row_number(rows, partition_indices, write_col),
                "rank" => apply_v_rank(rows, partition_indices, column_index, spec, write_col)?,
                "dense_rank" => {
                    apply_v_dense_rank(rows, partition_indices, column_index, spec, write_col)?
                }
                "ntile" => apply_v_ntile(rows, partition_indices, spec, write_col)?,
                "percent_rank" => {
                    apply_v_percent_rank(rows, partition_indices, column_index, spec, write_col)?
                }
                "cume_dist" => {
                    apply_v_cume_dist(rows, partition_indices, column_index, spec, write_col)?
                }
                "lag" => apply_v_lag(rows, partition_indices, column_index, spec, write_col)?,
                "lead" => apply_v_lead(rows, partition_indices, column_index, spec, write_col)?,
                "nth_value" => {
                    apply_v_nth_value(rows, partition_indices, column_index, spec, write_col)?
                }
                "sum" | "count" | "avg" | "min" | "max" | "first_value" | "last_value" => {
                    apply_v_aggregate(rows, partition_indices, column_index, spec, write_col)?
                }
                other => {
                    return Err(WindowError::ArgEval {
                        detail: format!(
                            "unknown window function '{other}'; valid names: row_number, rank, \
                             dense_rank, ntile, percent_rank, cume_dist, lag, lead, nth_value, \
                             sum, count, avg, min, max, first_value, last_value"
                        ),
                    });
                }
            }
        }

        new_cols.push(spec.alias.clone());
    }

    Ok(new_cols)
}

// ── Value comparison helpers (pub(super) for value_agg, value_partition) ─────

pub(super) fn cmp_values(a: &Value, b: &Value) -> std::cmp::Ordering {
    match (a, b) {
        (Value::Null, Value::Null) => std::cmp::Ordering::Equal,
        (Value::Null, _) => std::cmp::Ordering::Less,
        (_, Value::Null) => std::cmp::Ordering::Greater,
        (va, vb) => compare_values(va, vb),
    }
}

pub(super) fn order_keys_equal_v(
    rows: &[Vec<Value>],
    a: usize,
    b: usize,
    column_index: &HashMap<String, usize>,
    order_by: &[(SqlExpr, bool)],
) -> Result<bool, WindowError> {
    for (expr, _) in order_by {
        let row_a = rows.get(a).map(|r| r.as_slice()).unwrap_or(&[]);
        let row_b = rows.get(b).map(|r| r.as_slice()).unwrap_or(&[]);
        let va = eval_arg_for_row(expr, row_a, column_index)?;
        let vb = eval_arg_for_row(expr, row_b, column_index)?;
        if !matches!(cmp_values(&va, &vb), std::cmp::Ordering::Equal) {
            return Ok(false);
        }
    }
    Ok(true)
}

// ── Argument evaluation (pub(super) for value_agg) ────────────────────────────

/// Evaluate a window-function argument expression against one row.
///
/// This is the Value-native counterpart of `window::helpers::eval_expr_on_json`.
/// A division/modulo-by-zero surfaces as
/// `Err(EvalError::DivisionByZero)` — which the value-path callers convert into
/// `WindowError` via `?` — rather than being folded to `NULL`.
pub(super) fn eval_arg_for_row(
    expr: &SqlExpr,
    row: &[Value],
    column_index: &HashMap<String, usize>,
) -> Result<Value, crate::expr::EvalError> {
    match expr {
        SqlExpr::Column(name) => Ok(column_index
            .get(name.as_str())
            .and_then(|&idx| row.get(idx))
            .cloned()
            .unwrap_or(Value::Null)),
        SqlExpr::Literal(v) => Ok(v.clone()),
        other => {
            let doc = row_to_obj(row, column_index);
            Ok(other.eval(&doc)?)
        }
    }
}

fn row_to_obj(row: &[Value], column_index: &HashMap<String, usize>) -> Value {
    let mut map = HashMap::new();
    for (name, &idx) in column_index {
        if let Some(v) = row.get(idx) {
            map.insert(name.clone(), v.clone());
        }
    }
    Value::Object(map)
}

fn usize_arg(spec: &WindowFuncSpec, idx: usize, default: usize) -> usize {
    spec.args
        .get(idx)
        .and_then(|e| match e {
            SqlExpr::Literal(v) => v.as_f64().map(|n| n as usize),
            _ => None,
        })
        .unwrap_or(default)
}

fn default_arg_value(spec: &WindowFuncSpec, idx: usize) -> Value {
    spec.args
        .get(idx)
        .and_then(|e| match e {
            SqlExpr::Literal(v) => Some(v.clone()),
            _ => None,
        })
        .unwrap_or(Value::Null)
}

// ── Cell write helper (pub(super) for value_agg) ──────────────────────────────

pub(super) fn set_cell(rows: &mut [Vec<Value>], row_idx: usize, col_idx: usize, val: Value) {
    if let Some(row) = rows.get_mut(row_idx)
        && let Some(cell) = row.get_mut(col_idx)
    {
        *cell = val;
    }
}

// ── Ranking functions ─────────────────────────────────────────────────────────

fn apply_v_row_number(rows: &mut [Vec<Value>], indices: &[usize], write_col: usize) {
    for (rank, &i) in indices.iter().enumerate() {
        set_cell(rows, i, write_col, Value::Integer((rank + 1) as i64));
    }
}

fn apply_v_rank(
    rows: &mut [Vec<Value>],
    indices: &[usize],
    column_index: &HashMap<String, usize>,
    spec: &WindowFuncSpec,
    write_col: usize,
) -> Result<(), WindowError> {
    if indices.is_empty() {
        return Ok(());
    }
    let mut current_rank = 1usize;
    set_cell(rows, indices[0], write_col, Value::Integer(1));
    for pos in 1..indices.len() {
        if !order_keys_equal_v(
            rows,
            indices[pos - 1],
            indices[pos],
            column_index,
            &spec.order_by,
        )? {
            current_rank = pos + 1;
        }
        set_cell(
            rows,
            indices[pos],
            write_col,
            Value::Integer(current_rank as i64),
        );
    }
    Ok(())
}

fn apply_v_dense_rank(
    rows: &mut [Vec<Value>],
    indices: &[usize],
    column_index: &HashMap<String, usize>,
    spec: &WindowFuncSpec,
    write_col: usize,
) -> Result<(), WindowError> {
    if indices.is_empty() {
        return Ok(());
    }
    let mut current_rank = 1usize;
    set_cell(rows, indices[0], write_col, Value::Integer(1));
    for pos in 1..indices.len() {
        if !order_keys_equal_v(
            rows,
            indices[pos - 1],
            indices[pos],
            column_index,
            &spec.order_by,
        )? {
            current_rank += 1;
        }
        set_cell(
            rows,
            indices[pos],
            write_col,
            Value::Integer(current_rank as i64),
        );
    }
    Ok(())
}

fn apply_v_ntile(
    rows: &mut [Vec<Value>],
    indices: &[usize],
    spec: &WindowFuncSpec,
    write_col: usize,
) -> Result<(), WindowError> {
    let n = usize_arg(spec, 0, 1).max(1);
    let total = indices.len();
    if total == 0 {
        return Ok(());
    }
    for (pos, &i) in indices.iter().enumerate() {
        let bucket = (pos * n / total) + 1;
        set_cell(rows, i, write_col, Value::Integer(bucket as i64));
    }
    Ok(())
}

fn apply_v_percent_rank(
    rows: &mut [Vec<Value>],
    indices: &[usize],
    column_index: &HashMap<String, usize>,
    spec: &WindowFuncSpec,
    write_col: usize,
) -> Result<(), WindowError> {
    let total = indices.len();
    if total == 0 {
        return Ok(());
    }
    if total == 1 {
        set_cell(rows, indices[0], write_col, Value::Float(0.0));
        return Ok(());
    }
    let denom = (total - 1) as f64;
    let mut current_rank = 1usize;
    set_cell(rows, indices[0], write_col, Value::Float(0.0));
    for pos in 1..total {
        if !order_keys_equal_v(
            rows,
            indices[pos - 1],
            indices[pos],
            column_index,
            &spec.order_by,
        )? {
            current_rank = pos + 1;
        }
        let pr = (current_rank - 1) as f64 / denom;
        set_cell(rows, indices[pos], write_col, Value::Float(pr));
    }
    Ok(())
}

fn apply_v_cume_dist(
    rows: &mut [Vec<Value>],
    indices: &[usize],
    column_index: &HashMap<String, usize>,
    spec: &WindowFuncSpec,
    write_col: usize,
) -> Result<(), WindowError> {
    let total = indices.len();
    if total == 0 {
        return Ok(());
    }
    let denom = total as f64;
    let mut group_start = 0;
    while group_start < total {
        let mut group_end = group_start + 1;
        while group_end < total
            && order_keys_equal_v(
                rows,
                indices[group_start],
                indices[group_end],
                column_index,
                &spec.order_by,
            )?
        {
            group_end += 1;
        }
        let cd = group_end as f64 / denom;
        for &idx in &indices[group_start..group_end] {
            set_cell(rows, idx, write_col, Value::Float(cd));
        }
        group_start = group_end;
    }
    Ok(())
}

// ── Offset functions ──────────────────────────────────────────────────────────

fn collect_arg_values(
    rows: &[Vec<Value>],
    indices: &[usize],
    column_index: &HashMap<String, usize>,
    spec: &WindowFuncSpec,
) -> Result<Vec<Value>, WindowError> {
    indices
        .iter()
        .map(|&i| match (rows.get(i), spec.args.first()) {
            (Some(row), Some(expr)) => Ok(eval_arg_for_row(expr, row, column_index)?),
            _ => Ok(Value::Null),
        })
        .collect()
}

fn apply_v_lag(
    rows: &mut [Vec<Value>],
    indices: &[usize],
    column_index: &HashMap<String, usize>,
    spec: &WindowFuncSpec,
    write_col: usize,
) -> Result<(), WindowError> {
    let offset = usize_arg(spec, 1, 1);
    let default = default_arg_value(spec, 2);
    let values = collect_arg_values(rows, indices, column_index, spec)?;
    for (pos, &i) in indices.iter().enumerate() {
        let val = if pos >= offset {
            values[pos - offset].clone()
        } else {
            default.clone()
        };
        set_cell(rows, i, write_col, val);
    }
    Ok(())
}

fn apply_v_lead(
    rows: &mut [Vec<Value>],
    indices: &[usize],
    column_index: &HashMap<String, usize>,
    spec: &WindowFuncSpec,
    write_col: usize,
) -> Result<(), WindowError> {
    let offset = usize_arg(spec, 1, 1);
    let default = default_arg_value(spec, 2);
    let values = collect_arg_values(rows, indices, column_index, spec)?;
    for (pos, &i) in indices.iter().enumerate() {
        let val = if pos + offset < indices.len() {
            values[pos + offset].clone()
        } else {
            default.clone()
        };
        set_cell(rows, i, write_col, val);
    }
    Ok(())
}

fn apply_v_nth_value(
    rows: &mut [Vec<Value>],
    indices: &[usize],
    column_index: &HashMap<String, usize>,
    spec: &WindowFuncSpec,
    write_col: usize,
) -> Result<(), WindowError> {
    let n = usize_arg(spec, 1, 1).max(1);
    let values = collect_arg_values(rows, indices, column_index, spec)?;
    for (pos, &i) in indices.iter().enumerate() {
        let val = if pos + 1 >= n {
            values[n - 1].clone()
        } else {
            Value::Null
        };
        set_cell(rows, i, write_col, val);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::expr::types::SqlExpr;
    use crate::window::spec::WindowFrame;

    fn ci(names: &[&str]) -> HashMap<String, usize> {
        names
            .iter()
            .enumerate()
            .map(|(i, n)| (n.to_string(), i))
            .collect()
    }

    fn spec(
        func: &str,
        args: Vec<SqlExpr>,
        partition_by: Vec<SqlExpr>,
        order_by: Vec<(SqlExpr, bool)>,
    ) -> WindowFuncSpec {
        WindowFuncSpec {
            alias: format!("w_{func}"),
            func_name: func.to_string(),
            args,
            partition_by,
            order_by,
            frame: WindowFrame::default(),
        }
    }

    fn col(name: &str) -> SqlExpr {
        SqlExpr::Column(name.to_string())
    }

    /// Single-column rows under name "v"; the window result lands at index 1.
    fn rows_v(vals: &[i64]) -> Vec<Vec<Value>> {
        vals.iter().map(|&v| vec![Value::Integer(v)]).collect()
    }

    fn out_int(rows: &[Vec<Value>], col_idx: usize) -> Vec<i64> {
        rows.iter()
            .map(|r| match &r[col_idx] {
                Value::Integer(n) => *n,
                other => panic!("expected integer, got {other:?}"),
            })
            .collect()
    }

    fn out_f64(rows: &[Vec<Value>], col_idx: usize) -> Vec<f64> {
        rows.iter().map(|r| r[col_idx].as_f64().unwrap()).collect()
    }

    #[test]
    fn row_number_sequential() {
        let mut rows = rows_v(&[5, 5, 5]);
        let cols = ci(&["v"]);
        let s = spec("row_number", vec![], vec![], vec![]);
        evaluate_window_functions_value(&mut rows, &cols, &[s]).unwrap();
        assert_eq!(out_int(&rows, 1), vec![1, 2, 3]);
    }

    #[test]
    fn rank_handles_ties() {
        let mut rows = rows_v(&[10, 10, 20]);
        let cols = ci(&["v"]);
        let s = spec("rank", vec![], vec![], vec![(col("v"), true)]);
        evaluate_window_functions_value(&mut rows, &cols, &[s]).unwrap();
        assert_eq!(out_int(&rows, 1), vec![1, 1, 3]);
    }

    #[test]
    fn dense_rank_handles_ties() {
        let mut rows = rows_v(&[10, 10, 20]);
        let cols = ci(&["v"]);
        let s = spec("dense_rank", vec![], vec![], vec![(col("v"), true)]);
        evaluate_window_functions_value(&mut rows, &cols, &[s]).unwrap();
        assert_eq!(out_int(&rows, 1), vec![1, 1, 2]);
    }

    #[test]
    fn ntile_buckets() {
        let mut rows = rows_v(&[1, 2, 3]);
        let cols = ci(&["v"]);
        let s = spec(
            "ntile",
            vec![SqlExpr::Literal(Value::Integer(2))],
            vec![],
            vec![(col("v"), true)],
        );
        evaluate_window_functions_value(&mut rows, &cols, &[s]).unwrap();
        assert_eq!(out_int(&rows, 1), vec![1, 1, 2]);
    }

    #[test]
    fn lag_default_and_offset() {
        let mut rows = rows_v(&[1, 2, 3]);
        let cols = ci(&["v"]);
        let s = spec("lag", vec![col("v")], vec![], vec![(col("v"), true)]);
        evaluate_window_functions_value(&mut rows, &cols, &[s]).unwrap();
        // First row has no predecessor → Null; rest carry the prior value.
        assert!(matches!(rows[0][1], Value::Null));
        assert_eq!(rows[1][1].as_f64().unwrap() as i64, 1);
        assert_eq!(rows[2][1].as_f64().unwrap() as i64, 2);
    }

    #[test]
    fn lead_boundary() {
        let mut rows = rows_v(&[1, 2, 3]);
        let cols = ci(&["v"]);
        let s = spec("lead", vec![col("v")], vec![], vec![(col("v"), true)]);
        evaluate_window_functions_value(&mut rows, &cols, &[s]).unwrap();
        assert_eq!(rows[0][1].as_f64().unwrap() as i64, 2);
        assert_eq!(rows[1][1].as_f64().unwrap() as i64, 3);
        // Last row has no successor → Null.
        assert!(matches!(rows[2][1], Value::Null));
    }

    #[test]
    fn percent_rank_and_cume_dist() {
        let cols = ci(&["v"]);

        let mut rows = rows_v(&[10, 10, 20]);
        let pr = spec("percent_rank", vec![], vec![], vec![(col("v"), true)]);
        evaluate_window_functions_value(&mut rows, &cols, &[pr]).unwrap();
        let pr_out = out_f64(&rows, 1);
        assert!((pr_out[0] - 0.0).abs() < 1e-9);
        assert!((pr_out[1] - 0.0).abs() < 1e-9);
        assert!((pr_out[2] - 1.0).abs() < 1e-9);

        let mut rows = rows_v(&[10, 10, 20]);
        let cd = spec("cume_dist", vec![], vec![], vec![(col("v"), true)]);
        evaluate_window_functions_value(&mut rows, &cols, &[cd]).unwrap();
        let cd_out = out_f64(&rows, 1);
        assert!((cd_out[0] - 2.0 / 3.0).abs() < 1e-9);
        assert!((cd_out[1] - 2.0 / 3.0).abs() < 1e-9);
        assert!((cd_out[2] - 1.0).abs() < 1e-9);
    }

    #[test]
    fn partition_resets_row_number() {
        let cols = ci(&["g", "v"]);
        let mut rows = vec![
            vec![Value::Integer(1), Value::Integer(100)],
            vec![Value::Integer(1), Value::Integer(101)],
            vec![Value::Integer(2), Value::Integer(102)],
        ];
        let s = spec("row_number", vec![], vec![col("g")], vec![]);
        evaluate_window_functions_value(&mut rows, &cols, &[s]).unwrap();
        // Two rows in partition g=1 → 1,2; one row in g=2 → 1. Result at idx 2.
        assert_eq!(out_int(&rows, 2), vec![1, 2, 1]);
    }

    #[test]
    fn rank_orders_by_spec_order_by_not_row_arrival_order() {
        // Rows arrive as 10, 30, 20 — not sorted by value. RANK() OVER
        // (ORDER BY v DESC) must rank 30(1), 20(2), 10(3); the row array
        // order must stay 10, 30, 20.
        let mut rows = rows_v(&[10, 30, 20]);
        let cols = ci(&["v"]);
        let s = spec("rank", vec![], vec![], vec![(col("v"), false)]);
        evaluate_window_functions_value(&mut rows, &cols, &[s]).unwrap();
        assert_eq!(out_int(&rows, 0), vec![10, 30, 20]);
        assert_eq!(out_int(&rows, 1), vec![3, 1, 2]);
    }

    #[test]
    fn unknown_function_errors() {
        let mut rows = rows_v(&[1]);
        let cols = ci(&["v"]);
        let s = spec("nonexistent", vec![], vec![], vec![]);
        let err = evaluate_window_functions_value(&mut rows, &cols, &[s]).unwrap_err();
        assert!(matches!(err, WindowError::ArgEval { .. }));
    }

    #[test]
    fn missing_partition_column_is_null_keyed() {
        // Partitioning on an absent column must not panic — every row keys to
        // Null and lands in one partition.
        let cols = ci(&["v"]);
        let mut rows = rows_v(&[1, 2]);
        let s = spec("row_number", vec![], vec![col("missing")], vec![]);
        evaluate_window_functions_value(&mut rows, &cols, &[s]).unwrap();
        assert_eq!(out_int(&rows, 1), vec![1, 2]);
    }
}
