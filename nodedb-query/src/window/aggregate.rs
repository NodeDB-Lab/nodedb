// SPDX-License-Identifier: Apache-2.0

//! Aggregate functions used as windows: sum, count, avg, min, max,
//! first_value, last_value.
//!
//! Dispatch logic:
//! - `RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW` → fast running path
//!   (preserves SIMD accumulation).
//! - All other frame combinations → per-row frame evaluator that computes the
//!   concrete `[start_idx, end_idx]` slice for every row and aggregates over
//!   it.

use super::arg::{ArgValues, arg_at, eval_arg_values};
use super::extremum::json_replaces;
use super::frame::{build_peer_groups, evaluate_frame_bounds};
use super::helpers::{as_f64, set_window_col};
use super::running::running_aggregate;
use super::spec::{FrameBound, WindowFuncSpec};
use crate::numeric_sum::ExactSum;

pub(super) fn apply_aggregate_window(
    rows: &mut [(String, serde_json::Value)],
    indices: &[usize],
    spec: &WindowFuncSpec,
) -> Result<(), crate::expr::EvalError> {
    // The argument is an expression evaluated per row, not a column name: a
    // zero divisor inside it fails the statement here rather than folding the
    // aggregate to a silently-wrong total.
    let arg_values = eval_arg_values(rows, indices, spec, 0)?;

    // Fast path: RANGE UNBOUNDED PRECEDING TO CURRENT ROW is the most common
    // pattern (the PostgreSQL default for ordered windows). Use the running
    // accumulator rather than re-aggregating the slice from scratch each row.
    let use_running = spec.frame.mode == "range"
        && matches!(spec.frame.start, FrameBound::UnboundedPreceding)
        && matches!(spec.frame.end, FrameBound::CurrentRow);

    if use_running {
        running_aggregate(rows, indices, spec, &arg_values)?;
        return Ok(());
    }

    per_row_aggregate(rows, indices, spec, &arg_values)
}

/// Per-row frame evaluator.
///
/// For each row position `pos` in the partition:
/// 1. Resolve the concrete `[start_idx, end_idx]` frame slice via
///    `evaluate_frame_bounds`.
/// 2. Aggregate the evaluated argument over `indices[start_idx..=end_idx]`.
/// 3. Write the result back under `spec.alias`.
fn per_row_aggregate(
    rows: &mut [(String, serde_json::Value)],
    indices: &[usize],
    spec: &WindowFuncSpec,
    arg_values: &ArgValues,
) -> Result<(), crate::expr::EvalError> {
    let len = indices.len();
    if len == 0 {
        return Ok(());
    }

    // Extract order-by values for RANGE numeric offsets.
    let order_expr = spec.order_by.first().map(|(expr, _)| expr);
    let order_values: Vec<serde_json::Value> = indices
        .iter()
        .map(|&i| match order_expr {
            Some(expr) => super::helpers::eval_expr_on_json(expr, &rows[i].1),
            None => Ok(serde_json::Value::Null),
        })
        .collect::<Result<Vec<_>, _>>()?;

    // Peer groups needed for GROUPS mode (and for RANGE CurrentRow peer
    // awareness — reused from the frame module which handles both).
    let peer_groups: Vec<usize> = if spec.frame.mode == "groups" {
        build_peer_groups(&order_values)
    } else {
        Vec::new()
    };

    // Numeric view of the evaluated argument, one slot per partition position.
    let all_vals: Vec<Option<f64>> = (0..len)
        .map(|pos| as_f64(&arg_at(arg_values, pos)))
        .collect();

    let results: Vec<serde_json::Value> = (0..len)
        .map(|pos| {
            let (start_idx, end_idx) =
                evaluate_frame_bounds(&spec.frame, pos, len, &order_values, &peer_groups);

            aggregate_slice(&all_vals, arg_values, spec, start_idx, end_idx)
        })
        .collect::<Result<_, _>>()?;

    for (pos, result) in results.into_iter().enumerate() {
        let row_idx = indices[pos];
        set_window_col(&mut rows[row_idx].1, &spec.alias, result);
    }
    Ok(())
}

/// The exact SUM / AVG state of the frame slice `[start_idx, end_idx]`.
fn frame_sum(arg_values: &ArgValues, start_idx: usize, end_idx: usize) -> ExactSum {
    let mut acc = ExactSum::new();
    if let Some(values) = arg_values {
        for v in values.get(start_idx..=end_idx).unwrap_or(&[]) {
            acc.add_json(v);
        }
    }
    acc
}

/// Aggregate the evaluated argument over the frame slice
/// `[start_idx, end_idx]` (partition positions, not row indices).
fn aggregate_slice(
    all_vals: &[Option<f64>],
    arg_values: &ArgValues,
    spec: &WindowFuncSpec,
    start_idx: usize,
    end_idx: usize,
) -> Result<serde_json::Value, crate::expr::EvalError> {
    Ok(match spec.func_name.as_str() {
        "sum" => serde_json::Value::from(frame_sum(arg_values, start_idx, end_idx).sum()?),
        // `COUNT(*)` counts frame rows; `COUNT(expr)` counts the rows whose
        // argument is non-NULL, so a NULL argument is excluded rather than
        // inflating the count.
        "count" => match arg_values {
            None => serde_json::json!(end_idx - start_idx + 1),
            Some(values) => serde_json::json!(
                values[start_idx..=end_idx]
                    .iter()
                    .filter(|v| !v.is_null())
                    .count()
            ),
        },
        "avg" => serde_json::Value::from(frame_sum(arg_values, start_idx, end_idx).avg()?),
        "min" | "max" => {
            let want_max = spec.func_name == "max";
            let Some(values) = arg_values else {
                return Ok(serde_json::Value::Null);
            };
            let frame_values = values.get(start_idx..=end_idx).unwrap_or(&[]);
            let mut best: Option<&serde_json::Value> = None;
            for (numeric, candidate) in all_vals[start_idx..=end_idx].iter().zip(frame_values) {
                if numeric.is_none() {
                    continue;
                }
                if json_replaces(candidate, best, want_max) {
                    best = Some(candidate);
                }
            }
            best.cloned().unwrap_or(serde_json::Value::Null)
        }
        "first_value" => arg_at(arg_values, start_idx),
        "last_value" => arg_at(arg_values, end_idx),
        _ => serde_json::Value::Null,
    })
}

#[cfg(test)]
mod tests {
    use super::super::spec::{FrameBound, WindowFrame, WindowFuncSpec};
    use super::apply_aggregate_window;
    use crate::expr::SqlExpr;
    use serde_json::json;

    fn numbered(n: usize) -> Vec<(String, serde_json::Value)> {
        (1..=n)
            .map(|i| (i.to_string(), json!({ "n": i as i64 })))
            .collect()
    }

    fn make_spec(func: &str, field: &str, frame: WindowFrame) -> WindowFuncSpec {
        WindowFuncSpec {
            alias: "result".into(),
            func_name: func.into(),
            args: if field == "*" {
                vec![]
            } else {
                vec![SqlExpr::Column(field.into())]
            },
            partition_by: vec![],
            order_by: vec![(SqlExpr::Column("n".into()), true)],
            frame,
        }
    }

    fn rows_frame(start: FrameBound, end: FrameBound) -> WindowFrame {
        WindowFrame {
            mode: "rows".into(),
            start,
            end,
        }
    }

    fn range_frame(start: FrameBound, end: FrameBound) -> WindowFrame {
        WindowFrame {
            mode: "range".into(),
            start,
            end,
        }
    }

    fn groups_frame(start: FrameBound, end: FrameBound) -> WindowFrame {
        WindowFrame {
            mode: "groups".into(),
            start,
            end,
        }
    }

    // ── ROWS ──────────────────────────────────────────────────────────────────

    #[test]
    fn rows_1_preceding_1_following_sum() {
        let mut rows = numbered(5);
        let indices: Vec<usize> = (0..5).collect();
        let spec = make_spec(
            "sum",
            "n",
            rows_frame(FrameBound::Preceding(1), FrameBound::Following(1)),
        );
        apply_aggregate_window(&mut rows, &indices, &spec).unwrap();
        // row 0 (n=1): sum of [1,2] = 3
        // row 1 (n=2): sum of [1,2,3] = 6
        // row 2 (n=3): sum of [2,3,4] = 9
        // row 3 (n=4): sum of [3,4,5] = 12
        // row 4 (n=5): sum of [4,5] = 9
        assert_eq!(rows[0].1["result"], json!(3));
        assert_eq!(rows[1].1["result"], json!(6));
        assert_eq!(rows[2].1["result"], json!(9));
        assert_eq!(rows[3].1["result"], json!(12));
        assert_eq!(rows[4].1["result"], json!(9));
    }

    #[test]
    fn rows_unbounded_preceding_current_sum() {
        let mut rows = numbered(5);
        let indices: Vec<usize> = (0..5).collect();
        let spec = make_spec(
            "sum",
            "n",
            rows_frame(FrameBound::UnboundedPreceding, FrameBound::CurrentRow),
        );
        apply_aggregate_window(&mut rows, &indices, &spec).unwrap();
        assert_eq!(rows[0].1["result"], json!(1));
        assert_eq!(rows[1].1["result"], json!(3));
        assert_eq!(rows[2].1["result"], json!(6));
        assert_eq!(rows[3].1["result"], json!(10));
        assert_eq!(rows[4].1["result"], json!(15));
    }

    #[test]
    fn rows_current_unbounded_following_sum() {
        let mut rows = numbered(5);
        let indices: Vec<usize> = (0..5).collect();
        let spec = make_spec(
            "sum",
            "n",
            rows_frame(FrameBound::CurrentRow, FrameBound::UnboundedFollowing),
        );
        apply_aggregate_window(&mut rows, &indices, &spec).unwrap();
        // row 0: sum 1+2+3+4+5=15
        // row 1: sum 2+3+4+5=14
        // ...
        assert_eq!(rows[0].1["result"], json!(15));
        assert_eq!(rows[1].1["result"], json!(14));
        assert_eq!(rows[4].1["result"], json!(5));
    }

    // ── RANGE ─────────────────────────────────────────────────────────────────

    #[test]
    fn range_unbounded_preceding_current_row_with_ties() {
        // Values: n in [1, 1, 2, 3] — two rows with n=1 share same frame.
        let mut rows = vec![
            ("a".into(), json!({"n": 1i64})),
            ("b".into(), json!({"n": 1i64})),
            ("c".into(), json!({"n": 2i64})),
            ("d".into(), json!({"n": 3i64})),
        ];
        let indices: Vec<usize> = (0..4).collect();
        // Use the fast running path (RANGE UNBOUNDED PRECEDING TO CURRENT ROW)
        // This is the default frame; both rows a and b must see SUM=2 (both
        // peers are included up to CURRENT ROW which expands to the last peer).
        let spec = make_spec(
            "sum",
            "n",
            range_frame(FrameBound::UnboundedPreceding, FrameBound::CurrentRow),
        );
        apply_aggregate_window(&mut rows, &indices, &spec).unwrap();
        // Row a (n=1, pos=0): CURRENT ROW expands to last peer at pos=1, sum=1+1=2
        assert_eq!(rows[0].1["result"], json!(2));
        // Row b (n=1, pos=1): same
        assert_eq!(rows[1].1["result"], json!(2));
        // Row c (n=2): sum=1+1+2=4
        assert_eq!(rows[2].1["result"], json!(4));
        // Row d (n=3): sum=1+1+2+3=7
        assert_eq!(rows[3].1["result"], json!(7));
    }

    // ── SUM / AVG exactness ───────────────────────────────────────────────────

    #[test]
    fn frame_and_running_sum_keep_integers_above_2_pow_53_exact() {
        let vals = [
            json!(9_007_199_254_740_993_i64),
            json!(9_007_199_254_740_992_i64),
        ];
        let indices: Vec<usize> = (0..2).collect();

        let mut rows = keyed(&vals);
        apply_aggregate_window(&mut rows, &indices, &make_spec("sum", "v", whole())).unwrap();
        assert_eq!(rows[0].1["result"], json!(18_014_398_509_481_985_i64));

        let running = range_frame(FrameBound::UnboundedPreceding, FrameBound::CurrentRow);
        let mut rows = keyed(&vals);
        apply_aggregate_window(&mut rows, &indices, &make_spec("sum", "v", running)).unwrap();
        assert_eq!(
            results(&rows),
            vec![
                json!(9_007_199_254_740_993_i64),
                json!(18_014_398_509_481_985_i64)
            ]
        );

        let mut rows = keyed(&vals);
        apply_aggregate_window(&mut rows, &indices, &make_spec("avg", "v", whole())).unwrap();
        assert_eq!(rows[0].1["result"], json!(9_007_199_254_740_992.0_f64));
    }

    #[test]
    fn frame_sum_past_i64_and_u64_is_exact() {
        let vals = [json!(i64::MAX), json!(u64::MAX), json!(u64::MAX)];
        let indices: Vec<usize> = (0..3).collect();
        let mut rows = keyed(&vals);
        apply_aggregate_window(&mut rows, &indices, &make_spec("sum", "v", whole())).unwrap();
        let want = i128::from(i64::MAX) + 2 * i128::from(u64::MAX);
        assert_eq!(rows[0].1["result"], json!(want.to_string()));
    }

    #[test]
    fn frame_sum_mixed_int_float_is_float_and_empty_is_null() {
        let vals = [json!(2), json!(0.5), json!(null)];
        let indices: Vec<usize> = (0..3).collect();
        let mut rows = keyed(&vals);
        apply_aggregate_window(&mut rows, &indices, &make_spec("sum", "v", whole())).unwrap();
        assert_eq!(rows[0].1["result"], json!(2.5));

        let one_row = rows_frame(FrameBound::CurrentRow, FrameBound::CurrentRow);
        let mut rows = keyed(&vals);
        apply_aggregate_window(&mut rows, &indices, &make_spec("sum", "v", one_row)).unwrap();
        assert_eq!(rows[2].1["result"], serde_json::Value::Null);
    }

    // ── MIN / MAX exactness ───────────────────────────────────────────────────

    /// Rows with order key `n` = 1..; `v` carries the given values.
    fn keyed(vals: &[serde_json::Value]) -> Vec<(String, serde_json::Value)> {
        vals.iter()
            .enumerate()
            .map(|(i, v)| (i.to_string(), json!({ "n": i as i64 + 1, "v": v })))
            .collect()
    }

    fn results(rows: &[(String, serde_json::Value)]) -> Vec<serde_json::Value> {
        rows.iter().map(|(_, doc)| doc["result"].clone()).collect()
    }

    fn whole() -> WindowFrame {
        rows_frame(
            FrameBound::UnboundedPreceding,
            FrameBound::UnboundedFollowing,
        )
    }

    #[test]
    fn frame_min_max_keep_integers_above_2_pow_53_exact() {
        let vals = [
            json!(9_007_199_254_740_993_i64),
            json!(9_007_199_254_740_992_i64),
        ];
        let indices: Vec<usize> = (0..2).collect();

        let mut rows = keyed(&vals);
        apply_aggregate_window(&mut rows, &indices, &make_spec("min", "v", whole())).unwrap();
        assert_eq!(rows[0].1["result"], json!(9_007_199_254_740_992_i64));

        let mut rows = keyed(&vals);
        apply_aggregate_window(&mut rows, &indices, &make_spec("max", "v", whole())).unwrap();
        assert_eq!(rows[0].1["result"], json!(9_007_199_254_740_993_i64));
    }

    #[test]
    fn frame_min_max_over_nanosecond_timestamps() {
        let vals = [
            json!(1_700_000_000_000_000_002_i64),
            json!(1_700_000_000_000_000_001_i64),
            json!(1_700_000_000_000_000_003_i64),
        ];
        let indices: Vec<usize> = (0..3).collect();

        let mut rows = keyed(&vals);
        apply_aggregate_window(&mut rows, &indices, &make_spec("min", "v", whole())).unwrap();
        assert_eq!(rows[2].1["result"], json!(1_700_000_000_000_000_001_i64));

        let mut rows = keyed(&vals);
        apply_aggregate_window(&mut rows, &indices, &make_spec("max", "v", whole())).unwrap();
        assert_eq!(rows[2].1["result"], json!(1_700_000_000_000_000_003_i64));
    }

    #[test]
    fn frame_min_max_mixed_int_float_return_original_type() {
        let vals = [
            json!(9_007_199_254_740_993_i64),
            json!(9_007_199_254_740_992.0_f64),
            json!(1.5),
            json!(2),
        ];
        let indices: Vec<usize> = (0..4).collect();

        let mut rows = keyed(&vals);
        apply_aggregate_window(&mut rows, &indices, &make_spec("min", "v", whole())).unwrap();
        assert_eq!(rows[0].1["result"], json!(1.5));

        let mut rows = keyed(&vals);
        apply_aggregate_window(&mut rows, &indices, &make_spec("max", "v", whole())).unwrap();
        assert_eq!(rows[0].1["result"], json!(9_007_199_254_740_993_i64));
    }

    #[test]
    fn running_min_max_keep_integers_exact() {
        let vals = [
            json!(9_007_199_254_740_993_i64),
            json!(9_007_199_254_740_992_i64),
            json!(9_007_199_254_740_995_i64),
        ];
        let indices: Vec<usize> = (0..3).collect();
        let running = || range_frame(FrameBound::UnboundedPreceding, FrameBound::CurrentRow);

        let mut rows = keyed(&vals);
        apply_aggregate_window(&mut rows, &indices, &make_spec("min", "v", running())).unwrap();
        assert_eq!(
            results(&rows),
            vec![
                json!(9_007_199_254_740_993_i64),
                json!(9_007_199_254_740_992_i64),
                json!(9_007_199_254_740_992_i64),
            ]
        );

        let mut rows = keyed(&vals);
        apply_aggregate_window(&mut rows, &indices, &make_spec("max", "v", running())).unwrap();
        assert_eq!(
            results(&rows),
            vec![
                json!(9_007_199_254_740_993_i64),
                json!(9_007_199_254_740_993_i64),
                json!(9_007_199_254_740_995_i64),
            ]
        );
    }

    // ── GROUPS ────────────────────────────────────────────────────────────────

    #[test]
    fn groups_1_preceding_1_following_sum() {
        // Values: [1, 1, 2, 3, 3] — groups [0, 0, 1, 2, 2]
        let mut rows = vec![
            ("a".into(), json!({"n": 1i64})),
            ("b".into(), json!({"n": 1i64})),
            ("c".into(), json!({"n": 2i64})),
            ("d".into(), json!({"n": 3i64})),
            ("e".into(), json!({"n": 3i64})),
        ];
        let indices: Vec<usize> = (0..5).collect();
        let spec = make_spec(
            "sum",
            "n",
            groups_frame(FrameBound::Preceding(1), FrameBound::Following(1)),
        );
        apply_aggregate_window(&mut rows, &indices, &spec).unwrap();
        // pos=0 (group 0): frame → groups 0..=1 → rows 0..=2 → sum=1+1+2=4
        assert_eq!(rows[0].1["result"], json!(4));
        // pos=1 (group 0): same frame
        assert_eq!(rows[1].1["result"], json!(4));
        // pos=2 (group 1): frame → groups 0..=2 → rows 0..=4 → sum=1+1+2+3+3=10
        assert_eq!(rows[2].1["result"], json!(10));
        // pos=3 (group 2): frame → groups 1..=2 → rows 2..=4 → sum=2+3+3=8
        assert_eq!(rows[3].1["result"], json!(8));
        // pos=4 (group 2): same
        assert_eq!(rows[4].1["result"], json!(8));
    }
}
