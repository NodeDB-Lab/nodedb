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
//!
//! A float result keeps NaN and ±Infinity, as in PostgreSQL.

use nodedb_crs::CrsContext;
use nodedb_types::Value;

use super::arg::{ArgValues, arg_at, eval_arg_values};
use super::extremum::value_replaces;
use super::frame::{build_peer_groups, evaluate_frame_bounds};
use super::helpers::{as_f64, set_window_col};
use super::running::running_aggregate;
use super::spec::{FrameBound, WindowFuncSpec};
use crate::numeric_sum::{ExactSum, sum_input};

pub(super) fn apply_aggregate_window(
    rows: &mut [(String, Value)],
    indices: &[usize],
    spec: &WindowFuncSpec,
    crs: &CrsContext<'_>,
) -> Result<(), crate::expr::EvalError> {
    // The argument is an expression evaluated per row, not a column name: a
    // zero divisor inside it fails the statement here rather than folding the
    // aggregate to a silently-wrong total.
    let arg_values = eval_arg_values(rows, indices, spec, 0, crs)?;

    // Fast path: RANGE UNBOUNDED PRECEDING TO CURRENT ROW is the most common
    // pattern (the PostgreSQL default for ordered windows). Use the running
    // accumulator rather than re-aggregating the slice from scratch each row.
    let use_running = spec.frame.mode == "range"
        && matches!(spec.frame.start, FrameBound::UnboundedPreceding)
        && matches!(spec.frame.end, FrameBound::CurrentRow);

    if use_running {
        running_aggregate(rows, indices, spec, &arg_values, crs)?;
        return Ok(());
    }

    per_row_aggregate(rows, indices, spec, &arg_values, crs)
}

/// Per-row frame evaluator.
///
/// For each row position `pos` in the partition:
/// 1. Resolve the concrete `[start_idx, end_idx]` frame slice via
///    `evaluate_frame_bounds`.
/// 2. Aggregate the evaluated argument over `indices[start_idx..=end_idx]`.
/// 3. Write the result back under `spec.alias`.
fn per_row_aggregate(
    rows: &mut [(String, Value)],
    indices: &[usize],
    spec: &WindowFuncSpec,
    arg_values: &ArgValues,
    crs: &CrsContext<'_>,
) -> Result<(), crate::expr::EvalError> {
    let len = indices.len();
    if len == 0 {
        return Ok(());
    }

    // Extract order-by values for RANGE numeric offsets.
    let order_expr = spec.order_by.first().map(|(expr, _)| expr);
    let order_values: Vec<Value> = indices
        .iter()
        .map(|&i| match order_expr {
            Some(expr) => expr.eval(&rows[i].1, crs),
            None => Ok(Value::Null),
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

    let results: Vec<Value> = (0..len)
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
            if let Some(n) = sum_input(v) {
                acc.add_value(&n);
            }
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
) -> Result<Value, crate::expr::EvalError> {
    Ok(match spec.func_name.as_str() {
        "sum" => frame_sum(arg_values, start_idx, end_idx).sum()?,
        // `COUNT(*)` counts frame rows; `COUNT(expr)` counts the rows whose
        // argument is non-NULL, so a NULL argument is excluded rather than
        // inflating the count.
        "count" => match arg_values {
            None => Value::from_u64((end_idx - start_idx + 1) as u64),
            Some(values) => Value::from_u64(
                values[start_idx..=end_idx]
                    .iter()
                    .filter(|v| !v.is_null())
                    .count() as u64,
            ),
        },
        "avg" => frame_sum(arg_values, start_idx, end_idx).avg()?,
        "min" | "max" => {
            let want_max = spec.func_name == "max";
            let Some(values) = arg_values else {
                return Ok(Value::Null);
            };
            let frame_values = values.get(start_idx..=end_idx).unwrap_or(&[]);
            let mut best: Option<&Value> = None;
            for (numeric, candidate) in all_vals[start_idx..=end_idx].iter().zip(frame_values) {
                if numeric.is_none() {
                    continue;
                }
                if value_replaces(candidate, best, want_max) {
                    best = Some(candidate);
                }
            }
            best.cloned().unwrap_or(Value::Null)
        }
        "first_value" => arg_at(arg_values, start_idx),
        "last_value" => arg_at(arg_values, end_idx),
        _ => Value::Null,
    })
}

#[cfg(test)]
mod tests {
    use super::super::spec::{FrameBound, WindowFrame, WindowFuncSpec};
    use super::apply_aggregate_window;
    use crate::expr::SqlExpr;
    use nodedb_crs::CrsContext;
    use nodedb_types::Value;
    use serde_json::json;

    fn crs() -> CrsContext<'static> {
        CrsContext::builtin_only()
    }

    fn v(j: serde_json::Value) -> Value {
        Value::from(j)
    }

    fn res(row: &(String, Value)) -> Value {
        row.1.get("result").cloned().unwrap_or(Value::Null)
    }

    fn numbered(n: usize) -> Vec<(String, Value)> {
        (1..=n)
            .map(|i| (i.to_string(), v(json!({ "n": i as i64 }))))
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
        apply_aggregate_window(&mut rows, &indices, &spec, &crs()).unwrap();
        // row 0 (n=1): sum of [1,2] = 3
        // row 1 (n=2): sum of [1,2,3] = 6
        // row 2 (n=3): sum of [2,3,4] = 9
        // row 3 (n=4): sum of [3,4,5] = 12
        // row 4 (n=5): sum of [4,5] = 9
        assert_eq!(res(&rows[0]), Value::Integer(3));
        assert_eq!(res(&rows[1]), Value::Integer(6));
        assert_eq!(res(&rows[2]), Value::Integer(9));
        assert_eq!(res(&rows[3]), Value::Integer(12));
        assert_eq!(res(&rows[4]), Value::Integer(9));
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
        apply_aggregate_window(&mut rows, &indices, &spec, &crs()).unwrap();
        assert_eq!(res(&rows[0]), Value::Integer(1));
        assert_eq!(res(&rows[1]), Value::Integer(3));
        assert_eq!(res(&rows[2]), Value::Integer(6));
        assert_eq!(res(&rows[3]), Value::Integer(10));
        assert_eq!(res(&rows[4]), Value::Integer(15));
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
        apply_aggregate_window(&mut rows, &indices, &spec, &crs()).unwrap();
        // row 0: sum 1+2+3+4+5=15
        // row 1: sum 2+3+4+5=14
        // ...
        assert_eq!(res(&rows[0]), Value::Integer(15));
        assert_eq!(res(&rows[1]), Value::Integer(14));
        assert_eq!(res(&rows[4]), Value::Integer(5));
    }

    // ── RANGE ─────────────────────────────────────────────────────────────────

    #[test]
    fn range_unbounded_preceding_current_row_with_ties() {
        // Values: n in [1, 1, 2, 3] — two rows with n=1 share same frame.
        let mut rows = vec![
            ("a".into(), v(json!({"n": 1i64}))),
            ("b".into(), v(json!({"n": 1i64}))),
            ("c".into(), v(json!({"n": 2i64}))),
            ("d".into(), v(json!({"n": 3i64}))),
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
        apply_aggregate_window(&mut rows, &indices, &spec, &crs()).unwrap();
        // Row a (n=1, pos=0): CURRENT ROW expands to last peer at pos=1, sum=1+1=2
        assert_eq!(res(&rows[0]), Value::Integer(2));
        // Row b (n=1, pos=1): same
        assert_eq!(res(&rows[1]), Value::Integer(2));
        // Row c (n=2): sum=1+1+2=4
        assert_eq!(res(&rows[2]), Value::Integer(4));
        // Row d (n=3): sum=1+1+2+3=7
        assert_eq!(res(&rows[3]), Value::Integer(7));
    }

    // ── SUM / AVG exactness ───────────────────────────────────────────────────

    #[test]
    fn frame_and_running_sum_keep_integers_above_2_pow_53_exact() {
        let vals = [
            Value::Integer(9_007_199_254_740_993),
            Value::Integer(9_007_199_254_740_992),
        ];
        let indices: Vec<usize> = (0..2).collect();

        let mut rows = keyed(&vals);
        apply_aggregate_window(&mut rows, &indices, &make_spec("sum", "v", whole()), &crs())
            .unwrap();
        assert_eq!(res(&rows[0]), Value::Integer(18_014_398_509_481_985));

        let running = range_frame(FrameBound::UnboundedPreceding, FrameBound::CurrentRow);
        let mut rows = keyed(&vals);
        apply_aggregate_window(&mut rows, &indices, &make_spec("sum", "v", running), &crs())
            .unwrap();
        assert_eq!(
            results(&rows),
            vec![
                Value::Integer(9_007_199_254_740_993),
                Value::Integer(18_014_398_509_481_985)
            ]
        );

        let mut rows = keyed(&vals);
        apply_aggregate_window(&mut rows, &indices, &make_spec("avg", "v", whole()), &crs())
            .unwrap();
        assert_eq!(res(&rows[0]), Value::Float(9_007_199_254_740_992.0));
    }

    #[test]
    fn frame_sum_past_i64_and_u64_is_exact() {
        let vals = [
            Value::Integer(i64::MAX),
            Value::from_u64(u64::MAX),
            Value::from_u64(u64::MAX),
        ];
        let indices: Vec<usize> = (0..3).collect();
        let mut rows = keyed(&vals);
        apply_aggregate_window(&mut rows, &indices, &make_spec("sum", "v", whole()), &crs())
            .unwrap();
        let want = i128::from(i64::MAX) + 2 * i128::from(u64::MAX);
        assert_eq!(
            res(&rows[0]),
            Value::Decimal(rust_decimal::Decimal::from_i128_with_scale(want, 0))
        );
    }

    #[test]
    fn frame_sum_mixed_int_float_is_float_and_empty_is_null() {
        let vals = [Value::Integer(2), Value::Float(0.5), Value::Null];
        let indices: Vec<usize> = (0..3).collect();
        let mut rows = keyed(&vals);
        apply_aggregate_window(&mut rows, &indices, &make_spec("sum", "v", whole()), &crs())
            .unwrap();
        assert_eq!(res(&rows[0]), Value::Float(2.5));

        let one_row = rows_frame(FrameBound::CurrentRow, FrameBound::CurrentRow);
        let mut rows = keyed(&vals);
        apply_aggregate_window(&mut rows, &indices, &make_spec("sum", "v", one_row), &crs())
            .unwrap();
        assert_eq!(res(&rows[2]), Value::Null);
    }

    /// Float overflow is `Infinity` and `Infinity` plus `-Infinity` is NaN;
    /// both reach the window column as floats, as in PostgreSQL.
    #[test]
    fn frame_and_running_sum_keep_non_finite_floats() {
        let indices: Vec<usize> = (0..2).collect();
        let overflow = [Value::Float(1e308), Value::Float(1e308)];

        let mut rows = keyed(&overflow);
        apply_aggregate_window(&mut rows, &indices, &make_spec("sum", "v", whole()), &crs())
            .unwrap();
        assert_eq!(res(&rows[0]), Value::Float(f64::INFINITY));

        let running = range_frame(FrameBound::UnboundedPreceding, FrameBound::CurrentRow);
        let mut rows = keyed(&overflow);
        apply_aggregate_window(&mut rows, &indices, &make_spec("sum", "v", running), &crs())
            .unwrap();
        assert_eq!(
            results(&rows),
            vec![Value::Float(1e308), Value::Float(f64::INFINITY)]
        );

        let cancel = [Value::Float(f64::INFINITY), Value::Float(f64::NEG_INFINITY)];
        let mut rows = keyed(&cancel);
        apply_aggregate_window(&mut rows, &indices, &make_spec("avg", "v", whole()), &crs())
            .unwrap();
        assert!(matches!(res(&rows[0]), Value::Float(f) if f.is_nan()));
    }

    // ── MIN / MAX exactness ───────────────────────────────────────────────────

    /// Rows with order key `n` = 1..; `v` carries the given values.
    fn keyed(vals: &[Value]) -> Vec<(String, Value)> {
        vals.iter()
            .enumerate()
            .map(|(i, val)| {
                let doc = std::collections::HashMap::from([
                    ("n".to_string(), Value::Integer(i as i64 + 1)),
                    ("v".to_string(), val.clone()),
                ]);
                (i.to_string(), Value::Object(doc))
            })
            .collect()
    }

    fn results(rows: &[(String, Value)]) -> Vec<Value> {
        rows.iter().map(res).collect()
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
            Value::Integer(9_007_199_254_740_993),
            Value::Integer(9_007_199_254_740_992),
        ];
        let indices: Vec<usize> = (0..2).collect();

        let mut rows = keyed(&vals);
        apply_aggregate_window(&mut rows, &indices, &make_spec("min", "v", whole()), &crs())
            .unwrap();
        assert_eq!(res(&rows[0]), Value::Integer(9_007_199_254_740_992));

        let mut rows = keyed(&vals);
        apply_aggregate_window(&mut rows, &indices, &make_spec("max", "v", whole()), &crs())
            .unwrap();
        assert_eq!(res(&rows[0]), Value::Integer(9_007_199_254_740_993));
    }

    #[test]
    fn frame_min_max_over_nanosecond_timestamps() {
        let vals = [
            Value::Integer(1_700_000_000_000_000_002),
            Value::Integer(1_700_000_000_000_000_001),
            Value::Integer(1_700_000_000_000_000_003),
        ];
        let indices: Vec<usize> = (0..3).collect();

        let mut rows = keyed(&vals);
        apply_aggregate_window(&mut rows, &indices, &make_spec("min", "v", whole()), &crs())
            .unwrap();
        assert_eq!(res(&rows[2]), Value::Integer(1_700_000_000_000_000_001));

        let mut rows = keyed(&vals);
        apply_aggregate_window(&mut rows, &indices, &make_spec("max", "v", whole()), &crs())
            .unwrap();
        assert_eq!(res(&rows[2]), Value::Integer(1_700_000_000_000_000_003));
    }

    #[test]
    fn frame_min_max_mixed_int_float_return_original_type() {
        let vals = [
            Value::Integer(9_007_199_254_740_993),
            Value::Float(9_007_199_254_740_992.0),
            Value::Float(1.5),
            Value::Integer(2),
        ];
        let indices: Vec<usize> = (0..4).collect();

        let mut rows = keyed(&vals);
        apply_aggregate_window(&mut rows, &indices, &make_spec("min", "v", whole()), &crs())
            .unwrap();
        assert_eq!(res(&rows[0]), Value::Float(1.5));

        let mut rows = keyed(&vals);
        apply_aggregate_window(&mut rows, &indices, &make_spec("max", "v", whole()), &crs())
            .unwrap();
        assert_eq!(res(&rows[0]), Value::Integer(9_007_199_254_740_993));
    }

    /// NaN sorts above every number: MAX over a frame with NaN is NaN, and
    /// MIN skips it.
    #[test]
    fn frame_min_max_place_nan_above_every_number() {
        let vals = [Value::Float(f64::NAN), Value::Integer(3), Value::Float(1.5)];
        let indices: Vec<usize> = (0..3).collect();

        let mut rows = keyed(&vals);
        apply_aggregate_window(&mut rows, &indices, &make_spec("max", "v", whole()), &crs())
            .unwrap();
        assert!(matches!(res(&rows[0]), Value::Float(f) if f.is_nan()));

        let mut rows = keyed(&vals);
        apply_aggregate_window(&mut rows, &indices, &make_spec("min", "v", whole()), &crs())
            .unwrap();
        assert_eq!(res(&rows[0]), Value::Float(1.5));
    }

    #[test]
    fn running_min_max_keep_integers_exact() {
        let vals = [
            Value::Integer(9_007_199_254_740_993),
            Value::Integer(9_007_199_254_740_992),
            Value::Integer(9_007_199_254_740_995),
        ];
        let indices: Vec<usize> = (0..3).collect();
        let running = || range_frame(FrameBound::UnboundedPreceding, FrameBound::CurrentRow);

        let mut rows = keyed(&vals);
        apply_aggregate_window(
            &mut rows,
            &indices,
            &make_spec("min", "v", running()),
            &crs(),
        )
        .unwrap();
        assert_eq!(
            results(&rows),
            vec![
                Value::Integer(9_007_199_254_740_993),
                Value::Integer(9_007_199_254_740_992),
                Value::Integer(9_007_199_254_740_992),
            ]
        );

        let mut rows = keyed(&vals);
        apply_aggregate_window(
            &mut rows,
            &indices,
            &make_spec("max", "v", running()),
            &crs(),
        )
        .unwrap();
        assert_eq!(
            results(&rows),
            vec![
                Value::Integer(9_007_199_254_740_993),
                Value::Integer(9_007_199_254_740_993),
                Value::Integer(9_007_199_254_740_995),
            ]
        );
    }

    // ── GROUPS ────────────────────────────────────────────────────────────────

    #[test]
    fn groups_1_preceding_1_following_sum() {
        // Values: [1, 1, 2, 3, 3] — groups [0, 0, 1, 2, 2]
        let mut rows = vec![
            ("a".into(), v(json!({"n": 1i64}))),
            ("b".into(), v(json!({"n": 1i64}))),
            ("c".into(), v(json!({"n": 2i64}))),
            ("d".into(), v(json!({"n": 3i64}))),
            ("e".into(), v(json!({"n": 3i64}))),
        ];
        let indices: Vec<usize> = (0..5).collect();
        let spec = make_spec(
            "sum",
            "n",
            groups_frame(FrameBound::Preceding(1), FrameBound::Following(1)),
        );
        apply_aggregate_window(&mut rows, &indices, &spec, &crs()).unwrap();
        // pos=0 (group 0): frame → groups 0..=1 → rows 0..=2 → sum=1+1+2=4
        assert_eq!(res(&rows[0]), Value::Integer(4));
        // pos=1 (group 0): same frame
        assert_eq!(res(&rows[1]), Value::Integer(4));
        // pos=2 (group 1): frame → groups 0..=2 → rows 0..=4 → sum=1+1+2+3+3=10
        assert_eq!(res(&rows[2]), Value::Integer(10));
        // pos=3 (group 2): frame → groups 1..=2 → rows 2..=4 → sum=2+3+3=8
        assert_eq!(res(&rows[3]), Value::Integer(8));
        // pos=4 (group 2): same
        assert_eq!(res(&rows[4]), Value::Integer(8));
    }
}
