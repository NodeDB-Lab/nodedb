// SPDX-License-Identifier: Apache-2.0

//! Top-level dispatch for window-function evaluation.

use super::aggregate::apply_aggregate_window;
use super::helpers::build_partitions;
use super::offset::{apply_lag, apply_lead, apply_nth_value};
use super::ranking::{
    apply_cume_dist, apply_dense_rank, apply_ntile, apply_percent_rank, apply_rank,
    apply_row_number,
};
use super::spec::WindowFuncSpec;
use nodedb_crs::CrsContext;

/// Evaluate window functions over sorted, partitioned rows.
///
/// `rows` is the result set. Each row is a `(doc_id, Value::Object)`. The
/// same rows are mutated in place with window columns appended to each
/// document. A window result keeps NaN and ±Infinity. The row array keeps
/// its input order; each spec's partitions are ordered by that spec's own
/// ORDER BY, independent of the row array order.
///
/// Unknown window function names must be rejected by the planner before
/// reaching this dispatcher; an unrecognised name here is an internal bug
/// and panics rather than silently dropping the projection.
///
/// A division/modulo-by-zero in any PARTITION BY / ORDER BY / argument
/// expression propagates as `Err(EvalError::DivisionByZero)` rather than
/// folding to NULL.
///
/// Geo functions in any expression resolve SRIDs through `crs`.
pub fn evaluate_window_functions(
    rows: &mut [(String, nodedb_types::Value)],
    specs: &[WindowFuncSpec],
    crs: &CrsContext<'_>,
) -> Result<(), crate::expr::EvalError> {
    for spec in specs {
        let partitions = build_partitions(rows, &spec.partition_by, &spec.order_by, crs)?;

        for partition_indices in &partitions {
            match spec.func_name.as_str() {
                "row_number" => apply_row_number(rows, partition_indices, &spec.alias),
                "rank" => apply_rank(rows, partition_indices, &spec.alias, &spec.order_by, crs)?,
                "dense_rank" => {
                    apply_dense_rank(rows, partition_indices, &spec.alias, &spec.order_by, crs)?
                }
                "ntile" => apply_ntile(rows, partition_indices, spec),
                "percent_rank" => {
                    apply_percent_rank(rows, partition_indices, &spec.alias, &spec.order_by, crs)?
                }
                "cume_dist" => {
                    apply_cume_dist(rows, partition_indices, &spec.alias, &spec.order_by, crs)?
                }
                "lag" => apply_lag(rows, partition_indices, spec, crs)?,
                "lead" => apply_lead(rows, partition_indices, spec, crs)?,
                "nth_value" => apply_nth_value(rows, partition_indices, spec, crs)?,
                "sum" | "count" | "avg" | "min" | "max" | "first_value" | "last_value" => {
                    apply_aggregate_window(rows, partition_indices, spec, crs)?
                }
                other => {
                    unreachable!(
                        "invariant: SQL planner validates window function names before dispatch; '{other}' is unrecognized and should have been rejected at planning time"
                    )
                }
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::super::spec::{WindowFrame, WindowFuncSpec};
    use super::evaluate_window_functions;
    use crate::expr::SqlExpr;
    use nodedb_types::Value;
    use serde_json::json;

    type Row = (String, Value);

    fn row(id: &str, doc: serde_json::Value) -> Row {
        (id.to_string(), Value::from(doc))
    }

    /// Column `name` of `row` in its JSON form, NULL when absent.
    fn col(row: &Row, name: &str) -> serde_json::Value {
        serde_json::Value::from(row.1.get(name).cloned().unwrap_or(Value::Null))
    }

    fn make_rows() -> Vec<Row> {
        vec![
            row("1", json!({"dept": "eng", "salary": 100, "name": "Alice"})),
            row("2", json!({"dept": "eng", "salary": 120, "name": "Bob"})),
            row("3", json!({"dept": "eng", "salary": 90, "name": "Carol"})),
            row("4", json!({"dept": "sales", "salary": 80, "name": "Dave"})),
            row("5", json!({"dept": "sales", "salary": 110, "name": "Eve"})),
        ]
    }

    fn numbered(n: usize) -> Vec<Row> {
        (1..=n)
            .map(|i| row(&i.to_string(), json!({ "n": i })))
            .collect()
    }

    fn peers() -> Vec<Row> {
        vec![
            row("a", json!({"n": 1})),
            row("b", json!({"n": 1})),
            row("c", json!({"n": 2})),
            row("d", json!({"n": 3})),
        ]
    }

    fn ordered_by_n(alias: &str, func: &str) -> WindowFuncSpec {
        WindowFuncSpec {
            alias: alias.into(),
            func_name: func.into(),
            args: vec![],
            partition_by: vec![],
            order_by: vec![(SqlExpr::Column("n".into()), true)],
            frame: WindowFrame::default(),
        }
    }

    #[test]
    fn row_number_single_partition() {
        let mut rows = make_rows();
        let spec = WindowFuncSpec {
            alias: "rn".into(),
            func_name: "row_number".into(),
            args: vec![],
            partition_by: vec![],
            order_by: vec![],
            frame: WindowFrame::default(),
        };
        evaluate_window_functions(&mut rows, &[spec], &nodedb_crs::CrsContext::builtin_only())
            .unwrap();
        assert_eq!(col(&rows[0], "rn"), json!(1));
        assert_eq!(col(&rows[4], "rn"), json!(5));
    }

    #[test]
    fn row_number_partitioned() {
        let mut rows = make_rows();
        let spec = WindowFuncSpec {
            alias: "rn".into(),
            func_name: "row_number".into(),
            args: vec![],
            partition_by: vec![SqlExpr::Column("dept".into())],
            order_by: vec![],
            frame: WindowFrame::default(),
        };
        evaluate_window_functions(&mut rows, &[spec], &nodedb_crs::CrsContext::builtin_only())
            .unwrap();
        assert_eq!(col(&rows[0], "rn"), json!(1));
        assert_eq!(col(&rows[2], "rn"), json!(3));
        assert_eq!(col(&rows[3], "rn"), json!(1));
        assert_eq!(col(&rows[4], "rn"), json!(2));
    }

    #[test]
    fn running_sum() {
        let mut rows = make_rows();
        let spec = WindowFuncSpec {
            alias: "running_total".into(),
            func_name: "sum".into(),
            args: vec![SqlExpr::Column("salary".into())],
            partition_by: vec![SqlExpr::Column("dept".into())],
            order_by: vec![(SqlExpr::Column("salary".into()), true)],
            frame: WindowFrame::default(),
        };
        evaluate_window_functions(&mut rows, &[spec], &nodedb_crs::CrsContext::builtin_only())
            .unwrap();
        // The frame runs in salary order within each dept, not in row
        // arrival order: eng = Carol(90) → Alice(100) → Bob(120).
        // Integer salaries total exactly as integers.
        assert_eq!(col(&rows[0], "running_total"), json!(190));
        assert_eq!(col(&rows[1], "running_total"), json!(310));
        assert_eq!(col(&rows[2], "running_total"), json!(90));
        assert_eq!(col(&rows[3], "running_total"), json!(80));
        assert_eq!(col(&rows[4], "running_total"), json!(190));
    }

    /// A float overflow reaches the window column as `Infinity`, not NULL.
    #[test]
    fn running_sum_keeps_infinity() {
        let mut rows = vec![
            (
                "a".to_string(),
                Value::Object(
                    [
                        ("n".to_string(), Value::Integer(1)),
                        ("x".to_string(), Value::Float(1e308)),
                    ]
                    .into(),
                ),
            ),
            (
                "b".to_string(),
                Value::Object(
                    [
                        ("n".to_string(), Value::Integer(2)),
                        ("x".to_string(), Value::Float(1e308)),
                    ]
                    .into(),
                ),
            ),
        ];
        let mut spec = ordered_by_n("total", "sum");
        spec.args = vec![SqlExpr::Column("x".into())];
        evaluate_window_functions(&mut rows, &[spec], &nodedb_crs::CrsContext::builtin_only())
            .unwrap();
        assert_eq!(rows[0].1.get("total"), Some(&Value::Float(1e308)));
        assert_eq!(rows[1].1.get("total"), Some(&Value::Float(f64::INFINITY)));
    }

    /// NaN order keys are peers of each other and sort after every number.
    #[test]
    fn nan_order_keys_rank_last_as_peers() {
        let doc = |n: f64| Value::Object([("n".to_string(), Value::Float(n))].into());
        let mut rows = vec![
            ("a".to_string(), doc(f64::NAN)),
            ("b".to_string(), doc(2.0)),
            ("c".to_string(), doc(f64::NAN)),
            ("d".to_string(), doc(f64::INFINITY)),
        ];
        evaluate_window_functions(
            &mut rows,
            &[ordered_by_n("rnk", "rank")],
            &nodedb_crs::CrsContext::builtin_only(),
        )
        .unwrap();
        assert_eq!(col(&rows[1], "rnk"), json!(1));
        assert_eq!(col(&rows[3], "rnk"), json!(2));
        assert_eq!(col(&rows[0], "rnk"), json!(3));
        assert_eq!(col(&rows[2], "rnk"), json!(3));
    }

    #[test]
    fn percent_rank_distinct_keys() {
        let mut rows = numbered(5);
        evaluate_window_functions(
            &mut rows,
            &[ordered_by_n("pr", "percent_rank")],
            &nodedb_crs::CrsContext::builtin_only(),
        )
        .unwrap();
        assert_eq!(col(&rows[0], "pr"), json!(0.0));
        assert_eq!(col(&rows[1], "pr"), json!(0.25));
        assert_eq!(col(&rows[2], "pr"), json!(0.5));
        assert_eq!(col(&rows[3], "pr"), json!(0.75));
        assert_eq!(col(&rows[4], "pr"), json!(1.0));
    }

    #[test]
    fn percent_rank_with_peers() {
        // Peers share the leader's rank, so [1, 1, 2, 3] yields ranks
        // 1, 1, 3, 4 → percent_rank = 0, 0, 2/3, 3/3.
        let mut rows = peers();
        evaluate_window_functions(
            &mut rows,
            &[ordered_by_n("pr", "percent_rank")],
            &nodedb_crs::CrsContext::builtin_only(),
        )
        .unwrap();
        assert_eq!(col(&rows[0], "pr"), json!(0.0));
        assert_eq!(col(&rows[1], "pr"), json!(0.0));
        assert_eq!(col(&rows[2], "pr"), json!(2.0 / 3.0));
        assert_eq!(col(&rows[3], "pr"), json!(1.0));
    }

    #[test]
    fn cume_dist_distinct_keys() {
        let mut rows = numbered(5);
        evaluate_window_functions(
            &mut rows,
            &[ordered_by_n("cd", "cume_dist")],
            &nodedb_crs::CrsContext::builtin_only(),
        )
        .unwrap();
        assert_eq!(col(&rows[0], "cd"), json!(0.2));
        assert_eq!(col(&rows[1], "cd"), json!(0.4));
        assert_eq!(col(&rows[2], "cd"), json!(0.6));
        assert_eq!(col(&rows[3], "cd"), json!(0.8));
        assert_eq!(col(&rows[4], "cd"), json!(1.0));
    }

    #[test]
    fn cume_dist_with_peers() {
        let mut rows = peers();
        evaluate_window_functions(
            &mut rows,
            &[ordered_by_n("cd", "cume_dist")],
            &nodedb_crs::CrsContext::builtin_only(),
        )
        .unwrap();
        // Peers share value of last peer's position / N.
        assert_eq!(col(&rows[0], "cd"), json!(0.5));
        assert_eq!(col(&rows[1], "cd"), json!(0.5));
        assert_eq!(col(&rows[2], "cd"), json!(0.75));
        assert_eq!(col(&rows[3], "cd"), json!(1.0));
    }

    #[test]
    fn nth_value_returns_nth_then_holds() {
        let mut rows = numbered(5);
        let mut spec = ordered_by_n("nv", "nth_value");
        spec.args = vec![
            SqlExpr::Column("n".into()),
            SqlExpr::Literal(Value::Integer(2)),
        ];
        evaluate_window_functions(&mut rows, &[spec], &nodedb_crs::CrsContext::builtin_only())
            .unwrap();
        assert_eq!(col(&rows[0], "nv"), json!(null));
        assert_eq!(col(&rows[1], "nv"), json!(2));
        assert_eq!(col(&rows[2], "nv"), json!(2));
        assert_eq!(col(&rows[3], "nv"), json!(2));
        assert_eq!(col(&rows[4], "nv"), json!(2));
    }

    #[test]
    fn rank_orders_by_spec_order_by_not_row_arrival_order() {
        // Rows arrive as Alice(100), Bob(120), Carol(90) within dept "eng" —
        // not sorted by salary. RANK() OVER (ORDER BY salary DESC) must rank
        // by salary, and the row array order must stay unchanged.
        let mut rows = make_rows();
        let spec = WindowFuncSpec {
            alias: "rnk".into(),
            func_name: "rank".into(),
            args: vec![],
            partition_by: vec![SqlExpr::Column("dept".into())],
            order_by: vec![(SqlExpr::Column("salary".into()), false)],
            frame: WindowFrame::default(),
        };
        evaluate_window_functions(&mut rows, &[spec], &nodedb_crs::CrsContext::builtin_only())
            .unwrap();
        assert_eq!(col(&rows[0], "name"), json!("Alice"));
        assert_eq!(col(&rows[1], "name"), json!("Bob"));
        assert_eq!(col(&rows[2], "name"), json!("Carol"));
        assert_eq!(col(&rows[3], "name"), json!("Dave"));
        assert_eq!(col(&rows[4], "name"), json!("Eve"));
        assert_eq!(col(&rows[0], "rnk"), json!(2)); // Alice, salary 100
        assert_eq!(col(&rows[1], "rnk"), json!(1)); // Bob, salary 120
        assert_eq!(col(&rows[2], "rnk"), json!(3)); // Carol, salary 90
        assert_eq!(col(&rows[3], "rnk"), json!(2)); // Dave, salary 80
        assert_eq!(col(&rows[4], "rnk"), json!(1)); // Eve, salary 110
    }

    #[test]
    #[should_panic(expected = "should have been rejected at planning time")]
    fn unknown_function_panics_at_evaluator() {
        let mut rows = numbered(2);
        let spec = WindowFuncSpec {
            alias: "x".into(),
            func_name: "frobnicate".into(),
            args: vec![],
            partition_by: vec![],
            order_by: vec![],
            frame: WindowFrame::default(),
        };
        evaluate_window_functions(&mut rows, &[spec], &nodedb_crs::CrsContext::builtin_only())
            .unwrap();
    }
}
