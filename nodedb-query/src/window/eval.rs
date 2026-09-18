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

/// Evaluate window functions over sorted, partitioned rows.
///
/// `rows` is the sorted result set. Each row is a `(doc_id, serde_json::Value)`.
/// The same rows are mutated in place with window columns appended to each
/// document. The row array keeps its input order; each spec's partitions are
/// ordered by that spec's own ORDER BY, independent of the row array order.
///
/// Unknown window function names must be rejected by the planner before
/// reaching this dispatcher; an unrecognised name here is an internal bug
/// and panics rather than silently dropping the projection.
///
/// A division/modulo-by-zero in any PARTITION BY / ORDER BY / argument
/// expression propagates as `Err(EvalError::DivisionByZero)` rather than
/// folding to NULL.
pub fn evaluate_window_functions(
    rows: &mut [(String, serde_json::Value)],
    specs: &[WindowFuncSpec],
) -> Result<(), crate::expr::EvalError> {
    for spec in specs {
        let partitions = build_partitions(rows, &spec.partition_by, &spec.order_by)?;

        for partition_indices in &partitions {
            match spec.func_name.as_str() {
                "row_number" => apply_row_number(rows, partition_indices, &spec.alias),
                "rank" => apply_rank(rows, partition_indices, &spec.alias, &spec.order_by)?,
                "dense_rank" => {
                    apply_dense_rank(rows, partition_indices, &spec.alias, &spec.order_by)?
                }
                "ntile" => apply_ntile(rows, partition_indices, spec),
                "percent_rank" => {
                    apply_percent_rank(rows, partition_indices, &spec.alias, &spec.order_by)?
                }
                "cume_dist" => {
                    apply_cume_dist(rows, partition_indices, &spec.alias, &spec.order_by)?
                }
                "lag" => apply_lag(rows, partition_indices, spec)?,
                "lead" => apply_lead(rows, partition_indices, spec)?,
                "nth_value" => apply_nth_value(rows, partition_indices, spec)?,
                "sum" | "count" | "avg" | "min" | "max" | "first_value" | "last_value" => {
                    apply_aggregate_window(rows, partition_indices, spec)?
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
    use serde_json::json;

    fn make_rows() -> Vec<(String, serde_json::Value)> {
        vec![
            (
                "1".into(),
                json!({"dept": "eng", "salary": 100, "name": "Alice"}),
            ),
            (
                "2".into(),
                json!({"dept": "eng", "salary": 120, "name": "Bob"}),
            ),
            (
                "3".into(),
                json!({"dept": "eng", "salary": 90, "name": "Carol"}),
            ),
            (
                "4".into(),
                json!({"dept": "sales", "salary": 80, "name": "Dave"}),
            ),
            (
                "5".into(),
                json!({"dept": "sales", "salary": 110, "name": "Eve"}),
            ),
        ]
    }

    fn numbered(n: usize) -> Vec<(String, serde_json::Value)> {
        (1..=n)
            .map(|i| (i.to_string(), json!({ "n": i })))
            .collect()
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
        evaluate_window_functions(&mut rows, &[spec]).unwrap();
        assert_eq!(rows[0].1["rn"], json!(1));
        assert_eq!(rows[4].1["rn"], json!(5));
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
        evaluate_window_functions(&mut rows, &[spec]).unwrap();
        assert_eq!(rows[0].1["rn"], json!(1));
        assert_eq!(rows[2].1["rn"], json!(3));
        assert_eq!(rows[3].1["rn"], json!(1));
        assert_eq!(rows[4].1["rn"], json!(2));
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
        evaluate_window_functions(&mut rows, &[spec]).unwrap();
        // The frame runs in salary order within each dept, not in row
        // arrival order: eng = Carol(90) → Alice(100) → Bob(120).
        assert_eq!(rows[0].1["running_total"], json!(190.0));
        assert_eq!(rows[1].1["running_total"], json!(310.0));
        assert_eq!(rows[2].1["running_total"], json!(90.0));
        assert_eq!(rows[3].1["running_total"], json!(80.0));
        assert_eq!(rows[4].1["running_total"], json!(190.0));
    }

    #[test]
    fn percent_rank_distinct_keys() {
        let mut rows = numbered(5);
        let spec = WindowFuncSpec {
            alias: "pr".into(),
            func_name: "percent_rank".into(),
            args: vec![],
            partition_by: vec![],
            order_by: vec![(SqlExpr::Column("n".into()), true)],
            frame: WindowFrame::default(),
        };
        evaluate_window_functions(&mut rows, &[spec]).unwrap();
        assert_eq!(rows[0].1["pr"], json!(0.0));
        assert_eq!(rows[1].1["pr"], json!(0.25));
        assert_eq!(rows[2].1["pr"], json!(0.5));
        assert_eq!(rows[3].1["pr"], json!(0.75));
        assert_eq!(rows[4].1["pr"], json!(1.0));
    }

    #[test]
    fn percent_rank_with_peers() {
        // Peers share the leader's rank, so [1, 1, 2, 3] yields ranks
        // 1, 1, 3, 4 → percent_rank = 0, 0, 2/3, 3/3.
        let mut rows = vec![
            ("a".into(), json!({"n": 1})),
            ("b".into(), json!({"n": 1})),
            ("c".into(), json!({"n": 2})),
            ("d".into(), json!({"n": 3})),
        ];
        let spec = WindowFuncSpec {
            alias: "pr".into(),
            func_name: "percent_rank".into(),
            args: vec![],
            partition_by: vec![],
            order_by: vec![(SqlExpr::Column("n".into()), true)],
            frame: WindowFrame::default(),
        };
        evaluate_window_functions(&mut rows, &[spec]).unwrap();
        assert_eq!(rows[0].1["pr"], json!(0.0));
        assert_eq!(rows[1].1["pr"], json!(0.0));
        assert_eq!(rows[2].1["pr"], json!(2.0 / 3.0));
        assert_eq!(rows[3].1["pr"], json!(1.0));
    }

    #[test]
    fn cume_dist_distinct_keys() {
        let mut rows = numbered(5);
        let spec = WindowFuncSpec {
            alias: "cd".into(),
            func_name: "cume_dist".into(),
            args: vec![],
            partition_by: vec![],
            order_by: vec![(SqlExpr::Column("n".into()), true)],
            frame: WindowFrame::default(),
        };
        evaluate_window_functions(&mut rows, &[spec]).unwrap();
        assert_eq!(rows[0].1["cd"], json!(0.2));
        assert_eq!(rows[1].1["cd"], json!(0.4));
        assert_eq!(rows[2].1["cd"], json!(0.6));
        assert_eq!(rows[3].1["cd"], json!(0.8));
        assert_eq!(rows[4].1["cd"], json!(1.0));
    }

    #[test]
    fn cume_dist_with_peers() {
        let mut rows = vec![
            ("a".into(), json!({"n": 1})),
            ("b".into(), json!({"n": 1})),
            ("c".into(), json!({"n": 2})),
            ("d".into(), json!({"n": 3})),
        ];
        let spec = WindowFuncSpec {
            alias: "cd".into(),
            func_name: "cume_dist".into(),
            args: vec![],
            partition_by: vec![],
            order_by: vec![(SqlExpr::Column("n".into()), true)],
            frame: WindowFrame::default(),
        };
        evaluate_window_functions(&mut rows, &[spec]).unwrap();
        // Peers share value of last peer's position / N.
        assert_eq!(rows[0].1["cd"], json!(0.5));
        assert_eq!(rows[1].1["cd"], json!(0.5));
        assert_eq!(rows[2].1["cd"], json!(0.75));
        assert_eq!(rows[3].1["cd"], json!(1.0));
    }

    #[test]
    fn nth_value_returns_nth_then_holds() {
        let mut rows = numbered(5);
        let spec = WindowFuncSpec {
            alias: "nv".into(),
            func_name: "nth_value".into(),
            args: vec![
                SqlExpr::Column("n".into()),
                SqlExpr::Literal(nodedb_types::Value::Integer(2)),
            ],
            partition_by: vec![],
            order_by: vec![(SqlExpr::Column("n".into()), true)],
            frame: WindowFrame::default(),
        };
        evaluate_window_functions(&mut rows, &[spec]).unwrap();
        assert_eq!(rows[0].1["nv"], json!(null));
        assert_eq!(rows[1].1["nv"], json!(2));
        assert_eq!(rows[2].1["nv"], json!(2));
        assert_eq!(rows[3].1["nv"], json!(2));
        assert_eq!(rows[4].1["nv"], json!(2));
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
        evaluate_window_functions(&mut rows, &[spec]).unwrap();
        assert_eq!(rows[0].1["name"], json!("Alice"));
        assert_eq!(rows[1].1["name"], json!("Bob"));
        assert_eq!(rows[2].1["name"], json!("Carol"));
        assert_eq!(rows[3].1["name"], json!("Dave"));
        assert_eq!(rows[4].1["name"], json!("Eve"));
        assert_eq!(rows[0].1["rnk"], json!(2)); // Alice, salary 100
        assert_eq!(rows[1].1["rnk"], json!(1)); // Bob, salary 120
        assert_eq!(rows[2].1["rnk"], json!(3)); // Carol, salary 90
        assert_eq!(rows[3].1["rnk"], json!(2)); // Dave, salary 80
        assert_eq!(rows[4].1["rnk"], json!(1)); // Eve, salary 110
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
        evaluate_window_functions(&mut rows, &[spec]).unwrap();
    }
}
