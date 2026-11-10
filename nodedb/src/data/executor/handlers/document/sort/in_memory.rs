// SPDX-License-Identifier: BUSL-1.1

//! In-memory document sort.

use super::compare::{SortValues, compare_sort_values, eval_sort_values};
use nodedb_physical::physical_plan::SortKeySpec;

/// Sort stored rows by the ORDER BY terms. `decimal_keys` flags the keys
/// `decimal_sort_keys` found `DECIMAL`, and is empty for a collection with
/// no strict schema.
pub(in crate::data::executor) fn sort_rows(
    rows: &mut [(String, Vec<u8>)],
    sort_keys: &[SortKeySpec],
    decimal_keys: &[bool],
    crs: &nodedb_crs::CrsContext<'_>,
) -> crate::Result<()> {
    if sort_keys.is_empty() {
        return Ok(());
    }

    sort_rows_by_values(rows, sort_keys, decimal_keys, crs)
}

/// Evaluate native keys once per row before comparison.
fn sort_rows_by_values(
    rows: &mut [(String, Vec<u8>)],
    sort_keys: &[SortKeySpec],
    decimal_keys: &[bool],
    crs: &nodedb_crs::CrsContext<'_>,
) -> crate::Result<()> {
    let values: Vec<SortValues> = rows
        .iter()
        .map(|(_, bytes)| eval_sort_values(bytes, sort_keys, crs))
        .collect::<crate::Result<Vec<_>>>()?;

    let mut indices: Vec<usize> = (0..rows.len()).collect();
    indices
        .sort_by(|&ai, &bi| compare_sort_values(&values[ai], &values[bi], sort_keys, decimal_keys));

    drop(values);
    apply_permutation(rows, indices)
}

/// Apply a permutation to rows using the sorted index order.
///
/// `indices[i]` = the original row index that should appear at position `i`.
///
/// Returns `Err` if `indices` is not a valid permutation of `0..rows.len()`:
/// an out-of-range index or a duplicate (slot already consumed) both surface as
/// `crate::Error::Internal` rather than silently producing sentinel rows.
pub(in crate::data::executor) fn apply_permutation(
    rows: &mut [(String, Vec<u8>)],
    indices: Vec<usize>,
) -> crate::Result<()> {
    // Wrap each row in `Option` so we can move individual elements out by
    // index without cloning. Each slot is taken exactly once during the
    // scatter, so no element is ever double-moved.
    let mut src: Vec<Option<(String, Vec<u8>)>> =
        rows.iter_mut().map(|r| Some(std::mem::take(r))).collect();
    let n = src.len();
    for (target_pos, &src_idx) in indices.iter().enumerate() {
        // Checked access: out-of-range index is an invariant violation.
        let slot = src.get_mut(src_idx).ok_or_else(|| crate::Error::Internal {
            detail: format!(
                "apply_permutation: index {src_idx} out of range (len={n}, target_pos={target_pos})"
            ),
        })?;
        // None means this slot was already consumed — duplicate index in `indices`.
        let row = slot.take().ok_or_else(|| crate::Error::Internal {
            detail: format!(
                "apply_permutation: duplicate index {src_idx} at target_pos={target_pos} (len={n})"
            ),
        })?;
        rows[target_pos] = row;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn encode(v: &serde_json::Value) -> Vec<u8> {
        nodedb_types::json_msgpack::json_to_msgpack(v).expect("encode")
    }

    fn col(name: &str, asc: bool) -> SortKeySpec {
        SortKeySpec::column(name, asc)
    }

    fn rows_with_vals(vals: &[(&str, i64)]) -> Vec<(String, Vec<u8>)> {
        vals.iter()
            .map(|(id, v)| {
                (
                    (*id).to_string(),
                    encode(&serde_json::json!({"id": id, "val": v})),
                )
            })
            .collect()
    }

    #[test]
    fn sort_by_int_field_asc() {
        let mut rows = rows_with_vals(&[("a", 30), ("b", 10), ("c", 20)]);
        sort_rows(
            &mut rows,
            &[col("val", true)],
            &[],
            &nodedb_crs::CrsContext::builtin_only(),
        )
        .expect("sort_rows failed");
        let order: Vec<&str> = rows.iter().map(|(id, _)| id.as_str()).collect();
        assert_eq!(order, vec!["b", "c", "a"], "ASC by val: 10, 20, 30");
    }

    /// A `DECIMAL` key orders its text and `uint64` cells by value, on both
    /// the column and the expression comparator.
    #[test]
    fn decimal_key_orders_text_and_uint64_cells_by_value() {
        let rows = || -> Vec<(String, Vec<u8>)> {
            [
                ("max", serde_json::json!(u64::MAX)),
                ("mid", serde_json::json!(9_223_372_036_854_775_808_u64)),
                ("small", serde_json::json!("5")),
                ("ten", serde_json::json!("10")),
            ]
            .into_iter()
            .map(|(id, v)| (id.to_string(), encode(&serde_json::json!({ "v": v }))))
            .collect()
        };
        let mut by_column = rows();
        sort_rows(
            &mut by_column,
            &[col("v", true)],
            &[true],
            &nodedb_crs::CrsContext::builtin_only(),
        )
        .expect("sort_rows failed");
        let order: Vec<&str> = by_column.iter().map(|(id, _)| id.as_str()).collect();
        assert_eq!(order, vec!["small", "ten", "mid", "max"]);

        let key = SortKeySpec {
            expr: nodedb_query::SqlExpr::Column("v".into()),
            ascending: true,
            nulls_first: false,
        };
        let computed = SortKeySpec {
            expr: nodedb_query::SqlExpr::Literal(nodedb_types::Value::Integer(1)),
            ascending: true,
            nulls_first: false,
        };
        let mut by_expression = rows();
        sort_rows(
            &mut by_expression,
            &[key, computed],
            &[true, false],
            &nodedb_crs::CrsContext::builtin_only(),
        )
        .expect("sort_rows failed");
        let order: Vec<&str> = by_expression.iter().map(|(id, _)| id.as_str()).collect();
        assert_eq!(order, vec!["small", "ten", "mid", "max"]);
    }

    #[test]
    fn sort_by_int_field_desc() {
        let mut rows = rows_with_vals(&[("a", 30), ("b", 10), ("c", 20)]);
        sort_rows(
            &mut rows,
            &[col("val", false)],
            &[],
            &nodedb_crs::CrsContext::builtin_only(),
        )
        .expect("sort_rows failed");
        let order: Vec<&str> = rows.iter().map(|(id, _)| id.as_str()).collect();
        assert_eq!(order, vec!["a", "c", "b"], "DESC by val: 30, 20, 10");
    }

    #[test]
    fn sort_by_string_field_asc() {
        let mut rows = vec![
            ("a".into(), encode(&serde_json::json!({"name": "charlie"}))),
            ("b".into(), encode(&serde_json::json!({"name": "alpha"}))),
            ("c".into(), encode(&serde_json::json!({"name": "bravo"}))),
        ];
        sort_rows(
            &mut rows,
            &[col("name", true)],
            &[],
            &nodedb_crs::CrsContext::builtin_only(),
        )
        .expect("sort_rows failed");
        let order: Vec<&str> = rows.iter().map(|(id, _)| id.as_str()).collect();
        assert_eq!(order, vec!["b", "c", "a"]);
    }

    /// A computed key sorts by the evaluated value, not by any stored field.
    /// `100 / val` inverts the ordering of `val`, so a sort that silently fell
    /// back to the raw column — or dropped the key — cannot produce this order.
    #[test]
    fn sort_by_computed_expression_orders_by_evaluated_value() {
        let mut rows = rows_with_vals(&[("a", 2), ("b", 20), ("c", 5)]);
        let key = SortKeySpec {
            expr: nodedb_query::SqlExpr::BinaryOp {
                left: Box::new(nodedb_query::SqlExpr::Literal(
                    nodedb_types::Value::Integer(100),
                )),
                op: nodedb_query::BinaryOp::Div,
                right: Box::new(nodedb_query::SqlExpr::Column("val".into())),
            },
            ascending: true,
            nulls_first: false,
        };
        sort_rows(
            &mut rows,
            &[key],
            &[],
            &nodedb_crs::CrsContext::builtin_only(),
        )
        .expect("sort_rows failed");
        let order: Vec<&str> = rows.iter().map(|(id, _)| id.as_str()).collect();
        // 100/20 = 5, 100/5 = 20, 100/2 = 50.
        assert_eq!(order, vec!["b", "c", "a"]);
    }

    /// A zero divisor inside a sort key fails the sort instead of ordering the
    /// row under a folded NULL.
    #[test]
    fn sort_by_computed_expression_propagates_division_by_zero() {
        let mut rows = rows_with_vals(&[("a", 2), ("b", 0)]);
        let key = SortKeySpec {
            expr: nodedb_query::SqlExpr::BinaryOp {
                left: Box::new(nodedb_query::SqlExpr::Literal(
                    nodedb_types::Value::Integer(100),
                )),
                op: nodedb_query::BinaryOp::Div,
                right: Box::new(nodedb_query::SqlExpr::Column("val".into())),
            },
            ascending: true,
            nulls_first: false,
        };
        sort_rows(
            &mut rows,
            &[key],
            &[],
            &nodedb_crs::CrsContext::builtin_only(),
        )
        .expect_err("zero divisor in a sort key must fail the sort");
    }

    #[test]
    fn apply_permutation_valid_reorders_correctly() {
        let mut rows = rows_with_vals(&[("a", 1), ("b", 2), ("c", 3)]);
        apply_permutation(&mut rows, vec![2, 0, 1]).expect("valid permutation");
        let order: Vec<&str> = rows.iter().map(|(id, _)| id.as_str()).collect();
        assert_eq!(order, vec!["c", "a", "b"]);
    }

    #[test]
    fn apply_permutation_duplicate_index_errors_not_sentinel() {
        let mut rows = rows_with_vals(&[("a", 1), ("b", 2)]);
        apply_permutation(&mut rows, vec![0, 0]).expect_err("duplicate index must error");
    }

    #[test]
    fn apply_permutation_out_of_range_index_errors() {
        let mut rows = rows_with_vals(&[("a", 1), ("b", 2)]);
        apply_permutation(&mut rows, vec![0, 5]).expect_err("out-of-range index must error");
    }
    fn native_rows(values: &[(&str, nodedb_types::Value)]) -> Vec<(String, Vec<u8>)> {
        values
            .iter()
            .map(|(id, value)| {
                let doc = nodedb_types::Value::Object(
                    [("val".into(), value.clone())].into_iter().collect(),
                );
                (
                    (*id).to_string(),
                    zerompk::to_msgpack_vec(&doc).expect("native body"),
                )
            })
            .collect()
    }

    #[test]
    fn native_numeric_keys_keep_exact_order_and_stable_ties() {
        use nodedb_types::Value;
        let mut rows = native_rows(&[
            (
                "high",
                Value::Decimal(rust_decimal::Decimal::from_i128_with_scale(
                    90071992547409930001,
                    4,
                )),
            ),
            ("integer", Value::Integer(9_007_199_254_740_993)),
            ("float", Value::Float(9_007_199_254_740_992.0)),
            ("tie", Value::Integer(9_007_199_254_740_993)),
        ]);
        sort_rows(
            &mut rows,
            &[col("val", true)],
            &[],
            &nodedb_crs::CrsContext::builtin_only(),
        )
        .expect("sort");
        assert_eq!(
            rows.iter().map(|(id, _)| id.as_str()).collect::<Vec<_>>(),
            ["float", "integer", "tie", "high"]
        );
    }

    #[test]
    fn native_and_plain_mixed_keys_retain_each_sort_strategy() {
        use nodedb_types::Value;
        let values = [
            ("integer", Value::Integer(3)),
            ("string", Value::String("2".into())),
            ("null", Value::Null),
            ("false", Value::Bool(false)),
        ];
        for computed in [false, true] {
            let mut keys = vec![col("val", true)];
            if computed {
                keys.push(SortKeySpec {
                    expr: nodedb_query::SqlExpr::Literal(Value::Integer(1)),
                    ascending: true,
                    nulls_first: false,
                });
            }
            let mut native = native_rows(&values);
            native.push((
                "missing".into(),
                zerompk::to_msgpack_vec(&Value::Object(Default::default())).expect("missing"),
            ));
            let mut plain = values
                .iter()
                .map(|(id, value)| {
                    let doc = Value::Object([("val".into(), value.clone())].into_iter().collect());
                    (
                        (*id).to_string(),
                        nodedb_types::value_to_msgpack(&doc).expect("plain"),
                    )
                })
                .collect::<Vec<_>>();
            plain.push(("missing".into(), encode(&serde_json::json!({}))));
            for rows in [&mut native, &mut plain] {
                sort_rows(rows, &keys, &[], &nodedb_crs::CrsContext::builtin_only()).expect("sort");
                let expected = if computed {
                    vec!["string", "integer", "false", "null", "missing"]
                } else {
                    vec!["false", "integer", "string", "null", "missing"]
                };
                assert_eq!(
                    rows.iter().map(|(id, _)| id.as_str()).collect::<Vec<_>>(),
                    expected
                );
            }
        }
    }

    #[test]
    fn malformed_native_body_errors_before_rows_move() {
        let mut rows = native_rows(&[("valid", nodedb_types::Value::Integer(1))]);
        rows.push(("invalid".into(), vec![0x92, 0x01]));
        let before = rows.clone();
        assert!(
            sort_rows(
                &mut rows,
                &[col("val", true)],
                &[],
                &nodedb_crs::CrsContext::builtin_only()
            )
            .is_err()
        );
        assert_eq!(rows, before);
    }
}
