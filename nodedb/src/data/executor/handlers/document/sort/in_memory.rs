// SPDX-License-Identifier: BUSL-1.1

//! In-memory document sort.

use nodedb_physical::physical_plan::SortKeySpec;
use nodedb_query::msgpack_scan;

use super::compare::{
    SortValues, all_column_keys, compare_cells, compare_sort_values, eval_sort_values,
    is_decimal_key, is_null_range,
};

/// Pre-extracted sort key offsets for a single row.
/// Each entry is `Option<(usize, usize)>` — byte range of the sort key value.
type SortKeyOffsets = Vec<Option<(usize, usize)>>;

/// Sort stored rows by the ORDER BY terms. `decimal_keys` flags the keys
/// `decimal_sort_keys` found `DECIMAL`, and is empty for a collection with
/// no strict schema.
pub(in crate::data::executor) fn sort_rows(
    rows: &mut [(String, Vec<u8>)],
    sort_keys: &[SortKeySpec],
    decimal_keys: &[bool],
) -> crate::Result<()> {
    if sort_keys.is_empty() {
        return Ok(());
    }

    if all_column_keys(sort_keys) {
        return sort_rows_by_column(rows, sort_keys, decimal_keys);
    }
    sort_rows_by_expression(rows, sort_keys, decimal_keys)
}

/// Sort decoded document rows by the ORDER BY terms, with the same ordering
/// rules [`sort_rows`] applies to stored rows.
///
/// A scan with window functions sorts after the window pass, so ORDER BY can
/// name a window alias; its rows are decoded by then. Each row is encoded
/// once under its position, sorted by [`sort_rows`], and taken back in the
/// sorted order.
pub(in crate::data::executor) fn sort_decoded_rows(
    rows: Vec<(String, serde_json::Value)>,
    sort_keys: &[SortKeySpec],
    decimal_keys: &[bool],
) -> crate::Result<Vec<(String, serde_json::Value)>> {
    if sort_keys.is_empty() {
        return Ok(rows);
    }
    let mut keyed: Vec<(String, Vec<u8>)> = rows
        .iter()
        .enumerate()
        .map(|(i, (_, doc))| (i.to_string(), nodedb_types::json_to_msgpack_or_empty(doc)))
        .collect();
    sort_rows(&mut keyed, sort_keys, decimal_keys)?;
    let mut slots: Vec<Option<(String, serde_json::Value)>> = rows.into_iter().map(Some).collect();
    keyed
        .iter()
        .map(|(position, _)| {
            position
                .parse::<usize>()
                .ok()
                .and_then(|i| slots.get_mut(i).and_then(Option::take))
                .ok_or_else(|| crate::Error::Internal {
                    detail: format!("sort_decoded_rows: row position {position} is not a live row"),
                })
        })
        .collect()
}

/// Zero-decode path: every key names a stored field, so ordering is decided
/// straight from the msgpack bytes.
fn sort_rows_by_column(
    rows: &mut [(String, Vec<u8>)],
    sort_keys: &[SortKeySpec],
    decimal_keys: &[bool],
) -> crate::Result<()> {
    // Pre-extract sort key offsets for all rows — one scan per row instead
    // of O(N log N) scans during comparisons.
    let key_offsets: Vec<SortKeyOffsets> = rows
        .iter()
        .map(|(_, bytes)| {
            sort_keys
                .iter()
                .map(|k| {
                    k.as_column()
                        .and_then(|f| msgpack_scan::extract_field(bytes, 0, f))
                })
                .collect()
        })
        .collect();

    let mut indices: Vec<usize> = (0..rows.len()).collect();
    indices.sort_by(|&ai, &bi| {
        compare_with_preextracted(
            &rows[ai].1,
            &key_offsets[ai],
            &rows[bi].1,
            &key_offsets[bi],
            sort_keys,
            decimal_keys,
        )
    });

    // Apply permutation in-place. `key_offsets` is no longer needed after
    // sorting the index; it is dropped here.
    drop(key_offsets);
    apply_permutation(rows, indices)
}

/// Computed-key path: evaluate every key once per row, then order by the
/// resulting values.
fn sort_rows_by_expression(
    rows: &mut [(String, Vec<u8>)],
    sort_keys: &[SortKeySpec],
    decimal_keys: &[bool],
) -> crate::Result<()> {
    let values: Vec<SortValues> = rows
        .iter()
        .map(|(_, bytes)| eval_sort_values(bytes, sort_keys))
        .collect::<crate::Result<Vec<_>>>()?;

    let mut indices: Vec<usize> = (0..rows.len()).collect();
    indices
        .sort_by(|&ai, &bi| compare_sort_values(&values[ai], &values[bi], sort_keys, decimal_keys));

    drop(values);
    apply_permutation(rows, indices)
}

/// Compare two docs using pre-extracted sort key offsets.
fn compare_with_preextracted(
    a_bytes: &[u8],
    a_offsets: &[Option<(usize, usize)>],
    b_bytes: &[u8],
    b_offsets: &[Option<(usize, usize)>],
    sort_keys: &[SortKeySpec],
    decimal_keys: &[bool],
) -> std::cmp::Ordering {
    for (i, key) in sort_keys.iter().enumerate() {
        let ordered = match key.order_nulls(
            is_null_range(a_bytes, a_offsets[i]),
            is_null_range(b_bytes, b_offsets[i]),
        ) {
            Some(ord) => ord,
            None => match (a_offsets[i], b_offsets[i]) {
                (Some(ar), Some(br)) => key.direct(compare_cells(
                    a_bytes,
                    ar,
                    b_bytes,
                    br,
                    is_decimal_key(decimal_keys, i),
                )),
                _ => std::cmp::Ordering::Equal,
            },
        };
        if ordered != std::cmp::Ordering::Equal {
            return ordered;
        }
    }
    std::cmp::Ordering::Equal
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
        sort_rows(&mut rows, &[col("val", true)], &[]).expect("sort_rows failed");
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
        sort_rows(&mut by_column, &[col("v", true)], &[true]).expect("sort_rows failed");
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
        sort_rows(&mut by_expression, &[key, computed], &[true, false]).expect("sort_rows failed");
        let order: Vec<&str> = by_expression.iter().map(|(id, _)| id.as_str()).collect();
        assert_eq!(order, vec!["small", "ten", "mid", "max"]);
    }

    /// Decoded rows sort on a field the window pass appended, and keep their
    /// document ids.
    #[test]
    fn sort_decoded_rows_orders_by_an_appended_column() {
        let rows = vec![
            ("a".to_string(), serde_json::json!({"id": "a", "rn": 3})),
            ("b".to_string(), serde_json::json!({"id": "b", "rn": 1})),
            ("c".to_string(), serde_json::json!({"id": "c", "rn": 2})),
        ];
        let sorted = sort_decoded_rows(rows, &[col("rn", true)], &[]).expect("sort_decoded_rows");
        let order: Vec<&str> = sorted.iter().map(|(id, _)| id.as_str()).collect();
        assert_eq!(order, vec!["b", "c", "a"]);
        assert_eq!(sorted[0].1, serde_json::json!({"id": "b", "rn": 1}));
    }

    #[test]
    fn sort_by_int_field_desc() {
        let mut rows = rows_with_vals(&[("a", 30), ("b", 10), ("c", 20)]);
        sort_rows(&mut rows, &[col("val", false)], &[]).expect("sort_rows failed");
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
        sort_rows(&mut rows, &[col("name", true)], &[]).expect("sort_rows failed");
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
        sort_rows(&mut rows, &[key], &[]).expect("sort_rows failed");
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
        sort_rows(&mut rows, &[key], &[])
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
}
