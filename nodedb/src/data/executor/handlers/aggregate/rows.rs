// SPDX-License-Identifier: BUSL-1.1

//! Post-aggregate row helpers: user-alias renaming and ORDER BY sorting.

use nodedb_physical::physical_plan::AggregateSpec;

pub(super) fn apply_user_aliases_to_rows(
    rows: &mut [serde_json::Value],
    aggregates: &[AggregateSpec],
) {
    let renames: Vec<(&str, &str)> = aggregates
        .iter()
        .filter_map(|agg| {
            agg.user_alias
                .as_deref()
                .filter(|alias| *alias != agg.alias)
                .map(|alias| (agg.alias.as_str(), alias))
        })
        .collect();

    if renames.is_empty() {
        return;
    }

    for row in rows {
        if let Some(obj) = row.as_object_mut() {
            for (from, to) in &renames {
                if let Some(value) = obj.remove(*from) {
                    obj.insert((*to).to_string(), value);
                }
            }
        }
    }
}

/// Sort finalized group rows by the post-aggregate ORDER BY terms.
///
/// Each row is a `serde_json::Value::Object` keyed by output column name. A
/// key naming one of those columns reads straight out of the row; a computed
/// key (`ORDER BY 1000 / SUM(amount)`) is evaluated against it, with the
/// planner having already bound each aggregate call to the column it lands in.
///
/// Evaluation is fallible — a zero divisor in a sort key fails the statement
/// with SQLSTATE `22012` — so every row's keys are evaluated up front, where
/// the error can propagate, rather than inside the comparator. Keys missing
/// from a row sort as NULL, placed by the key's NULLS FIRST/LAST setting. The
/// sort is stable to preserve relative order of equal-key rows.
pub(super) fn sort_aggregated_rows(
    rows: &mut [serde_json::Value],
    sort_keys: &[nodedb_physical::physical_plan::SortKeySpec],
) -> crate::Result<()> {
    if sort_keys.is_empty() {
        return Ok(());
    }

    let keyed: Vec<Vec<serde_json::Value>> = rows
        .iter()
        .map(|row| {
            sort_keys
                .iter()
                .map(|k| nodedb_query::eval_expr_on_json(&k.expr, row).map_err(crate::Error::from))
                .collect::<crate::Result<Vec<_>>>()
        })
        .collect::<crate::Result<Vec<_>>>()?;

    let mut order: Vec<usize> = (0..rows.len()).collect();
    order.sort_by(|&a, &b| {
        for (idx, key) in sort_keys.iter().enumerate() {
            let av = keyed[a].get(idx).filter(|v| !v.is_null());
            let bv = keyed[b].get(idx).filter(|v| !v.is_null());
            let ord = match (av, bv) {
                (Some(x), Some(y)) => key.direct(nodedb_query::compare_json(x, y)),
                // At least one side is NULL, so `order_nulls` returns `Some`.
                _ => key
                    .order_nulls(av.is_none(), bv.is_none())
                    .unwrap_or(std::cmp::Ordering::Equal),
            };
            if ord != std::cmp::Ordering::Equal {
                return ord;
            }
        }
        std::cmp::Ordering::Equal
    });

    let original = rows.to_vec();
    for (dst, &src) in order.iter().enumerate() {
        rows[dst] = original[src].clone();
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use nodedb_physical::physical_plan::SortKeySpec;
    use serde_json::json;

    fn sorted(mut rows: Vec<serde_json::Value>, key: SortKeySpec) -> Vec<serde_json::Value> {
        sort_aggregated_rows(&mut rows, &[key]).expect("sort");
        rows
    }

    fn column(rows: &[serde_json::Value], name: &str) -> Vec<serde_json::Value> {
        rows.iter().map(|r| r[name].clone()).collect()
    }

    /// `2^53 + 1` and `2^53` collapse to one `f64`. MAX outputs order exactly.
    #[test]
    fn max_outputs_past_two_pow_53_order_exactly() {
        let rows = vec![
            json!({"g": "a", "max_v": 9_007_199_254_740_993_i64}),
            json!({"g": "b", "max_v": 9_007_199_254_740_992_i64}),
        ];
        let asc = sorted(rows.clone(), SortKeySpec::column("max_v", true));
        assert_eq!(column(&asc, "g"), vec![json!("b"), json!("a")]);
        let desc = sorted(rows, SortKeySpec::column("max_v", false));
        assert_eq!(column(&desc, "g"), vec![json!("a"), json!("b")]);
    }

    #[test]
    fn min_outputs_nanosecond_timestamps_order_exactly() {
        let rows = vec![
            json!({"g": "late", "min_ts": 1_700_000_000_000_000_002_i64}),
            json!({"g": "early", "min_ts": 1_700_000_000_000_000_001_i64}),
            json!({"g": "mid", "min_ts": 1_700_000_000_000_000_001.5_f64}),
        ];
        let asc = sorted(rows, SortKeySpec::column("min_ts", true));
        // The float rounds to `1_700_000_000_000_000_000`, below both integers.
        assert_eq!(
            column(&asc, "g"),
            vec![json!("mid"), json!("early"), json!("late")]
        );
    }

    #[test]
    fn u64_outputs_order_above_i64() {
        let rows = vec![
            json!({"g": "u", "v": u64::MAX}),
            json!({"g": "i", "v": i64::MAX}),
            json!({"g": "neg", "v": i64::MIN}),
        ];
        let asc = sorted(rows, SortKeySpec::column("v", true));
        assert_eq!(
            column(&asc, "g"),
            vec![json!("neg"), json!("i"), json!("u")]
        );
    }

    #[test]
    fn small_values_and_nulls_keep_their_order() {
        let rows = vec![
            json!({"g": "two", "v": 2}),
            json!({"g": "null", "v": null}),
            json!({"g": "half", "v": 1.5}),
            json!({"g": "absent"}),
            json!({"g": "one", "v": 1}),
        ];
        let asc = sorted(rows.clone(), SortKeySpec::column("v", true));
        assert_eq!(
            column(&asc, "g"),
            vec![
                json!("one"),
                json!("half"),
                json!("two"),
                json!("null"),
                json!("absent")
            ]
        );
        let desc = sorted(rows, SortKeySpec::column("v", false));
        assert_eq!(
            column(&desc, "g"),
            vec![
                json!("null"),
                json!("absent"),
                json!("two"),
                json!("half"),
                json!("one")
            ]
        );
    }
}
