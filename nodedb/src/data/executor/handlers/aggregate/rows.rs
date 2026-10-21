// SPDX-License-Identifier: BUSL-1.1

//! Post-aggregate row helpers: user-alias renaming, HAVING, and ORDER BY
//! sorting.
//!
//! Rows are `nodedb_types::Value::Object` maps keyed by output column name.
//! `Value` holds every aggregate result as computed, NaN and ±Infinity
//! included, which a JSON number cannot.

use std::cmp::Ordering;

use nodedb_physical::physical_plan::AggregateSpec;
use nodedb_types::Value;

use crate::bridge::scan_filter::ScanFilter;

/// Rename each aggregate output column from its canonical alias to the
/// user alias, where the two differ.
pub(in crate::data::executor::handlers) fn apply_user_aliases_to_rows(
    rows: &mut [Value],
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
        if let Value::Object(obj) = row {
            for (from, to) in &renames {
                if let Some(value) = obj.remove(*from) {
                    obj.insert((*to).to_string(), value);
                }
            }
        }
    }
}

/// Keep the rows that match every HAVING predicate. An evaluation error in
/// a predicate fails the statement.
pub(in crate::data::executor::handlers) fn retain_having(
    rows: &mut Vec<Value>,
    predicates: &[ScanFilter],
    crs: &nodedb_crs::CrsContext<'_>,
) -> crate::Result<()> {
    if predicates.is_empty() {
        return Ok(());
    }
    let mut kept = Vec::with_capacity(rows.len());
    for row in rows.drain(..) {
        let mp = nodedb_types::value_to_msgpack(&row).map_err(|e| crate::Error::Codec {
            detail: format!("HAVING row encode: {e}"),
        })?;
        if ScanFilter::all_match_binary(predicates, &mp, crs)? {
            kept.push(row);
        }
    }
    *rows = kept;
    Ok(())
}

/// Sort finalized group rows by the post-aggregate ORDER BY terms.
///
/// A key naming an output column reads straight out of the row; a computed
/// key (`ORDER BY 1000 / SUM(amount)`) is evaluated against it, with the
/// planner having already bound each aggregate call to the column it lands in.
///
/// Evaluation is fallible — a zero divisor in a sort key fails the statement
/// with SQLSTATE `22012` — so every row's keys are evaluated up front, where
/// the error can propagate, rather than inside the comparator. Keys missing
/// from a row sort as NULL, placed by the key's NULLS FIRST/LAST setting. The
/// sort is stable to preserve relative order of equal-key rows.
pub(in crate::data::executor::handlers) fn sort_aggregated_rows(
    rows: &mut [Value],
    sort_keys: &[nodedb_physical::physical_plan::SortKeySpec],
    crs: &nodedb_crs::CrsContext<'_>,
) -> crate::Result<()> {
    if sort_keys.is_empty() {
        return Ok(());
    }

    let keyed: Vec<Vec<Value>> = rows
        .iter()
        .map(|row| {
            sort_keys
                .iter()
                .map(|k| k.expr.eval(row, crs).map_err(crate::Error::from))
                .collect::<crate::Result<Vec<_>>>()
        })
        .collect::<crate::Result<Vec<_>>>()?;

    let mut order: Vec<usize> = (0..rows.len()).collect();
    order.sort_by(|&a, &b| {
        for (idx, key) in sort_keys.iter().enumerate() {
            let av = keyed[a].get(idx).filter(|v| !v.is_null());
            let bv = keyed[b].get(idx).filter(|v| !v.is_null());
            let ord = match (av, bv) {
                (Some(x), Some(y)) => {
                    key.direct(nodedb_query::value_ops::compare_sort_values(x, y))
                }
                // At least one side is NULL, so `order_nulls` returns `Some`.
                _ => key
                    .order_nulls(av.is_none(), bv.is_none())
                    .unwrap_or(Ordering::Equal),
            };
            if ord != Ordering::Equal {
                return ord;
            }
        }
        Ordering::Equal
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
    use std::collections::HashMap;

    fn row(pairs: &[(&str, Value)]) -> Value {
        Value::Object(
            pairs
                .iter()
                .map(|(k, v)| ((*k).to_string(), v.clone()))
                .collect::<HashMap<_, _>>(),
        )
    }

    fn sorted(mut rows: Vec<Value>, key: SortKeySpec) -> Vec<Value> {
        sort_aggregated_rows(&mut rows, &[key], &nodedb_crs::CrsContext::builtin_only())
            .expect("sort");
        rows
    }

    fn column(rows: &[Value], name: &str) -> Vec<Value> {
        rows.iter()
            .map(|r| match r {
                Value::Object(m) => m.get(name).cloned().unwrap_or(Value::Null),
                _ => Value::Null,
            })
            .collect()
    }

    fn s(text: &str) -> Value {
        Value::String(text.into())
    }

    /// `2^53 + 1` and `2^53` collapse to one `f64`. MAX outputs order exactly.
    #[test]
    fn max_outputs_past_two_pow_53_order_exactly() {
        let rows = vec![
            row(&[
                ("g", s("a")),
                ("max_v", Value::Integer(9_007_199_254_740_993)),
            ]),
            row(&[
                ("g", s("b")),
                ("max_v", Value::Integer(9_007_199_254_740_992)),
            ]),
        ];
        let asc = sorted(rows.clone(), SortKeySpec::column("max_v", true));
        assert_eq!(column(&asc, "g"), vec![s("b"), s("a")]);
        let desc = sorted(rows, SortKeySpec::column("max_v", false));
        assert_eq!(column(&desc, "g"), vec![s("a"), s("b")]);
    }

    #[test]
    fn min_outputs_nanosecond_timestamps_order_exactly() {
        let rows = vec![
            row(&[
                ("g", s("late")),
                ("min_ts", Value::Integer(1_700_000_000_000_000_002)),
            ]),
            row(&[
                ("g", s("early")),
                ("min_ts", Value::Integer(1_700_000_000_000_000_001)),
            ]),
            row(&[
                ("g", s("mid")),
                ("min_ts", Value::Float(1_700_000_000_000_000_001.5)),
            ]),
        ];
        let asc = sorted(rows, SortKeySpec::column("min_ts", true));
        // The float rounds to `1_700_000_000_000_000_000`, below both integers.
        assert_eq!(column(&asc, "g"), vec![s("mid"), s("early"), s("late")]);
    }

    #[test]
    fn u64_outputs_order_above_i64() {
        let rows = vec![
            row(&[("g", s("u")), ("v", Value::from_u64(u64::MAX))]),
            row(&[("g", s("i")), ("v", Value::Integer(i64::MAX))]),
            row(&[("g", s("neg")), ("v", Value::Integer(i64::MIN))]),
        ];
        let asc = sorted(rows, SortKeySpec::column("v", true));
        assert_eq!(column(&asc, "g"), vec![s("neg"), s("i"), s("u")]);
    }

    #[test]
    fn small_values_and_nulls_keep_their_order() {
        let rows = vec![
            row(&[("g", s("two")), ("v", Value::Integer(2))]),
            row(&[("g", s("null")), ("v", Value::Null)]),
            row(&[("g", s("half")), ("v", Value::Float(1.5))]),
            row(&[("g", s("absent"))]),
            row(&[("g", s("one")), ("v", Value::Integer(1))]),
        ];
        let asc = sorted(rows.clone(), SortKeySpec::column("v", true));
        assert_eq!(
            column(&asc, "g"),
            vec![s("one"), s("half"), s("two"), s("null"), s("absent")]
        );
        let desc = sorted(rows, SortKeySpec::column("v", false));
        assert_eq!(
            column(&desc, "g"),
            vec![s("null"), s("absent"), s("two"), s("half"), s("one")]
        );
    }

    /// NaN and ±Infinity outputs keep their float values and order as in
    /// PostgreSQL: NaN above every number.
    #[test]
    fn non_finite_outputs_order_as_postgres() {
        let rows = vec![
            row(&[("g", s("nan")), ("v", Value::Float(f64::NAN))]),
            row(&[("g", s("inf")), ("v", Value::Float(f64::INFINITY))]),
            row(&[("g", s("one")), ("v", Value::Integer(1))]),
            row(&[("g", s("neg")), ("v", Value::Float(f64::NEG_INFINITY))]),
        ];
        let asc = sorted(rows.clone(), SortKeySpec::column("v", true));
        assert_eq!(
            column(&asc, "g"),
            vec![s("neg"), s("one"), s("inf"), s("nan")]
        );
        let desc = sorted(rows, SortKeySpec::column("v", false));
        assert_eq!(
            column(&desc, "g"),
            vec![s("nan"), s("inf"), s("one"), s("neg")]
        );
    }

    #[test]
    fn text_outputs_order_by_bytes() {
        let rows = vec![
            row(&[("g", s("10"))]),
            row(&[("g", s("9"))]),
            row(&[("g", s("abc"))]),
        ];
        let asc = sorted(rows, SortKeySpec::column("g", true));
        assert_eq!(column(&asc, "g"), vec![s("10"), s("9"), s("abc")]);
    }

    #[test]
    fn user_aliases_rename_output_columns() {
        let mut rows = vec![row(&[("sum(v)", Value::Float(f64::INFINITY))])];
        let spec = AggregateSpec {
            function: "sum".into(),
            alias: "sum(v)".into(),
            user_alias: Some("total".into()),
            field: "v".into(),
            expr: None,
            param: None,
        };
        apply_user_aliases_to_rows(&mut rows, std::slice::from_ref(&spec));
        assert_eq!(column(&rows, "total"), vec![Value::Float(f64::INFINITY)]);
        assert_eq!(column(&rows, "sum(v)"), vec![Value::Null]);
    }
}
