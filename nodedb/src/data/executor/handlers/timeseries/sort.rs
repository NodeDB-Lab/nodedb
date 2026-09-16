// SPDX-License-Identifier: BUSL-1.1

//! `ORDER BY` for timeseries results.
//!
//! Both timeseries result shapes — raw rows from the memtable / partitions and
//! encoded aggregate rows — are `rmpv::Value::Map`s keyed by the name the
//! client sees, so one comparator orders either. Sorting happens before the
//! row limit is applied: `ORDER BY … LIMIT n` must return the first `n` rows
//! of the requested order, not `n` arbitrary rows in that order.

use std::cmp::Ordering;

use nodedb_physical::physical_plan::SortKeySpec;
use nodedb_types::json_msgpack::instant_from_ext;

use crate::util::rmpv_value::{rmpv_to_value, value_to_rmpv};

/// Sort materialized result rows by the planner's ORDER BY terms.
///
/// A key that names a column is read from the row. A computed key
/// (`ORDER BY 100 / value`) is evaluated against the row, so the sort orders
/// by the requested value instead of dropping the key and returning the
/// engine's natural order under a sort the client asked for. Evaluation can
/// fail — a zero divisor surfaces as SQLSTATE `22012` — so every row's keys
/// are evaluated up front, where the error can propagate, rather than inside
/// the comparator.
///
/// Rows are compared on the keys in significance order. A field absent from a
/// row compares as NULL, and NULL sorts last ascending — PostgreSQL's default.
pub(in crate::data::executor) fn sort_rows(
    rows: &mut [rmpv::Value],
    sort_keys: &[SortKeySpec],
) -> crate::Result<()> {
    if sort_keys.is_empty() {
        return Ok(());
    }

    let keyed: Vec<Vec<rmpv::Value>> = rows
        .iter()
        .map(|row| eval_row_keys(row, sort_keys))
        .collect::<crate::Result<Vec<_>>>()?;

    let mut order: Vec<usize> = (0..rows.len()).collect();
    order.sort_by(|&a, &b| compare_key_rows(&keyed[a], &keyed[b], sort_keys));

    let original = rows.to_vec();
    for (dst, &src) in order.iter().enumerate() {
        rows[dst] = original[src].clone();
    }
    Ok(())
}

/// Evaluate every sort key against one result row.
fn eval_row_keys(row: &rmpv::Value, sort_keys: &[SortKeySpec]) -> crate::Result<Vec<rmpv::Value>> {
    let mut out = Vec::with_capacity(sort_keys.len());
    let mut row_value: Option<nodedb_types::Value> = None;

    for key in sort_keys {
        if let Some(field) = key.as_column() {
            out.push(field_of(row, field).cloned().unwrap_or(rmpv::Value::Nil));
            continue;
        }

        // Build the row's evaluable form once, and only when a computed key
        // needs it.
        let value = match row_value {
            Some(ref v) => v,
            None => row_value.insert(rmpv_to_value(row)),
        };
        let evaluated = key.expr.eval(value).map_err(crate::Error::from)?;
        out.push(value_to_rmpv(&evaluated));
    }
    Ok(out)
}

fn compare_key_rows(a: &[rmpv::Value], b: &[rmpv::Value], sort_keys: &[SortKeySpec]) -> Ordering {
    for (idx, key) in sort_keys.iter().enumerate() {
        let av = a.get(idx).filter(|v| !matches!(v, rmpv::Value::Nil));
        let bv = b.get(idx).filter(|v| !matches!(v, rmpv::Value::Nil));
        // NULL placement comes from the key's NULLS FIRST/LAST setting, which
        // the sort direction never flips.
        let ord = match key.order_nulls(av.is_none(), bv.is_none()) {
            Some(ord) => ord,
            None => key.direct(compare_values(av, bv)),
        };
        if ord != Ordering::Equal {
            return ord;
        }
    }
    Ordering::Equal
}

/// Look up a field in an rmpv map row. `None` for a non-map row or a missing
/// field — both treated as NULL by the comparator.
fn field_of<'a>(row: &'a rmpv::Value, field: &str) -> Option<&'a rmpv::Value> {
    let rmpv::Value::Map(entries) = row else {
        return None;
    };
    entries
        .iter()
        .find(|(key, _)| match key {
            rmpv::Value::String(name) => name.as_str() == Some(field),
            _ => false,
        })
        .map(|(_, value)| value)
        .filter(|value| !matches!(value, rmpv::Value::Nil))
}

fn compare_values(a: Option<&rmpv::Value>, b: Option<&rmpv::Value>) -> Ordering {
    // NULL / absent sorts last in ascending order (PostgreSQL default).
    let (a, b) = match (a, b) {
        (Some(a), Some(b)) => (a, b),
        (None, None) => return Ordering::Equal,
        (None, Some(_)) => return Ordering::Greater,
        (Some(_), None) => return Ordering::Less,
    };
    if let (Some(x), Some(y)) = (as_f64(a), as_f64(b)) {
        return x.partial_cmp(&y).unwrap_or(Ordering::Equal);
    }
    match (a, b) {
        (rmpv::Value::String(x), rmpv::Value::String(y)) => {
            x.as_str().unwrap_or("").cmp(y.as_str().unwrap_or(""))
        }
        (rmpv::Value::Boolean(x), rmpv::Value::Boolean(y)) => x.cmp(y),
        // A computed key that evaluates to an instant arrives as the instant
        // ext; two instants order by their epoch microseconds.
        (rmpv::Value::Ext(tx, px), rmpv::Value::Ext(ty, py)) => {
            match (instant_from_ext(*tx, px), instant_from_ext(*ty, py)) {
                (Some((_, x)), Some((_, y))) => x.cmp(&y),
                _ => Ordering::Equal,
            }
        }
        // Exotic shapes never appear in a timeseries result row; keep the
        // order stable rather than inventing one.
        _ => Ordering::Equal,
    }
}

/// Numeric view of a value, so an integer column and a float column compare
/// against each other the way SQL expects.
fn as_f64(value: &rmpv::Value) -> Option<f64> {
    match value {
        rmpv::Value::Integer(n) => n.as_i64().map(|i| i as f64).or_else(|| n.as_f64()),
        rmpv::Value::F32(f) => Some(*f as f64),
        rmpv::Value::F64(f) => Some(*f),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn row(pairs: &[(&str, rmpv::Value)]) -> rmpv::Value {
        rmpv::Value::Map(
            pairs
                .iter()
                .map(|(k, v)| (rmpv::Value::String((*k).into()), v.clone()))
                .collect(),
        )
    }

    fn ints(field: &str, rows: &[rmpv::Value]) -> Vec<i64> {
        rows.iter()
            .map(|r| match field_of(r, field) {
                Some(rmpv::Value::Integer(n)) => n.as_i64().unwrap_or(0),
                _ => i64::MIN,
            })
            .collect()
    }

    #[test]
    fn descending_order_reverses() {
        let mut rows = vec![
            row(&[("ts", rmpv::Value::Integer(100.into()))]),
            row(&[("ts", rmpv::Value::Integer(300.into()))]),
            row(&[("ts", rmpv::Value::Integer(200.into()))]),
        ];
        sort_rows(&mut rows, &[SortKeySpec::column("ts", false)]).expect("sort");
        assert_eq!(ints("ts", &rows), vec![300, 200, 100]);
    }

    #[test]
    fn ascending_order_sorts_up() {
        let mut rows = vec![
            row(&[("ts", rmpv::Value::Integer(300.into()))]),
            row(&[("ts", rmpv::Value::Integer(100.into()))]),
        ];
        sort_rows(&mut rows, &[SortKeySpec::column("ts", true)]).expect("sort");
        assert_eq!(ints("ts", &rows), vec![100, 300]);
    }

    #[test]
    fn secondary_key_breaks_ties() {
        let mut rows = vec![
            row(&[
                ("host", rmpv::Value::String("a".into())),
                ("ts", rmpv::Value::Integer(200.into())),
            ]),
            row(&[
                ("host", rmpv::Value::String("a".into())),
                ("ts", rmpv::Value::Integer(100.into())),
            ]),
        ];
        sort_rows(
            &mut rows,
            &[
                SortKeySpec::column("host", true),
                SortKeySpec::column("ts", true),
            ],
        )
        .expect("sort");
        assert_eq!(ints("ts", &rows), vec![100, 200]);
    }

    #[test]
    fn nulls_and_missing_fields_sort_last_ascending() {
        let mut rows = vec![
            row(&[("ts", rmpv::Value::Nil)]),
            row(&[("other", rmpv::Value::Integer(1.into()))]),
            row(&[("ts", rmpv::Value::Integer(5.into()))]),
        ];
        sort_rows(&mut rows, &[SortKeySpec::column("ts", true)]).expect("sort");
        assert_eq!(ints("ts", &rows)[0], 5);
    }

    #[test]
    fn integers_and_floats_compare_numerically() {
        let mut rows = vec![
            row(&[("v", rmpv::Value::F64(2.5))]),
            row(&[("v", rmpv::Value::Integer(2.into()))]),
            row(&[("v", rmpv::Value::F64(1.5))]),
        ];
        sort_rows(&mut rows, &[SortKeySpec::column("v", true)]).expect("sort");
        let vs: Vec<f64> = rows
            .iter()
            .map(|r| as_f64(field_of(r, "v").unwrap()).unwrap())
            .collect();
        assert_eq!(vs, vec![1.5, 2.0, 2.5]);
    }

    #[test]
    fn an_unknown_column_leaves_the_order_untouched() {
        let mut rows = vec![
            row(&[("ts", rmpv::Value::Integer(2.into()))]),
            row(&[("ts", rmpv::Value::Integer(1.into()))]),
        ];
        sort_rows(&mut rows, &[SortKeySpec::column("nope", true)]).expect("sort");
        assert_eq!(ints("ts", &rows), vec![2, 1]);
    }

    #[test]
    fn no_sort_keys_is_a_no_op() {
        let mut rows = vec![
            row(&[("ts", rmpv::Value::Integer(2.into()))]),
            row(&[("ts", rmpv::Value::Integer(1.into()))]),
        ];
        sort_rows(&mut rows, &[]).expect("sort");
        assert_eq!(ints("ts", &rows), vec![2, 1]);
    }
}
