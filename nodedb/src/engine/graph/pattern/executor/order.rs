// SPDX-License-Identifier: BUSL-1.1

//! MATCH `ORDER BY`, `OFFSET` and `LIMIT`.
//!
//! Every core and shard returns part of the result, so the window is a
//! property of the merged rows, never of one core's.
//!
//! - **Core** ([`attach_order_keys`], [`push_down`]): each ORDER BY key is
//!   evaluated from the row's bindings before projection and carried as a
//!   hidden key ([`super::row::order_key`]), so a key the RETURN list does
//!   not project still orders. A core keeps only its own first
//!   `OFFSET + LIMIT` rows by those keys: a row past that bound on its core
//!   is past it in the merge too.
//! - **Coordinator** ([`finish_match_rows`]): the merged rows are deduplicated
//!   for DISTINCT, sorted by the typed keys, cut to `OFFSET`/`LIMIT`, and
//!   stripped of every hidden key.
//!
//! Keys compare with `value_ops::compare_sort_values`, the ORDER BY order of
//! SQL output rows. NULLs follow PostgreSQL's default
//! (`window::helpers::null_order`): last for ASC, first for DESC.

use std::cmp::Ordering;
use std::collections::HashSet;

use nodedb_query::value_ops::compare_sort_values;
use nodedb_query::window::helpers::null_order;
use nodedb_types::Value;

use super::super::ast::{MatchQuery, OrderByColumn};
use super::properties::QueryTail;
use super::row::{order_key, strip_hidden, visible_fingerprint};
use super::types::BindingRow;

/// The value of each ORDER BY key of `row`, read from its bindings. An
/// ORDER BY expr that names a RETURN column or alias reads that column's
/// expr. A dotted expr reads a node or relationship property, a bare one a
/// binding. A missing value is NULL.
pub(super) fn order_values(
    query: &MatchQuery,
    tail: &QueryTail<'_>,
    row: &BindingRow,
) -> crate::Result<Vec<Value>> {
    query
        .order_by
        .iter()
        .map(|col| {
            let expr = query
                .return_columns
                .iter()
                .find(|ret| ret.alias.as_deref() == Some(col.expr.as_str()))
                .map_or(col.expr.as_str(), |ret| ret.expr.as_str());
            let value = match expr.split_once('.') {
                Some((binding, field)) => tail.property(row, binding, field)?,
                None => row.get(expr).cloned(),
            };
            Ok(value.unwrap_or(Value::Null))
        })
        .collect()
}

/// Carry `keys` on `row` as hidden ORDER BY keys.
pub(super) fn attach_order_keys(row: &mut BindingRow, keys: Vec<Value>) {
    for (index, key) in keys.into_iter().enumerate() {
        row.insert(order_key(index), key);
    }
}

/// One core's share of the window: its rows sorted by the ORDER BY keys and
/// cut to its first `OFFSET + LIMIT`. With no LIMIT every row is kept: the
/// OFFSET applies to the merged rows.
pub(super) fn push_down(query: &MatchQuery, rows: &mut Vec<BindingRow>) {
    sort_rows(&query.order_by, rows);
    if let Some(limit) = query.limit {
        rows.truncate(limit.saturating_add(query.offset.unwrap_or(0)));
    }
}

/// The coordinator's window over the rows every core and shard returned:
/// DISTINCT over the visible columns, then ORDER BY, OFFSET and LIMIT. The
/// rows leave with no hidden key.
pub fn finish_match_rows(
    query: &MatchQuery,
    mut rows: Vec<BindingRow>,
) -> crate::Result<Vec<BindingRow>> {
    if query.distinct {
        let mut seen = HashSet::new();
        let mut kept = Vec::with_capacity(rows.len());
        for row in rows {
            if seen.insert(visible_fingerprint(&row)?) {
                kept.push(row);
            }
        }
        rows = kept;
    }
    sort_rows(&query.order_by, &mut rows);
    let offset = query.offset.unwrap_or(0).min(rows.len());
    rows.drain(..offset);
    if let Some(limit) = query.limit {
        rows.truncate(limit);
    }
    rows.iter_mut().for_each(strip_hidden);
    Ok(rows)
}

/// Stable sort of `rows` by their hidden ORDER BY keys.
fn sort_rows(order_by: &[OrderByColumn], rows: &mut [BindingRow]) {
    if order_by.is_empty() {
        return;
    }
    let keys: Vec<String> = (0..order_by.len()).map(order_key).collect();
    rows.sort_by(|a, b| {
        for (col, key) in order_by.iter().zip(&keys) {
            let (va, vb) = (
                a.get(key).unwrap_or(&Value::Null),
                b.get(key).unwrap_or(&Value::Null),
            );
            let ord = null_order(va.is_null(), vb.is_null(), col.ascending).unwrap_or_else(|| {
                let c = compare_sort_values(va, vb);
                if col.ascending { c } else { c.reverse() }
            });
            if ord != Ordering::Equal {
                return ord;
            }
        }
        Ordering::Equal
    });
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::graph::pattern::compiler::parse;

    fn row(name: &str, key: Value) -> BindingRow {
        let mut row = BindingRow::from([("b".to_string(), Value::String(name.into()))]);
        attach_order_keys(&mut row, vec![key]);
        row
    }

    fn names(rows: &[BindingRow]) -> Vec<&str> {
        rows.iter().filter_map(|r| r["b"].as_str()).collect()
    }

    fn decimal(text: &str) -> Value {
        Value::Decimal(text.parse().expect("decimal"))
    }

    /// Rows merged from several cores order by their typed keys: an integer,
    /// a float and a decimal compare by value. NULL sorts last ascending and
    /// first descending. OFFSET and LIMIT cut the merged order, and no hidden
    /// key survives.
    #[test]
    fn the_merged_rows_order_by_typed_keys_with_sql_null_placement() {
        let merged = || {
            vec![
                row("ten", Value::Integer(10)),
                row("null", Value::Null),
                row("twelve-fifty", decimal("12.50")),
                row("two", Value::Float(2.0)),
            ]
        };
        let asc = parse("MATCH (a)-[:L]->(b) RETURN b ORDER BY x").unwrap();
        let rows = finish_match_rows(&asc, merged()).unwrap();
        assert_eq!(names(&rows), vec!["two", "ten", "twelve-fifty", "null"]);
        assert!(rows.iter().all(|r| r.len() == 1), "hidden keys stripped");

        let desc = parse("MATCH (a)-[:L]->(b) RETURN b ORDER BY x DESC LIMIT 2 OFFSET 1").unwrap();
        let rows = finish_match_rows(&desc, merged()).unwrap();
        assert_eq!(names(&rows), vec!["twelve-fifty", "ten"]);
    }

    /// A core keeps its first OFFSET + LIMIT rows, so the merge still holds
    /// every row of the global window.
    #[test]
    fn a_core_keeps_its_first_offset_plus_limit_rows() {
        let query = parse("MATCH (a)-[:L]->(b) RETURN b ORDER BY x LIMIT 1 OFFSET 1").unwrap();
        let mut rows = vec![
            row("c", Value::Integer(3)),
            row("a", Value::Integer(1)),
            row("b", Value::Integer(2)),
        ];
        push_down(&query, &mut rows);
        assert_eq!(names(&rows), vec!["a", "b"]);
    }

    /// Without ORDER BY, LIMIT still caps the merged rows.
    #[test]
    fn limit_without_order_by_caps_the_merged_rows() {
        let query = parse("MATCH (a)-[:L]->(b) RETURN b LIMIT 2").unwrap();
        let merged: Vec<BindingRow> = ["a", "b", "c", "d"]
            .iter()
            .map(|n| BindingRow::from([("b".to_string(), Value::String((*n).into()))]))
            .collect();
        assert_eq!(finish_match_rows(&query, merged).unwrap().len(), 2);
    }

    /// DISTINCT compares the visible columns only.
    #[test]
    fn distinct_ignores_hidden_keys() {
        let query = parse("MATCH (a)-[:L]->(b) RETURN DISTINCT b").unwrap();
        let rows = vec![row("x", Value::Integer(1)), row("x", Value::Integer(2))];
        assert_eq!(finish_match_rows(&query, rows).unwrap().len(), 1);
    }

    /// Two cores, each holding part of `alice`'s edges. Each core's rows go
    /// through the executor (with its per-core window), and the coordinator
    /// merges them: ORDER BY an unprojected relationship property, NULL first
    /// for DESC, last for ASC, and LIMIT holds over the merge.
    #[test]
    fn order_by_and_limit_hold_over_rows_merged_from_two_cores() {
        use std::collections::HashMap;

        use super::super::core::triple::tests::{make_sparse, props_for};
        use super::super::edge_props::tests::{graph_with, run};

        let priced = |p: Value| HashMap::from([("price".to_string(), p)]);
        let core_a = graph_with(vec![
            ("bob", priced(decimal("12.50"))),
            ("carol", priced(Value::Integer(3))),
        ]);
        let core_b = graph_with(vec![
            ("dave", HashMap::new()),
            ("erin", priced(Value::Float(20.0))),
        ]);
        let (sparse, _sdir) = make_sparse();
        let merged = |text: &str| {
            let query = parse(text).unwrap();
            let mut rows = Vec::new();
            for (csr, store, _dir) in [&core_a, &core_b] {
                let props = props_for(&sparse, csr);
                rows.extend(run(text, csr, store, &props));
            }
            finish_match_rows(&query, rows).unwrap()
        };

        let desc = merged("MATCH (a)-[r:KNOWS]->(b) RETURN b ORDER BY r.price DESC LIMIT 2");
        assert_eq!(names(&desc), vec!["dave", "erin"], "NULL first for DESC");
        assert!(
            desc.iter().all(|r| r.len() == 1),
            "the order key is not projected"
        );

        let asc = merged("MATCH (a)-[r:KNOWS]->(b) RETURN b ORDER BY r.price LIMIT 3");
        assert_eq!(
            names(&asc),
            vec!["carol", "bob", "erin"],
            "typed order, NULL last"
        );

        let offset = merged("MATCH (a)-[r:KNOWS]->(b) RETURN b ORDER BY r.price OFFSET 3");
        assert_eq!(names(&offset), vec!["dave"]);

        let capped = merged("MATCH (a)-[r:KNOWS]->(b) RETURN b LIMIT 3");
        assert_eq!(
            capped.len(),
            3,
            "LIMIT without ORDER BY caps the merged rows"
        );
    }

    /// ORDER BY a node property orders by its typed value, through a RETURN
    /// alias.
    #[test]
    fn order_by_a_typed_node_property_through_an_alias() {
        use super::super::core::triple::tests::{make_sparse, props_for};
        use super::super::edge_props::tests::{graph_with, run};

        let (mut csr, store, _dir) = graph_with(vec![
            ("bob", Default::default()),
            ("carol", Default::default()),
        ]);
        let (sparse, _sdir) = make_sparse();
        for (node, surrogate, rank) in [("bob", 7, decimal("2.5")), ("carol", 8, Value::Integer(1))]
        {
            let surrogate = nodedb_types::Surrogate::new(surrogate);
            csr.set_node_surrogate(node, surrogate);
            let body = zerompk::to_msgpack_vec(&Value::Object([("rank".to_string(), rank)].into()))
                .unwrap();
            sparse
                .put(
                    0,
                    1,
                    "col",
                    &nodedb_types::StorageKey::for_surrogate(surrogate),
                    &body,
                    1,
                )
                .unwrap();
        }
        let props = props_for(&sparse, &csr);
        let text = "MATCH (a)-[:KNOWS]->(b) IN 'col' RETURN b, b.rank AS rank ORDER BY rank DESC";
        let rows =
            finish_match_rows(&parse(text).unwrap(), run(text, &csr, &store, &props)).unwrap();
        assert_eq!(names(&rows), vec!["bob", "carol"]);
        assert_eq!(rows[0]["rank"], decimal("2.5"));
    }
}
