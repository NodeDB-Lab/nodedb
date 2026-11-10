// SPDX-License-Identifier: BUSL-1.1

//! LEFT JOIN merge for `OPTIONAL MATCH` clauses.

use crate::engine::graph::pattern::ast::MatchClause;
use crate::engine::graph::pattern::executor::types::BindingRow;

/// LEFT JOIN: merge clause results with existing rows.
pub(super) fn left_join_rows(
    input: &[BindingRow],
    clause_rows: &[BindingRow],
    clause: &MatchClause,
) -> Vec<BindingRow> {
    let new_vars: Vec<String> = clause
        .patterns
        .iter()
        .flat_map(|chain| {
            chain.triples.iter().flat_map(|t| {
                let mut vars = Vec::new();
                if let Some(ref n) = t.src.name {
                    vars.push(n.clone());
                }
                if let Some(ref n) = t.dst.name {
                    vars.push(n.clone());
                }
                if let Some(ref n) = t.edge.name {
                    vars.push(n.clone());
                }
                vars
            })
        })
        .collect();

    let mut result = Vec::new();

    for input_row in input {
        let matches: Vec<&BindingRow> = clause_rows
            .iter()
            .filter(|cr| {
                input_row
                    .iter()
                    .all(|(k, v)| cr.get(k).is_none_or(|cv| cv == v))
            })
            .collect();

        if matches.is_empty() {
            let mut row = input_row.clone();
            // An unmatched variable binds SQL NULL.
            for var in &new_vars {
                row.entry(var.clone()).or_insert(nodedb_types::Value::Null);
            }
            result.push(row);
        } else {
            result.extend(matches.into_iter().cloned());
        }
    }

    result
}
