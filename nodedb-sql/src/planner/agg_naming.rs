// SPDX-License-Identifier: Apache-2.0

//! Canonical output names for aggregate expressions.
//!
//! One aggregate has two names: the *canonical* key it is computed and stored
//! under in a finalized group row (`sum(amount)`), and the *user alias* the
//! client sees (`SELECT SUM(amount) AS total`). The rename to the user alias
//! happens last, after HAVING has filtered, so anything that has to address an
//! aggregate mid-pipeline must use the canonical key. Deriving that key in one
//! place keeps the planner's HAVING rewrite and the physical spec builder from
//! drifting into two different names for the same column.

use crate::types::{AggregateExpr, SqlExpr};

/// The accumulator name for an aggregate, accounting for `DISTINCT`.
pub fn aggregate_function_name(a: &AggregateExpr) -> String {
    if !a.distinct {
        return a.function.clone();
    }
    match a.function.as_str() {
        "count" => "count_distinct".into(),
        "array_agg" => "array_agg_distinct".into(),
        // SUM(DISTINCT col) / AVG(DISTINCT col) route to a dedicated
        // accumulator that dedupes input values before summing. The plain
        // "sum"/"avg" accumulator does not dedupe, so without this remap
        // `DISTINCT` would be silently ignored.
        "sum" => "sum_distinct".into(),
        "avg" => "avg_distinct".into(),
        // MIN(DISTINCT) and MAX(DISTINCT) yield the same result as their
        // non-distinct counterparts (the smallest / largest value is the same
        // whether or not duplicates are deduped), so we accept the DISTINCT
        // modifier but route to the regular accumulator.
        _ => a.function.clone(),
    }
}

/// The field name an aggregate accumulates over: the bare column when the
/// argument is one, and `*` for a wildcard or a computed argument (which the
/// accumulator receives as an expression rather than a field).
pub fn aggregate_field_name(a: &AggregateExpr) -> String {
    a.args
        .first()
        .map(|arg| match arg {
            SqlExpr::Column { name, .. } => name.clone(),
            _ => "*".to_string(),
        })
        .unwrap_or_else(|| "*".to_string())
}

/// The canonical output-column key an aggregate's value is stored under in a
/// finalized group row.
///
/// A computed argument is named by its source text and a `FILTER` clause is
/// appended, so `SUM(a * 2)`, `SUM(b * 3)` and `COUNT(*) FILTER (WHERE ...)`
/// each land under their own key.
pub fn aggregate_output_key(a: &AggregateExpr) -> String {
    let arg = a
        .arg_text
        .clone()
        .unwrap_or_else(|| aggregate_field_name(a));
    let base = nodedb_query::agg_key::canonical_agg_key(&aggregate_function_name(a), &arg);
    match &a.filter_text {
        None => base,
        Some(text) => format!("{base} filter (where {text})"),
    }
}

/// The internal name of a computed (non-column) GROUP BY key: a stable
/// `group_{index}` placeholder. The executor emits the evaluated key under
/// it, and the response shaper reads the value back by it. The SELECT alias
/// never reaches this name; it is a display name only.
pub fn computed_group_key_name(index: usize) -> String {
    format!("group_{index}")
}

/// The key a finalized group row carries one GROUP BY key under. A column
/// key keeps its bare column name; a computed key uses
/// [`computed_group_key_name`].
pub fn group_key_row_name(key: &SqlExpr, index: usize) -> String {
    match key {
        SqlExpr::Column { name, .. } => name.clone(),
        SqlExpr::Function { .. }
        | SqlExpr::BinaryOp { .. }
        | SqlExpr::UnaryOp { .. }
        | SqlExpr::Cast { .. }
        | SqlExpr::IsNull { .. }
        | SqlExpr::Case { .. }
        | SqlExpr::InList { .. }
        | SqlExpr::Between { .. }
        | SqlExpr::Like { .. }
        | SqlExpr::ArrayLiteral(_)
        | SqlExpr::Literal(_)
        | SqlExpr::Subquery(_)
        | SqlExpr::Wildcard => computed_group_key_name(index),
    }
}

/// The key each GROUP BY key ends under in a final aggregate row, once its
/// SELECT alias applies.
///
/// `row_names` holds each key's row name and `aliases` is parallel to it. A
/// missing entry, a `None` entry, or an alias equal to the row name keeps
/// the row name. `aggregate_names` are the keys the aggregates end under.
///
/// SQL output names can repeat (`SELECT a AS x, b AS x ... GROUP BY a, b`),
/// and a row holds one cell per key. An alias therefore renames its key only
/// when no other cell of the row ends under that name: an aggregate, a key
/// that keeps its row name, or an earlier renamed key. A key whose alias is
/// taken keeps its row name, and the alias names its output column only, as
/// an aliased column of a plain SELECT does.
pub fn group_key_final_names(
    row_names: &[String],
    aliases: &[Option<String>],
    aggregate_names: &[&str],
) -> Vec<String> {
    use std::collections::HashSet;

    let wanted: Vec<Option<&str>> = row_names
        .iter()
        .enumerate()
        .map(|(index, row)| {
            aliases
                .get(index)
                .and_then(|alias| alias.as_deref())
                .filter(|alias| alias != row)
        })
        .collect();
    let mut renamed: Vec<bool> = wanted.iter().map(Option::is_some).collect();
    // A refused rename leaves its row name in the row, which can take an
    // alias an earlier pass granted. Refusals only grow, so this ends.
    loop {
        let mut taken: HashSet<&str> = aggregate_names.iter().copied().collect();
        taken.extend(
            row_names
                .iter()
                .zip(&renamed)
                .filter(|(_, renamed)| !**renamed)
                .map(|(row, _)| row.as_str()),
        );
        let mut changed = false;
        for (index, alias) in wanted.iter().enumerate() {
            let Some(alias) = alias else { continue };
            if !renamed[index] {
                continue;
            }
            if !taken.insert(*alias) {
                renamed[index] = false;
                changed = true;
            }
        }
        if !changed {
            break;
        }
    }
    row_names
        .iter()
        .zip(wanted.iter().zip(&renamed))
        .map(|(row, (alias, renamed))| match (alias, renamed) {
            (Some(alias), true) => (*alias).to_string(),
            _ => row.clone(),
        })
        .collect()
}

#[cfg(test)]
mod final_name_tests {
    use super::group_key_final_names;

    fn names(names: &[&str]) -> Vec<String> {
        names.iter().map(|n| n.to_string()).collect()
    }

    fn aliases(aliases: &[Option<&str>]) -> Vec<Option<String>> {
        aliases.iter().map(|a| a.map(str::to_string)).collect()
    }

    #[test]
    fn distinct_aliases_rename_their_keys() {
        assert_eq!(
            group_key_final_names(&names(&["a", "b"]), &aliases(&[Some("x"), None]), &["n"]),
            names(&["x", "b"])
        );
    }

    #[test]
    fn a_repeated_alias_renames_only_the_first_key() {
        assert_eq!(
            group_key_final_names(&names(&["a", "b"]), &aliases(&[Some("x"), Some("x")]), &[]),
            names(&["x", "b"])
        );
    }

    #[test]
    fn an_alias_an_aggregate_or_a_kept_key_holds_keeps_the_row_name() {
        assert_eq!(
            group_key_final_names(&names(&["a"]), &aliases(&[Some("n")]), &["n"]),
            names(&["a"])
        );
        assert_eq!(
            group_key_final_names(&names(&["a", "b"]), &aliases(&[Some("b"), None]), &[]),
            names(&["a", "b"])
        );
    }

    #[test]
    fn swapped_aliases_both_apply() {
        assert_eq!(
            group_key_final_names(&names(&["a", "b"]), &aliases(&[Some("b"), Some("a")]), &[]),
            names(&["b", "a"])
        );
    }

    #[test]
    fn a_refused_rename_frees_no_name_another_rename_took() {
        // `b AS n` is refused by the aggregate `n`, so `b` stays in the row
        // and `a AS b` must not take it.
        assert_eq!(
            group_key_final_names(
                &names(&["a", "b"]),
                &aliases(&[Some("b"), Some("n")]),
                &["n"]
            ),
            names(&["a", "b"])
        );
    }
}
