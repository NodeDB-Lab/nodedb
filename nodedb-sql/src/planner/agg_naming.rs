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
pub fn aggregate_output_key(a: &AggregateExpr) -> String {
    nodedb_query::agg_key::canonical_agg_key(&aggregate_function_name(a), &aggregate_field_name(a))
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
