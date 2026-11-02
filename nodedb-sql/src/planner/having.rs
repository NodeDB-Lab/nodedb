// SPDX-License-Identifier: Apache-2.0

//! HAVING planning: bind the predicate to computed aggregate columns.
//!
//! A HAVING predicate is evaluated against finalized group rows, so every
//! aggregate it mentions must (a) actually be computed and (b) be addressed by
//! the column name it lands under.
//!
//! Left untranslated, `HAVING SUM(amount) > 0` reaches the executor as a
//! literal `sum(...)` *call* over a group row that has no such column and no
//! scalar `sum` to apply — every group fails the predicate and the query
//! returns nothing, for data that plainly satisfies it. And an aggregate named
//! only in HAVING was never added to the aggregate list, so it was never
//! computed at all.
//!
//! HAVING filters *before* aggregates are renamed to their user aliases, so it
//! binds to the canonical key.

use sqlparser::ast;

use crate::aggregate_walk::contains_aggregate;
use crate::error::{Result, SqlError};
use crate::functions::registry::FunctionRegistry;
use crate::planner::agg_bind::{BindName, bind_aggregate_calls};
use crate::planner::agg_naming::aggregate_output_key;
use crate::planner::select::select_output_aliases;
use crate::resolver::columns::{ColumnNaming, TableScope};
use crate::types::{AggregateExpr, Filter};

/// Convert a HAVING clause into filters over finalized group rows.
///
/// `aggregates` is extended in place with any aggregate that appears only in
/// HAVING, so it is computed alongside the projected ones.
pub fn plan_having(
    having: &ast::Expr,
    projection: &[ast::SelectItem],
    aggregates: &mut Vec<AggregateExpr>,
    functions: &FunctionRegistry,
    scope: &TableScope,
) -> Result<Vec<Filter>> {
    let rewritten = bind_aggregate_calls(
        having,
        projection,
        aggregates,
        functions,
        BindName::Canonical,
        scope,
    )?;

    // Any aggregate call still standing is one this rewrite did not reach.
    // Failing loudly beats handing the executor a predicate that silently
    // matches no group.
    if contains_aggregate(&rewritten, functions) {
        return Err(SqlError::Unsupported {
            detail: format!(
                "HAVING predicate shape is not supported: aggregate call in `{having}` could not \
                 be bound to a computed group column"
            ),
        });
    }

    // The rewritten predicate addresses computed group columns: an
    // aggregate's canonical key, its output alias, or a SELECT-list alias.
    // Those are the aggregate's output rows. Over a join they key a group
    // column by its bare name, never by the merged row's `qualifier.column`.
    let output_naming = match scope.column_naming() {
        ColumnNaming::JoinRow | ColumnNaming::OutputRow => ColumnNaming::OutputRow,
        ColumnNaming::AsWritten | ColumnNaming::JoinWriteRow => ColumnNaming::AsWritten,
    };
    let having_scope = scope
        .with_output_names(
            aggregates
                .iter()
                .map(aggregate_output_key)
                .chain(aggregates.iter().map(|a| a.alias.clone()))
                .chain(select_output_aliases(projection)),
        )
        .with_column_naming(output_naming);
    crate::planner::select::convert_where_to_filters(&rewritten, &having_scope)
}
