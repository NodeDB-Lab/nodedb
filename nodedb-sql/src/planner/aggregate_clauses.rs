// SPDX-License-Identifier: Apache-2.0

//! The GROUP BY, aggregate, HAVING and output-order clauses of one SELECT,
//! planned once for every aggregate source: a single collection or a join.

use sqlparser::ast;

use crate::error::{Result, SqlError};
use crate::functions::registry::FunctionRegistry;
use crate::planner::agg_bind::{BindName, bind_aggregate_calls};
use crate::planner::group_by::{convert_group_by_with_projection, group_by_output_aliases};
use crate::planner::grouping_sets::expand_group_by;
use crate::resolver::columns::TableScope;
use crate::types::{AggOutputSlot, AggregateExpr, ColumnInfo, Filter, SqlExpr};

/// The planned aggregate clauses of a SELECT.
pub struct AggregateClauses {
    /// GROUP BY keys. With grouping sets, the canonical deduplicated keys.
    pub group_by: Vec<SqlExpr>,
    /// ROLLUP / CUBE / GROUPING SETS expansion, as indices into `group_by`.
    pub grouping_sets: Option<Vec<Vec<usize>>>,
    /// SELECT-list aggregates, then any aggregate only HAVING names, then
    /// the `GROUPING(col)` pseudo-aggregates.
    pub aggregates: Vec<AggregateExpr>,
    /// HAVING predicates bound to the aggregates' output keys.
    pub having: Vec<Filter>,
    /// SELECT-list output aliases of the GROUP BY keys.
    pub group_by_aliases: Vec<Option<String>>,
    /// SELECT-list interleaving of keys and aggregates.
    pub output_order: Vec<AggOutputSlot>,
    /// Whether HAVING or ORDER BY computes an aggregate the SELECT list does
    /// not show.
    pub hidden_aggregates: bool,
}

/// Plan the aggregate clauses of `select` against `scope`.
///
/// `table_columns` holds the declared columns of a single-collection
/// source, so a GROUP BY name that is an input column wins over a SELECT
/// alias. A join passes an empty slice. `order_by` is the enclosing query's
/// ORDER BY, read only to learn whether it names an aggregate of its own.
pub fn plan_aggregate_clauses(
    select: &ast::Select,
    order_by: Option<&ast::OrderBy>,
    table_columns: &[ColumnInfo],
    scope: &TableScope,
    functions: &FunctionRegistry,
) -> Result<AggregateClauses> {
    refuse_star_without_group_by(select)?;
    let (group_by, grouping_sets) = match expand_group_by(&select.group_by, scope)? {
        Some(expansion) => (expansion.canonical_keys, Some(expansion.grouping_sets)),
        None => (
            convert_group_by_with_projection(
                &select.group_by,
                &select.projection,
                table_columns,
                scope,
            )?,
            None,
        ),
    };

    let mut aggregates =
        super::aggregate::extract_aggregates_from_projection(&select.projection, functions, scope)?;
    let projected = aggregates.len();
    // HAVING binds to the aggregates' computed output keys. An aggregate it
    // alone introduces is appended so it is computed.
    let having = match &select.having {
        Some(expr) => {
            super::having::plan_having(expr, &select.projection, &mut aggregates, functions, scope)?
        }
        None => Vec::new(),
    };
    let hidden_aggregates = aggregates.len() > projected
        || sort_names_new_aggregate(order_by, &select.projection, &aggregates, functions, scope)?;

    if grouping_sets.is_some() {
        let grouping_aggs =
            super::aggregate::extract_grouping_calls(&select.projection, &group_by, scope)?;
        aggregates.extend(grouping_aggs);
    }

    let output_order = super::aggregate_order::compute_output_order(
        &select.projection,
        &group_by,
        functions,
        scope,
    )?;
    let group_by_aliases = group_by_output_aliases(&select.projection, &group_by, scope);

    Ok(AggregateClauses {
        group_by,
        grouping_sets,
        aggregates,
        having,
        group_by_aliases,
        output_order,
        hidden_aggregates,
    })
}

/// Whether ORDER BY names an aggregate that neither the SELECT list nor
/// HAVING computes.
fn sort_names_new_aggregate(
    order_by: Option<&ast::OrderBy>,
    projection: &[ast::SelectItem],
    aggregates: &[AggregateExpr],
    functions: &FunctionRegistry,
    scope: &TableScope,
) -> Result<bool> {
    let Some(ast::OrderBy {
        kind: ast::OrderByKind::Expressions(exprs),
        ..
    }) = order_by
    else {
        return Ok(false);
    };
    let mut extended = aggregates.to_vec();
    for o in exprs {
        bind_aggregate_calls(
            &o.expr,
            projection,
            &mut extended,
            functions,
            BindName::Canonical,
            scope,
        )?;
    }
    Ok(extended.len() > aggregates.len())
}

/// Refuse a star in an aggregate query with no GROUP BY. Every column the
/// star reads is ungrouped, as PostgreSQL rules with `42803`.
fn refuse_star_without_group_by(select: &ast::Select) -> Result<()> {
    let ungrouped = matches!(
        &select.group_by,
        ast::GroupByExpr::Expressions(exprs, _) if exprs.is_empty()
    );
    let star = select.projection.iter().any(|item| {
        matches!(
            item,
            ast::SelectItem::Wildcard(_) | ast::SelectItem::QualifiedWildcard(..)
        )
    });
    if ungrouped && star {
        return Err(SqlError::UngroupedColumn { column: "*".into() });
    }
    Ok(())
}
