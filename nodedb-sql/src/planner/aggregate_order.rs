// SPDX-License-Identifier: Apache-2.0

//! Records the SELECT-list interleaving of GROUP BY keys and aggregate
//! expressions so the output-schema builder emits aggregate result columns in
//! the user's SELECT order rather than a hardcoded group-keys-first order.

use sqlparser::ast;

use crate::error::Result;
use crate::functions::registry::FunctionRegistry;
use crate::parser::normalize::normalize_ident;
use crate::planner::aggregate::{
    extract_aggregates_from_projection, function_args_exprs, normalize_function_name,
};
use crate::planner::group_by::{expr_column_name, key_column_name};
use crate::resolver::ColumnScope;
use crate::resolver::columns::TableScope;
use crate::resolver::expr::convert_expr;
use crate::types::query::AggOutputSlot;
use crate::types_expr::SqlExpr;

/// Walk `projection` in output order and classify each item as a GROUP BY key
/// slot or an aggregate slot, producing the interleaving that `output_schema`
/// replays to emit columns in SELECT-list order.
///
/// `group_by` is the canonical key list; [`AggOutputSlot::GroupKey`] indexes
/// it. Real aggregates are numbered in projection order from 0;
/// `GROUPING(col)` pseudo-aggregates are numbered after all real aggregates,
/// mirroring how the planner appends them to the aggregate list. Items that
/// are neither a bare group key nor an aggregate (a bare non-key expression)
/// produce no slot — they have no output column in the aggregate result today.
pub fn compute_output_order(
    projection: &[ast::SelectItem],
    group_by: &[SqlExpr],
    functions: &FunctionRegistry,
    scope: &TableScope,
) -> Result<Vec<AggOutputSlot>> {
    let real_agg_count = extract_aggregates_from_projection(projection, functions, scope)?.len();
    let mut order = Vec::new();
    let mut agg_cursor = 0usize;
    let mut grouping_cursor = 0usize;
    for item in projection {
        let expr = match item {
            ast::SelectItem::UnnamedExpr(expr) => expr,
            ast::SelectItem::ExprWithAlias { expr, .. } => expr,
            _ => continue,
        };
        // Bare column that names one of the GROUP BY keys.
        if let Some(name) = expr_column_name(expr)
            && let Some(index) = group_by
                .iter()
                .position(|key| key_column_name(key) == Some(name.as_str()))
        {
            order.push(AggOutputSlot::GroupKey(index));
            continue;
        }
        // Computed expression that structurally matches a GROUP BY key
        // (e.g. `UPPER(label)` selected AND grouped). It is neither a bare
        // column nor an aggregate, so it must be classified before the
        // grouping / aggregate checks below. `SqlExpr` has no `PartialEq`
        // (it can carry a subquery plan), so compare the canonical `Debug`
        // rendering — both the projection expr and the GROUP BY key pass
        // through the same `convert_expr`, so equal expressions render
        // identically.
        if let Ok(converted) = convert_expr(expr, &ColumnScope::Relations(scope)) {
            let rendered = format!("{converted:?}");
            if let Some(index) = group_by
                .iter()
                .position(|key| format!("{key:?}") == rendered)
            {
                order.push(AggOutputSlot::GroupKey(index));
                continue;
            }
        }
        // `GROUPING(col)` pseudo-aggregates: not in the function registry, so
        // `contains_aggregate` misses them, and the planner appends them after
        // every real aggregate.
        let grouping_count = count_grouping_calls(expr);
        if grouping_count > 0 {
            for _ in 0..grouping_count {
                order.push(AggOutputSlot::Aggregate(real_agg_count + grouping_cursor));
                grouping_cursor += 1;
            }
            continue;
        }
        // Ordinary aggregate expression(s).
        if crate::aggregate_walk::contains_aggregate(expr, functions) {
            let alias = match item {
                ast::SelectItem::ExprWithAlias { alias, .. } => normalize_ident(alias),
                _ => crate::planner::ast_helpers::unaliased_projection_alias(expr),
            };
            let produced =
                crate::aggregate_walk::extract_aggregates(expr, &alias, functions, scope)?.len();
            for _ in 0..produced {
                order.push(AggOutputSlot::Aggregate(agg_cursor));
                agg_cursor += 1;
            }
        }
    }
    Ok(order)
}

/// Count the `GROUPING(col)` calls reachable in `expr`, mirroring the planner's
/// grouping extraction: each argument of a `GROUPING(...)` call yields one
/// pseudo-aggregate, and binary operators are traversed on both sides.
fn count_grouping_calls(expr: &ast::Expr) -> usize {
    match expr {
        ast::Expr::Function(f) if normalize_function_name(f).eq_ignore_ascii_case("grouping") => {
            function_args_exprs(f).len()
        }
        ast::Expr::BinaryOp { left, right, .. } => {
            count_grouping_calls(left) + count_grouping_calls(right)
        }
        _ => 0,
    }
}
