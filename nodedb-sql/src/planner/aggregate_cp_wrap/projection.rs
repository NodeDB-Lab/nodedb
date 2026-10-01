// SPDX-License-Identifier: Apache-2.0

//! Grouped output projection and sequence-accessor checks.

use super::expression::unqualify_columns;
use crate::aggregate_walk::contains_aggregate;
use crate::error::{Result, SqlError};
use crate::functions::registry::FunctionRegistry;
use crate::parser::normalize::normalize_ident;
use crate::planner::agg_naming::group_key_row_name;
use crate::planner::aggregate_order::compute_output_order_by_item;
use crate::planner::cp_projection::ast_calls_sequence_accessor;
use crate::resolver::ColumnScope;
use crate::resolver::columns::TableScope;
use crate::resolver::expr::convert_expr;
use crate::types::plan::{first_sequence_accessor, referenced_columns};
use crate::types::{AggOutputSlot, AggregateExpr, Projection, SqlExpr, SqlPlan};
use sqlparser::ast;

/// The projection restating `plan`'s output columns in SELECT-list order,
/// with a [`Projection::CpComputed`] entry at each accessor item's position.
/// `plan` is the `Aggregate` the items were planned into.
pub(super) fn aggregate_cp_projection(
    plan: &SqlPlan,
    items: &[ast::SelectItem],
    functions: &FunctionRegistry,
    scope: &TableScope,
) -> Result<Vec<Projection>> {
    let (group_by, aggregates) = match plan {
        SqlPlan::Aggregate {
            group_by,
            aggregates,
            ..
        } => (group_by, aggregates),
        other => {
            return Err(SqlError::Unsupported {
                detail: format!("aggregate wrap over a {} plan", other.variant_name()),
            });
        }
    };
    let key_names: Vec<String> = group_by
        .iter()
        .enumerate()
        .map(|(index, key)| group_key_row_name(key, index))
        .collect();
    let by_item = compute_output_order_by_item(items, group_by, functions, scope)?;
    // The item resolves against the input relations, so an unknown column is
    // the usual resolve error, and against the group-key row names, so a
    // computed key (`group_0`) is addressable. The reference check below
    // then narrows to the keys: the finalized group row carries nothing
    // else the Control Plane can read.
    let cp_scope = scope
        .with_output_names(key_names.iter().cloned())
        .allowing_cp_functions();

    let mut projection = Vec::with_capacity(items.len());
    for (item, slots) in items.iter().zip(by_item) {
        let (expr, alias) = match item {
            ast::SelectItem::UnnamedExpr(expr) => (expr, format!("{expr}").to_lowercase()),
            ast::SelectItem::ExprWithAlias { expr, alias } => (expr, normalize_ident(alias)),
            ast::SelectItem::ExprWithAliases { .. }
            | ast::SelectItem::Wildcard(_)
            | ast::SelectItem::QualifiedWildcard(..) => continue,
        };
        if ast_calls_sequence_accessor(expr) {
            let converted = convert_expr(expr, &ColumnScope::Relations(&cp_scope))?;
            if let Some(name) = first_sequence_accessor(&converted) {
                let name = name.to_string();
                projection.push(cp_item(
                    converted, name, alias, expr, &key_names, functions,
                )?);
                continue;
            }
        }
        for slot in slots {
            projection.push(Projection::Column(slot_row_name(
                slot, &key_names, aggregates,
            )?));
        }
    }
    Ok(projection)
}

/// The Control-Plane entry for one accessor item over a grouped result.
///
/// The item holds no aggregate: the Control Plane evaluates it over the
/// finalized group row, which carries aggregate values under their output
/// names but no per-row aggregate state. Every column it references is a
/// group key, unqualified so the reference matches the row's bare key.
fn cp_item(
    converted: SqlExpr,
    accessor: String,
    alias: String,
    raw: &ast::Expr,
    key_names: &[String],
    functions: &FunctionRegistry,
) -> Result<Projection> {
    if contains_aggregate(raw, functions) {
        return Err(SqlError::SequencePerRowUnsupported { name: accessor });
    }
    for column in referenced_columns(&converted) {
        let bare = column.rsplit('.').next().unwrap_or(&column);
        if !key_names.iter().any(|key| key.eq_ignore_ascii_case(bare)) {
            return Err(SqlError::Unsupported {
                detail: format!(
                    "column '{column}' beside a sequence accessor in a grouped SELECT list \
                     must be a GROUP BY key"
                ),
            });
        }
    }
    Ok(Projection::CpComputed {
        expr: unqualify_columns(converted),
        alias,
    })
}

/// The key a finalized group row carries one output slot under.
fn slot_row_name(
    slot: AggOutputSlot,
    key_names: &[String],
    aggregates: &[AggregateExpr],
) -> Result<String> {
    match slot {
        AggOutputSlot::GroupKey(index) => key_names.get(index).cloned(),
        AggOutputSlot::Aggregate(index) => aggregates.get(index).map(|a| a.alias.clone()),
    }
    .ok_or_else(|| SqlError::Unsupported {
        detail: format!("aggregate output slot {slot:?} names no output column"),
    })
}
