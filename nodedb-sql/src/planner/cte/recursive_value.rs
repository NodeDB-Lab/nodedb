// SPDX-License-Identifier: Apache-2.0

//! Value-generating `WITH RECURSIVE` CTE planning (no collection reference).

use sqlparser::ast::{self, SetExpr};

use crate::error::{Result, SqlError};
use crate::parser::normalize::normalize_ident;
use crate::types::*;

use super::recursive_scan::DEFAULT_MAX_RECURSION_DEPTH;

/// Plan a value-generating WITH RECURSIVE CTE (no collection reference).
///
/// Produces a `SqlPlan::RecursiveValue` that carries the anchor and step
/// expressions as raw SQL text for evaluation in the Data Plane.
pub(super) fn plan_recursive_value(
    left: &SetExpr,
    right: &SetExpr,
    cte_name: &str,
    declared_columns: &[String],
    distinct: bool,
) -> Result<SqlPlan> {
    let anchor_items = extract_anchor_items(left).ok_or_else(|| SqlError::Parse {
        detail: "WITH RECURSIVE anchor must be a SELECT".into(),
    })?;
    let init_exprs: Vec<String> = anchor_items.iter().map(|i| i.text.clone()).collect();

    // Validate column count against declared columns list.
    if !declared_columns.is_empty() && init_exprs.len() != declared_columns.len() {
        return Err(SqlError::RecursiveColumnMismatch {
            cte_name: cte_name.to_owned(),
            anchor_cols: init_exprs.len(),
            declared_cols: declared_columns.len(),
        });
    }

    let (step_exprs, condition) =
        extract_step_exprs_and_condition(right).ok_or_else(|| SqlError::Parse {
            detail: "WITH RECURSIVE step must be a SELECT".into(),
        })?;

    // Infer column names from the anchor when the query declares none, the way
    // PostgreSQL does. The recursive step resolves its own column references
    // against these names, so an unconditional `colN` leaves a step that says
    // `SELECT n + 1 FROM c` pointing at nothing.
    let columns = if declared_columns.is_empty() {
        anchor_items
            .iter()
            .enumerate()
            .map(|(i, item)| item.name.clone().unwrap_or_else(|| format!("col{i}")))
            .collect()
    } else {
        declared_columns.to_vec()
    };

    // Rows are keyed by column name in the executor, so two columns sharing a
    // name collapse into one. Refuse the CTE rather than return a row missing a
    // column the query asked for.
    let mut seen: std::collections::HashSet<&str> = std::collections::HashSet::new();
    for column in &columns {
        if !seen.insert(column.as_str()) {
            return Err(SqlError::DuplicateRecursiveColumn {
                cte_name: cte_name.to_owned(),
                column: column.clone(),
            });
        }
    }

    Ok(SqlPlan::RecursiveValue(RecursiveValuePlan {
        cte_name: cte_name.to_owned(),
        columns,
        init_exprs,
        step_exprs,
        condition,
        max_depth: DEFAULT_MAX_RECURSION_DEPTH,
        distinct,
    }))
}

/// One anchor projection item: the expression as SQL text, plus the output
/// name it contributes to the CTE when no column list is declared.
struct AnchorItem {
    text: String,
    /// `None` where the item has no name of its own (a bare literal, an
    /// arithmetic expression, a wildcard) and the caller must synthesize one.
    name: Option<String>,
}

/// Extract anchor projection items as SQL text plus their output names.
///
/// The output name follows PostgreSQL: an explicit alias names the column, a
/// bare or qualified column reference contributes its own name, and anything
/// else has no name. These become the CTE's columns when the query declares no
/// column list, which is what lets the recursive step refer to them.
fn extract_anchor_items(expr: &SetExpr) -> Option<Vec<AnchorItem>> {
    let select = match expr {
        SetExpr::Select(s) => s,
        _ => return None,
    };
    Some(
        select
            .projection
            .iter()
            .map(|item| match item {
                ast::SelectItem::UnnamedExpr(e) => AnchorItem {
                    text: format!("{e}"),
                    name: expr_output_name(e),
                },
                ast::SelectItem::ExprWithAlias { expr: e, alias } => AnchorItem {
                    text: format!("{e}"),
                    name: Some(normalize_ident(alias)),
                },
                // A multi-alias item binds one expression to several names,
                // which no single CTE column carries. `convert_projection`
                // refuses the shape; the name stays synthetic here so this pass
                // picks no winner among the aliases.
                ast::SelectItem::ExprWithAliases { expr: e, .. } => AnchorItem {
                    text: format!("{e}"),
                    name: None,
                },
                ast::SelectItem::Wildcard(_) => AnchorItem {
                    text: "*".into(),
                    name: None,
                },
                ast::SelectItem::QualifiedWildcard(name, _) => AnchorItem {
                    text: format!("{name}.*"),
                    name: None,
                },
            })
            .collect(),
    )
}

/// The name a projected expression contributes when it carries no alias.
fn expr_output_name(expr: &ast::Expr) -> Option<String> {
    match expr {
        ast::Expr::Identifier(ident) => Some(normalize_ident(ident)),
        // `c.n` is named `n`; the qualifier is not part of the output name.
        ast::Expr::CompoundIdentifier(parts) => parts.last().map(normalize_ident),
        _ => None,
    }
}

/// Extract step SELECT expressions and optional WHERE condition as SQL text.
///
/// Returns `(step_exprs, condition)`.
fn extract_step_exprs_and_condition(expr: &SetExpr) -> Option<(Vec<String>, Option<String>)> {
    let select = match expr {
        SetExpr::Select(s) => s,
        _ => return None,
    };
    let step_exprs = select
        .projection
        .iter()
        .map(|item| match item {
            ast::SelectItem::UnnamedExpr(e) => format!("{e}"),
            ast::SelectItem::ExprWithAlias { expr: e, .. }
            | ast::SelectItem::ExprWithAliases { expr: e, .. } => format!("{e}"),
            ast::SelectItem::Wildcard(_) => "*".into(),
            ast::SelectItem::QualifiedWildcard(name, _) => format!("{name}.*"),
        })
        .collect();
    let condition = select.selection.as_ref().map(|e| format!("{e}"));
    Some((step_exprs, condition))
}
