// SPDX-License-Identifier: Apache-2.0

//! `IN (SELECT ...)` and `NOT IN (SELECT ...)` lowered to semi / anti joins.

use sqlparser::ast::{self, Expr, SetExpr};

use crate::error::{Result, SqlError};
use crate::functions::registry::FunctionRegistry;
use crate::parser::normalize::{SCHEMA_QUALIFIED_MSG, normalize_ident};
use crate::types::*;

use super::extract::SubqueryJoin;

/// Try to plan `col IN (SELECT col2 FROM tbl ...)` as a semi/anti join.
pub(super) fn try_plan_in_subquery(
    outer_expr: &Expr,
    subquery: &ast::Query,
    negated: bool,
    outer: &crate::resolver::columns::TableScope,
    catalog: &dyn SqlCatalog,
    functions: &FunctionRegistry,
    temporal: crate::TemporalScope,
) -> Result<Option<SubqueryJoin>> {
    // This rewrite consumes the whole predicate, so the outer operand never
    // reaches the expression converter. It is checked here or nowhere.
    let outer_col = match outer_expr {
        Expr::Identifier(ident) => {
            let col = normalize_ident(ident);
            outer.check_name(None, &col)?;
            col
        }
        Expr::CompoundIdentifier(parts) if parts.len() >= 3 => {
            let qualified: String = parts
                .iter()
                .map(normalize_ident)
                .collect::<Vec<_>>()
                .join(".");
            return Err(SqlError::Unsupported {
                detail: format!(
                    "schema-qualified column reference '{qualified}': {SCHEMA_QUALIFIED_MSG}"
                ),
            });
        }
        Expr::CompoundIdentifier(parts) if parts.len() == 2 => {
            let qualifier = normalize_ident(&parts[0]);
            let col = normalize_ident(&parts[1]);
            outer.check_name(Some(&qualifier), &col)?;
            col
        }
        _ => return Ok(None), // Complex expression, can't rewrite.
    };

    // Plan the inner SELECT.
    let inner_plan = crate::planner::select::plan_query(subquery, catalog, functions, temporal)?;

    // Extract the projected column from the inner plan.
    let inner_col = extract_single_projected_column(subquery)?;

    Ok(Some(SubqueryJoin {
        on: vec![(outer_col, inner_col)],
        inner_plan,
        // `NOT IN` is null-aware: a NULL on either side makes the predicate
        // unknown, unlike `NOT EXISTS`.
        join_type: if negated {
            JoinType::AntiNullAware
        } else {
            JoinType::Semi
        },
    }))
}

/// Extract the single column name from a subquery's SELECT list.
///
/// For `SELECT user_id FROM orders`, returns `"user_id"`.
fn extract_single_projected_column(query: &ast::Query) -> Result<String> {
    let select = match &*query.body {
        SetExpr::Select(s) => s,
        _ => {
            return Err(SqlError::Unsupported {
                detail: "subquery must be a simple SELECT".into(),
            });
        }
    };

    if select.projection.len() != 1 {
        return Err(SqlError::Unsupported {
            detail: format!(
                "subquery must select exactly 1 column, got {}",
                select.projection.len()
            ),
        });
    }

    match &select.projection[0] {
        ast::SelectItem::UnnamedExpr(expr) => match expr {
            Expr::Identifier(ident) => Ok(normalize_ident(ident)),
            Expr::CompoundIdentifier(parts) if parts.len() >= 3 => {
                let qualified: String = parts
                    .iter()
                    .map(normalize_ident)
                    .collect::<Vec<_>>()
                    .join(".");
                Err(SqlError::Unsupported {
                    detail: format!(
                        "schema-qualified column reference '{qualified}': {SCHEMA_QUALIFIED_MSG}"
                    ),
                })
            }
            Expr::CompoundIdentifier(parts) if parts.len() == 2 => Ok(normalize_ident(&parts[1])),
            _ => Err(SqlError::Unsupported {
                detail: "subquery projection must be a column reference".into(),
            }),
        },
        ast::SelectItem::ExprWithAlias { alias, .. } => Ok(normalize_ident(alias)),
        _ => Err(SqlError::Unsupported {
            detail: "subquery projection must be a column reference".into(),
        }),
    }
}
