// SPDX-License-Identifier: Apache-2.0

//! Extraction of the working-table equi-join link for collection-backed
//! recursive CTEs whose recursive arm references the CTE name directly
//! (so normal planning against the catalog fails).

use sqlparser::ast::{self, SetExpr};

use crate::error::{Result, SqlError};
use crate::functions::registry::FunctionRegistry;
use crate::parser::normalize::{normalize_ident, table_name_from_factor};
use crate::planner::select::CteCatalog;
use crate::resolver::columns::TableScope;
use crate::temporal::TemporalScope;
use crate::types::*;

/// Extract recursive info from the AST when normal planning fails
/// because the FROM clause references the CTE name.
///
/// Returns `(filters, join_link)` where `join_link` is the
/// `(collection_field, working_table_field)` pair for the working-table
/// hash-join.
type RecursiveInfo = (Vec<Filter>, Option<(String, String)>);

pub(super) fn extract_recursive_info(
    expr: &SetExpr,
    cte_name: &str,
    catalog: &dyn SqlCatalog,
    functions: &FunctionRegistry,
    temporal: TemporalScope,
) -> Result<RecursiveInfo> {
    let select = match expr {
        SetExpr::Select(s) => s,
        _ => {
            return Err(SqlError::Unsupported {
                detail: "recursive CTE branch must be SELECT".into(),
            });
        }
    };

    let mut real_table_alias = None;
    let mut cte_alias = None;
    let mut join_on_expr = None;

    for from in &select.from {
        if let Some((name, alias, is_unqualified)) = extract_table_reference(&from.relation)? {
            if is_unqualified && name.eq_ignore_ascii_case(cte_name) {
                cte_alias = alias.or(Some(name));
            } else {
                real_table_alias = alias.or(Some(name));
            }
        }

        for join in &from.joins {
            if let Some((name, alias, is_unqualified)) = extract_table_reference(&join.relation)? {
                if is_unqualified && name.eq_ignore_ascii_case(cte_name) {
                    cte_alias = alias.or(Some(name));
                    if let Some(cond) = extract_join_on_condition(&join.join_operator) {
                        join_on_expr = Some(cond.clone());
                    }
                } else {
                    real_table_alias = alias.or(Some(name));
                    if join_on_expr.is_none()
                        && let Some(cond) = extract_join_on_condition(&join.join_operator)
                    {
                        join_on_expr = Some(cond.clone());
                    }
                }
            }
        }
    }

    // The working table is absent from the ordinary catalog, so the CTE name
    // resolves as an open relation for the duration of this arm.
    let arm_catalog = CteCatalog::open(catalog, cte_name);
    let scope = TableScope::resolve_from(&arm_catalog, functions, temporal, &select.from)?;

    // Extract the join link from the ON condition.
    let join_link = if let (Some(real_alias), Some(cte_al), Some(on_expr)) =
        (&real_table_alias, &cte_alias, &join_on_expr)
    {
        extract_equi_link(on_expr, real_alias, cte_al)
    } else {
        None
    };
    if let Some((real_field, cte_field)) = &join_link {
        if let Some(real_alias) = &real_table_alias {
            scope.check_name(Some(real_alias), real_field)?;
        }
        if let Some(cte_al) = &cte_alias {
            scope.check_name(Some(cte_al), cte_field)?;
        }
    }

    let mut filters = Vec::new();
    if let Some(where_expr) = &select.selection {
        // Route through the shared WHERE→filter converter so the predicate is
        // operand-order canonicalized like every other WHERE clause.
        filters.extend(crate::planner::select::convert_where_to_filters(
            where_expr, &scope,
        )?);
    }

    Ok((filters, join_link))
}

/// Extract `(collection_field, cte_field)` from an equi-join ON clause.
fn extract_equi_link(
    expr: &ast::Expr,
    real_alias: &str,
    cte_alias: &str,
) -> Option<(String, String)> {
    match expr {
        ast::Expr::BinaryOp {
            left,
            op: ast::BinaryOperator::Eq,
            right,
        } => {
            let left_parts = extract_qualified_column(left)?;
            let right_parts = extract_qualified_column(right)?;

            if left_parts.0.eq_ignore_ascii_case(real_alias)
                && right_parts.0.eq_ignore_ascii_case(cte_alias)
            {
                Some((left_parts.1, right_parts.1))
            } else if right_parts.0.eq_ignore_ascii_case(real_alias)
                && left_parts.0.eq_ignore_ascii_case(cte_alias)
            {
                Some((right_parts.1, left_parts.1))
            } else {
                None
            }
        }
        ast::Expr::BinaryOp {
            left,
            op: ast::BinaryOperator::And,
            right,
        } => extract_equi_link(left, real_alias, cte_alias)
            .or_else(|| extract_equi_link(right, real_alias, cte_alias)),
        _ => None,
    }
}

fn extract_qualified_column(expr: &ast::Expr) -> Option<(String, String)> {
    match expr {
        ast::Expr::CompoundIdentifier(parts) if parts.len() == 2 => {
            Some((normalize_ident(&parts[0]), normalize_ident(&parts[1])))
        }
        _ => None,
    }
}

fn extract_table_reference(
    relation: &ast::TableFactor,
) -> Result<Option<(String, Option<String>, bool)>> {
    let is_unqualified = matches!(
        relation,
        ast::TableFactor::Table { name, .. } if name.0.len() == 1
    );
    Ok(table_name_from_factor(relation)?.map(|(name, alias)| (name, alias, is_unqualified)))
}

fn extract_join_on_condition(op: &ast::JoinOperator) -> Option<&ast::Expr> {
    use ast::JoinOperator::*;
    let constraint = match op {
        Inner(c) | LeftOuter(c) | RightOuter(c) | FullOuter(c) => c,
        _ => return None,
    };
    match constraint {
        ast::JoinConstraint::On(expr) => Some(expr),
        _ => None,
    }
}
