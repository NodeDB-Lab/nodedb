// SPDX-License-Identifier: Apache-2.0

//! Extraction over a LATERAL subquery's AST: its inner relation, its
//! non-correlated filters, and its row bounds.

use sqlparser::ast;

use crate::error::{Result, SqlError};
use crate::parser::normalize::normalize_ident;
use crate::reserved::check_ast_identifier;
use crate::resolver::columns::TableScope;
use crate::types::Filter;

/// Extract the alias of the single-table inner SELECT, if present.
pub(super) fn extract_inner_alias(select: &sqlparser::ast::Select) -> Result<Option<String>> {
    let Some(from) = select.from.first() else {
        return Ok(None);
    };
    match &from.relation {
        ast::TableFactor::Table { alias, .. } => alias
            .as_ref()
            .map(|alias| check_ast_identifier(&alias.name))
            .transpose(),
        _ => Ok(None),
    }
}

/// Extract the collection name from a single-table inner SELECT.
pub(super) fn extract_inner_collection(select: &sqlparser::ast::Select) -> Result<String> {
    let from = select.from.first().ok_or_else(|| SqlError::Unsupported {
        detail: "LATERAL subquery must have a FROM clause".into(),
    })?;
    crate::parser::normalize::table_name_from_factor(&from.relation)?
        .map(|(name, _)| name)
        .ok_or_else(|| SqlError::Unsupported {
            detail: "LATERAL LateralTopK subquery must reference a plain table".into(),
        })
}

/// Extract filters from the inner SELECT that do NOT reference the outer alias.
pub(super) fn inner_non_correlated_filters(
    select: &sqlparser::ast::Select,
    outer_alias: &str,
    scope: &TableScope,
) -> Result<Vec<Filter>> {
    let Some(where_expr) = &select.selection else {
        return Ok(Vec::new());
    };
    let remaining = strip_outer_refs(where_expr, outer_alias);
    match remaining {
        Some(expr) => crate::planner::select::convert_where_to_filters(&expr, scope),
        None => Ok(Vec::new()),
    }
}

/// Remove all predicates referencing `outer_alias` from a WHERE expression.
fn strip_outer_refs(expr: &ast::Expr, outer_alias: &str) -> Option<ast::Expr> {
    match expr {
        ast::Expr::BinaryOp {
            left,
            op: ast::BinaryOperator::And,
            right,
        } => {
            let l = strip_outer_refs(left, outer_alias);
            let r = strip_outer_refs(right, outer_alias);
            match (l, r) {
                (None, None) => None,
                (Some(e), None) | (None, Some(e)) => Some(e),
                (Some(l), Some(r)) => Some(ast::Expr::BinaryOp {
                    left: Box::new(l),
                    op: ast::BinaryOperator::And,
                    right: Box::new(r),
                }),
            }
        }
        ast::Expr::BinaryOp { left, right, .. } => {
            if refs_outer(left, outer_alias) || refs_outer(right, outer_alias) {
                None
            } else {
                Some(expr.clone())
            }
        }
        ast::Expr::Nested(inner) => strip_outer_refs(inner, outer_alias),
        _ => Some(expr.clone()),
    }
}

fn refs_outer(expr: &ast::Expr, outer_alias: &str) -> bool {
    match expr {
        ast::Expr::CompoundIdentifier(parts) if parts.len() == 2 => {
            normalize_ident(&parts[0]).eq_ignore_ascii_case(outer_alias)
        }
        ast::Expr::BinaryOp { left, right, .. } => {
            refs_outer(left, outer_alias) || refs_outer(right, outer_alias)
        }
        _ => false,
    }
}

/// Extract the LIMIT value from a query, or fail on a bound that does not
/// resolve to `[0, usize::MAX]`. `LIMIT NULL` / `LIMIT ALL` and an absent
/// clause all mean no bound, so both map to `None`.
pub(super) fn limit_from_query(query: &ast::Query) -> Result<Option<usize>> {
    match &query.limit_clause {
        Some(ast::LimitClause::LimitOffset {
            limit: Some(limit), ..
        })
        | Some(ast::LimitClause::OffsetCommaLimit { limit, .. }) => {
            Ok(crate::coerce::checked_row_bound("LIMIT", limit)?.limit())
        }
        Some(ast::LimitClause::LimitOffset { limit: None, .. }) | None => Ok(None),
    }
}

/// Reject an inner OFFSET on a LATERAL subquery.
///
/// `SqlPlan::LateralTopK` carries no offset field and `SqlPlan::LateralLoop`
/// carries neither limit nor offset. A per-outer-row OFFSET needs a new plan
/// field plus Data Plane execution that skips rows per outer row, so this
/// rejects rather than silently drops the clause. `OFFSET 0` and `OFFSET
/// NULL` skip nothing and plan cleanly; a resolved offset above zero fails
/// with `SqlError::Unsupported`. An offset literal outside `[0, usize::MAX]`
/// fails first, inside `checked_row_bound`, with `SqlError::InvalidLimitValue`.
pub(super) fn reject_lateral_offset(query: &ast::Query) -> Result<()> {
    let offset_expr = match &query.limit_clause {
        Some(ast::LimitClause::LimitOffset {
            offset: Some(offset),
            ..
        }) => Some(&offset.value),
        Some(ast::LimitClause::OffsetCommaLimit { offset, .. }) => Some(offset),
        Some(ast::LimitClause::LimitOffset { offset: None, .. }) | None => None,
    };
    let Some(expr) = offset_expr else {
        return Ok(());
    };
    if crate::coerce::checked_row_bound("OFFSET", expr)?.offset() > 0 {
        return Err(SqlError::Unsupported {
            detail: "OFFSET inside a LATERAL subquery is not supported".into(),
        });
    }
    Ok(())
}

/// Extract and validate a LATERAL alias from a `TableFactor::Derived`.
pub fn lateral_alias_from_factor(factor: &ast::TableFactor) -> Result<Option<String>> {
    match factor {
        ast::TableFactor::Derived { alias, .. } => alias
            .as_ref()
            .map(|alias| check_ast_identifier(&alias.name))
            .transpose(),
        _ => Ok(None),
    }
}

/// True when a `TableFactor` is a LATERAL derived subquery.
pub fn is_lateral_derived(factor: &ast::TableFactor) -> bool {
    matches!(factor, ast::TableFactor::Derived { lateral: true, .. })
}

/// Extract the subquery from a `TableFactor::Derived`.
pub fn subquery_from_factor(factor: &ast::TableFactor) -> Option<&ast::Query> {
    match factor {
        ast::TableFactor::Derived { subquery, .. } => Some(subquery),
        _ => None,
    }
}

/// The subquery of a LATERAL item. A factor with no subquery is a typed error.
pub fn lateral_subquery(factor: &ast::TableFactor) -> Result<&ast::Query> {
    subquery_from_factor(factor).ok_or_else(|| SqlError::Unsupported {
        detail: format!("LATERAL expects a subquery, got `{factor}`; write LATERAL (SELECT ...)"),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::parser::statement::parse_sql;

    #[test]
    fn a_factor_with_no_subquery_is_a_typed_error() {
        let statements = parse_sql("SELECT * FROM t").expect("parses");
        let ast::Statement::Query(query) = &statements[0] else {
            panic!("not a query");
        };
        let ast::SetExpr::Select(select) = query.body.as_ref() else {
            panic!("not a SELECT");
        };
        assert!(matches!(
            lateral_subquery(&select.from[0].relation),
            Err(SqlError::Unsupported { .. })
        ));
    }
}
