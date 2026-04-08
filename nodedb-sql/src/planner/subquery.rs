//! Subquery planning: IN (SELECT ...), NOT IN (SELECT ...), scalar subqueries.
//!
//! Rewrites WHERE-clause subqueries into semi/anti joins so the existing
//! hash-join executor handles them without a dedicated subquery engine.
//!
//! Supported patterns:
//!   - `WHERE col IN (SELECT col2 FROM tbl ...)`  → semi-join
//!   - `WHERE col NOT IN (SELECT col2 FROM tbl ...)` → anti-join
//!   - `WHERE col > (SELECT AGG(...) FROM tbl ...)` → scalar subquery (materialized)

use sqlparser::ast::{self, Expr, SetExpr};

use crate::error::{Result, SqlError};
use crate::functions::registry::FunctionRegistry;
use crate::parser::normalize::normalize_ident;
use crate::types::*;

/// Result of extracting subqueries from a WHERE clause.
pub struct SubqueryExtraction {
    /// Semi/anti joins to wrap around the base scan.
    pub joins: Vec<SubqueryJoin>,
    /// Remaining WHERE expression with subqueries removed (None if nothing remains).
    pub remaining_where: Option<Expr>,
}

/// A subquery that was rewritten as a join.
pub struct SubqueryJoin {
    /// The column on the outer table to join on.
    pub outer_column: String,
    /// The planned inner SELECT.
    pub inner_plan: SqlPlan,
    /// The column from the inner SELECT to join on.
    pub inner_column: String,
    /// Semi (IN) or Anti (NOT IN).
    pub join_type: JoinType,
}

/// Extract `IN (SELECT ...)` and `NOT IN (SELECT ...)` patterns from a WHERE clause.
///
/// Returns the extracted subquery joins and the remaining WHERE expression
/// (with subquery predicates removed). If the entire WHERE is a single
/// subquery predicate, `remaining_where` is `None`.
pub fn extract_subqueries(
    expr: &Expr,
    catalog: &dyn SqlCatalog,
    functions: &FunctionRegistry,
) -> Result<SubqueryExtraction> {
    let mut joins = Vec::new();
    let remaining = extract_recursive(expr, &mut joins, catalog, functions)?;
    Ok(SubqueryExtraction {
        joins,
        remaining_where: remaining,
    })
}

/// Recursively walk the WHERE expression, extracting subquery predicates.
///
/// Returns `None` if the entire expression was consumed (subquery-only),
/// or `Some(expr)` with the remaining non-subquery predicates.
fn extract_recursive(
    expr: &Expr,
    joins: &mut Vec<SubqueryJoin>,
    catalog: &dyn SqlCatalog,
    functions: &FunctionRegistry,
) -> Result<Option<Expr>> {
    match expr {
        // AND: recurse both sides, reconstruct with remaining parts.
        Expr::BinaryOp {
            left,
            op: ast::BinaryOperator::And,
            right,
        } => {
            let left_remaining = extract_recursive(left, joins, catalog, functions)?;
            let right_remaining = extract_recursive(right, joins, catalog, functions)?;
            match (left_remaining, right_remaining) {
                (None, None) => Ok(None),
                (Some(l), None) => Ok(Some(l)),
                (None, Some(r)) => Ok(Some(r)),
                (Some(l), Some(r)) => Ok(Some(Expr::BinaryOp {
                    left: Box::new(l),
                    op: ast::BinaryOperator::And,
                    right: Box::new(r),
                })),
            }
        }

        // IN (SELECT ...): rewrite as semi-join.
        Expr::InSubquery {
            expr: outer_expr,
            subquery,
            negated,
        } => {
            if let Some(join) =
                try_plan_in_subquery(outer_expr, subquery, *negated, catalog, functions)?
            {
                joins.push(join);
                Ok(None) // This predicate is consumed.
            } else {
                // Cannot plan as join — return original expression.
                Ok(Some(expr.clone()))
            }
        }

        // Scalar subquery comparison: `col > (SELECT AGG(...) FROM ...)`
        Expr::BinaryOp { left, op, right } if is_comparison_op(op) => {
            if let Expr::Subquery(subquery) = right.as_ref() {
                if let Some(scalar) = try_plan_scalar_subquery(subquery, catalog, functions)? {
                    joins.push(scalar.join);
                    Ok(Some(Expr::BinaryOp {
                        left: left.clone(),
                        op: op.clone(),
                        right: Box::new(scalar.replacement_expr),
                    }))
                } else {
                    Ok(Some(expr.clone()))
                }
            } else {
                Ok(Some(expr.clone()))
            }
        }

        // EXISTS (SELECT ...): rewrite as semi-join.
        // NOT EXISTS (SELECT ...): rewrite as anti-join.
        Expr::Exists { subquery, negated } => {
            if let Some(join) = try_plan_exists_subquery(subquery, *negated, catalog, functions)? {
                joins.push(join);
                Ok(None)
            } else {
                Ok(Some(expr.clone()))
            }
        }

        // Nested parentheses.
        Expr::Nested(inner) => extract_recursive(inner, joins, catalog, functions),

        // Not a subquery pattern — return as-is.
        _ => Ok(Some(expr.clone())),
    }
}

/// Try to plan `col IN (SELECT col2 FROM tbl ...)` as a semi/anti join.
fn try_plan_in_subquery(
    outer_expr: &Expr,
    subquery: &ast::Query,
    negated: bool,
    catalog: &dyn SqlCatalog,
    functions: &FunctionRegistry,
) -> Result<Option<SubqueryJoin>> {
    // Extract outer column name.
    let outer_col = match outer_expr {
        Expr::Identifier(ident) => normalize_ident(ident),
        Expr::CompoundIdentifier(parts) if parts.len() == 2 => normalize_ident(&parts[1]),
        _ => return Ok(None), // Complex expression, can't rewrite.
    };

    // Plan the inner SELECT.
    let inner_plan = super::select::plan_query(subquery, catalog, functions)?;

    // Extract the projected column from the inner plan.
    let inner_col = extract_single_projected_column(subquery)?;

    Ok(Some(SubqueryJoin {
        outer_column: outer_col,
        inner_plan,
        inner_column: inner_col,
        join_type: if negated {
            JoinType::Anti
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

/// Plan `EXISTS (SELECT 1 FROM tbl WHERE tbl.col = outer.col)` as a semi/anti join.
///
/// Extracts the correlated column from the subquery's WHERE clause.
fn try_plan_exists_subquery(
    subquery: &ast::Query,
    negated: bool,
    catalog: &dyn SqlCatalog,
    functions: &FunctionRegistry,
) -> Result<Option<SubqueryJoin>> {
    let select = match &*subquery.body {
        SetExpr::Select(s) => s,
        _ => return Ok(None),
    };

    // Look for a correlated predicate in the WHERE: inner.col = outer.col
    let (outer_col, inner_col) = match &select.selection {
        Some(expr) => match extract_correlated_eq(expr) {
            Some(pair) => pair,
            None => return Ok(None),
        },
        None => return Ok(None),
    };

    // Build a simplified subquery without the correlated predicate for planning.
    let inner_plan = super::select::plan_query(subquery, catalog, functions)?;

    Ok(Some(SubqueryJoin {
        outer_column: outer_col,
        inner_plan,
        inner_column: inner_col,
        join_type: if negated {
            JoinType::Anti
        } else {
            JoinType::Semi
        },
    }))
}

/// Extract a correlated equality predicate from a WHERE clause.
///
/// Looks for patterns like `o.user_id = u.id` and returns (outer_col, inner_col).
/// The "inner" column is the one qualified with the subquery's table alias;
/// the "outer" column is the one referencing the outer query's table.
fn extract_correlated_eq(expr: &Expr) -> Option<(String, String)> {
    match expr {
        Expr::BinaryOp {
            left,
            op: ast::BinaryOperator::Eq,
            right,
        } => {
            let left_parts = extract_qualified_column(left);
            let right_parts = extract_qualified_column(right);
            match (left_parts, right_parts) {
                (Some((_lt, lc)), Some((_rt, rc))) => {
                    // Convention: left is inner (subquery table), right is outer.
                    // But we can't distinguish without schema, so just return both.
                    Some((rc, lc))
                }
                _ => None,
            }
        }
        // For AND, try to find a correlated eq in either side.
        Expr::BinaryOp {
            left,
            op: ast::BinaryOperator::And,
            right,
        } => extract_correlated_eq(left).or_else(|| extract_correlated_eq(right)),
        Expr::Nested(inner) => extract_correlated_eq(inner),
        _ => None,
    }
}

/// Extract table.column from a qualified identifier.
fn extract_qualified_column(expr: &Expr) -> Option<(String, String)> {
    match expr {
        Expr::CompoundIdentifier(parts) if parts.len() == 2 => {
            Some((normalize_ident(&parts[0]), normalize_ident(&parts[1])))
        }
        Expr::Identifier(ident) => Some((String::new(), normalize_ident(ident))),
        _ => None,
    }
}

fn is_comparison_op(op: &ast::BinaryOperator) -> bool {
    matches!(
        op,
        ast::BinaryOperator::Gt
            | ast::BinaryOperator::GtEq
            | ast::BinaryOperator::Lt
            | ast::BinaryOperator::LtEq
            | ast::BinaryOperator::Eq
            | ast::BinaryOperator::NotEq
    )
}

/// Result of planning a scalar subquery.
struct ScalarSubqueryResult {
    join: SubqueryJoin,
    replacement_expr: Expr,
}

/// Plan a scalar subquery (e.g., `(SELECT AVG(amount) FROM orders)`).
///
/// Rewrites `col > (SELECT AVG(amount) FROM orders)` as:
///   cross-join with the aggregate result (1 row), then filter `col > result_col`.
///
/// The cross-join produces a cartesian product, but since the aggregate returns
/// exactly 1 row, every outer row gets paired with that single result row.
fn try_plan_scalar_subquery(
    subquery: &ast::Query,
    catalog: &dyn SqlCatalog,
    functions: &FunctionRegistry,
) -> Result<Option<ScalarSubqueryResult>> {
    let inner_plan = super::select::plan_query(subquery, catalog, functions)?;

    // Extract the result column name from the subquery's SELECT list.
    let result_col = match extract_scalar_column(subquery) {
        Some(col) => col,
        None => return Ok(None),
    };

    let replacement = Expr::Identifier(ast::Ident::new(&result_col));

    Ok(Some(ScalarSubqueryResult {
        join: SubqueryJoin {
            outer_column: String::new(),
            inner_plan,
            inner_column: String::new(),
            join_type: JoinType::Cross,
        },
        replacement_expr: replacement,
    }))
}

/// Extract the projected column name from a scalar subquery.
///
/// Handles aliased aggregates like `SELECT AVG(amount) AS avg_amount`.
/// For unaliased aggregates, synthesizes a name.
fn extract_scalar_column(query: &ast::Query) -> Option<String> {
    let select = match &*query.body {
        SetExpr::Select(s) => s,
        _ => return None,
    };
    if select.projection.len() != 1 {
        return None;
    }
    match &select.projection[0] {
        ast::SelectItem::ExprWithAlias { alias, .. } => Some(normalize_ident(alias)),
        ast::SelectItem::UnnamedExpr(expr) => match expr {
            Expr::Identifier(ident) => Some(normalize_ident(ident)),
            Expr::Function(func) => {
                // Synthesize name matching aggregate handler convention:
                // AVG(amount) → "avg_amount", COUNT(*) → "count_all"
                let func_name = func
                    .name
                    .0
                    .iter()
                    .map(|p| match p {
                        ast::ObjectNamePart::Identifier(ident) => normalize_ident(ident),
                        _ => String::new(),
                    })
                    .collect::<Vec<_>>()
                    .join(".")
                    .to_lowercase();
                let arg = match &func.args {
                    ast::FunctionArguments::List(arg_list) => arg_list
                        .args
                        .first()
                        .and_then(|a| match a {
                            ast::FunctionArg::Unnamed(ast::FunctionArgExpr::Expr(
                                Expr::Identifier(ident),
                            )) => Some(normalize_ident(ident)),
                            ast::FunctionArg::Unnamed(
                                ast::FunctionArgExpr::Wildcard
                                | ast::FunctionArgExpr::QualifiedWildcard(_),
                            ) => Some("all".to_string()),
                            _ => None,
                        })
                        .unwrap_or_else(|| "all".to_string()),
                    _ => "all".to_string(),
                };
                Some(format!("{func_name}_{arg}"))
            }
            _ => None,
        },
        _ => None,
    }
}
