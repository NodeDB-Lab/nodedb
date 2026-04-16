//! CTE (WITH clause) and WITH RECURSIVE planning.

use sqlparser::ast::{self, Query, SetExpr};

use crate::error::{Result, SqlError};
use crate::functions::registry::FunctionRegistry;
use crate::parser::normalize::{normalize_ident, normalize_object_name};
use crate::types::*;

/// Plan a WITH RECURSIVE query.
///
/// Supports table-based recursive CTEs where the base case scans a real
/// collection and the recursive step references both the collection and
/// the CTE. Value-generating CTEs (no underlying collection) return an
/// explicit unsupported error.
pub fn plan_recursive_cte(
    query: &Query,
    catalog: &dyn SqlCatalog,
    functions: &FunctionRegistry,
) -> Result<SqlPlan> {
    let with = query.with.as_ref().ok_or_else(|| SqlError::Parse {
        detail: "expected WITH clause".into(),
    })?;

    let cte = with.cte_tables.first().ok_or_else(|| SqlError::Parse {
        detail: "empty WITH clause".into(),
    })?;

    let cte_name = normalize_ident(&cte.alias.name);

    let cte_query = &cte.query;

    // The CTE body should be a UNION of base case and recursive case.
    let (left, right, set_quantifier) = match &*cte_query.body {
        SetExpr::SetOperation {
            op: ast::SetOperator::Union,
            left,
            right,
            set_quantifier,
        } => (left, right, set_quantifier),
        _ => {
            return Err(SqlError::Unsupported {
                detail: "WITH RECURSIVE requires UNION in CTE body".into(),
            });
        }
    };

    // UNION ALL → distinct = false; UNION → distinct = true.
    let distinct = !matches!(set_quantifier, ast::SetQuantifier::All);

    // Plan the base case (should not reference the CTE name).
    let base = plan_cte_branch(left, catalog, functions)?;

    // Extract the source collection from the base case.
    let collection = extract_collection(&base).unwrap_or_default();

    // Plan the recursive branch. The recursive branch references the CTE
    // name in its FROM clause — either directly (value-gen) or via a JOIN
    // with a real table. We attempt to plan it; if it fails because the
    // CTE name isn't in the catalog, we try to extract the real table from
    // a JOIN and use it with the CTE self-reference as the recursive filter.
    let (recursive_filters, join_link) = match plan_cte_branch(right, catalog, functions) {
        Ok(plan) => (extract_filters(&plan), None),
        Err(_) => {
            // The recursive branch references the CTE name. Try to extract
            // the real collection, filters, and join link from the AST.
            extract_recursive_info(right, &cte_name)?
        }
    };

    if collection.is_empty() {
        return Err(SqlError::Unsupported {
            detail: "WITH RECURSIVE requires a base case that scans a collection; \
                     value-generating recursive CTEs are not yet supported"
                .into(),
        });
    }

    Ok(SqlPlan::RecursiveScan {
        collection,
        base_filters: extract_filters(&base),
        recursive_filters,
        join_link,
        max_iterations: 100,
        distinct,
        limit: 10000,
    })
}

/// Extract recursive info from the AST when normal planning fails
/// because the FROM clause references the CTE name.
///
/// Returns `(filters, join_link)` where `join_link` is the
/// `(collection_field, working_table_field)` pair for the working-table
/// hash-join.
///
/// Handles the common tree-traversal pattern:
/// `SELECT t.id FROM tree t INNER JOIN cte_name d ON t.parent_id = d.id`
/// → join_link = `("parent_id", "id")`
/// `(filters, join_link)` where `join_link` is `(collection_field, working_table_field)`.
type RecursiveInfo = (Vec<Filter>, Option<(String, String)>);

fn extract_recursive_info(expr: &SetExpr, cte_name: &str) -> Result<RecursiveInfo> {
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
        let table_name = extract_table_name(&from.relation);
        let table_alias = extract_table_alias(&from.relation);

        if let Some(name) = &table_name {
            if name.eq_ignore_ascii_case(cte_name) {
                cte_alias = table_alias.or_else(|| Some(name.clone()));
            } else {
                real_table_alias = table_alias.or_else(|| Some(name.clone()));
            }
        }

        for join in &from.joins {
            let join_table = extract_table_name(&join.relation);
            let join_alias = extract_table_alias(&join.relation);
            if let Some(jt) = &join_table {
                if jt.eq_ignore_ascii_case(cte_name) {
                    cte_alias = join_alias.or_else(|| Some(jt.clone()));
                    if let Some(cond) = extract_join_on_condition(&join.join_operator) {
                        join_on_expr = Some(cond.clone());
                    }
                } else {
                    real_table_alias = join_alias.or_else(|| Some(jt.clone()));
                    if join_on_expr.is_none()
                        && let Some(cond) = extract_join_on_condition(&join.join_operator)
                    {
                        join_on_expr = Some(cond.clone());
                    }
                }
            }
        }
    }

    // Extract the join link from the ON condition.
    let join_link = if let (Some(real_alias), Some(cte_al), Some(on_expr)) =
        (&real_table_alias, &cte_alias, &join_on_expr)
    {
        extract_equi_link(on_expr, real_alias, cte_al)
    } else {
        None
    };

    // Convert the WHERE clause to filters if present.
    let mut filters = Vec::new();
    if let Some(where_expr) = &select.selection {
        let converted = crate::resolver::expr::convert_expr(where_expr)?;
        filters.push(Filter {
            expr: FilterExpr::Expr(converted),
        });
    }

    Ok((filters, join_link))
}

/// Extract `(collection_field, cte_field)` from an equi-join ON clause.
///
/// Given `t.parent_id = d.id` where `t` is the real table alias and `d`
/// is the CTE alias, returns `("parent_id", "id")`.
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

            // Determine which side is the real table and which is the CTE.
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
        // For AND-combined conditions, take the first equi-link found.
        ast::Expr::BinaryOp {
            left,
            op: ast::BinaryOperator::And,
            right,
        } => extract_equi_link(left, real_alias, cte_alias)
            .or_else(|| extract_equi_link(right, real_alias, cte_alias)),
        _ => None,
    }
}

/// Extract `(table_or_alias, column)` from a qualified column reference.
fn extract_qualified_column(expr: &ast::Expr) -> Option<(String, String)> {
    match expr {
        ast::Expr::CompoundIdentifier(parts) if parts.len() == 2 => {
            Some((normalize_ident(&parts[0]), normalize_ident(&parts[1])))
        }
        _ => None,
    }
}

fn extract_table_name(relation: &ast::TableFactor) -> Option<String> {
    match relation {
        ast::TableFactor::Table { name, .. } => Some(normalize_object_name(name)),
        _ => None,
    }
}

fn extract_table_alias(relation: &ast::TableFactor) -> Option<String> {
    match relation {
        ast::TableFactor::Table { alias, .. } => alias.as_ref().map(|a| normalize_ident(&a.name)),
        _ => None,
    }
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

fn plan_cte_branch(
    expr: &SetExpr,
    catalog: &dyn SqlCatalog,
    functions: &FunctionRegistry,
) -> Result<SqlPlan> {
    match expr {
        SetExpr::Select(select) => {
            let query = Query {
                with: None,
                body: Box::new(SetExpr::Select(select.clone())),
                order_by: None,
                limit_clause: None,
                fetch: None,
                locks: Vec::new(),
                for_clause: None,
                settings: None,
                format_clause: None,
                pipe_operators: Vec::new(),
            };
            super::select::plan_query(&query, catalog, functions)
        }
        _ => Err(SqlError::Unsupported {
            detail: "CTE branch must be SELECT".into(),
        }),
    }
}

fn extract_collection(plan: &SqlPlan) -> Option<String> {
    match plan {
        SqlPlan::Scan { collection, .. } => Some(collection.clone()),
        _ => None,
    }
}

fn extract_filters(plan: &SqlPlan) -> Vec<Filter> {
    match plan {
        SqlPlan::Scan { filters, .. } => filters.clone(),
        _ => Vec::new(),
    }
}
