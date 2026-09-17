// SPDX-License-Identifier: Apache-2.0

//! Entry point and collection-backed recursive-scan planning for
//! `WITH RECURSIVE` CTEs.

use sqlparser::ast::{self, Query, SetExpr};

use crate::error::{Result, SqlError};
use crate::functions::registry::FunctionRegistry;
use crate::reserved::check_ast_identifier;
use crate::types::*;

use super::recursive_value::plan_recursive_value;
use super::validate::{count_select_cols, validate_self_ref_count};

/// Default maximum recursion depth for WITH RECURSIVE queries.
pub const DEFAULT_MAX_RECURSION_DEPTH: usize = 1000;

/// Plan a WITH RECURSIVE query.
///
/// Dispatches to either `plan_recursive_scan` (collection-backed) or
/// `plan_recursive_value` (pure expression / value-generating) based on
/// whether the anchor arm references a real collection.
pub fn plan_recursive_cte(
    query: &Query,
    catalog: &dyn SqlCatalog,
    functions: &FunctionRegistry,
    temporal: crate::TemporalScope,
) -> Result<SqlPlan> {
    let with = query.with.as_ref().ok_or_else(|| SqlError::Parse {
        detail: "expected WITH clause".into(),
    })?;

    let cte = with.cte_tables.first().ok_or_else(|| SqlError::Parse {
        detail: "empty WITH clause".into(),
    })?;

    let cte_name = check_ast_identifier(&cte.alias.name)?;
    let declared_columns: Vec<String> = cte
        .alias
        .columns
        .iter()
        .map(|column| check_ast_identifier(&column.name))
        .collect::<Result<_>>()?;

    let cte_query = &cte.query;

    // Validate set operator: only UNION / UNION ALL permitted.
    let (left, right, set_quantifier) = match &*cte_query.body {
        SetExpr::SetOperation {
            op: ast::SetOperator::Union,
            left,
            right,
            set_quantifier,
        } => (left, right, set_quantifier),
        SetExpr::SetOperation { op, .. } => {
            return Err(SqlError::InvalidRecursiveSetOp {
                op: format!("{op}"),
            });
        }
        _ => {
            return Err(SqlError::InvalidRecursiveSetOp {
                op: "non-set-operation".into(),
            });
        }
    };

    // Validate every relation name and alias before any branch-planning
    // fallback. Recursive planning intentionally interprets some planning
    // failures as working-table references, so identifier failures must be
    // rejected first rather than swallowed by that fallback.
    validate_relation_identifiers(left)?;
    validate_relation_identifiers(right)?;

    // Validate self-reference count in the recursive arm.
    validate_self_ref_count(right, &cte_name)?;

    let distinct = !matches!(set_quantifier, ast::SetQuantifier::All);

    // Classify value-generating anchors syntactically. Never reinterpret a
    // planning error as a different CTE shape: doing so would swallow invalid
    // relation names and other fail-closed planner errors.
    let SetExpr::Select(anchor_select) = left.as_ref() else {
        return plan_recursive_value(left, right, &cte_name, &declared_columns, distinct);
    };
    if anchor_select.from.is_empty() {
        return plan_recursive_value(left, right, &cte_name, &declared_columns, distinct);
    }

    let base = plan_cte_branch(left, catalog, functions, temporal)?;
    let collection = extract_collection(&base);
    if collection.is_empty() {
        return Err(SqlError::Unsupported {
            detail: "collection-backed recursive CTE anchor did not produce a relation".into(),
        });
    }
    plan_recursive_scan_from_parts(
        &cte_name,
        &base,
        &RecursiveParts {
            left,
            right,
            declared_columns: &declared_columns,
            distinct,
        },
        catalog,
        functions,
        temporal,
    )
}

fn validate_relation_identifiers(expr: &SetExpr) -> Result<()> {
    match expr {
        SetExpr::Select(select) => {
            for from in &select.from {
                validate_table_factor(&from.relation)?;
                for join in &from.joins {
                    validate_table_factor(&join.relation)?;
                }
            }
            Ok(())
        }
        SetExpr::SetOperation { left, right, .. } => {
            validate_relation_identifiers(left)?;
            validate_relation_identifiers(right)
        }
        SetExpr::Query(query) => validate_query_identifiers(query),
        _ => Ok(()),
    }
}

fn validate_query_identifiers(query: &Query) -> Result<()> {
    if let Some(with) = &query.with {
        for cte in &with.cte_tables {
            check_ast_identifier(&cte.alias.name)?;
            for column in &cte.alias.columns {
                check_ast_identifier(&column.name)?;
            }
            validate_query_identifiers(&cte.query)?;
        }
    }
    validate_relation_identifiers(&query.body)
}

fn validate_table_factor(factor: &ast::TableFactor) -> Result<()> {
    match factor {
        ast::TableFactor::Table { .. } => {
            crate::parser::normalize::table_name_from_factor(factor)?;
            Ok(())
        }
        ast::TableFactor::Derived {
            subquery, alias, ..
        } => {
            if let Some(alias) = alias {
                crate::reserved::check_ast_identifier(&alias.name)?;
            }
            validate_query_identifiers(subquery)
        }
        _ => Ok(()),
    }
}

// ── Collection-backed recursive scan ─────────────────────────────────────────

struct RecursiveParts<'a> {
    left: &'a SetExpr,
    right: &'a SetExpr,
    declared_columns: &'a [String],
    distinct: bool,
}

fn plan_recursive_scan_from_parts(
    cte_name: &str,
    base: &SqlPlan,
    parts: &RecursiveParts<'_>,
    catalog: &dyn SqlCatalog,
    functions: &FunctionRegistry,
    temporal: crate::TemporalScope,
) -> Result<SqlPlan> {
    let RecursiveParts {
        left,
        right,
        declared_columns,
        distinct,
    } = parts;
    let collection = extract_collection(base);

    // Validate column count if columns were declared.
    if !declared_columns.is_empty() {
        let anchor_cols = count_select_cols(left);
        if anchor_cols != 0 && anchor_cols != declared_columns.len() {
            return Err(SqlError::RecursiveColumnMismatch {
                cte_name: cte_name.to_owned(),
                anchor_cols,
                declared_cols: declared_columns.len(),
            });
        }
    }

    // The recursive arm contains the working-table self-reference, which is
    // intentionally absent from the ordinary catalog. Parse that supported
    // shape directly instead of attempting ordinary planning and swallowing
    // whichever error happens to occur first.
    let (recursive_filters, join_link) =
        super::join_link::extract_recursive_info(right, cte_name, catalog, functions, temporal)?;

    // The anchor plan carries the CTE's resolved output columns; propagate
    // them so the recursive scan self-describes its output schema.
    let projection = match base {
        SqlPlan::Scan { projection, .. } | SqlPlan::Join { projection, .. } => projection.clone(),
        _ => Vec::new(),
    };

    Ok(SqlPlan::RecursiveScan {
        collection,
        base_filters: extract_filters(base),
        recursive_filters,
        join_link,
        max_iterations: DEFAULT_MAX_RECURSION_DEPTH,
        distinct: *distinct,
        limit: 10000,
        projection,
    })
}

pub(super) fn plan_cte_branch(
    expr: &SetExpr,
    catalog: &dyn SqlCatalog,
    functions: &FunctionRegistry,
    temporal: crate::TemporalScope,
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
            crate::planner::select::plan_query(&query, catalog, functions, temporal)
        }
        _ => Err(SqlError::Unsupported {
            detail: "CTE branch must be SELECT".into(),
        }),
    }
}

pub(super) fn extract_collection(plan: &SqlPlan) -> String {
    match plan {
        SqlPlan::Scan { collection, .. } => collection.clone(),
        _ => String::new(),
    }
}

pub(super) fn extract_filters(plan: &SqlPlan) -> Vec<Filter> {
    match plan {
        SqlPlan::Scan { filters, .. } => filters.clone(),
        _ => Vec::new(),
    }
}
