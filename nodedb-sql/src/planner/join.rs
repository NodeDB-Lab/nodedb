//! JOIN planning: extract left/right tables, equi-join keys, join type.

use sqlparser::ast::{self, Select};

use crate::error::{Result, SqlError};
use crate::functions::registry::FunctionRegistry;
use crate::parser::normalize::normalize_ident;
use crate::resolver::columns::TableScope;
use crate::resolver::expr::convert_expr;
use crate::types::*;

/// Plan a JOIN from a SELECT statement with JOINs in FROM clause.
pub fn plan_join_from_select(
    select: &Select,
    scope: &TableScope,
    catalog: &dyn SqlCatalog,
    functions: &FunctionRegistry,
) -> Result<Option<SqlPlan>> {
    let from = &select.from[0];
    let left_table = scope
        .tables
        .values()
        .find(|t| {
            let rel_name =
                crate::parser::normalize::table_name_from_factor(&from.relation).map(|(n, _)| n);
            rel_name.as_deref() == Some(&t.name) || rel_name.as_deref() == t.alias.as_deref()
        })
        .ok_or_else(|| SqlError::Unsupported {
            detail: "cannot resolve left table in JOIN".into(),
        })?;

    // Build left scan.
    let left_plan = SqlPlan::Scan {
        collection: left_table.name.clone(),
        alias: left_table.alias.clone(),
        engine: left_table.info.engine,
        filters: Vec::new(),
        projection: Vec::new(),
        sort_keys: Vec::new(),
        limit: None,
        offset: 0,
        distinct: false,
        window_functions: Vec::new(),
    };

    let mut current_plan = left_plan;

    for join_item in &from.joins {
        let (right_name, right_alias) = crate::parser::normalize::table_name_from_factor(
            &join_item.relation,
        )
        .ok_or_else(|| SqlError::Unsupported {
            detail: "non-table JOIN target".into(),
        })?;
        let right_table = scope
            .tables
            .values()
            .find(|t| t.name == right_name)
            .ok_or_else(|| SqlError::UnknownTable {
                name: right_name.clone(),
            })?;

        let right_plan = SqlPlan::Scan {
            collection: right_table.name.clone(),
            alias: right_alias,
            engine: right_table.info.engine,
            filters: Vec::new(),
            projection: Vec::new(),
            sort_keys: Vec::new(),
            limit: None,
            offset: 0,
            distinct: false,
            window_functions: Vec::new(),
        };

        let (join_type, on_keys, condition) = extract_join_spec(&join_item.join_operator)?;

        current_plan = SqlPlan::Join {
            left: Box::new(current_plan),
            right: Box::new(right_plan),
            on: on_keys,
            join_type,
            condition,
            limit: 10000,
            projection: Vec::new(),
            filters: Vec::new(),
        };
    }

    // Extract subqueries from WHERE and wrap as additional joins.
    let (subquery_joins, effective_where) = if let Some(expr) = &select.selection {
        let extraction = super::subquery::extract_subqueries(expr, catalog, functions)?;
        (extraction.joins, extraction.remaining_where)
    } else {
        (Vec::new(), None)
    };

    // Apply remaining WHERE as filters and SELECT projection on the join plan.
    let projection = super::select::convert_projection(&select.projection)?;
    let filters = match &effective_where {
        Some(expr) => super::select::convert_where_to_filters(expr)?,
        None => Vec::new(),
    };

    // Wrap with subquery joins (semi/anti/cross) if any.
    for sq in subquery_joins {
        current_plan = SqlPlan::Join {
            left: Box::new(current_plan),
            right: Box::new(sq.inner_plan),
            on: vec![(sq.outer_column, sq.inner_column)],
            join_type: sq.join_type,
            condition: None,
            limit: 10000,
            projection: Vec::new(),
            filters: Vec::new(),
        };
    }

    // Check if there's aggregation on the join.
    let group_by_non_empty = match &select.group_by {
        ast::GroupByExpr::All(_) => true,
        ast::GroupByExpr::Expressions(exprs, _) => !exprs.is_empty(),
    };
    if super::select::convert_projection(&select.projection).is_ok() && group_by_non_empty {
        let aggregates =
            super::aggregate::extract_aggregates_from_projection(&select.projection, functions)?;
        let group_by = super::aggregate::convert_group_by(&select.group_by)?;
        let having = match &select.having {
            Some(expr) => super::select::convert_where_to_filters(expr)?,
            None => Vec::new(),
        };
        return Ok(Some(SqlPlan::Aggregate {
            input: Box::new(current_plan),
            group_by,
            aggregates,
            having,
            limit: 10000,
        }));
    }

    // Attach SELECT projection and WHERE filters to the outermost join.
    if let SqlPlan::Join {
        projection: ref mut proj,
        filters: ref mut filt,
        ..
    } = current_plan
    {
        *proj = projection;
        *filt = filters;
    }
    Ok(Some(current_plan))
}

/// (join_type, equi_keys, non-equi condition)
type JoinSpec = (JoinType, Vec<(String, String)>, Option<SqlExpr>);

/// Extract join type, equi-join keys, and non-equi condition.
fn extract_join_spec(op: &ast::JoinOperator) -> Result<JoinSpec> {
    match op {
        ast::JoinOperator::Inner(constraint) | ast::JoinOperator::Join(constraint) => {
            let (keys, cond) = extract_join_constraint(constraint)?;
            Ok((JoinType::Inner, keys, cond))
        }
        ast::JoinOperator::Left(constraint) | ast::JoinOperator::LeftOuter(constraint) => {
            let (keys, cond) = extract_join_constraint(constraint)?;
            Ok((JoinType::Left, keys, cond))
        }
        ast::JoinOperator::Right(constraint) | ast::JoinOperator::RightOuter(constraint) => {
            let (keys, cond) = extract_join_constraint(constraint)?;
            Ok((JoinType::Right, keys, cond))
        }
        ast::JoinOperator::FullOuter(constraint) => {
            let (keys, cond) = extract_join_constraint(constraint)?;
            Ok((JoinType::Full, keys, cond))
        }
        ast::JoinOperator::CrossJoin(constraint) => {
            let (keys, cond) = extract_join_constraint(constraint)?;
            Ok((JoinType::Cross, keys, cond))
        }
        ast::JoinOperator::Semi(constraint) | ast::JoinOperator::LeftSemi(constraint) => {
            let (keys, cond) = extract_join_constraint(constraint)?;
            Ok((JoinType::Semi, keys, cond))
        }
        ast::JoinOperator::Anti(constraint) | ast::JoinOperator::LeftAnti(constraint) => {
            let (keys, cond) = extract_join_constraint(constraint)?;
            Ok((JoinType::Anti, keys, cond))
        }
        _ => Err(SqlError::Unsupported {
            detail: format!("join type: {op:?}"),
        }),
    }
}

/// (equi_keys, non-equi condition)
type JoinConstraintResult = (Vec<(String, String)>, Option<SqlExpr>);

/// Extract equi-join keys from ON clause.
fn extract_join_constraint(constraint: &ast::JoinConstraint) -> Result<JoinConstraintResult> {
    match constraint {
        ast::JoinConstraint::On(expr) => {
            let mut keys = Vec::new();
            let mut non_equi = Vec::new();
            extract_equi_keys(expr, &mut keys, &mut non_equi)?;
            let cond = if non_equi.is_empty() {
                None
            } else {
                // Fold ALL non-equi predicates with AND — not just the first.
                let mut combined = convert_expr(&non_equi[0])?;
                for pred in &non_equi[1..] {
                    combined = SqlExpr::BinaryOp {
                        left: Box::new(combined),
                        op: crate::types::BinaryOp::And,
                        right: Box::new(convert_expr(pred)?),
                    };
                }
                Some(combined)
            };
            Ok((keys, cond))
        }
        ast::JoinConstraint::Using(columns) => {
            let keys = columns
                .iter()
                .map(|c| {
                    let name = crate::parser::normalize::normalize_object_name(c);
                    (name.clone(), name)
                })
                .collect();
            Ok((keys, None))
        }
        ast::JoinConstraint::Natural => Err(crate::error::SqlError::Unsupported {
            detail: "NATURAL JOIN is not supported; use explicit ON or USING clause".into(),
        }),
        ast::JoinConstraint::None => Err(crate::error::SqlError::Unsupported {
            detail: "implicit cross join (no ON/USING clause) is not supported".into(),
        }),
    }
}

/// Recursively extract equality join keys from an ON expression.
fn extract_equi_keys(
    expr: &ast::Expr,
    keys: &mut Vec<(String, String)>,
    non_equi: &mut Vec<ast::Expr>,
) -> Result<()> {
    match expr {
        ast::Expr::BinaryOp {
            left,
            op: ast::BinaryOperator::And,
            right,
        } => {
            extract_equi_keys(left, keys, non_equi)?;
            extract_equi_keys(right, keys, non_equi)?;
        }
        ast::Expr::BinaryOp {
            left,
            op: ast::BinaryOperator::Eq,
            right,
        } => {
            if let (Some(l), Some(r)) = (extract_col_ref(left), extract_col_ref(right)) {
                keys.push((l, r));
            } else {
                non_equi.push(expr.clone());
            }
        }
        _ => {
            non_equi.push(expr.clone());
        }
    }
    Ok(())
}

/// Extract a column reference (possibly qualified) from an expression.
fn extract_col_ref(expr: &ast::Expr) -> Option<String> {
    match expr {
        ast::Expr::Identifier(ident) => Some(normalize_ident(ident)),
        ast::Expr::CompoundIdentifier(parts) if !parts.is_empty() => Some(
            parts
                .iter()
                .map(normalize_ident)
                .collect::<Vec<_>>()
                .join("."),
        ),
        _ => None,
    }
}
