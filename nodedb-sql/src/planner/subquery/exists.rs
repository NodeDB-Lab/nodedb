// SPDX-License-Identifier: Apache-2.0

//! `EXISTS` / `NOT EXISTS` lowered to a semi / anti join.
//!
//! The inner query becomes the build side. Each AND-ed equality that links an
//! outer relation to an inner one becomes a probe key; every other conjunct
//! stays a local predicate on the inner scan.

use std::ops::ControlFlow;

use sqlparser::ast::{self, SetExpr};

use crate::error::{Result, SqlError};
use crate::functions::registry::FunctionRegistry;
use crate::parser::normalize::normalize_ident;
use crate::planner::ast_helpers::{flatten_and_expr, rebuild_and_expr};
use crate::planner::select::{convert_projection, plan_query};
use crate::resolver::ColumnScope;
use crate::resolver::columns::TableScope;
use crate::resolver::expr::convert_expr;
use crate::types::*;

use super::extract::SubqueryJoin;

const SET_OPERATION: &str = "EXISTS over a set operation (UNION / INTERSECT / EXCEPT) \
     is not supported; wrap the set operation in a derived table and select from that";

const OUTER_ONLY_EQUALITY: &str = "an equality naming only outer relations inside EXISTS \
     is not supported; move it to the enclosing WHERE clause";

const CORRELATION_UNDER_OR: &str = "a correlated reference under OR inside EXISTS \
     is not supported; each correlation must be an AND-ed equality";

/// Plan `EXISTS (SELECT ... )` as a semi join, `NOT EXISTS` as an anti join.
///
/// `outer` is the enclosing query's scope. The inner FROM clause resolves
/// nested inside it, so a qualifier naming no inner relation reaches the outer
/// query instead of erroring as an unknown relation.
pub(super) fn plan_exists_subquery(
    subquery: &ast::Query,
    negated: bool,
    outer: &TableScope,
    catalog: &dyn SqlCatalog,
    functions: &FunctionRegistry,
    temporal: crate::TemporalScope,
) -> Result<SubqueryJoin> {
    let SetExpr::Select(select) = &*subquery.body else {
        return Err(SqlError::Unsupported {
            detail: SET_OPERATION.into(),
        });
    };

    let local = TableScope::resolve_from(catalog, functions, temporal, &select.from)?;
    let nested = local.clone().nested_in(outer.clone());

    // EXISTS discards the projected values. The conversion still runs so a
    // column the inner SELECT list names but no relation declares is rejected.
    convert_projection(&select.projection, &nested)?;

    let mut on = Vec::new();
    let mut local_conjuncts = Vec::new();
    if let Some(where_expr) = &select.selection {
        let mut conjuncts = Vec::new();
        flatten_and_expr(where_expr, &mut conjuncts);
        for conjunct in conjuncts {
            // Converting against the nested scope gates both inner and outer
            // column names before the conjunct is classified.
            convert_expr(&conjunct, &ColumnScope::Relations(&nested))?;
            match correlation_pair(&conjunct, &local, outer)? {
                Some(pair) => on.push(pair),
                None => local_conjuncts.push(conjunct),
            }
        }
    }

    let inner_plan = plan_local_query(
        subquery,
        select,
        local_conjuncts,
        catalog,
        functions,
        temporal,
    )?;

    Ok(SubqueryJoin {
        on,
        inner_plan,
        join_type: if negated {
            JoinType::Anti
        } else {
            JoinType::Semi
        },
    })
}

/// Plan the inner query carrying only its local predicates.
///
/// The projection widens to `*`: EXISTS ignores the values, but the scan must
/// still carry the correlation key columns for the join probe.
fn plan_local_query(
    subquery: &ast::Query,
    select: &ast::Select,
    local_conjuncts: Vec<ast::Expr>,
    catalog: &dyn SqlCatalog,
    functions: &FunctionRegistry,
    temporal: crate::TemporalScope,
) -> Result<SqlPlan> {
    let mut inner_select = select.clone();
    inner_select.projection = vec![ast::SelectItem::Wildcard(
        ast::WildcardAdditionalOptions::default(),
    )];
    inner_select.selection = if local_conjuncts.is_empty() {
        None
    } else {
        Some(rebuild_and_expr(local_conjuncts))
    };

    let mut inner_query = subquery.clone();
    *inner_query.body = SetExpr::Select(Box::new(inner_select));
    plan_query(&inner_query, catalog, functions, temporal)
}

/// Which query a column reference belongs to.
enum Side {
    /// A column of a relation in the subquery's own FROM clause.
    Inner(String),
    /// A column of a relation in the enclosing query.
    Outer(String),
    /// Anything else: a literal, a function call, a computed expression.
    Other,
}

/// Classify one conjunct of the inner WHERE clause.
///
/// Returns the `(outer column, inner column)` probe key for a correlation, or
/// `None` for a predicate local to the inner query. A conjunct that reads an
/// outer relation in any other shape raises a typed error naming that shape.
fn correlation_pair(
    expr: &ast::Expr,
    inner: &TableScope,
    outer: &TableScope,
) -> Result<Option<(String, String)>> {
    let bare = unnest(expr);
    if let ast::Expr::BinaryOp { left, op, right } = bare {
        let l = classify(left, inner, outer);
        let r = classify(right, inner, outer);
        match (op, &l, &r) {
            (ast::BinaryOperator::Eq, Side::Inner(ic), Side::Outer(oc))
            | (ast::BinaryOperator::Eq, Side::Outer(oc), Side::Inner(ic)) => {
                return Ok(Some((oc.clone(), ic.clone())));
            }
            (ast::BinaryOperator::Eq, Side::Outer(_), Side::Outer(_)) => {
                return Err(SqlError::Unsupported {
                    detail: OUTER_ONLY_EQUALITY.into(),
                });
            }
            (ast::BinaryOperator::Or, _, _)
                if references_outer(left, inner, outer)
                    || references_outer(right, inner, outer) =>
            {
                return Err(SqlError::Unsupported {
                    detail: CORRELATION_UNDER_OR.into(),
                });
            }
            (op, Side::Inner(_), Side::Outer(_)) | (op, Side::Outer(_), Side::Inner(_))
                if is_inequality(op) =>
            {
                return Err(SqlError::Unsupported {
                    detail: format!(
                        "a correlated inequality ('{op}') inside EXISTS is not supported; \
                         a semi join probes on equality keys only"
                    ),
                });
            }
            _ => {}
        }
    }

    if references_outer(bare, inner, outer) {
        return Err(SqlError::Unsupported {
            detail: format!(
                "the correlated predicate '{expr}' inside EXISTS is not supported; \
                 a correlation must be an AND-ed equality between one outer column \
                 and one inner column"
            ),
        });
    }
    Ok(None)
}

/// Decide which query a column reference belongs to by relation membership.
///
/// A qualifier names the relation directly. A bare name goes to whichever
/// scope resolves it, inner first, matching the resolution order the
/// expression converter already applied.
fn classify(expr: &ast::Expr, inner: &TableScope, outer: &TableScope) -> Side {
    match expr {
        ast::Expr::CompoundIdentifier(parts) if parts.len() == 2 => {
            let qualifier = normalize_ident(&parts[0]);
            let column = normalize_ident(&parts[1]);
            if inner.table_by_ref(&qualifier).is_some() {
                Side::Inner(column)
            } else if outer.table_by_ref(&qualifier).is_some() {
                Side::Outer(column)
            } else {
                Side::Other
            }
        }
        ast::Expr::Identifier(ident) => {
            let column = normalize_ident(ident);
            if inner.check_name(None, &column).is_ok() {
                Side::Inner(column)
            } else if outer.check_name(None, &column).is_ok() {
                Side::Outer(column)
            } else {
                Side::Other
            }
        }
        ast::Expr::Nested(nested) => classify(nested, inner, outer),
        _ => Side::Other,
    }
}

/// Whether any column reference anywhere in `expr` names an outer relation.
fn references_outer(expr: &ast::Expr, inner: &TableScope, outer: &TableScope) -> bool {
    let walk = ast::visit_expressions(expr, |node| {
        if matches!(classify(node, inner, outer), Side::Outer(_)) {
            ControlFlow::Break(())
        } else {
            ControlFlow::Continue(())
        }
    });
    walk.is_break()
}

fn unnest(expr: &ast::Expr) -> &ast::Expr {
    match expr {
        ast::Expr::Nested(inner) => unnest(inner),
        other => other,
    }
}

fn is_inequality(op: &ast::BinaryOperator) -> bool {
    matches!(
        op,
        ast::BinaryOperator::Gt
            | ast::BinaryOperator::GtEq
            | ast::BinaryOperator::Lt
            | ast::BinaryOperator::LtEq
            | ast::BinaryOperator::NotEq
    )
}
