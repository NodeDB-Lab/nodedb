// SPDX-License-Identifier: Apache-2.0

//! Shared AST manipulation helpers for DML planners.

use sqlparser::ast;

use crate::error::{Result, SqlError};
use crate::parser::normalize::{normalize_ident, normalize_object_name_checked};
use crate::planner::select::convert_where_to_filters;
use crate::types::Filter;

/// Return `(table, column)` for a `table.col` compound identifier, or `None`.
pub fn qualified_ident_pair(expr: &ast::Expr) -> Option<(String, String)> {
    match expr {
        ast::Expr::CompoundIdentifier(parts) if parts.len() == 2 => {
            Some((normalize_ident(&parts[0]), normalize_ident(&parts[1])))
        }
        _ => None,
    }
}

/// The output name an unaliased projection item takes: the lowercased SQL
/// text of the expression.
///
/// This is the single derivation every consumer must use. A window spec is
/// matched back to its projection by alias, so deriving the name twice — once
/// lowercased here, once verbatim in the window extractor — silently detached
/// every unaliased windowed projection from its spec and answered a NULL
/// column with no error.
pub fn unaliased_projection_alias(expr: &ast::Expr) -> String {
    format!("{expr}").to_lowercase()
}

/// Flatten a right-leaning AND expression tree into a list of conjuncts.
pub fn flatten_and_expr(expr: &ast::Expr, out: &mut Vec<ast::Expr>) {
    match expr {
        ast::Expr::BinaryOp {
            left,
            op: ast::BinaryOperator::And,
            right,
        } => {
            flatten_and_expr(left, out);
            flatten_and_expr(right, out);
        }
        other => out.push(other.clone()),
    }
}

/// Reassemble conjuncts into a right-leaning AND tree. Panics if empty.
pub fn rebuild_and_expr(mut conjuncts: Vec<ast::Expr>) -> ast::Expr {
    let last = conjuncts.pop().expect("non-empty conjuncts");
    conjuncts
        .into_iter()
        .rfold(last, |acc, next| ast::Expr::BinaryOp {
            left: Box::new(next),
            op: ast::BinaryOperator::And,
            right: Box::new(acc),
        })
}

/// Walk an expression and replace every `table.col` compound identifier where
/// `table == qualifier` with a bare `col` identifier. Lets target-side
/// predicates like `t.score > 15` be evaluated against documents that store
/// fields without a table qualifier.
pub fn strip_table_qualifier(expr: &ast::Expr, qualifier: &str) -> ast::Expr {
    match expr {
        ast::Expr::CompoundIdentifier(parts) if parts.len() == 2 => {
            if normalize_ident(&parts[0]) == qualifier {
                ast::Expr::Identifier(parts[1].clone())
            } else {
                expr.clone()
            }
        }
        ast::Expr::BinaryOp { left, op, right } => ast::Expr::BinaryOp {
            left: Box::new(strip_table_qualifier(left, qualifier)),
            op: op.clone(),
            right: Box::new(strip_table_qualifier(right, qualifier)),
        },
        ast::Expr::UnaryOp { op, expr: inner } => ast::Expr::UnaryOp {
            op: *op,
            expr: Box::new(strip_table_qualifier(inner, qualifier)),
        },
        ast::Expr::Nested(inner) => {
            ast::Expr::Nested(Box::new(strip_table_qualifier(inner, qualifier)))
        }
        ast::Expr::IsNull(inner) => {
            ast::Expr::IsNull(Box::new(strip_table_qualifier(inner, qualifier)))
        }
        ast::Expr::IsNotNull(inner) => {
            ast::Expr::IsNotNull(Box::new(strip_table_qualifier(inner, qualifier)))
        }
        other => other.clone(),
    }
}

/// Walk `expr` and return the first qualifier on a `table.col` compound
/// identifier that does not match any of `valid_qualifiers`. Used by the
/// single-table WHERE planner to reject a table qualifier that refers to
/// neither the table's name nor its alias (e.g. `WHERE wrong.name = ...`)
/// with a typed error, instead of silently stripping it into a bare field
/// lookup that would never match any row.
pub fn find_foreign_qualifier(expr: &ast::Expr, valid_qualifiers: &[&str]) -> Option<String> {
    match expr {
        ast::Expr::CompoundIdentifier(parts) if parts.len() == 2 => {
            let qualifier = normalize_ident(&parts[0]);
            if valid_qualifiers.contains(&qualifier.as_str()) {
                None
            } else {
                Some(qualifier)
            }
        }
        ast::Expr::BinaryOp { left, right, .. } => find_foreign_qualifier(left, valid_qualifiers)
            .or_else(|| find_foreign_qualifier(right, valid_qualifiers)),
        ast::Expr::UnaryOp { expr: inner, .. }
        | ast::Expr::Nested(inner)
        | ast::Expr::IsNull(inner)
        | ast::Expr::IsNotNull(inner) => find_foreign_qualifier(inner, valid_qualifiers),
        _ => None,
    }
}

/// Reject a `table.col` qualifier in `expr` that matches none of
/// `valid_qualifiers`, mapping it to a typed `UnknownTable` error.
fn reject_foreign_qualifier(expr: &ast::Expr, valid_qualifiers: &[&str]) -> Result<()> {
    match find_foreign_qualifier(expr, valid_qualifiers) {
        Some(bad) => Err(SqlError::UnknownTable { name: bad }),
        None => Ok(()),
    }
}

/// Reject a single qualifier string that matches none of `valid_qualifiers`.
fn reject_qualifier(qualifier: &str, valid_qualifiers: &[&str]) -> Result<()> {
    if valid_qualifiers.contains(&qualifier) {
        Ok(())
    } else {
        Err(SqlError::UnknownTable {
            name: qualifier.to_string(),
        })
    }
}

/// Strip every qualifier in `qualifiers` from column refs in `expr`. Each pass
/// is a no-op when its qualifier is absent, so passing both a table's name and
/// its alias collapses `t.col` / `alias.col` to a bare `col`.
fn strip_table_qualifiers(expr: &ast::Expr, qualifiers: &[&str]) -> ast::Expr {
    qualifiers
        .iter()
        .fold(expr.clone(), |acc, q| strip_table_qualifier(&acc, q))
}

/// Normalize a SINGLE-TABLE `SELECT` by stripping the (always-redundant) table
/// qualifier from column refs in the projection, WHERE, and GROUP BY, returning
/// a rewritten copy. `t.col` / `alias.col` collapse to `col` before the
/// qualified name can become a literal `"t.col"` field-lookup string that no
/// stored document ever has (silent zero-row / empty-value bug). A qualifier
/// that matches neither the table's name nor its alias is a typed error, never
/// a silent match-nothing. Subquery bodies are intentionally not descended into
/// (their qualifiers belong to their own tables).
pub fn strip_single_table_qualifiers(
    select: &ast::Select,
    valid_qualifiers: &[&str],
) -> Result<ast::Select> {
    let mut out = select.clone();

    for item in &mut out.projection {
        match item {
            ast::SelectItem::UnnamedExpr(expr)
            | ast::SelectItem::ExprWithAlias { expr, .. }
            | ast::SelectItem::ExprWithAliases { expr, .. } => {
                reject_foreign_qualifier(expr, valid_qualifiers)?;
                *expr = strip_table_qualifiers(expr, valid_qualifiers);
            }
            ast::SelectItem::QualifiedWildcard(kind, opts) => {
                if let ast::SelectItemQualifiedWildcardKind::ObjectName(name) = kind {
                    let qualifier = normalize_object_name_checked(name)?;
                    reject_qualifier(&qualifier, valid_qualifiers)?;
                    // `t.*` on the single table is exactly `*`.
                    let opts = opts.clone();
                    *item = ast::SelectItem::Wildcard(opts);
                }
            }
            ast::SelectItem::Wildcard(_) => {}
        }
    }

    if let Some(expr) = &mut out.selection {
        reject_foreign_qualifier(expr, valid_qualifiers)?;
        *expr = strip_table_qualifiers(expr, valid_qualifiers);
    }

    if let ast::GroupByExpr::Expressions(exprs, _) = &mut out.group_by {
        for expr in exprs {
            reject_foreign_qualifier(expr, valid_qualifiers)?;
            *expr = strip_table_qualifiers(expr, valid_qualifiers);
        }
    }

    Ok(out)
}

/// Strip `qualifier.` from all compound identifiers in `expr`, then convert
/// the result to `Vec<Filter>` via `convert_where_to_filters`.
pub fn strip_and_convert_filters(
    conjuncts: Vec<ast::Expr>,
    qualifier: &str,
    scope: &crate::resolver::columns::TableScope,
) -> Result<Vec<Filter>> {
    if conjuncts.is_empty() {
        return Ok(Vec::new());
    }
    let stripped: Vec<ast::Expr> = conjuncts
        .into_iter()
        .map(|c| strip_table_qualifier(&c, qualifier))
        .collect();
    let rebuilt = rebuild_and_expr(stripped);
    convert_where_to_filters(&rebuilt, scope)
}
