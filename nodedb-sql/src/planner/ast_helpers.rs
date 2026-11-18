// SPDX-License-Identifier: Apache-2.0

//! Shared AST manipulation helpers for DML planners.

use std::ops::ControlFlow;

use sqlparser::ast::{self, Visit, VisitMut, Visitor, VisitorMut};

use crate::error::{Result, SqlError};
use crate::parser::normalize::{normalize_ident, normalize_object_name_checked};
use crate::planner::select::convert_where_to_filters;
use crate::resolver::columns::ResolvedTable;
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

/// Replaces `qualifier.col` with `col` in every expression of a tree.
///
/// A subquery body keeps its own qualifiers, so the walk counts query depth
/// and rewrites only at depth 0.
struct QualifierStripper<'a> {
    qualifier: &'a str,
    query_depth: usize,
}

impl VisitorMut for QualifierStripper<'_> {
    type Break = ();

    fn pre_visit_query(&mut self, _query: &mut ast::Query) -> ControlFlow<Self::Break> {
        self.query_depth += 1;
        ControlFlow::Continue(())
    }

    fn post_visit_query(&mut self, _query: &mut ast::Query) -> ControlFlow<Self::Break> {
        self.query_depth -= 1;
        ControlFlow::Continue(())
    }

    fn post_visit_expr(&mut self, expr: &mut ast::Expr) -> ControlFlow<Self::Break> {
        if self.query_depth == 0
            && let ast::Expr::CompoundIdentifier(parts) = expr
            && parts.len() == 2
            && normalize_ident(&parts[0]) == self.qualifier
        {
            *expr = ast::Expr::Identifier(parts[1].clone());
        }
        ControlFlow::Continue(())
    }
}

/// Walk an expression and replace every `table.col` compound identifier where
/// `table == qualifier` with a bare `col` identifier. Lets target-side
/// predicates like `t.score > 15` be evaluated against documents that store
/// fields without a table qualifier.
///
/// The walk reaches every expression shape (`IN`, `BETWEEN`, `LIKE`, function
/// arguments, `CASE`, `CAST`), so no predicate keeps a qualified field name
/// that no stored document has.
pub fn strip_table_qualifier(expr: &ast::Expr, qualifier: &str) -> ast::Expr {
    let mut out = expr.clone();
    let mut stripper = QualifierStripper {
        qualifier,
        query_depth: 0,
    };
    // Both `Visit` and `VisitMut` are in scope: name the mutable one.
    let _ = VisitMut::visit(&mut out, &mut stripper);
    out
}

/// Finds the first qualifier of a `table.col` identifier outside every
/// subquery that is not among `valid`.
struct ForeignQualifierFinder<'a> {
    valid: &'a [&'a str],
    query_depth: usize,
    found: Option<String>,
}

impl Visitor for ForeignQualifierFinder<'_> {
    type Break = ();

    fn pre_visit_query(&mut self, _query: &ast::Query) -> ControlFlow<Self::Break> {
        self.query_depth += 1;
        ControlFlow::Continue(())
    }

    fn post_visit_query(&mut self, _query: &ast::Query) -> ControlFlow<Self::Break> {
        self.query_depth -= 1;
        ControlFlow::Continue(())
    }

    fn pre_visit_expr(&mut self, expr: &ast::Expr) -> ControlFlow<Self::Break> {
        if self.query_depth == 0
            && let ast::Expr::CompoundIdentifier(parts) = expr
            && parts.len() == 2
        {
            let qualifier = normalize_ident(&parts[0]);
            if !self.valid.contains(&qualifier.as_str()) {
                self.found = Some(qualifier);
                return ControlFlow::Break(());
            }
        }
        ControlFlow::Continue(())
    }
}

/// Walk `expr` and return the first qualifier on a `table.col` compound
/// identifier that does not match any of `valid_qualifiers`. Used by the
/// single-table WHERE planner to reject a table qualifier that refers to
/// neither the table's name nor its alias (e.g. `WHERE wrong.name = ...`)
/// with a typed error, instead of silently stripping it into a bare field
/// lookup that would never match any row. The walk reaches every expression
/// shape outside a subquery body.
pub fn find_foreign_qualifier(expr: &ast::Expr, valid_qualifiers: &[&str]) -> Option<String> {
    let mut finder = ForeignQualifierFinder {
        valid: valid_qualifiers,
        query_depth: 0,
        found: None,
    };
    let _ = expr.visit(&mut finder);
    finder.found
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

/// The qualifiers a single-table query's column references may carry: the
/// table's name, and its alias when it has one.
pub(crate) fn single_table_qualifiers(table: &ResolvedTable) -> Vec<&str> {
    let ref_name = table.ref_name();
    if ref_name == table.name {
        vec![table.name.as_str()]
    } else {
        vec![table.name.as_str(), ref_name]
    }
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::parser::statement::parse_sql;

    fn where_of(sql: &str) -> ast::Expr {
        let ast::Statement::Query(query) = parse_sql(sql).unwrap().into_iter().next().unwrap()
        else {
            panic!("not a query");
        };
        let ast::SetExpr::Select(select) = *query.body else {
            panic!("not a SELECT");
        };
        select.selection.expect("a WHERE clause")
    }

    #[test]
    fn strips_the_qualifier_in_every_predicate_shape() {
        for predicate in [
            "t.id IN ('a', 'b')",
            "t.id NOT IN ('a', 'b')",
            "t.n BETWEEN 1 AND 2",
            "t.label LIKE 'x%'",
            "t.label ILIKE 'x%'",
            "upper(t.label) = 'X'",
            "CAST(t.n AS TEXT) = '1'",
            "CASE WHEN t.n > 1 THEN true ELSE false END",
        ] {
            let stripped = strip_table_qualifier(
                &where_of(&format!("SELECT 1 FROM t WHERE {predicate}")),
                "t",
            );
            assert!(
                !format!("{stripped}").contains("t."),
                "{predicate}: {stripped}"
            );
        }
    }

    #[test]
    fn keeps_a_qualifier_inside_a_subquery() {
        let expr = where_of("SELECT 1 FROM t WHERE t.id IN (SELECT t.id FROM u AS t)");
        let stripped = format!("{}", strip_table_qualifier(&expr, "t"));
        assert!(stripped.starts_with("id IN"), "{stripped}");
        assert!(stripped.contains("SELECT t.id"), "{stripped}");
    }

    #[test]
    fn finds_a_foreign_qualifier_in_every_predicate_shape() {
        for predicate in [
            "wrong.id IN ('a')",
            "wrong.n BETWEEN 1 AND 2",
            "wrong.label LIKE 'x%'",
            "upper(wrong.label) = 'X'",
        ] {
            let expr = where_of(&format!("SELECT 1 FROM t WHERE {predicate}"));
            assert_eq!(
                find_foreign_qualifier(&expr, &["t"]).as_deref(),
                Some("wrong"),
                "{predicate}"
            );
        }
    }

    #[test]
    fn a_valid_qualifier_and_a_subquery_qualifier_are_not_foreign() {
        let expr = where_of("SELECT 1 FROM t WHERE t.id IN (SELECT w.id FROM u AS w)");
        assert_eq!(find_foreign_qualifier(&expr, &["t"]), None);
    }
}
