// SPDX-License-Identifier: Apache-2.0

//! Control-Plane-computed SELECT-list items.
//!
//! A SELECT-list item over a relation that calls a sequence accessor cannot
//! run on the Data Plane: the row evaluator holds no sequence state. The
//! planner marks such an item [`Projection::CpComputed`], and the Control
//! Plane evaluates it once per output row after the rows return. Detection
//! runs on the AST, so an item with no accessor converts exactly as before,
//! against the ordinary scope.

use core::ops::ControlFlow;

use sqlparser::ast::{self, Expr, Visit, Visitor};

use crate::error::Result;
use crate::functions::sequence_accessor::is_sequence_accessor;
use crate::resolver::ColumnScope;
use crate::resolver::columns::TableScope;
use crate::resolver::expr::convert_expr;
use crate::types::Projection;
use crate::types::plan::contains_sequence_accessor;

/// A copy of `scope` that resolves sequence accessors, when `scope` iterates
/// rows and backs the statement's output SELECT. A FROM-less SELECT gets
/// `None`: the planner evaluates the accessor itself there, and the item
/// stays an ordinary `Computed` entry. A nested SELECT (subquery, CTE
/// body, UNION branch, derived table, INSERT or MERGE source) gets `None`
/// too: the Control Plane evaluates the statement's output rows only, so
/// the ordinary scope refuses the accessor there.
pub fn cp_projection_scope(scope: &TableScope) -> Option<TableScope> {
    (scope.is_row_scope() && scope.is_statement_output())
        .then(|| scope.clone().allowing_cp_functions())
}

/// The lowercase name of the first sequence accessor `expr` calls, at any
/// nesting depth.
pub fn ast_sequence_accessor(expr: &Expr) -> Option<String> {
    let mut detector = AccessorDetector { found: None };
    let _ = expr.visit(&mut detector);
    detector.found
}

/// Whether `expr` calls a sequence accessor anywhere, at any nesting depth.
pub fn ast_calls_sequence_accessor(expr: &Expr) -> bool {
    ast_sequence_accessor(expr).is_some()
}

struct AccessorDetector {
    found: Option<String>,
}

impl Visitor for AccessorDetector {
    type Break = ();

    fn pre_visit_expr(&mut self, expr: &Expr) -> ControlFlow<()> {
        if let Expr::Function(f) = expr
            && f.name.0.len() == 1
            && let Some(ast::ObjectNamePart::Identifier(ident)) = f.name.0.first()
            && is_sequence_accessor(&ident.value)
        {
            self.found = Some(ident.value.to_ascii_lowercase());
            return ControlFlow::Break(());
        }
        ControlFlow::Continue(())
    }
}

/// Convert one SELECT item into a [`Projection::CpComputed`] when it calls a
/// sequence accessor over a relation. `None` when the item holds no accessor
/// or no relation is in scope; the caller converts it the ordinary way.
///
/// An accessor inside a subquery is not the item's own: the subquery body
/// resolves in a nested scope, which refuses it.
pub fn convert_cp_item(
    expr: &Expr,
    alias: String,
    cp_scope: Option<&TableScope>,
) -> Result<Option<Projection>> {
    let Some(cp_scope) = cp_scope else {
        return Ok(None);
    };
    if !ast_calls_sequence_accessor(expr) {
        return Ok(None);
    }
    let converted = convert_expr(expr, &ColumnScope::Relations(cp_scope))?;
    if !contains_sequence_accessor(&converted) {
        return Ok(None);
    }
    Ok(Some(Projection::CpComputed {
        expr: converted,
        alias,
    }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::resolver::columns::test_support::open_scope;
    use crate::types::SqlExpr;
    use sqlparser::dialect::GenericDialect;
    use sqlparser::parser::Parser;

    fn select_expr(sql: &str) -> Expr {
        let stmts = Parser::parse_sql(&GenericDialect {}, sql).unwrap();
        match stmts.into_iter().next().unwrap() {
            ast::Statement::Query(q) => match *q.body {
                ast::SetExpr::Select(s) => match s.projection.into_iter().next().unwrap() {
                    ast::SelectItem::UnnamedExpr(e)
                    | ast::SelectItem::ExprWithAlias { expr: e, .. } => e,
                    other => panic!("unexpected projection item {other:?}"),
                },
                _ => panic!("expected SELECT"),
            },
            _ => panic!("expected query"),
        }
    }

    #[test]
    fn ast_detection_sees_nested_accessors_only() {
        assert!(ast_calls_sequence_accessor(&select_expr(
            "SELECT CASE WHEN a > 0 THEN nextval('s') * 2 ELSE 0 END"
        )));
        assert!(ast_calls_sequence_accessor(&select_expr(
            "SELECT coalesce(currval('s'), 0)"
        )));
        assert!(!ast_calls_sequence_accessor(&select_expr(
            "SELECT upper(a) || 'nextval'"
        )));
    }

    #[test]
    fn a_from_less_scope_never_marks_an_item() {
        let scope = TableScope::new().as_statement_output();
        assert!(cp_projection_scope(&scope).is_none());
        let item = convert_cp_item(&select_expr("SELECT nextval('s')"), "n".into(), None).unwrap();
        assert!(item.is_none());
    }

    #[test]
    fn a_nested_select_scope_never_marks_an_item() {
        assert!(cp_projection_scope(&open_scope("t")).is_none());
        let nested = open_scope("t")
            .as_statement_output()
            .nested_in(TableScope::new());
        assert!(cp_projection_scope(&nested).is_none());
    }

    #[test]
    fn an_accessor_item_over_a_relation_is_cp_computed() {
        let cp_scope = cp_projection_scope(&open_scope("t").as_statement_output())
            .expect("statement-output row scope");
        let item = convert_cp_item(
            &select_expr("SELECT nextval('s') + id"),
            "n".into(),
            Some(&cp_scope),
        )
        .unwrap()
        .expect("accessor item is Control-Plane computed");
        match item {
            Projection::CpComputed { expr, alias } => {
                assert_eq!(alias, "n");
                assert!(matches!(expr, SqlExpr::BinaryOp { .. }));
            }
            other => panic!("expected CpComputed, got {other:?}"),
        }
    }

    #[test]
    fn a_plain_item_over_a_relation_is_left_to_the_ordinary_path() {
        let cp_scope = cp_projection_scope(&open_scope("t").as_statement_output())
            .expect("statement-output row scope");
        let item =
            convert_cp_item(&select_expr("SELECT id + 1"), "n".into(), Some(&cp_scope)).unwrap();
        assert!(item.is_none());
    }
}
