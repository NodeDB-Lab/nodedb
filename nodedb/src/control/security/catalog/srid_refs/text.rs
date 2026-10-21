// SPDX-License-Identifier: BUSL-1.1

//! Whether definition text names an SRID in an SRID role.
//!
//! The text parses with the expression parser the write path evaluates
//! definitions with. Text that parser does not take, such as a CHECK with
//! a subquery or an event action, parses as SQL statements instead. Both
//! apply the same rules from `nodedb_query::geo_functions::srid_refs`.
//! Text that parses neither way never evaluates, so it names nothing.

use std::ops::ControlFlow;

use nodedb_query::geo_functions::{
    expr_names_srid, literal_geometry_srid, literal_srid_code, srid_arg_position,
};
use nodedb_types::Value;
use sqlparser::ast::{
    Expr, FunctionArg, FunctionArgExpr, FunctionArguments, ObjectNamePart, Statement,
    Value as SqlValue, visit_expressions,
};
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;

/// Whether `text` names `code` in an SRID role.
pub fn text_names_srid(text: &str, code: u32) -> bool {
    if text.trim().is_empty() {
        return false;
    }
    if let Ok((expr, _)) = nodedb_query::expr_parse::parse_generated_expr(text) {
        return expr_names_srid(&expr, code);
    }
    match statements(text) {
        Some(parsed) => visit_expressions(&parsed, |expr| {
            if sql_expr_names_srid(expr, code) {
                ControlFlow::Break(())
            } else {
                ControlFlow::Continue(())
            }
        })
        .is_break(),
        None => false,
    }
}

/// `text` as statements, else as the expression of a `SELECT`.
fn statements(text: &str) -> Option<Vec<Statement>> {
    let dialect = GenericDialect {};
    // reconstructed-sql: parser-only scans stored SQL text for SRID literals without dispatch
    if let Ok(statements) = Parser::parse_sql(&dialect, text) {
        return Some(statements);
    }
    let select = format!("SELECT ({text})");
    // reconstructed-sql: parser-only scans a stored SQL expression for SRID literals without dispatch
    Parser::parse_sql(&dialect, &select).ok()
}

/// Whether one SQL expression node names `code`: a geometry literal, or a
/// literal in a geo function's SRID argument. The visitor reaches every
/// nested node.
fn sql_expr_names_srid(expr: &Expr, code: u32) -> bool {
    match expr {
        Expr::Value(v) => {
            literal_value(&v.value).is_some_and(|value| literal_geometry_srid(&value) == Some(code))
        }
        Expr::Function(function) => {
            let Some(ObjectNamePart::Identifier(name)) = function.name.0.last() else {
                return false;
            };
            let (Some(at), FunctionArguments::List(list)) =
                (srid_arg_position(&name.value), &function.args)
            else {
                return false;
            };
            match list.args.get(at) {
                Some(FunctionArg::Unnamed(FunctionArgExpr::Expr(arg))) => {
                    srid_literal(arg) == Some(i64::from(code))
                }
                _ => false,
            }
        }
        _ => false,
    }
}

/// The code an SRID argument holds when it is a literal, through casts.
fn srid_literal(expr: &Expr) -> Option<i64> {
    match expr {
        Expr::Value(v) => literal_value(&v.value).and_then(|value| literal_srid_code(&value)),
        Expr::Cast { expr, .. } | Expr::Nested(expr) => srid_literal(expr),
        _ => None,
    }
}

/// A number or string literal as a `Value`.
fn literal_value(value: &SqlValue) -> Option<Value> {
    match value {
        SqlValue::Number(n, _) => n
            .parse::<i64>()
            .map(Value::Integer)
            .ok()
            .or_else(|| n.parse::<f64>().ok().map(Value::Float)),
        SqlValue::SingleQuotedString(s) | SqlValue::EscapedStringLiteral(s) => {
            Some(Value::String(s.clone()))
        }
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const CODE: u32 = 910_042;

    #[test]
    fn expression_text_names_only_srid_roles() {
        assert!(text_names_srid("ST_Transform(geom, 910042)", CODE));
        assert!(!text_names_srid("amount < 910042", CODE));
        assert!(!text_names_srid("", CODE));
    }

    #[test]
    fn check_text_with_new_and_subqueries_parses_as_sql() {
        assert!(text_names_srid(
            "ST_SRID(ST_Transform(NEW.loc, 910042)) = 910042",
            CODE
        ));
        assert!(!text_names_srid("NEW.amount < 910042", CODE));
        assert!(text_names_srid(
            "EXISTS (SELECT 1 FROM zones z WHERE ST_Intersects(z.g, \
             ST_SetSRID(NEW.loc, 910042)))",
            CODE
        ));
        assert!(!text_names_srid(
            "EXISTS (SELECT 1 FROM zones z WHERE z.code = 910042)",
            CODE
        ));
    }

    #[test]
    fn statement_text_names_geometry_literals() {
        assert!(text_names_srid(
            "INSERT INTO log (g) VALUES ('SRID=910042;POINT(1 2)')",
            CODE
        ));
        assert!(!text_names_srid(
            "INSERT INTO log (n) VALUES (910042)",
            CODE
        ));
    }
}
