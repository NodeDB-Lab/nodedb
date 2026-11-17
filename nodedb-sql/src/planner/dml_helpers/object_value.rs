// SPDX-License-Identifier: Apache-2.0

//! `jsonb_build_object('k', v, ...)` in a DML value position: a typed object.
//!
//! A nested `{ ... }` object literal in `INSERT INTO c { ... }` rewrites to
//! this call, so each field value, a bound `$N` included, resolves through
//! the same value rule as a top-level `VALUES` cell and keeps its exact
//! type. The keys are string literals. The result is [`SqlValue::Object`].

use nodedb_crs::CrsContext;
use sqlparser::ast;

use crate::error::{Result, SqlError};
use crate::types::SqlValue;

use super::value_convert::expr_to_sql_value;

/// The function name the object-literal rewrite emits.
pub(crate) const OBJECT_FUNCTION: &str = "jsonb_build_object";

/// The typed object `func` builds, or `None` when `func` is not
/// `jsonb_build_object`.
pub(super) fn object_value(func: &ast::Function, crs: &CrsContext<'_>) -> Option<Result<SqlValue>> {
    let name = func.name.0.last()?.as_ident()?;
    if !name.value.eq_ignore_ascii_case(OBJECT_FUNCTION) {
        return None;
    }
    Some(build(func, crs))
}

fn build(func: &ast::Function, crs: &CrsContext<'_>) -> Result<SqlValue> {
    let args = match &func.args {
        ast::FunctionArguments::List(list) => &list.args,
        ast::FunctionArguments::None => return Ok(SqlValue::Object(Vec::new())),
        ast::FunctionArguments::Subquery(_) => return Err(malformed("a subquery argument")),
    };
    if args.len() % 2 != 0 {
        return Err(malformed("an odd number of arguments"));
    }
    let mut fields = Vec::with_capacity(args.len() / 2);
    for [key, value] in args.as_chunks::<2>().0 {
        let (key, value) = (unnamed(key)?, unnamed(value)?);
        let ast::Expr::Value(key) = key else {
            return Err(malformed("a key that is not a string literal"));
        };
        let ast::Value::SingleQuotedString(key) = &key.value else {
            return Err(malformed("a key that is not a string literal"));
        };
        fields.push((key.clone(), expr_to_sql_value(value, crs)?));
    }
    Ok(SqlValue::Object(fields))
}

fn unnamed(arg: &ast::FunctionArg) -> Result<&ast::Expr> {
    match arg {
        ast::FunctionArg::Unnamed(ast::FunctionArgExpr::Expr(expr)) => Ok(expr),
        _ => Err(malformed("a named or wildcard argument")),
    }
}

fn malformed(what: &str) -> SqlError {
    SqlError::Parse {
        detail: format!(
            "{OBJECT_FUNCTION}(...) in a VALUES row takes 'key', value pairs; found {what}"
        ),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::parser::statement::parse_sql;

    fn first_value(sql: &str) -> Result<SqlValue> {
        let stmts = parse_sql(sql).expect("parses");
        let sqlparser::ast::Statement::Insert(insert) = &stmts[0] else {
            panic!("an insert");
        };
        let Some(source) = &insert.source else {
            panic!("a source");
        };
        let sqlparser::ast::SetExpr::Values(values) = source.body.as_ref() else {
            panic!("values");
        };
        let cell = values
            .rows
            .first()
            .and_then(|row| row.iter().next())
            .expect("one cell");
        expr_to_sql_value(cell, &CrsContext::builtin_only())
    }

    #[test]
    fn a_nested_object_keeps_typed_fields_in_order() {
        let uuid = "550e8400-e29b-41d4-a716-446655440000";
        assert_eq!(
            first_value(&format!(
                "INSERT INTO c (m) VALUES (jsonb_build_object('b', UUID '{uuid}', \
                 'n', jsonb_build_object('x', 1)))"
            )),
            Ok(SqlValue::Object(vec![
                ("b".to_string(), SqlValue::Uuid(uuid.into())),
                (
                    "n".to_string(),
                    SqlValue::Object(vec![("x".to_string(), SqlValue::Int(1))])
                ),
            ]))
        );
    }

    #[test]
    fn a_non_literal_key_is_refused() {
        assert!(first_value("INSERT INTO c (m) VALUES (jsonb_build_object(1, 2))").is_err());
        assert!(first_value("INSERT INTO c (m) VALUES (jsonb_build_object('a'))").is_err());
    }
}
