// SPDX-License-Identifier: Apache-2.0

//! Conversion of parsed `VALUES`-clause expressions into planner `SqlValue`s.
//!
//! This is the single surface every DML path (`INSERT`, `UPSERT`, KV entries,
//! `WHERE pk = <literal>` extraction) uses to turn an `ast::Expr` into a
//! concrete value, so constant folding and spatial-constructor handling never
//! drift between them.

use nodedb_crs::CrsContext;
use sqlparser::ast;

use crate::error::{Result, SqlError};
use crate::resolver::expr::convert_value;
use crate::types::*;

pub(crate) fn convert_value_rows(
    columns: &[String],
    rows: &[ast::Parens<Vec<ast::Expr>>],
    crs: &CrsContext<'_>,
) -> Result<Vec<Vec<(String, SqlValue)>>> {
    rows.iter()
        .map(|row| {
            row.iter()
                .enumerate()
                .map(|(i, expr)| {
                    let col = columns.get(i).cloned().unwrap_or_else(|| format!("col{i}"));
                    let val = expr_to_sql_value(expr, crs)?;
                    Ok((col, val))
                })
                .collect::<Result<Vec<_>>>()
        })
        .collect()
}

/// One `VALUES` cell written as text, resolved by the `VALUES` cell rule:
/// a typed literal (`UUID '...'`, `TIMESTAMPTZ '...'`, `BYTEA '...'`) to its
/// typed value, `jsonb_build_object(...)` to a typed object, any other
/// constant expression folded. For a caller that splits `VALUES` text
/// itself, such as a pre-write handler.
pub fn parse_values_cell(text: &str, crs: &CrsContext<'_>) -> Result<SqlValue> {
    let dialect = sqlparser::dialect::PostgreSqlDialect {};
    let mut parser = sqlparser::parser::Parser::new(&dialect)
        .try_with_sql(text)
        .map_err(|e| SqlError::Parse {
            detail: e.to_string(),
        })?;
    let expr = parser.parse_expr().map_err(|e| SqlError::Parse {
        detail: e.to_string(),
    })?;
    if parser.peek_token().token != sqlparser::tokenizer::Token::EOF {
        return Err(SqlError::Parse {
            detail: format!("VALUES cell '{text}' has trailing text"),
        });
    }
    expr_to_sql_value(&expr, crs)
}

/// Geo functions resolve SRIDs through `crs`.
pub(crate) fn expr_to_sql_value(expr: &ast::Expr, crs: &CrsContext<'_>) -> Result<SqlValue> {
    match expr {
        ast::Expr::Value(v) => convert_value(&v.value),
        // Array literals lower element-wise into `SqlValue::Array`; there is
        // no array-literal `SqlValue` the constant folder could produce.
        ast::Expr::Array(ast::Array { elem, .. }) => {
            let vals = elem
                .iter()
                .map(|e| expr_to_sql_value(e, crs))
                .collect::<Result<_>>()?;
            Ok(SqlValue::Array(vals))
        }
        // A geometry-producing call resolves through the geometry-expression
        // resolver, which turns a malformed geometry into an error rather
        // than the NULL the generic folder would produce — storing NULL for
        // `ST_GeomFromText('POINT(')` would lose the row's geometry silently.
        ast::Expr::Function(func) => {
            // A typed object, the form a nested object literal rewrites to.
            if let Some(object) = super::object_value::object_value(func, crs) {
                return object;
            }
            match crate::planner::geometry_expr::fold_geometry_function(func, crs) {
                Some(result) => result,
                // Non-geometry functions (`now()`, `date_add(...)`, registered
                // scalars) fold through the shared pipeline below.
                None => fold_constant_value(expr, crs),
            }
        }
        // Everything else — `::TYPE` / `CAST(... AS TYPE)` casts, arithmetic,
        // string concatenation, parenthesised literals — goes through the
        // same resolver and constant folder the `SELECT` projection path
        // uses, so the two surfaces never drift. Only genuinely row- or
        // runtime-dependent expressions (column refs, subqueries, unknown
        // functions) fail here.
        _ => fold_constant_value(expr, crs),
    }
}

fn fold_constant_value(expr: &ast::Expr, crs: &CrsContext<'_>) -> Result<SqlValue> {
    let sql_expr =
        crate::resolver::expr::convert_expr(expr, &crate::resolver::ColumnScope::Unchecked)?;
    crate::planner::const_fold::fold_constant_default(&sql_expr, crs)?.ok_or_else(|| {
        SqlError::Unsupported {
            detail: format!("value expression: {expr}"),
        }
    })
}
