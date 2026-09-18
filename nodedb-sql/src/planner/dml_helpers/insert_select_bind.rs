// SPDX-License-Identifier: Apache-2.0

//! Target-column binding for `INSERT ... SELECT`.

use sqlparser::ast;

use crate::error::{Result, SqlError};
use crate::parser::normalize::normalize_ident;
use crate::resolver::ColumnScope;
use crate::resolver::columns::TableScope;
use crate::resolver::expr::convert_expr;
use crate::types::*;

/// Bind a source `SELECT` list to the target column list.
///
/// `target_columns` is the explicit `INSERT INTO t (a, b)` list, empty when
/// the statement gives none. An empty target list with a `SELECT *` source
/// copies the row unchanged and needs no per-column expression.
pub(crate) fn bind_insert_select_columns(
    catalog: &dyn SqlCatalog,
    functions: &crate::functions::registry::FunctionRegistry,
    temporal: crate::TemporalScope,
    target_columns: &[String],
    select: &ast::Select,
    target: &CollectionInfo,
) -> Result<Vec<(String, SqlExpr)>> {
    let source_is_star = select.projection.iter().all(|item| {
        matches!(
            item,
            ast::SelectItem::Wildcard(_) | ast::SelectItem::QualifiedWildcard(..)
        )
    });

    if source_is_star {
        if target_columns.is_empty() {
            return Ok(Vec::new());
        }
        return Err(SqlError::Unsupported {
            detail: "INSERT ... SELECT * cannot bind to an explicit target column list; name the \
                     source columns explicitly"
                .into(),
        });
    }

    let names = target_names(target_columns, select)?;

    if !target.open_schema {
        for name in &names {
            if !target.columns.iter().any(|c| &c.name == name) {
                return Err(SqlError::UnknownColumn {
                    table: target.name.clone(),
                    column: name.clone(),
                });
            }
        }
    }

    // The source scope gates every column the SELECT list names: one the
    // source does not carry raises `UnknownColumn` here, at plan time.
    let source_scope = TableScope::resolve_from(catalog, functions, temporal, &select.from)?;
    let scope = ColumnScope::Relations(&source_scope);

    let mut bound = Vec::with_capacity(names.len());
    for (name, item) in names.into_iter().zip(select.projection.iter()) {
        bound.push((name, convert_expr(projection_expr(item)?, &scope)?));
    }
    Ok(bound)
}

/// The target column each projection item writes, in projection order.
fn target_names(target_columns: &[String], select: &ast::Select) -> Result<Vec<String>> {
    if !target_columns.is_empty() {
        if target_columns.len() != select.projection.len() {
            return Err(SqlError::Arity {
                detail: format!(
                    "INSERT has {} target columns but the SELECT list has {} expressions",
                    target_columns.len(),
                    select.projection.len()
                ),
            });
        }
        return Ok(target_columns.to_vec());
    }
    select.projection.iter().map(output_name).collect()
}

/// The output name of a projection item: its alias, else the column it names.
fn output_name(item: &ast::SelectItem) -> Result<String> {
    match item {
        ast::SelectItem::ExprWithAlias { alias, .. } => Ok(normalize_ident(alias)),
        ast::SelectItem::UnnamedExpr(expr) => match expr {
            ast::Expr::Identifier(ident) => Ok(normalize_ident(ident)),
            ast::Expr::CompoundIdentifier(parts) if parts.len() == 2 => {
                Ok(normalize_ident(&parts[1]))
            }
            _ => Err(unnamed_projection_error()),
        },
        ast::SelectItem::ExprWithAliases { .. }
        | ast::SelectItem::Wildcard(_)
        | ast::SelectItem::QualifiedWildcard(..) => Err(unnamed_projection_error()),
    }
}

fn unnamed_projection_error() -> SqlError {
    SqlError::Unsupported {
        detail: "INSERT ... SELECT needs a target column for every projected expression; add an \
                 alias or an explicit target column list"
            .into(),
    }
}

/// The expression a projection item evaluates per source row.
fn projection_expr(item: &ast::SelectItem) -> Result<&ast::Expr> {
    match item {
        ast::SelectItem::UnnamedExpr(expr) | ast::SelectItem::ExprWithAlias { expr, .. } => {
            Ok(expr)
        }
        ast::SelectItem::ExprWithAliases { .. }
        | ast::SelectItem::Wildcard(_)
        | ast::SelectItem::QualifiedWildcard(..) => Err(SqlError::Unsupported {
            detail: "INSERT ... SELECT cannot mix a wildcard with named projections".into(),
        }),
    }
}
