// SPDX-License-Identifier: Apache-2.0

//! Argument resolution for the search functions that name a column:
//! `text_match(column, q, ...)`, `bm25_score(column, q, ...)`, and the vector
//! column of `vector_distance(column, q)` inside `rrf_score(...)`.

use sqlparser::ast;

use super::helpers::{extract_column_name, extract_string_literal};
use super::text_options::{TextOptions, parse_text_options};
use crate::error::{Result, SqlError};
use crate::parser::normalize::{normalize_ident, normalize_object_name_checked};
use crate::resolver::columns::ResolvedTable;

/// The column, query, and options of `text_match(column, q, ...)` /
/// `bm25_score(column, q, ...)`.
pub(super) struct TextCall {
    /// `None` for `*` or `<table>.*`: the whole-document index.
    pub field: Option<String>,
    pub query: String,
    /// The named `mode` / `fuzzy` options.
    pub options: TextOptions,
}

/// Resolve a column reference to `table`: `col` or `<table>.col`, where the
/// qualifier is the table's name or alias. `None` when `expr` is no column
/// reference. A qualifier naming another relation is `UnknownTable`.
pub(super) fn resolve_table_column(
    expr: &ast::Expr,
    table: &ResolvedTable,
) -> Result<Option<String>> {
    match expr {
        ast::Expr::Identifier(ident) => Ok(Some(normalize_ident(ident))),
        ast::Expr::CompoundIdentifier(parts) if parts.len() == 2 => {
            let qualifier = normalize_ident(&parts[0]);
            check_qualifier(&qualifier, table)?;
            Ok(Some(normalize_ident(&parts[1])))
        }
        ast::Expr::CompoundIdentifier(_) => extract_column_name(expr).map(Some),
        _ => Ok(None),
    }
}

/// Accept `qualifier` when it names `table` or its alias.
fn check_qualifier(qualifier: &str, table: &ResolvedTable) -> Result<()> {
    if qualifier == table.name || table.alias.as_deref() == Some(qualifier) {
        Ok(())
    } else {
        Err(SqlError::UnknownTable {
            name: qualifier.to_owned(),
        })
    }
}

/// Resolve the arguments of a text-search call against `table`.
///
/// The first argument is a text column of `table` (bare or qualified by the
/// table's name or alias), or `*` / `<table>.*` for the whole document. Any
/// other expression is a typed error. Named options follow the query; see
/// [`parse_text_options`].
pub(super) fn resolve_text_call(
    function: &str,
    func: &ast::Function,
    table: &ResolvedTable,
) -> Result<TextCall> {
    let arity = || SqlError::Arity {
        detail: format!("{function}() takes (column, 'query', [mode => ..., fuzzy => ...])"),
    };
    let ast::FunctionArguments::List(list) = &func.args else {
        return Err(arity());
    };
    let options = parse_text_options(function, &list.args)?;
    let mut unnamed = list.args.iter().filter_map(|a| match a {
        ast::FunctionArg::Unnamed(e) => Some(e),
        _ => None,
    });
    let first = unnamed.next().ok_or_else(arity)?;
    let ast::FunctionArgExpr::Expr(query_expr) = unnamed.next().ok_or_else(arity)? else {
        return Err(arity());
    };
    let field = match first {
        ast::FunctionArgExpr::Wildcard => None,
        ast::FunctionArgExpr::QualifiedWildcard(name) => {
            check_qualifier(&normalize_object_name_checked(name)?, table)?;
            None
        }
        ast::FunctionArgExpr::Expr(expr) => match resolve_table_column(expr, table)? {
            Some(column) => {
                table.check_text_column(function, &column)?;
                Some(column)
            }
            None => return Err(not_a_column(function, table, first)),
        },
        ast::FunctionArgExpr::WildcardWithOptions(_) => {
            return Err(not_a_column(function, table, first));
        }
    };
    Ok(TextCall {
        field,
        query: extract_string_literal(query_expr)?,
        options,
    })
}

/// The error for a text-search argument that is no column reference.
fn not_a_column(function: &str, table: &ResolvedTable, arg: &ast::FunctionArgExpr) -> SqlError {
    SqlError::TextColumn {
        function: function.to_owned(),
        collection: table.name.clone(),
        column: arg.to_string(),
        fault: nodedb_types::text_search::TextColumnFault::NotAColumn,
    }
}
