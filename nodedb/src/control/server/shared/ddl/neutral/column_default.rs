// SPDX-License-Identifier: BUSL-1.1

//! DDL-time gate for declared value-producing clauses.
//!
//! A column `DEFAULT`, a typeguard `DEFAULT` and a typeguard `VALUE` all pass
//! through here, so one registry decides which function names a declaration
//! can name.
//!
//! A `DEFAULT` clause is stored as raw text on the column and is evaluated
//! only when an INSERT omits the column. Without this gate a `CREATE` or a
//! `CONVERT` accepts a call to a function that does not exist, and the author
//! learns about it at the first insert instead of at the declaration.
//!
//! The check runs `nodedb_sql`'s DEFAULT classifier, which consults the same
//! `FunctionRegistry` the resolver's undefined-function gate consults. No
//! second registry and no second name list exist here.

use nodedb_sql::SqlError;
use nodedb_sql::ddl_ast::collection_type::parse_column_type_str_full;
use nodedb_types::error::sqlstate;

use super::super::result::DdlError;

/// Refuse a declared column `DEFAULT` the server cannot evaluate.
///
/// Each pair carries a column name and its declared type text, and the
/// `DEFAULT` clause is read out of that text.
pub(super) fn validate_column_defaults(columns: &[(String, String)]) -> Result<(), DdlError> {
    for (column, type_str) in columns {
        let (_, _, _, default_expr) = parse_column_type_str_full(type_str);
        let Some(expr) = default_expr else {
            continue;
        };
        validate_column_default(column, &expr)?;
    }
    Ok(())
}

/// Refuse one declared column `DEFAULT` the server cannot evaluate.
pub(super) fn validate_column_default(column: &str, expr: &str) -> Result<(), DdlError> {
    validate_constant_clause_expr("DEFAULT", column, expr)
}

/// Refuse a value-producing clause whose value must be constant.
///
/// A column `DEFAULT` is const-folded once, with no row in scope, so an
/// expression that names another column can never produce a value there.
/// Refusing it at this gate — the one every producer of a column `DEFAULT`
/// calls — keeps the refusal at the declaration instead of the first insert's
/// `UnevaluableDefault`.
pub(super) fn validate_constant_clause_expr(
    clause: &str,
    owner: &str,
    expr: &str,
) -> Result<(), DdlError> {
    validate_clause_expr(clause, owner, expr)?;
    let references_column = nodedb_sql::planner::defaults::default_expr_references_columns(expr)
        .map_err(|error| clause_error(clause, owner, &error))?;
    if references_column {
        return Err(DdlError::new(
            sqlstate::SYNTAX_ERROR,
            format!(
                "{clause} for '{owner}' references another column; it is evaluated with no row \
                 in scope, so give a constant expression"
            ),
        ));
    }
    Ok(())
}

/// Refuse one declared value-producing clause the server cannot evaluate.
///
/// `clause` names the keyword the author wrote and `owner` the column or field
/// carrying it, so a typeguard `VALUE` reports itself rather than borrowing the
/// `DEFAULT` wording.
///
/// The expression is classified and parsed, never evaluated, so a
/// `DEFAULT nextval('s')` column never advances its sequence at DDL time.
///
/// An unregistered function name raises SQLSTATE `42883`; every other
/// rejection raises SQLSTATE `42601`.
pub(super) fn validate_clause_expr(clause: &str, owner: &str, expr: &str) -> Result<(), DdlError> {
    nodedb_sql::planner::defaults::validate_default_expr(expr, owner)
        .map_err(|error| clause_error(clause, owner, &error))
}

/// Map a clause validation error onto its SQLSTATE.
fn clause_error(clause: &str, owner: &str, error: &SqlError) -> DdlError {
    let sqlstate = match error {
        SqlError::UndefinedFunction { .. } => sqlstate::UNDEFINED_FUNCTION,
        _ => sqlstate::SYNTAX_ERROR,
    };
    DdlError::new(
        sqlstate,
        format!("{clause} for '{owner}' is invalid: {error}"),
    )
}
