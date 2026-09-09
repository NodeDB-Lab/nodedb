// SPDX-License-Identifier: BUSL-1.1

//! DDL-time gate for column `DEFAULT` expressions.
//!
//! A `DEFAULT` clause is stored as raw text on the column type string and is
//! evaluated only when an INSERT omits the column. Without this gate a
//! `CREATE` accepts a call to a function that does not exist, and the author
//! learns about it at the first insert instead of at the declaration.
//!
//! The check runs `nodedb_sql`'s DEFAULT classifier, which consults the same
//! `FunctionRegistry` the resolver's undefined-function gate consults. No
//! second registry and no second name list exist here.

use nodedb_sql::SqlError;
use nodedb_sql::ddl_ast::collection_type::parse_column_type_str_full;
use nodedb_types::error::sqlstate;

use super::super::super::super::result::DdlError;

/// Refuse a declared column `DEFAULT` the server cannot evaluate.
///
/// The expression is classified and parsed, never evaluated, so a
/// `DEFAULT nextval('s')` column never advances its sequence at `CREATE`.
///
/// An unregistered function name raises SQLSTATE `42883`; every other
/// rejection raises SQLSTATE `42601`.
pub(super) fn validate_column_defaults(columns: &[(String, String)]) -> Result<(), DdlError> {
    for (column, type_str) in columns {
        let (_, _, _, default_expr) = parse_column_type_str_full(type_str);
        let Some(expr) = default_expr else {
            continue;
        };
        nodedb_sql::planner::defaults::validate_default_expr(&expr, column)
            .map_err(|error| default_error(column, &error))?;
    }
    Ok(())
}

/// Map a DEFAULT validation error onto its SQLSTATE.
fn default_error(column: &str, error: &SqlError) -> DdlError {
    let sqlstate = match error {
        SqlError::UndefinedFunction { .. } => sqlstate::UNDEFINED_FUNCTION,
        _ => sqlstate::SYNTAX_ERROR,
    };
    DdlError::new(
        sqlstate,
        format!("DEFAULT for column '{column}' is invalid: {error}"),
    )
}
