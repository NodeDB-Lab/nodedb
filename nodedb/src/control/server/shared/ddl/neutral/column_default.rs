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
//!
//! A column `DEFAULT` that spells a literal is also checked against the
//! column's declared type, through the same coercion an INSERT applies to
//! the materialized value. `DEFAULT 'not a date'` on a `TIMESTAMP` column
//! and `DEFAULT 999999` on a `SMALLINT` column are refused at the
//! declaration, so no insert can ever materialize a value the column cannot
//! hold.

use nodedb_sql::SqlError;
use nodedb_sql::ddl_ast::collection_type::parse_column_type_str_full;
use nodedb_sql::planner::declared_type_coerce::coerce_write_literal;
use nodedb_sql::planner::defaults::{CompiledDefault, default_value_to_sql};
use nodedb_types::error::sqlstate;

use super::super::result::DdlError;
use crate::control::planner::catalog_adapter::declared_column_info;

/// One declared column as the DDL gate sees it.
pub(super) struct DeclaredColumn<'a> {
    pub name: &'a str,
    /// The declared type text, modifiers included (`SMALLINT NOT NULL`,
    /// `TIMESTAMP TIME_KEY`). The bare type resolves out of it the same way
    /// the catalog adapter resolves a tracked field.
    pub declared_type: &'a str,
    /// Whether the column is the primary key. The key keeps its literal as
    /// written on every write path, so its DEFAULT is not re-typed either.
    pub primary_key: bool,
}

/// Refuse a declared column `DEFAULT` the server cannot evaluate, or that the
/// column's declared type cannot hold.
///
/// Each pair carries a column name and its declared type text, and the
/// `DEFAULT` clause is read out of that text.
pub(super) fn validate_column_defaults(columns: &[(String, String)]) -> Result<(), DdlError> {
    for (column, type_str) in columns {
        let (_, primary_key, _, default_expr) = parse_column_type_str_full(type_str);
        let Some(expr) = default_expr else {
            continue;
        };
        validate_column_default(
            &DeclaredColumn {
                name: column,
                declared_type: type_str,
                primary_key,
            },
            &expr,
        )?;
    }
    Ok(())
}

/// Refuse one declared column `DEFAULT` the server cannot evaluate, or that
/// the column's declared type cannot hold.
///
/// The expression is classified and parsed, never evaluated, so a
/// `DEFAULT nextval('s')` column never advances its sequence at DDL time. A
/// literal is then coerced to the declared type and range-checked exactly as
/// an INSERT coerces the materialized value; a generator or an expression has
/// no value to check until it is evaluated.
///
/// An unregistered function name raises SQLSTATE `42883`, a literal the
/// declared type cannot represent `42804`, a literal past the declared
/// numeric width `22003`, and every other rejection `42601`.
pub(super) fn validate_column_default(
    column: &DeclaredColumn<'_>,
    expr: &str,
) -> Result<(), DdlError> {
    let compiled = CompiledDefault::declare(column.name, expr)
        .map_err(|error| clause_error("DEFAULT", column.name, &error))?;
    let Some(literal) = compiled.literal() else {
        return Ok(());
    };
    let mut info = declared_column_info(column.name, column.declared_type);
    info.is_primary_key = column.primary_key;
    let value = default_value_to_sql(column.name, literal.clone())
        .map_err(|error| clause_error("DEFAULT", column.name, &error))?;
    coerce_write_literal(&info, value)
        .map(|_| ())
        .map_err(|error| clause_error("DEFAULT", column.name, &error))
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
        SqlError::TypeMismatch { .. } => sqlstate::DATATYPE_MISMATCH,
        SqlError::IntegerOutOfRange { .. } | SqlError::FloatOutOfRange { .. } => {
            sqlstate::NUMERIC_VALUE_OUT_OF_RANGE
        }
        _ => sqlstate::SYNTAX_ERROR,
    };
    DdlError::new(
        sqlstate,
        format!("{clause} for '{owner}' is invalid: {error}"),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    fn declared(name: &str, declared_type: &str) -> (String, String) {
        (name.to_string(), declared_type.to_string())
    }

    /// Text that spells no instant is refused on a TIMESTAMP column, naming
    /// the column, with the datatype-mismatch SQLSTATE.
    #[test]
    fn a_text_default_with_no_instant_is_refused_on_a_timestamp_column() {
        let error = validate_column_defaults(&[declared("at", "TIMESTAMP DEFAULT 'not a date'")])
            .expect_err("'not a date' is not an instant");
        assert_eq!(error.sqlstate, sqlstate::DATATYPE_MISMATCH);
        assert!(error.message.contains("'at'"), "{}", error.message);
    }

    /// An integer past the declared SMALLINT width is refused with the
    /// out-of-range SQLSTATE, naming the column.
    #[test]
    fn an_out_of_range_default_is_refused_on_a_smallint_column() {
        let error = validate_column_defaults(&[declared("s", "SMALLINT DEFAULT 999999")])
            .expect_err("999999 does not fit SMALLINT");
        assert_eq!(error.sqlstate, sqlstate::NUMERIC_VALUE_OUT_OF_RANGE);
        assert!(error.message.contains("'s'"), "{}", error.message);
    }

    /// A literal the column can hold is accepted, in every spelling the
    /// INSERT coercion accepts: epoch milliseconds and ISO text on a
    /// TIMESTAMP column, an in-range integer on SMALLINT, numeric text on a
    /// FLOAT column, and a TIME_KEY modifier before the clause.
    #[test]
    fn a_representable_default_is_accepted() {
        validate_column_defaults(&[
            declared("at", "TIMESTAMP DEFAULT 1583402400000"),
            declared("at2", "TIMESTAMP DEFAULT '2020-03-05 10:00:00'"),
            declared("ts", "TIMESTAMP TIME_KEY DEFAULT 1583402400000"),
            declared("s", "SMALLINT DEFAULT 7"),
            declared("r", "FLOAT DEFAULT '1.5'"),
            declared("name", "TEXT DEFAULT 'h0'"),
        ])
        .expect("every default is representable");
    }

    /// The primary key keeps its literal as written, so its DEFAULT is not
    /// re-typed; a generator has no value to check at declaration.
    #[test]
    fn primary_key_and_generator_defaults_pass_the_type_gate() {
        validate_column_defaults(&[
            declared("id", "INT PRIMARY KEY DEFAULT 'k'"),
            declared("u", "TEXT DEFAULT UUID_V7"),
            declared("n", "INT DEFAULT nextval('s')"),
        ])
        .expect("no literal reaches the type gate");
    }

    /// An unregistered function name keeps its own SQLSTATE.
    #[test]
    fn an_unregistered_function_keeps_undefined_function() {
        let error = validate_column_defaults(&[declared("a", "TEXT DEFAULT no_such_fn('x')")])
            .expect_err("unknown function refused");
        assert_eq!(error.sqlstate, sqlstate::UNDEFINED_FUNCTION);
    }
}
