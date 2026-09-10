// SPDX-License-Identifier: BUSL-1.1

//! DDL-time gate for a typeguard `DEFAULT` / `VALUE` expression.
//!
//! Both clauses store raw text and produce a field value on every write. The
//! Data Plane's `inject_defaults` reads that text with
//! `nodedb_query::expr_parse::parse_generated_expr` and evaluates it with
//! `SqlExpr::eval`, so a declaration the write path cannot evaluate is refused
//! here instead of at the first write.
//!
//! Two checks run, and neither evaluates the expression:
//! - The canonical declared-clause gate resolves every function name through
//!   the `FunctionRegistry`. `nodedb_query::functions::eval_function` answers
//!   `Null` for a name it does not know, so an unregistered name would store
//!   `Null` on every write and report nothing.
//! - The write path's own parser refuses what it cannot evaluate, including
//!   the non-deterministic functions a guard expression must not call.
//!
//! An unregistered function name raises SQLSTATE `42883`; every other
//! rejection raises SQLSTATE `42601`. `nextval` is refused outright, so
//! declaring a typeguard cannot advance a sequence.

use super::super::super::result::DdlError;
use super::super::column_default::validate_clause_expr;
use super::parse::err;

/// Refuse a typeguard `DEFAULT` or `VALUE` the engine cannot evaluate.
///
/// `clause` names the keyword the author wrote, so the message points at it.
pub(super) fn validate_injected_expr(
    field: &str,
    clause: &str,
    expr: &str,
) -> Result<(), DdlError> {
    validate_clause_expr(clause, field, expr)?;
    nodedb_query::expr_parse::parse_generated_expr(expr).map_err(|error| {
        err(
            "42601",
            &format!("field '{field}': {clause} expression '{expr}' is invalid: {error}"),
        )
    })?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Every clause form the write path evaluates stays accepted.
    #[test]
    fn evaluable_clause_expressions_are_accepted() {
        for (clause, expr) in [
            ("DEFAULT", "'draft'"),
            ("DEFAULT", "1"),
            ("DEFAULT", "0"),
            ("VALUE", "'server_computed'"),
            ("VALUE", "name"),
            ("VALUE", "LOWER(REPLACE(title, ' ', '-'))"),
        ] {
            validate_injected_expr("f", clause, expr)
                .unwrap_or_else(|e| panic!("{clause} {expr} must be accepted: {}", e.message));
        }
    }

    /// An unregistered function name raises `42883` rather than storing `Null`
    /// on every write.
    #[test]
    fn an_unregistered_function_raises_undefined_function() {
        let error = validate_injected_expr("status", "DEFAULT", "no_such_function()")
            .expect_err("an unregistered function must refuse");
        assert_eq!(error.sqlstate, "42883");
        assert!(
            error.message.contains("status"),
            "the error must name the field: {}",
            error.message
        );
    }

    /// The same refusal covers a `VALUE` clause, and names that clause.
    #[test]
    fn an_unregistered_function_in_a_value_clause_names_the_clause() {
        let error = validate_injected_expr("computed", "VALUE", "no_such_function()")
            .expect_err("an unregistered function must refuse");
        assert_eq!(error.sqlstate, "42883");
        assert!(
            error.message.contains("VALUE"),
            "the error must name the clause: {}",
            error.message
        );
    }

    /// A function the write path refuses as non-deterministic is refused at
    /// declaration with `42601`.
    #[test]
    fn a_non_deterministic_function_is_refused() {
        for expr in ["now()", "uuid_v7()", "nextval('s')"] {
            let error = validate_injected_expr("at", "DEFAULT", expr)
                .expect_err("a non-deterministic function must refuse");
            assert_eq!(error.sqlstate, "42601", "on {expr}");
        }
    }

    /// A malformed expression is a syntax error, not a stored clause.
    #[test]
    fn a_malformed_expression_is_refused() {
        let error = validate_injected_expr("f", "DEFAULT", "'unterminated")
            .expect_err("a malformed expression must refuse");
        assert_eq!(error.sqlstate, "42601");
    }
}
