// SPDX-License-Identifier: BUSL-1.1

//! Simple (no-subquery) CHECK constraint evaluation + `NEW.field` substitution
//! helpers shared with the subquery evaluator.

use std::collections::HashMap;

use super::enforce::ddl_err;
use crate::control::security::catalog::types::CheckConstraintDef;
use crate::control::server::shared::ddl::result::DdlError;

/// Evaluate a simple CHECK constraint (no subquery) using the `SqlExpr` evaluator.
///
/// Strips `NEW.` prefixes so `NEW.amount > 0` becomes `amount > 0`, then
/// evaluates against the document fields directly.
pub(super) fn enforce_simple_check(
    constraint: &CheckConstraintDef,
    fields: &HashMap<String, nodedb_types::Value>,
    crs: &nodedb_crs::CrsContext<'_>,
) -> Result<(), DdlError> {
    // Strip NEW. prefixes to get bare column references.
    let bare_expr = strip_new_prefix(&constraint.check_sql);

    // Parse into SqlExpr using the shared expression parser.
    let (expr, _deps) =
        nodedb_query::expr_parse::parse_generated_expr(&bare_expr).map_err(|e| {
            ddl_err(
                "23514",
                &format!(
                    "CHECK constraint '{}' failed to parse: {}",
                    constraint.name, e
                ),
            )
        })?;

    // Build a Value::Object from the fields for evaluation.
    let doc = nodedb_types::Value::Object(fields.clone());

    // Evaluate the expression against the document. An evaluation error is
    // neither a PASS nor an ordinary FAIL. It keeps its own SQLSTATE rather
    // than folding into the 23514 (check_violation) of the failure arm below.
    let result = expr.eval(&doc, crs).map_err(|e| {
        ddl_err(
            crate::control::server::shared::ddl::sqlstate::eval_error_sqlstate(&e),
            &format!(
                "CHECK constraint '{}' failed to evaluate: {}: {e}",
                constraint.name, constraint.check_sql
            ),
        )
    })?;

    // NULL passes CHECK (SQL semantics: NULL is not FALSE).
    match result {
        nodedb_types::Value::Bool(true) => Ok(()),
        nodedb_types::Value::Null => Ok(()),
        nodedb_types::Value::Integer(n) if n != 0 => Ok(()),
        _ => Err(ddl_err(
            "23514",
            &format!(
                "CHECK constraint '{}' violated: {}",
                constraint.name, constraint.check_sql
            ),
        )),
    }
}

/// Strip `NEW.` prefix from field references (case-insensitive).
///
/// Converts `NEW.amount > 0` → `amount > 0` so the expression can be parsed
/// as bare column references by `parse_generated_expr`.
pub(super) fn strip_new_prefix(sql: &str) -> String {
    let chars: Vec<char> = sql.chars().collect();
    let mut result = String::with_capacity(sql.len());
    let mut i = 0;

    while i < chars.len() {
        if i + 4 <= chars.len() {
            let window: String = chars[i..i + 4].iter().collect();
            if window.eq_ignore_ascii_case("NEW.") {
                if i > 0 && (chars[i - 1].is_ascii_alphanumeric() || chars[i - 1] == '_') {
                    result.push(chars[i]);
                    i += 1;
                    continue;
                }
                i += 4;
                continue;
            }
        }
        result.push(chars[i]);
        i += 1;
    }
    result
}

/// Convert a `Value` to SQL literal text through the canonical shared implementation.
pub(super) fn value_to_sql_literal(value: &nodedb_types::Value) -> String {
    value.to_sql_literal()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn value_to_sql_literal_escapes_quotes() {
        let val = nodedb_types::Value::String("it's a test".into());
        assert_eq!(value_to_sql_literal(&val), "'it''s a test'");
    }

    #[test]
    fn value_to_sql_literal_delegates_to_shared_implementation() {
        let value = nodedb_types::Value::Record {
            table: "ta'ble".into(),
            id: "id".into(),
        };
        assert_eq!(value_to_sql_literal(&value), value.to_sql_literal());
    }
}
