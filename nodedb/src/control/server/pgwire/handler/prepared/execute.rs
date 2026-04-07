//! Execute a prepared statement from an extended query portal.
//!
//! Binds parameter values from the portal into the SQL, then executes
//! through the same `execute_sql` path as SimpleQuery — preserving
//! all DDL dispatch, transaction handling, and permission checks.

use std::fmt::Debug;

use bytes::Bytes;
use futures::sink::Sink;
use pgwire::api::portal::Portal;
use pgwire::api::results::Response;
use pgwire::api::{ClientInfo, ClientPortalStore, Type};
use pgwire::error::{ErrorInfo, PgWireError, PgWireResult};
use pgwire::messages::PgWireBackendMessage;

use super::super::core::NodeDbPgHandler;
use super::statement::ParsedStatement;

impl NodeDbPgHandler {
    /// Execute a prepared statement from a portal.
    ///
    /// Called by the `ExtendedQueryHandler::do_query` implementation.
    /// Substitutes bound parameters into the SQL, then delegates to `execute_sql`.
    pub(crate) async fn execute_prepared<C>(
        &self,
        client: &mut C,
        portal: &Portal<ParsedStatement>,
        _max_rows: usize,
    ) -> PgWireResult<Response>
    where
        C: ClientInfo + ClientPortalStore + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        let addr = client.socket_addr();
        let identity = self.resolve_identity(client)?;
        let stmt = &portal.statement.statement;

        // Build the final SQL by substituting parameter values.
        let final_sql = substitute_params(&stmt.sql, &portal.parameters, &stmt.param_types)?;

        // Bind-time specialization: detect partition-prunable patterns in the
        // substituted SQL. These hints are logged as trace spans and can be used
        // by the planner for optimization (e.g., timeseries partition pruning).
        if final_sql.contains(" > ") || final_sql.contains(" >= ") || final_sql.contains(" < ") {
            tracing::trace!(
                sql = %final_sql,
                "bind-time specialization: range predicate detected — partition pruning candidate"
            );
        }

        // Execute through the standard path (DDL dispatch, transaction handling,
        // permission checks, quota, plan, dispatch).
        let mut results = self.execute_sql(&identity, &addr, &final_sql).await?;
        Ok(results.pop().unwrap_or(Response::EmptyQuery))
    }
}

/// Substitute `$1`, `$2`, ... placeholders in SQL with literal values from the portal.
///
/// This produces a concrete SQL string that can be planned and executed through
/// the standard pipeline. Parameter values are properly escaped/quoted to prevent
/// SQL injection.
fn substitute_params(
    sql: &str,
    params: &[Option<Bytes>],
    param_types: &[Option<Type>],
) -> PgWireResult<String> {
    if params.is_empty() {
        return Ok(sql.to_owned());
    }

    let mut replacements = vec![String::new(); params.len()];
    for (i, param) in params.iter().enumerate() {
        replacements[i] = match param {
            None => "NULL".to_string(),
            Some(bytes) => {
                let text = std::str::from_utf8(bytes).map_err(|_| {
                    PgWireError::UserError(Box::new(ErrorInfo::new(
                        "ERROR".to_owned(),
                        "22021".to_owned(),
                        format!("invalid UTF-8 in parameter ${}", i + 1),
                    )))
                })?;

                let pg_type = param_types
                    .get(i)
                    .and_then(|t| t.as_ref())
                    .unwrap_or(&Type::UNKNOWN);

                format_param_value(text, pg_type)
            }
        };
    }

    Ok(rewrite_sql_placeholders(sql, &replacements))
}

/// Format a parameter value as a SQL literal, properly escaped.
///
/// Numeric types are passed through unquoted. String/text/date types
/// are single-quoted with internal quotes escaped.
fn format_param_value(text: &str, pg_type: &Type) -> String {
    // Numeric types: pass through as literal (no quoting needed).
    if matches!(
        *pg_type,
        Type::INT2
            | Type::INT4
            | Type::INT8
            | Type::FLOAT4
            | Type::FLOAT8
            | Type::NUMERIC
            | Type::BOOL
    ) {
        // Validate that the value looks numeric/boolean to prevent injection.
        if is_safe_numeric_literal(text) {
            return text.to_string();
        }
    }

    // Boolean special handling.
    if *pg_type == Type::BOOL {
        let lower = text.to_lowercase();
        if lower == "t" || lower == "true" || lower == "1" {
            return "TRUE".to_string();
        }
        if lower == "f" || lower == "false" || lower == "0" {
            return "FALSE".to_string();
        }
    }

    // Everything else: quote as a string literal with proper escaping.
    // PostgreSQL standard_conforming_strings = on: use '' for literal quotes.
    let escaped = text.replace('\'', "''");
    format!("'{escaped}'")
}

/// Check if a string is a safe numeric literal (integer, float, NaN, Infinity).
fn is_safe_numeric_literal(s: &str) -> bool {
    if s.is_empty() {
        return false;
    }
    // Allow NaN, Infinity, -Infinity.
    let lower = s.to_lowercase();
    if lower == "nan" || lower == "infinity" || lower == "-infinity" {
        return true;
    }
    // Allow optional leading sign, digits, optional decimal point, optional exponent.
    let mut chars = s.chars().peekable();
    if chars.peek() == Some(&'-') || chars.peek() == Some(&'+') {
        chars.next();
    }
    let mut has_digit = false;
    let mut has_dot = false;
    while let Some(&c) = chars.peek() {
        if c.is_ascii_digit() {
            has_digit = true;
            chars.next();
        } else if c == '.' && !has_dot {
            has_dot = true;
            chars.next();
        } else if (c == 'e' || c == 'E') && has_digit {
            chars.next();
            // Optional sign after exponent.
            if chars.peek() == Some(&'-') || chars.peek() == Some(&'+') {
                chars.next();
            }
            // Must have digits after exponent.
            let mut exp_digits = false;
            while chars.peek().is_some_and(|c| c.is_ascii_digit()) {
                exp_digits = true;
                chars.next();
            }
            return exp_digits && chars.peek().is_none();
        } else {
            return false;
        }
    }
    has_digit && chars.peek().is_none()
}

use super::sql_placeholder::rewrite_sql_placeholders;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn substitute_no_params() {
        let result = substitute_params("SELECT 1", &[], &[]).unwrap();
        assert_eq!(result, "SELECT 1");
    }

    #[test]
    fn substitute_single_param() {
        let params = vec![Some(Bytes::from_static(b"42"))];
        let types = vec![Some(Type::INT8)];
        let result =
            substitute_params("SELECT * FROM users WHERE id = $1", &params, &types).unwrap();
        assert_eq!(result, "SELECT * FROM users WHERE id = 42");
    }

    #[test]
    fn substitute_null_param() {
        let params = vec![None];
        let types = vec![Some(Type::INT8)];
        let result =
            substitute_params("SELECT * FROM users WHERE id = $1", &params, &types).unwrap();
        assert_eq!(result, "SELECT * FROM users WHERE id = NULL");
    }

    #[test]
    fn substitute_string_param_escaping() {
        let params = vec![Some(Bytes::from_static(b"O'Brien"))];
        let types = vec![Some(Type::TEXT)];
        let result =
            substitute_params("SELECT * FROM users WHERE name = $1", &params, &types).unwrap();
        assert_eq!(result, "SELECT * FROM users WHERE name = 'O''Brien'");
    }

    #[test]
    fn substitute_multiple_params_correct_order() {
        let params = vec![
            Some(Bytes::from_static(b"hello")),
            Some(Bytes::from_static(b"42")),
        ];
        let types = vec![Some(Type::TEXT), Some(Type::INT4)];
        let result =
            substitute_params("INSERT INTO t (name, age) VALUES ($1, $2)", &params, &types)
                .unwrap();
        assert_eq!(result, "INSERT INTO t (name, age) VALUES ('hello', 42)");
    }

    #[test]
    fn safe_numeric_literals() {
        assert!(is_safe_numeric_literal("42"));
        assert!(is_safe_numeric_literal("-3.14"));
        assert!(is_safe_numeric_literal("1.5e10"));
        assert!(is_safe_numeric_literal("NaN"));
        assert!(is_safe_numeric_literal("Infinity"));
        assert!(!is_safe_numeric_literal(""));
        assert!(!is_safe_numeric_literal("42; DROP TABLE users"));
        assert!(!is_safe_numeric_literal("abc"));
    }

    #[test]
    fn format_boolean_params() {
        assert_eq!(format_param_value("t", &Type::BOOL), "TRUE");
        assert_eq!(format_param_value("f", &Type::BOOL), "FALSE");
        assert_eq!(format_param_value("true", &Type::BOOL), "TRUE");
        assert_eq!(format_param_value("false", &Type::BOOL), "FALSE");
    }

    #[test]
    fn substitute_ignores_placeholders_inside_literals_and_comments() {
        let params = vec![Some(Bytes::from_static(b"42"))];
        let types = vec![Some(Type::INT8)];
        let sql = "SELECT '$1' AS literal, col FROM t WHERE id = $1 -- $1\n/* $1 */";
        let result = substitute_params(sql, &params, &types).unwrap();
        assert_eq!(
            result,
            "SELECT '$1' AS literal, col FROM t WHERE id = 42 -- $1\n/* $1 */"
        );
    }
}
