//! SQL-level PREPARE / EXECUTE / DEALLOCATE handling.
//!
//! These are explicit SQL DDL statements — separate from the wire-level
//! Parse/Bind/Execute messages handled by the ExtendedQueryHandler trait.
//!
//! Syntax:
//!   PREPARE name [(type, ...)] AS query
//!   EXECUTE name [(value, ...)]
//!   DEALLOCATE name
//!   DEALLOCATE ALL

use pgwire::api::results::{Response, Tag};
use pgwire::error::{ErrorInfo, PgWireError, PgWireResult};

use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::server::pgwire::session::prepared_cache::SqlPreparedStatement;

use super::core::NodeDbPgHandler;

impl NodeDbPgHandler {
    /// Handle `PREPARE name [(type, ...)] AS query`.
    pub(super) fn handle_prepare(
        &self,
        addr: &std::net::SocketAddr,
        sql: &str,
    ) -> PgWireResult<Vec<Response>> {
        let (name, param_type_names, body_sql) = parse_prepare_statement(sql)?;

        self.sessions
            .prepare_sql_statement(
                addr,
                name,
                SqlPreparedStatement {
                    sql: body_sql,
                    param_type_names,
                },
            )
            .map_err(|e| {
                PgWireError::UserError(Box::new(ErrorInfo::new(
                    "ERROR".to_owned(),
                    "53000".to_owned(),
                    e.to_string(),
                )))
            })?;

        Ok(vec![Response::Execution(Tag::new("PREPARE"))])
    }

    /// Handle `EXECUTE name [(value, ...)]`.
    ///
    /// Retrieves the prepared statement, substitutes parameter values into
    /// the SQL body, and executes through the standard pipeline.
    ///
    /// Uses `Box::pin` because this creates async recursion:
    /// `execute_sql` → `handle_execute` → `execute_sql` (with the substituted body).
    /// The substituted SQL is the PREPARE body (e.g., a SELECT), not another EXECUTE,
    /// so the recursion terminates in one level.
    pub(super) fn handle_execute<'a>(
        &'a self,
        identity: &'a AuthenticatedIdentity,
        addr: &'a std::net::SocketAddr,
        sql: &'a str,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = PgWireResult<Vec<Response>>> + Send + 'a>>
    {
        Box::pin(async move {
            let (name, param_values) = parse_execute_statement(sql)?;

            let stmt = self.sessions.get_sql_prepared(addr, &name).ok_or_else(|| {
                PgWireError::UserError(Box::new(ErrorInfo::new(
                    "ERROR".to_owned(),
                    "26000".to_owned(),
                    format!("prepared statement \"{name}\" does not exist"),
                )))
            })?;

            // Validate parameter count.
            let expected_count = count_placeholders(&stmt.sql);
            if !param_values.is_empty() && param_values.len() != expected_count {
                return Err(PgWireError::UserError(Box::new(ErrorInfo::new(
                    "ERROR".to_owned(),
                    "08P01".to_owned(),
                    format!(
                        "wrong number of parameters for prepared statement \"{name}\": \
                         expected {expected_count}, got {}",
                        param_values.len()
                    ),
                ))));
            }

            // Substitute parameters into the SQL body.
            let final_sql = substitute_sql_params(&stmt.sql, &param_values);

            // Execute through the standard pipeline. The substituted SQL is the
            // PREPARE body (e.g., SELECT/INSERT), never another EXECUTE statement,
            // so this does not recurse further.
            self.execute_sql(identity, addr, &final_sql).await
        })
    }

    /// Handle `DEALLOCATE name` or `DEALLOCATE ALL`.
    pub(super) fn handle_deallocate(
        &self,
        addr: &std::net::SocketAddr,
        sql: &str,
    ) -> PgWireResult<Vec<Response>> {
        let trimmed = sql.trim();
        let rest = trimmed[11..].trim(); // skip "DEALLOCATE "

        // DEALLOCATE ALL
        if rest.eq_ignore_ascii_case("ALL") {
            self.sessions.deallocate_all_sql_prepared(addr);
            return Ok(vec![Response::Execution(Tag::new("DEALLOCATE ALL"))]);
        }

        // DEALLOCATE PREPARE name (PG compatibility — PREPARE keyword is optional).
        let name = if rest.to_uppercase().starts_with("PREPARE ") {
            rest[8..].trim()
        } else {
            rest
        };

        // Strip quotes if present.
        let name = name.trim_matches('"');

        if !self.sessions.deallocate_sql_prepared(addr, name) {
            return Err(PgWireError::UserError(Box::new(ErrorInfo::new(
                "ERROR".to_owned(),
                "26000".to_owned(),
                format!("prepared statement \"{name}\" does not exist"),
            ))));
        }

        Ok(vec![Response::Execution(Tag::new("DEALLOCATE"))])
    }
}

/// Parse `PREPARE name [(type, ...)] AS query`.
///
/// Returns (name, param_type_names, body_sql).
fn parse_prepare_statement(sql: &str) -> PgWireResult<(String, Vec<String>, String)> {
    let trimmed = sql.trim();
    let rest = trimmed[8..].trim(); // skip "PREPARE "

    // Find the AS keyword (case-insensitive).
    let upper_rest = rest.to_uppercase();
    let as_pos = upper_rest.find(" AS ").ok_or_else(|| {
        PgWireError::UserError(Box::new(ErrorInfo::new(
            "ERROR".to_owned(),
            "42601".to_owned(),
            "syntax error: PREPARE name [(type, ...)] AS query".to_owned(),
        )))
    })?;

    let before_as = rest[..as_pos].trim();
    let body_sql = rest[as_pos + 4..].trim().to_string();

    if body_sql.is_empty() {
        return Err(PgWireError::UserError(Box::new(ErrorInfo::new(
            "ERROR".to_owned(),
            "42601".to_owned(),
            "PREPARE statement body is empty".to_owned(),
        ))));
    }

    // Parse name and optional type list: "name(type1, type2)" or "name (type1, type2)" or "name".
    let (name, param_type_names) = if let Some(paren_pos) = before_as.find('(') {
        let name = before_as[..paren_pos].trim().to_lowercase();
        let types_str = before_as[paren_pos + 1..].trim_end_matches(')').trim();
        let type_names: Vec<String> = if types_str.is_empty() {
            Vec::new()
        } else {
            types_str
                .split(',')
                .map(|t| t.trim().to_uppercase())
                .collect()
        };
        (name, type_names)
    } else {
        (before_as.to_lowercase(), Vec::new())
    };

    if name.is_empty() {
        return Err(PgWireError::UserError(Box::new(ErrorInfo::new(
            "ERROR".to_owned(),
            "42601".to_owned(),
            "PREPARE statement name is empty".to_owned(),
        ))));
    }

    Ok((name, param_type_names, body_sql))
}

/// Parse `EXECUTE name [(value, ...)]`.
///
/// Returns (name, param_values). Values are raw strings — they will be
/// substituted into the SQL body as literals.
fn parse_execute_statement(sql: &str) -> PgWireResult<(String, Vec<String>)> {
    let trimmed = sql.trim();
    let rest = trimmed[8..].trim(); // skip "EXECUTE "

    // Check for parenthesized parameters.
    if let Some(paren_pos) = rest.find('(') {
        let name = rest[..paren_pos].trim().to_lowercase();
        let params_str = rest[paren_pos + 1..]
            .trim_end_matches(')')
            .trim_end_matches(';')
            .trim();

        let values = if params_str.is_empty() {
            Vec::new()
        } else {
            parse_value_list(params_str)
        };

        Ok((name, values))
    } else {
        // No parameters.
        let name = rest.trim_end_matches(';').trim().to_lowercase();
        Ok((name, Vec::new()))
    }
}

/// Parse a comma-separated value list, respecting single-quoted strings.
fn parse_value_list(s: &str) -> Vec<String> {
    let mut values = Vec::new();
    let mut current = String::new();
    let mut in_quote = false;

    for ch in s.chars() {
        match ch {
            '\'' if !in_quote => {
                in_quote = true;
                // Don't include the quote in the value.
            }
            '\'' if in_quote => {
                in_quote = false;
            }
            ',' if !in_quote => {
                values.push(current.trim().to_string());
                current.clear();
            }
            _ => {
                current.push(ch);
            }
        }
    }

    let last = current.trim().to_string();
    if !last.is_empty() {
        values.push(last);
    }

    values
}

/// Count the number of `$N` placeholders in SQL.
fn count_placeholders(sql: &str) -> usize {
    let mut max_idx = 0usize;
    for (_, _, idx) in placeholder_ranges(sql) {
        if idx > max_idx {
            max_idx = idx;
        }
    }
    max_idx
}

/// Substitute `$1`, `$2`, ... in SQL with literal values.
///
/// Values are properly quoted as SQL string literals to prevent injection.
fn substitute_sql_params(sql: &str, values: &[String]) -> String {
    if values.is_empty() {
        return sql.to_owned();
    }

    let replacements: Vec<String> = values
        .iter()
        .map(|value| {
            if value.eq_ignore_ascii_case("NULL") {
                "NULL".to_string()
            } else if is_numeric_or_bool(value) {
                value.to_string()
            } else {
                // Quote as string literal with escaping.
                let escaped = value.replace('\'', "''");
                format!("'{escaped}'")
            }
        })
        .collect();

    rewrite_sql_placeholders(sql, &replacements)
}

/// Check if a value looks like a number or boolean (safe to use unquoted).
fn is_numeric_or_bool(s: &str) -> bool {
    let lower = s.to_lowercase();
    if matches!(lower.as_str(), "true" | "false") {
        return true;
    }
    // Try parsing as a number.
    s.parse::<f64>().is_ok()
}

use super::prepared::sql_placeholder::{placeholder_ranges, rewrite_sql_placeholders};

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_prepare_simple() {
        let (name, types, body) =
            parse_prepare_statement("PREPARE get_user AS SELECT * FROM users WHERE id = $1")
                .unwrap();
        assert_eq!(name, "get_user");
        assert!(types.is_empty());
        assert_eq!(body, "SELECT * FROM users WHERE id = $1");
    }

    #[test]
    fn parse_prepare_with_types() {
        let (name, types, body) = parse_prepare_statement(
            "PREPARE get_user(BIGINT, TEXT) AS SELECT * FROM users WHERE id = $1 AND name = $2",
        )
        .unwrap();
        assert_eq!(name, "get_user");
        assert_eq!(types, vec!["BIGINT", "TEXT"]);
        assert_eq!(body, "SELECT * FROM users WHERE id = $1 AND name = $2");
    }

    #[test]
    fn parse_execute_no_params() {
        let (name, values) = parse_execute_statement("EXECUTE get_user").unwrap();
        assert_eq!(name, "get_user");
        assert!(values.is_empty());
    }

    #[test]
    fn parse_execute_with_params() {
        let (name, values) = parse_execute_statement("EXECUTE get_user(42, 'alice')").unwrap();
        assert_eq!(name, "get_user");
        assert_eq!(values, vec!["42", "alice"]);
    }

    #[test]
    fn count_placeholders_basic() {
        assert_eq!(count_placeholders("SELECT $1, $2, $3"), 3);
        assert_eq!(count_placeholders("SELECT 1"), 0);
        assert_eq!(count_placeholders("WHERE id = $1 AND name = $1"), 1);
        assert_eq!(count_placeholders("$10 $2"), 10);
        assert_eq!(count_placeholders("SELECT '$1', $2 -- $3"), 2);
    }

    #[test]
    fn substitute_params() {
        let sql = "SELECT * FROM users WHERE id = $1 AND name = $2";
        let values = vec!["42".to_string(), "alice".to_string()];
        let result = substitute_sql_params(sql, &values);
        assert_eq!(
            result,
            "SELECT * FROM users WHERE id = 42 AND name = 'alice'"
        );
    }

    #[test]
    fn substitute_null_value() {
        let sql = "INSERT INTO t (a) VALUES ($1)";
        let values = vec!["NULL".to_string()];
        let result = substitute_sql_params(sql, &values);
        assert_eq!(result, "INSERT INTO t (a) VALUES (NULL)");
    }

    #[test]
    fn value_list_with_quotes() {
        let values = parse_value_list("42, 'hello world', true");
        assert_eq!(values, vec!["42", "hello world", "true"]);
    }

    #[test]
    fn substitute_params_skips_literals_and_comments() {
        let sql = "SELECT '$1' AS literal, col FROM t WHERE id = $1 -- $1";
        let values = vec!["42".to_string()];
        let result = substitute_sql_params(sql, &values);
        assert_eq!(
            result,
            "SELECT '$1' AS literal, col FROM t WHERE id = 42 -- $1"
        );
    }
}
