//! Control Plane enforcement for general CHECK constraints.
//!
//! General CHECK constraints may contain subqueries (`SELECT ...`) and `NEW.field`
//! references. They are evaluated on the Control Plane before writes are dispatched
//! to the Data Plane via SPSC.
//!
//! For each CHECK constraint:
//! 1. Substitute `NEW.field` references with literal values from the document
//! 2. Build `SELECT CASE WHEN (expr) THEN 1 ELSE 0 END`
//! 3. Plan + dispatch + read the result
//! 4. If the result is 0, reject the write

use std::collections::HashMap;

use pgwire::error::PgWireResult;

use crate::control::security::catalog::types::CheckConstraintDef;
use crate::control::state::SharedState;

/// Evaluate all general CHECK constraints for a document being written.
///
/// Returns `Ok(())` if all constraints pass, or a pgwire error with the
/// constraint name and expression on failure.
///
/// Two evaluation paths:
/// - **Simple CHECK** (no subquery): strip `NEW.` prefixes, parse into `SqlExpr`,
///   evaluate directly against the document — same evaluator as typeguard CHECK.
/// - **Subquery CHECK**: substitute `NEW.field` with literal SQL values, plan and
///   dispatch a `SELECT` query, check the result.
pub async fn enforce_check_constraints(
    state: &SharedState,
    tenant_id: nodedb_types::TenantId,
    constraints: &[CheckConstraintDef],
    fields: &HashMap<String, nodedb_types::Value>,
) -> PgWireResult<()> {
    for constraint in constraints {
        if constraint.has_subquery {
            enforce_subquery_check(state, tenant_id, constraint, fields).await?;
        } else {
            enforce_simple_check(constraint, fields)?;
        }
    }

    Ok(())
}

/// Evaluate a simple CHECK constraint (no subquery) using the `SqlExpr` evaluator.
///
/// Strips `NEW.` prefixes so `NEW.amount > 0` becomes `amount > 0`, then
/// evaluates against the document fields directly.
fn enforce_simple_check(
    constraint: &CheckConstraintDef,
    fields: &HashMap<String, nodedb_types::Value>,
) -> PgWireResult<()> {
    // Strip NEW. prefixes to get bare column references.
    let bare_expr = strip_new_prefix(&constraint.check_sql);

    // Parse into SqlExpr using the shared expression parser.
    let (expr, _deps) =
        nodedb_query::expr_parse::parse_generated_expr(&bare_expr).map_err(|e| {
            pgwire_err(
                "23514",
                &format!(
                    "CHECK constraint '{}' failed to parse: {}",
                    constraint.name, e
                ),
            )
        })?;

    // Build a Value::Object from the fields for evaluation.
    let doc = nodedb_types::Value::Object(fields.clone());

    // Evaluate the expression against the document.
    let result = expr.eval(&doc);

    // NULL passes CHECK (SQL semantics: NULL is not FALSE).
    match result {
        nodedb_types::Value::Bool(true) => Ok(()),
        nodedb_types::Value::Null => Ok(()),
        nodedb_types::Value::Integer(n) if n != 0 => Ok(()),
        _ => Err(pgwire_err(
            "23514",
            &format!(
                "CHECK constraint '{}' violated: {}",
                constraint.name, constraint.check_sql
            ),
        )),
    }
}

/// Evaluate a CHECK constraint with subqueries via SQL planning and dispatch.
async fn enforce_subquery_check(
    state: &SharedState,
    tenant_id: nodedb_types::TenantId,
    constraint: &CheckConstraintDef,
    fields: &HashMap<String, nodedb_types::Value>,
) -> PgWireResult<()> {
    let substituted = substitute_new_refs(&constraint.check_sql, fields);

    // Restructure the subquery CHECK into an executable SQL query.
    // Pattern: `val IN (SELECT col FROM tbl ...)` → `SELECT COUNT(*) AS cnt FROM tbl WHERE col = val ...`
    // General fallback: wrap in subselect.
    let restructured = restructure_subquery_check(&substituted);

    let query_ctx = crate::control::planner::context::QueryContext::for_state(state);

    let tasks = match query_ctx.plan_sql(&restructured.sql, tenant_id).await {
        Ok(t) => t,
        Err(e) => {
            return Err(pgwire_err(
                "23514",
                &format!(
                    "CHECK constraint '{}' failed to evaluate: {}",
                    constraint.name, e
                ),
            ));
        }
    };

    let mut passed = false;
    for task in tasks {
        let resp = crate::control::server::dispatch_utils::dispatch_to_data_plane(
            state,
            tenant_id,
            task.vshard_id,
            task.plan,
            0,
        )
        .await;

        match resp {
            Ok(response) => {
                let json = crate::data::executor::response_codec::decode_payload_to_json(
                    &response.payload,
                );
                if !json.is_empty() && check_count_is_positive(&json) {
                    passed = true;
                }
            }
            Err(e) => {
                return Err(pgwire_err(
                    "23514",
                    &format!(
                        "CHECK constraint '{}' failed to evaluate: {}",
                        constraint.name, e
                    ),
                ));
            }
        }
    }

    // For NOT IN: negate — count > 0 means constraint violated.
    // For IN: count > 0 means constraint passed.
    let constraint_ok = if restructured.negate { !passed } else { passed };

    if !constraint_ok {
        return Err(pgwire_err(
            "23514",
            &format!(
                "CHECK constraint '{}' violated: {}",
                constraint.name, constraint.check_sql
            ),
        ));
    }

    Ok(())
}

/// Check if a COUNT(*) JSON response indicates a positive count.
///
/// Response format is typically `{"cnt":N}` or `[{"cnt":N}]`.
fn check_count_is_positive(json: &str) -> bool {
    // Parse as JSON to reliably check the count value.
    if let Ok(v) = serde_json::from_str::<serde_json::Value>(json) {
        // Check for {"cnt": N} or [{"cnt": N}]
        let obj = if let Some(arr) = v.as_array() {
            arr.first().and_then(|r| r.as_object())
        } else {
            v.as_object()
        };
        if let Some(obj) = obj {
            for (_, val) in obj {
                if let Some(n) = val.as_i64() {
                    return n > 0;
                }
                if let Some(n) = val.as_f64() {
                    return n > 0.0;
                }
            }
        }
    }
    // Empty array or unparseable — constraint failed (no matching rows).
    false
}

/// Result of restructuring a subquery CHECK expression.
struct RestructuredCheck {
    /// The SQL query to execute.
    sql: String,
    /// If true, a positive COUNT means the constraint is VIOLATED (NOT IN case).
    negate: bool,
}

/// Restructure a subquery CHECK expression into an executable SQL query.
///
/// Handles:
/// - `'val' IN (SELECT col FROM tbl ...)` → COUNT > 0 means pass
/// - `'val' NOT IN (SELECT col FROM tbl ...)` → COUNT = 0 means pass
fn restructure_subquery_check(expr: &str) -> RestructuredCheck {
    let upper = expr.to_uppercase();

    // Detect NOT IN vs IN.
    let (in_pos, negate) = if let Some(pos) = upper.find(" NOT IN (SELECT ") {
        (pos, true)
    } else if let Some(pos) = upper.find(" NOT IN(SELECT ") {
        (pos, true)
    } else if let Some(pos) = upper.find(" IN (SELECT ") {
        (pos, false)
    } else if let Some(pos) = upper.find(" IN(SELECT ") {
        (pos, false)
    } else {
        // Should not reach here — validated at DDL time.
        return RestructuredCheck {
            sql: format!("SELECT ({expr}) AS _check"),
            negate: false,
        };
    };

    let value_part = expr[..in_pos].trim();
    let keyword_len = if negate { " NOT IN (" } else { " IN (" };
    let select_part = &expr[in_pos + keyword_len.len()..];
    let inner = select_part.trim().trim_end_matches(')').trim();

    if let Some(from_pos) = inner.to_uppercase().find(" FROM ") {
        let col = inner["SELECT ".len()..from_pos].trim();
        let after_from = &inner[from_pos + 6..];
        let (table, existing_where) = if let Some(w) = after_from.to_uppercase().find(" WHERE ") {
            (&after_from[..w], Some(&after_from[w + 7..]))
        } else {
            (after_from.trim(), None)
        };

        let sql = if let Some(where_clause) = existing_where {
            format!(
                "SELECT COUNT(*) AS cnt FROM {} WHERE {} = {} AND {}",
                table.trim(),
                col,
                value_part,
                where_clause
            )
        } else {
            format!(
                "SELECT COUNT(*) AS cnt FROM {} WHERE {} = {}",
                table.trim(),
                col,
                value_part
            )
        };

        return RestructuredCheck { sql, negate };
    }

    RestructuredCheck {
        sql: format!("SELECT ({expr}) AS _check"),
        negate: false,
    }
}

/// Strip `NEW.` prefix from field references (case-insensitive).
///
/// Converts `NEW.amount > 0` → `amount > 0` so the expression can be parsed
/// as bare column references by `parse_generated_expr`.
fn strip_new_prefix(sql: &str) -> String {
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

/// Substitute `NEW.field` references in the CHECK expression with literal SQL values.
///
/// Handles: `NEW.field_name` → `'value'` (strings), `123` (ints), `1.5` (floats),
/// `TRUE`/`FALSE` (bools), `NULL` (null/absent).
fn substitute_new_refs(sql: &str, fields: &HashMap<String, nodedb_types::Value>) -> String {
    let mut result = sql.to_string();

    // Find all NEW.xxx patterns and replace with literal values.
    // We iterate from longest field names first to avoid partial matches.
    let mut field_names: Vec<&String> = fields.keys().collect();
    field_names.sort_by_key(|b| std::cmp::Reverse(b.len()));

    for field_name in field_names {
        let pattern_upper = format!("NEW.{}", field_name.to_uppercase());
        let pattern_lower = format!("NEW.{}", field_name.to_lowercase());
        let pattern_orig = format!("NEW.{field_name}");
        let literal = value_to_sql_literal(&fields[field_name]);

        // Case-insensitive replacement: try original case, uppercase, lowercase.
        result = replace_case_insensitive(&result, &pattern_orig, &literal);
        if pattern_orig != pattern_upper {
            result = replace_case_insensitive(&result, &pattern_upper, &literal);
        }
        if pattern_orig != pattern_lower {
            result = replace_case_insensitive(&result, &pattern_lower, &literal);
        }
    }

    // Replace any remaining NEW.xxx that aren't in fields with NULL.
    result = replace_remaining_new_refs(&result);

    result
}

/// Replace any remaining `NEW.xxx` references (not matched by known fields) with NULL.
fn replace_remaining_new_refs(text: &str) -> String {
    let chars: Vec<char> = text.chars().collect();
    let mut result = String::with_capacity(text.len());
    let mut i = 0;

    while i < chars.len() {
        // Check for "NEW." prefix (case insensitive).
        if i + 4 <= chars.len() {
            let window: String = chars[i..i + 4].iter().collect();
            if window.eq_ignore_ascii_case("NEW.") {
                if i > 0 && (chars[i - 1].is_ascii_alphanumeric() || chars[i - 1] == '_') {
                    result.push(chars[i]);
                    i += 1;
                    continue;
                }
                // Find the end of the identifier after "NEW.".
                let start = i + 4;
                let mut end = start;
                while end < chars.len() && (chars[end].is_ascii_alphanumeric() || chars[end] == '_')
                {
                    end += 1;
                }
                if end > start {
                    result.push_str("NULL");
                    i = end;
                    continue;
                }
            }
        }
        result.push(chars[i]);
        i += 1;
    }
    result
}

/// Replace all occurrences of `pattern` in `text` case-insensitively.
fn replace_case_insensitive(text: &str, pattern: &str, replacement: &str) -> String {
    let upper_text = text.to_uppercase();
    let upper_pattern = pattern.to_uppercase();
    let mut result = String::with_capacity(text.len());
    let mut last_end = 0;

    for (start, _) in upper_text.match_indices(&upper_pattern) {
        // Verify word boundary: the char before must not be alphanumeric/underscore.
        if start > 0 {
            let prev = text.as_bytes()[start - 1];
            if prev.is_ascii_alphanumeric() || prev == b'_' {
                continue;
            }
        }
        // The char after must not be alphanumeric/underscore.
        let end = start + pattern.len();
        if end < text.len() {
            let next = text.as_bytes()[end];
            if next.is_ascii_alphanumeric() || next == b'_' {
                continue;
            }
        }

        result.push_str(&text[last_end..start]);
        result.push_str(replacement);
        last_end = end;
    }
    result.push_str(&text[last_end..]);
    result
}

/// Convert a `Value` to a SQL literal string for interpolation.
fn value_to_sql_literal(val: &nodedb_types::Value) -> String {
    match val {
        nodedb_types::Value::Null => "NULL".to_string(),
        nodedb_types::Value::Bool(b) => if *b { "TRUE" } else { "FALSE" }.to_string(),
        nodedb_types::Value::Integer(i) => i.to_string(),
        nodedb_types::Value::Float(f) => format!("{f}"),
        nodedb_types::Value::String(s) => {
            // Escape single quotes for SQL safety.
            let escaped = s.replace('\'', "''");
            format!("'{escaped}'")
        }
        nodedb_types::Value::DateTime(dt) => format!("'{dt}'"),
        _ => "NULL".to_string(),
    }
}

fn pgwire_err(code: &str, msg: &str) -> pgwire::error::PgWireError {
    pgwire::error::PgWireError::UserError(Box::new(pgwire::error::ErrorInfo::new(
        "ERROR".to_owned(),
        code.to_owned(),
        msg.to_owned(),
    )))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn substitute_new_refs_basic() {
        let mut fields = HashMap::new();
        fields.insert(
            "email".to_string(),
            nodedb_types::Value::String("alice@example.com".into()),
        );
        fields.insert("age".to_string(), nodedb_types::Value::Integer(25));

        let sql = "NEW.email LIKE '%@%.%' AND NEW.age >= 18";
        let result = substitute_new_refs(sql, &fields);
        assert_eq!(result, "'alice@example.com' LIKE '%@%.%' AND 25 >= 18");
    }

    #[test]
    fn substitute_new_refs_case_insensitive() {
        let mut fields = HashMap::new();
        fields.insert(
            "name".to_string(),
            nodedb_types::Value::String("Bob".into()),
        );

        let sql = "new.name IS NOT NULL";
        let result = substitute_new_refs(sql, &fields);
        assert_eq!(result, "'Bob' IS NOT NULL");
    }

    #[test]
    fn substitute_new_refs_missing_field() {
        let fields = HashMap::new();
        let sql = "NEW.unknown_field IS NOT NULL";
        let result = substitute_new_refs(sql, &fields);
        assert_eq!(result, "NULL IS NOT NULL");
    }

    #[test]
    fn substitute_new_refs_with_subquery() {
        let mut fields = HashMap::new();
        fields.insert(
            "email".to_string(),
            nodedb_types::Value::String("test@x.com".into()),
        );
        fields.insert("id".to_string(), nodedb_types::Value::String("u1".into()));

        let sql = "NEW.email NOT IN (SELECT email FROM users WHERE id != NEW.id)";
        let result = substitute_new_refs(sql, &fields);
        assert_eq!(
            result,
            "'test@x.com' NOT IN (SELECT email FROM users WHERE id != 'u1')"
        );
    }

    #[test]
    fn value_to_sql_literal_escapes_quotes() {
        let val = nodedb_types::Value::String("it's a test".into());
        assert_eq!(value_to_sql_literal(&val), "'it''s a test'");
    }

    #[test]
    fn value_to_sql_literal_types() {
        assert_eq!(value_to_sql_literal(&nodedb_types::Value::Null), "NULL");
        assert_eq!(
            value_to_sql_literal(&nodedb_types::Value::Bool(true)),
            "TRUE"
        );
        assert_eq!(
            value_to_sql_literal(&nodedb_types::Value::Integer(42)),
            "42"
        );
        assert_eq!(
            value_to_sql_literal(&nodedb_types::Value::Float(3.5)),
            "3.5"
        );
    }
}
