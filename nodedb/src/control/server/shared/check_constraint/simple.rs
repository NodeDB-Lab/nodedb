// SPDX-License-Identifier: BUSL-1.1

//! Simple (no-subquery) CHECK constraint evaluation + `NEW.field` substitution
//! helpers shared with the subquery evaluator.

use std::collections::HashMap;

use crate::control::security::catalog::types::CheckConstraintDef;
use crate::control::server::shared::ddl::result::DdlError;
use nodedb_sql::parser::preprocess::lex::find_ascii_case_insensitive_from;

use super::enforce::ddl_err;

/// Evaluate a simple CHECK constraint (no subquery) using the `SqlExpr` evaluator.
///
/// Strips `NEW.` prefixes so `NEW.amount > 0` becomes `amount > 0`, then
/// evaluates against the document fields directly.
pub(super) fn enforce_simple_check(
    constraint: &CheckConstraintDef,
    fields: &HashMap<String, nodedb_types::Value>,
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

    // Evaluate the expression against the document.
    let result = expr.eval(&doc);

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

/// Substitute `NEW.field` references in the CHECK expression with literal SQL values.
///
/// Handles: `NEW.field_name` → `'value'` (strings), `123` (ints), `1.5` (floats),
/// `TRUE`/`FALSE` (bools), `NULL` (null/absent).
pub(super) fn substitute_new_refs(
    sql: &str,
    fields: &HashMap<String, nodedb_types::Value>,
) -> String {
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
    replace_remaining_new_refs(&result)
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
    if pattern.is_empty() {
        return text.to_string();
    }

    let mut result = String::with_capacity(text.len());
    let mut search_from = 0;
    let mut copied_until = 0;
    while let Some(start) = if pattern.is_ascii() {
        find_ascii_case_insensitive_from(text, pattern, search_from)
    } else {
        text[search_from..]
            .find(pattern)
            .map(|position| search_from + position)
    } {
        let end = start + pattern.len();
        search_from = end;

        // Verify word boundary: the char before must not be alphanumeric/underscore.
        if start > 0 {
            let prev = text.as_bytes()[start - 1];
            if prev.is_ascii_alphanumeric() || prev == b'_' {
                continue;
            }
        }
        // The char after must not be alphanumeric/underscore.
        if end < text.len() {
            let next = text.as_bytes()[end];
            if next.is_ascii_alphanumeric() || next == b'_' {
                continue;
            }
        }

        result.push_str(&text[copied_until..start]);
        result.push_str(replacement);
        copied_until = end;
    }
    result.push_str(&text[copied_until..]);
    result
}

/// Convert a `Value` to a SQL literal string for interpolation.
pub(super) fn value_to_sql_literal(val: &nodedb_types::Value) -> String {
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
        nodedb_types::Value::DateTime(dt) | nodedb_types::Value::NaiveDateTime(dt) => {
            format!("'{dt}'")
        }
        _ => "NULL".to_string(),
    }
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
    fn substitute_new_refs_after_expanding_unicode_preserves_original_offsets() {
        let mut fields = HashMap::new();
        fields.insert("id".to_string(), nodedb_types::Value::Integer(7));

        let result = substitute_new_refs("'ﬀﬀ' = 'ﬀﬀ' AND NEW.id = 7", &fields);
        assert_eq!(result, "'ﬀﬀ' = 'ﬀﬀ' AND 7 = 7");
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
