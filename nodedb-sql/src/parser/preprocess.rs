//! SQL pre-processing: rewrite NodeDB-specific syntax into standard SQL
//! before handing to sqlparser-rs.
//!
//! Handles:
//! - `UPSERT INTO coll (cols) VALUES (vals)` → `INSERT INTO coll (cols) VALUES (vals)` + upsert flag
//! - `INSERT INTO coll { key: 'val', ... }` → `INSERT INTO coll (key) VALUES ('val')` + object literal flag
//! - `UPSERT INTO coll { key: 'val', ... }` → both rewrites combined

use super::object_literal::{parse_object_literal, parse_object_literal_array};

/// Result of pre-processing a SQL string.
pub struct PreprocessedSql {
    /// The rewritten SQL (standard SQL that sqlparser can handle).
    pub sql: String,
    /// Whether the original statement was UPSERT (not INSERT).
    pub is_upsert: bool,
}

/// Pre-process a SQL string, rewriting NodeDB-specific syntax.
///
/// Returns `None` if no rewriting was needed (pass through to sqlparser as-is).
pub fn preprocess(sql: &str) -> Option<PreprocessedSql> {
    let trimmed = sql.trim();
    let upper = trimmed.to_uppercase();

    // Check for UPSERT INTO.
    let is_upsert = upper.starts_with("UPSERT INTO ");

    if is_upsert {
        // Rewrite UPSERT INTO → INSERT INTO, then check for { } literal.
        let rewritten = format!("INSERT INTO {}", &trimmed["UPSERT INTO ".len()..]);
        if let Some(result) = try_rewrite_object_literal(&rewritten) {
            return Some(PreprocessedSql {
                sql: result,
                is_upsert: true,
            });
        }
        return Some(PreprocessedSql {
            sql: rewritten,
            is_upsert: true,
        });
    }

    // Check for INSERT INTO coll { ... } object literal syntax.
    if upper.starts_with("INSERT INTO ")
        && let Some(result) = try_rewrite_object_literal(trimmed)
    {
        return Some(PreprocessedSql {
            sql: result,
            is_upsert: false,
        });
    }

    // Apply expression-level rewrites: `<->` operator, `{ }` in function args.
    let mut sql_buf = trimmed.to_string();
    let mut any_rewrite = false;

    // Rewrite pgvector `<->` operator to `vector_distance()` function call.
    if sql_buf.contains("<->")
        && let Some(rewritten) = rewrite_arrow_distance(&sql_buf)
    {
        sql_buf = rewritten;
        any_rewrite = true;
    }

    // Rewrite `{ key: val }` inside function args to JSON string literals.
    // e.g., text_match(body, 'q', { fuzzy: true }) → text_match(body, 'q', '{"fuzzy":true}')
    if (sql_buf.contains("{ ") || sql_buf.contains("{f") || sql_buf.contains("{d"))
        && let Some(rewritten) = rewrite_object_literal_args(&sql_buf)
    {
        sql_buf = rewritten;
        any_rewrite = true;
    }

    if any_rewrite {
        return Some(PreprocessedSql {
            sql: sql_buf,
            is_upsert: false,
        });
    }

    None
}

/// Try to rewrite `INSERT INTO coll { ... }` or `INSERT INTO coll [{ ... }, { ... }]`
/// into standard `INSERT INTO coll (cols) VALUES (row1), (row2)`.
///
/// Returns `None` if the statement doesn't use object literal syntax.
fn try_rewrite_object_literal(sql: &str) -> Option<String> {
    // Find collection name after INSERT INTO.
    let after_into = sql["INSERT INTO ".len()..].trim_start();
    let coll_end = after_into.find(|c: char| c.is_whitespace())?;
    let coll_name = &after_into[..coll_end];
    let rest = after_into[coll_end..].trim_start();

    // Strip trailing semicolon before parsing.
    let obj_str = rest.trim_end_matches(';').trim_end();

    if obj_str.starts_with('[') {
        // Array form: INSERT INTO coll [{ ... }, { ... }]
        return rewrite_array_form(coll_name, obj_str);
    }

    if !obj_str.starts_with('{') {
        return None;
    }

    // Single object form: INSERT INTO coll { ... }
    let fields = parse_object_literal(obj_str)?.ok()?;
    if fields.is_empty() {
        return None;
    }
    Some(fields_to_values_sql(coll_name, &[fields]))
}

/// Rewrite `[{ ... }, { ... }]` → multi-row VALUES.
fn rewrite_array_form(coll_name: &str, obj_str: &str) -> Option<String> {
    let objects = parse_object_literal_array(obj_str)?.ok()?;
    if objects.is_empty() {
        return None;
    }
    Some(fields_to_values_sql(coll_name, &objects))
}

/// Build `INSERT INTO coll (col_union) VALUES (row1), (row2), ...`
///
/// Collects the union of all keys across all rows. Missing keys get NULL.
fn fields_to_values_sql(
    coll_name: &str,
    rows: &[std::collections::HashMap<String, nodedb_types::Value>],
) -> String {
    // Collect union of all keys, sorted for deterministic output.
    let mut all_keys: Vec<String> = rows
        .iter()
        .flat_map(|r| r.keys().cloned())
        .collect::<std::collections::BTreeSet<_>>()
        .into_iter()
        .collect();
    all_keys.sort();

    let col_list = all_keys.join(", ");

    let row_strs: Vec<String> = rows
        .iter()
        .map(|row| {
            let vals: Vec<String> = all_keys
                .iter()
                .map(|k| match row.get(k) {
                    Some(v) => value_to_sql_literal(v),
                    None => "NULL".to_string(),
                })
                .collect();
            format!("({})", vals.join(", "))
        })
        .collect();

    format!(
        "INSERT INTO {} ({}) VALUES {}",
        coll_name,
        col_list,
        row_strs.join(", ")
    )
}

/// Rewrite `{ key: val }` object literals inside function argument positions
/// to JSON string literals: `'{"key": val}'`.
///
/// Detects patterns like `func(arg1, arg2, { key: val })` and rewrites the
/// `{ }` to a single-quoted JSON string. Only rewrites `{ }` that appear
/// inside parentheses (function calls), not at statement level (INSERT).
fn rewrite_object_literal_args(sql: &str) -> Option<String> {
    let mut result = String::with_capacity(sql.len());
    let chars: Vec<char> = sql.chars().collect();
    let mut i = 0;
    let mut found = false;
    let mut paren_depth: i32 = 0;

    while i < chars.len() {
        match chars[i] {
            '(' => {
                paren_depth += 1;
                result.push('(');
                i += 1;
            }
            ')' => {
                paren_depth = paren_depth.saturating_sub(1);
                result.push(')');
                i += 1;
            }
            '\'' => {
                // Skip quoted strings entirely.
                result.push('\'');
                i += 1;
                while i < chars.len() {
                    result.push(chars[i]);
                    if chars[i] == '\'' {
                        // Handle escaped quotes ('').
                        if i + 1 < chars.len() && chars[i + 1] == '\'' {
                            i += 1;
                            result.push(chars[i]);
                        } else {
                            break;
                        }
                    }
                    i += 1;
                }
                i += 1;
            }
            '{' if paren_depth > 0 => {
                // Object literal inside function args — parse and convert to JSON string.
                let remaining: String = chars[i..].iter().collect();
                if let Some(Ok(fields)) = parse_object_literal(&remaining) {
                    // Find the end of the object literal to know how many chars to skip.
                    let obj_end = find_matching_brace(&chars, i);
                    if let Some(end) = obj_end {
                        let json = value_map_to_json(&fields);
                        result.push('\'');
                        result.push_str(&json);
                        result.push('\'');
                        i = end + 1;
                        found = true;
                        continue;
                    }
                }
                // Not a valid object literal — pass through.
                result.push('{');
                i += 1;
            }
            _ => {
                result.push(chars[i]);
                i += 1;
            }
        }
    }

    if found { Some(result) } else { None }
}

/// Convert a parsed field map to a JSON string without external serializer.
fn value_map_to_json(fields: &std::collections::HashMap<String, nodedb_types::Value>) -> String {
    let mut parts = Vec::with_capacity(fields.len());
    let mut entries: Vec<_> = fields.iter().collect();
    entries.sort_by_key(|(k, _)| k.as_str());
    for (key, val) in entries {
        parts.push(format!("\"{}\":{}", key, value_to_json(val)));
    }
    format!("{{{}}}", parts.join(","))
}

/// Convert a single `Value` to JSON text.
fn value_to_json(value: &nodedb_types::Value) -> String {
    match value {
        nodedb_types::Value::String(s) => {
            format!("\"{}\"", s.replace('\\', "\\\\").replace('"', "\\\""))
        }
        nodedb_types::Value::Integer(n) => n.to_string(),
        nodedb_types::Value::Float(f) => format!("{f}"),
        nodedb_types::Value::Bool(b) => if *b { "true" } else { "false" }.to_string(),
        nodedb_types::Value::Null => "null".to_string(),
        nodedb_types::Value::Array(items) => {
            let inner: Vec<String> = items.iter().map(value_to_json).collect();
            format!("[{}]", inner.join(","))
        }
        nodedb_types::Value::Object(map) => value_map_to_json(map),
        _ => format!("\"{}\"", format!("{value:?}").replace('"', "\\\"")),
    }
}

/// Find the index of the matching `}` for a `{` at position `start`.
fn find_matching_brace(chars: &[char], start: usize) -> Option<usize> {
    let mut depth = 0;
    let mut in_string = false;
    let mut i = start;
    while i < chars.len() {
        match chars[i] {
            '\'' if !in_string => in_string = true,
            '\'' if in_string => {
                if i + 1 < chars.len() && chars[i + 1] == '\'' {
                    // Skip '' escape — advance past both quotes.
                    i += 2;
                    continue;
                }
                in_string = false;
            }
            '{' if !in_string => depth += 1,
            '}' if !in_string => {
                depth -= 1;
                if depth == 0 {
                    return Some(i);
                }
            }
            _ => {}
        }
        i += 1;
    }
    None
}

/// Rewrite all occurrences of `expr <-> expr` to `vector_distance(expr, expr)`.
///
/// Handles: `column_name <-> ARRAY[...]`, `column <-> $param`, etc.
/// Returns `None` if no valid `<->` patterns are found.
fn rewrite_arrow_distance(sql: &str) -> Option<String> {
    let mut result = String::with_capacity(sql.len());
    let mut remaining = sql;
    let mut found = false;

    while let Some(arrow_pos) = remaining.find("<->") {
        // Extract left operand: walk backwards from `<->` to find the identifier/expression.
        let before = &remaining[..arrow_pos];
        let left = extract_left_operand(before)?;
        let left_start = arrow_pos - left.len();

        // Extract right operand: walk forward from after `<->`.
        let after = &remaining[arrow_pos + 3..];
        let (right, right_len) = extract_right_operand(after.trim_start())?;
        let ws_skip = after.len() - after.trim_start().len();

        // Build: everything before left_operand + vector_distance(left, right) + rest
        result.push_str(&remaining[..left_start]);
        result.push_str(&format!("vector_distance({left}, {right})"));
        remaining = &remaining[arrow_pos + 3 + ws_skip + right_len..];
        found = true;
    }

    if !found {
        return None;
    }

    result.push_str(remaining);
    Some(result)
}

/// Extract the left operand before `<->`: a column name or dotted path.
fn extract_left_operand(before: &str) -> Option<String> {
    let trimmed = before.trim_end();
    // Walk backwards to find the start of the identifier.
    let start = trimmed
        .rfind(|c: char| !c.is_ascii_alphanumeric() && c != '_' && c != '.')
        .map(|p| p + 1)
        .unwrap_or(0);
    let ident = &trimmed[start..];
    if ident.is_empty() {
        return None;
    }
    Some(ident.to_string())
}

/// Extract the right operand after `<->`: ARRAY[...], $param, or identifier.
/// Returns (operand_text, consumed_length).
fn extract_right_operand(after: &str) -> Option<(String, usize)> {
    let trimmed = after.trim_start();
    let upper = trimmed.to_uppercase();

    if upper.starts_with("ARRAY[") {
        // Find matching `]`.
        let mut depth = 0;
        for (i, c) in trimmed.char_indices() {
            match c {
                '[' => depth += 1,
                ']' => {
                    depth -= 1;
                    if depth == 0 {
                        return Some((trimmed[..=i].to_string(), i + 1));
                    }
                }
                _ => {}
            }
        }
        None // Unmatched bracket.
    } else if trimmed.starts_with('$') {
        // Parameter reference: $1, $query_vec, etc.
        let end = trimmed
            .find(|c: char| !c.is_ascii_alphanumeric() && c != '_' && c != '$')
            .unwrap_or(trimmed.len());
        Some((trimmed[..end].to_string(), end))
    } else {
        // Identifier: column name.
        let end = trimmed
            .find(|c: char| !c.is_ascii_alphanumeric() && c != '_' && c != '.')
            .unwrap_or(trimmed.len());
        if end == 0 {
            return None;
        }
        Some((trimmed[..end].to_string(), end))
    }
}

/// Convert a `nodedb_types::Value` to a SQL literal string.
///
/// Used by pre-processing and by Origin's pgwire handlers to build SQL
/// from parsed field maps. Handles all Value variants.
pub fn value_to_sql_literal(value: &nodedb_types::Value) -> String {
    match value {
        nodedb_types::Value::String(s) => format!("'{}'", s.replace('\'', "''")),
        nodedb_types::Value::Integer(n) => n.to_string(),
        nodedb_types::Value::Float(f) => format!("{f}"),
        nodedb_types::Value::Bool(b) => if *b { "TRUE" } else { "FALSE" }.to_string(),
        nodedb_types::Value::Null => "NULL".to_string(),
        nodedb_types::Value::Array(items) => {
            let inner: Vec<String> = items.iter().map(value_to_sql_literal).collect();
            format!("ARRAY[{}]", inner.join(", "))
        }
        nodedb_types::Value::Bytes(b) => {
            let hex: String = b.iter().map(|byte| format!("{byte:02x}")).collect();
            format!("'\\x{hex}'")
        }
        nodedb_types::Value::Object(_) => "NULL".to_string(),
        nodedb_types::Value::Uuid(u) => format!("'{u}'"),
        nodedb_types::Value::Ulid(u) => format!("'{u}'"),
        nodedb_types::Value::DateTime(dt) => format!("'{dt}'"),
        nodedb_types::Value::Duration(d) => format!("'{d}'"),
        nodedb_types::Value::Decimal(d) => d.to_string(),
        // Exotic types: format as string literal for SQL passthrough.
        other => format!("'{}'", format!("{other:?}").replace('\'', "''")),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn passthrough_standard_sql() {
        assert!(preprocess("SELECT * FROM users").is_none());
        assert!(preprocess("INSERT INTO users (name) VALUES ('alice')").is_none());
        assert!(preprocess("DELETE FROM users WHERE id = 1").is_none());
    }

    #[test]
    fn upsert_rewrite() {
        let result = preprocess("UPSERT INTO users (name) VALUES ('alice')").unwrap();
        assert!(result.is_upsert);
        assert_eq!(result.sql, "INSERT INTO users (name) VALUES ('alice')");
    }

    #[test]
    fn object_literal_insert() {
        let result = preprocess("INSERT INTO users { name: 'alice', age: 30 }").unwrap();
        assert!(!result.is_upsert);
        assert!(result.sql.starts_with("INSERT INTO users ("));
        assert!(result.sql.contains("'alice'"));
        assert!(result.sql.contains("30"));
    }

    #[test]
    fn object_literal_upsert() {
        let result = preprocess("UPSERT INTO users { name: 'bob' }").unwrap();
        assert!(result.is_upsert);
        assert!(result.sql.starts_with("INSERT INTO users ("));
        assert!(result.sql.contains("'bob'"));
    }

    #[test]
    fn batch_array_insert() {
        let result =
            preprocess("INSERT INTO users [{ name: 'alice', age: 30 }, { name: 'bob', age: 25 }]")
                .unwrap();
        assert!(!result.is_upsert);
        // Should produce multi-row VALUES: ... VALUES (...), (...)
        assert!(result.sql.contains("VALUES"));
        assert!(result.sql.contains("'alice'"));
        assert!(result.sql.contains("'bob'"));
        assert!(result.sql.contains("30"));
        assert!(result.sql.contains("25"));
        // Two row groups separated by comma
        let values_part = result.sql.split("VALUES").nth(1).unwrap();
        let row_count = values_part.matches('(').count();
        assert_eq!(row_count, 2, "should have 2 row groups: {}", result.sql);
    }

    #[test]
    fn batch_array_heterogeneous_keys() {
        let result =
            preprocess("INSERT INTO docs [{ id: 'a', name: 'Alice' }, { id: 'b', role: 'admin' }]")
                .unwrap();
        // Union of keys: id, name, role — missing keys get NULL.
        assert!(result.sql.contains("NULL"));
        assert!(result.sql.contains("'Alice'"));
        assert!(result.sql.contains("'admin'"));
    }

    #[test]
    fn batch_array_upsert() {
        let result =
            preprocess("UPSERT INTO users [{ id: 'u1', name: 'a' }, { id: 'u2', name: 'b' }]")
                .unwrap();
        assert!(result.is_upsert);
        assert!(result.sql.contains("VALUES"));
    }

    #[test]
    fn arrow_distance_operator_select() {
        let result = preprocess(
            "SELECT title FROM articles ORDER BY embedding <-> ARRAY[0.1, 0.2, 0.3] LIMIT 5",
        )
        .unwrap();
        assert!(
            result
                .sql
                .contains("vector_distance(embedding, ARRAY[0.1, 0.2, 0.3])"),
            "got: {}",
            result.sql
        );
        assert!(!result.sql.contains("<->"));
    }

    #[test]
    fn arrow_distance_operator_where() {
        let result =
            preprocess("SELECT * FROM docs WHERE embedding <-> ARRAY[1.0, 2.0] < 0.5").unwrap();
        assert!(
            result
                .sql
                .contains("vector_distance(embedding, ARRAY[1.0, 2.0])"),
            "got: {}",
            result.sql
        );
    }

    #[test]
    fn arrow_distance_no_match() {
        // No <-> in SQL — should return None.
        assert!(preprocess("SELECT * FROM users WHERE age > 30").is_none());
    }

    #[test]
    fn arrow_distance_with_alias() {
        let result =
            preprocess("SELECT embedding <-> ARRAY[0.1, 0.2] AS dist FROM articles").unwrap();
        assert!(
            result
                .sql
                .contains("vector_distance(embedding, ARRAY[0.1, 0.2]) AS dist"),
            "got: {}",
            result.sql
        );
    }

    #[test]
    fn fuzzy_object_literal_in_function() {
        // Test the rewriter directly first.
        let direct = rewrite_object_literal_args(
            "SELECT * FROM articles WHERE text_match(body, 'query', { fuzzy: true })",
        );
        assert!(direct.is_some(), "rewrite_object_literal_args should match");
        let rewritten = direct.unwrap();
        assert!(
            rewritten.contains("\"fuzzy\""),
            "direct rewrite should contain JSON, got: {}",
            rewritten
        );

        let result =
            preprocess("SELECT * FROM articles WHERE text_match(body, 'query', { fuzzy: true })")
                .unwrap();
        assert!(
            !result.sql.contains("{ fuzzy"),
            "should not contain object literal, got: {}",
            result.sql
        );
    }

    #[test]
    fn fuzzy_object_literal_with_distance() {
        let result = preprocess(
            "SELECT * FROM articles WHERE text_match(title, 'test', { fuzzy: true, distance: 2 })",
        )
        .unwrap();
        assert!(result.sql.contains("\"fuzzy\""), "got: {}", result.sql);
        assert!(result.sql.contains("\"distance\""), "got: {}", result.sql);
    }

    #[test]
    fn object_literal_not_rewritten_outside_function() {
        // INSERT { } should NOT be touched by the function-arg rewriter.
        // It goes through try_rewrite_object_literal instead.
        let result = preprocess("INSERT INTO docs { name: 'Alice' }").unwrap();
        // Should be VALUES, not a JSON string.
        assert!(result.sql.contains("VALUES"), "got: {}", result.sql);
    }
}
