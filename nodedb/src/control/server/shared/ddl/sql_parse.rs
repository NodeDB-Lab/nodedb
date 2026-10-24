// SPDX-License-Identifier: BUSL-1.1

//! SQL parsing helpers shared across DDL handlers.

use nodedb_sql::parser::preprocess::lex::{
    find_ascii_case_insensitive, find_ascii_case_insensitive_from,
};

use super::result::DdlError;

/// Split VALUES content respecting quoted strings and brackets.
///
/// `'hello', 42, 'it''s'` → `["'hello'", "42", "'it''s'"]`
pub(crate) fn split_values(s: &str) -> Vec<&str> {
    let mut results = Vec::new();
    let mut start = 0;
    let mut in_quote = false;
    let mut bracket_depth: i32 = 0;
    let bytes = s.as_bytes();

    for i in 0..bytes.len() {
        match bytes[i] {
            b'\'' if bracket_depth == 0 => in_quote = !in_quote,
            b'[' | b'(' if !in_quote => bracket_depth += 1,
            b']' | b')' if !in_quote => bracket_depth = (bracket_depth - 1).max(0),
            b',' if !in_quote && bracket_depth == 0 => {
                results.push(&s[start..i]);
                start = i + 1;
            }
            _ => {}
        }
    }
    if start < s.len() {
        results.push(&s[start..]);
    }
    results
}

/// Split the row list after `VALUES` into the inner text of each tuple.
///
/// `('a', 1), ('b', f(2))` → `["'a', 1", "'b', f(2)"]`. Parentheses inside a
/// quoted string or a nested call are part of their tuple. Text between
/// tuples other than whitespace and one comma is an error, and so is text
/// after the last tuple.
pub(crate) fn split_value_tuples(s: &str) -> Result<Vec<&str>, String> {
    let bytes = s.as_bytes();
    let mut tuples = Vec::new();
    let mut i = 0;
    let mut expect_comma = false;
    while i < bytes.len() {
        match bytes[i] {
            b if b.is_ascii_whitespace() => i += 1,
            b',' if expect_comma => {
                expect_comma = false;
                i += 1;
            }
            b'(' if !expect_comma => {
                let close = tuple_close(bytes, i).ok_or_else(|| {
                    format!("missing closing ) for VALUES row {}", tuples.len() + 1)
                })?;
                tuples.push(&s[i + 1..close]);
                expect_comma = true;
                i = close + 1;
            }
            _ => {
                return Err(format!(
                    "unexpected text in VALUES list at '{}'",
                    s[i..].chars().take(24).collect::<String>()
                ));
            }
        }
    }
    if tuples.is_empty() {
        return Err("missing VALUES (...)".into());
    }
    if !expect_comma {
        return Err("VALUES list ends with a comma".into());
    }
    Ok(tuples)
}

/// The index of the `)` that closes the `(` at `open`, skipping quoted
/// strings (`''` is an escaped quote) and nested parentheses.
fn tuple_close(bytes: &[u8], open: usize) -> Option<usize> {
    let mut depth = 0usize;
    let mut in_quote = false;
    let mut i = open;
    while i < bytes.len() {
        match bytes[i] {
            b'\'' => in_quote = !in_quote,
            b'(' if !in_quote => depth += 1,
            b')' if !in_quote => {
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

/// Parse a SQL literal value to a `serde_json::Value`.
pub(crate) fn parse_sql_value(val: &str, crs: &nodedb_crs::CrsContext<'_>) -> nodedb_types::Value {
    let trimmed = val.trim();
    if nodedb_types::starts_with_ascii_case_insensitive(trimmed, "ARRAY[") && trimmed.ends_with(']')
    {
        let inner = trimmed
            .get("ARRAY[".len()..trimmed.len().saturating_sub(1))
            .unwrap_or_default();
        let items = if inner.trim().is_empty() {
            Vec::new()
        } else {
            split_values(inner)
                .into_iter()
                .map(|item| parse_sql_value(item, crs))
                .collect()
        };
        return nodedb_types::Value::Array(items);
    }
    if trimmed.eq_ignore_ascii_case("NULL") {
        return nodedb_types::Value::Null;
    }
    if trimmed.eq_ignore_ascii_case("TRUE") {
        return nodedb_types::Value::Bool(true);
    }
    if trimmed.eq_ignore_ascii_case("FALSE") {
        return nodedb_types::Value::Bool(false);
    }
    if trimmed.starts_with('\'') && trimmed.ends_with('\'') {
        let inner = &trimmed[1..trimmed.len() - 1];
        let unescaped = inner.replace("''", "'");
        return nodedb_types::Value::String(unescaped);
    }
    if let Ok(i) = trimmed.parse::<i64>() {
        return nodedb_types::Value::Integer(i);
    }
    if let Ok(f) = trimmed.parse::<f64>() {
        return nodedb_types::Value::Float(f);
    }
    // Scalar function call like `now()` or `date_add(now(), '1h')`, or a
    // bare identifier like `current_timestamp` that SQL treats as a
    // zero-arg function. Route through the shared evaluator so the
    // UPSERT fast-path stays aligned with the SQL planner's VALUES path.
    // Unknown names fall through to the legacy string behavior.
    if let Some(v) = try_eval_scalar_function(trimmed, crs) {
        return v;
    }
    nodedb_types::Value::String(trimmed.to_string())
}

/// Evaluate a scalar function expression like `now()` or a bare SQL
/// keyword like `current_timestamp` via the shared `nodedb_query`
/// evaluator. Returns `None` if the input isn't a recognizable call
/// form or the function is unknown.
fn try_eval_scalar_function(
    s: &str,
    crs: &nodedb_crs::CrsContext<'_>,
) -> Option<nodedb_types::Value> {
    // Bare identifier: SQL treats `current_timestamp`, `current_date`,
    // etc. as zero-arg function references without parentheses.
    let is_bare_ident = s.chars().all(|c| c.is_ascii_alphanumeric() || c == '_')
        && !s.is_empty()
        && !s.chars().next().is_some_and(|c| c.is_ascii_digit());

    if is_bare_ident {
        let name = s.to_lowercase();
        // Only fold if the registry knows this name. Gate via nodedb-sql's
        // registry so we don't accidentally evaluate user identifiers.
        let registry = nodedb_sql::planner::const_fold::default_registry();
        if registry.lookup(&name).is_some() {
            // Zero-arg call: `math::try_eval`'s zero-modulus arm (the only
            // fallible arm in the scalar-function table) cannot be reached
            // with an empty argument list, so folding the
            // `Result` to `Value::Null` on error is unreachable in practice,
            // not a silent swallow.
            let val = nodedb_query::functions::eval_function(&name, &[], crs)
                .unwrap_or(nodedb_types::Value::Null);
            if !matches!(val, nodedb_types::Value::Null) {
                return Some(val);
            }
        }
        return None;
    }

    // Call form `name(args...)`. Parse via sqlparser + fold via const_fold.
    if !s.ends_with(')') || !s.contains('(') {
        return None;
    }
    let stmt_sql = format!("SELECT {s}");
    let dialect = sqlparser::dialect::PostgreSqlDialect {};
    // reconstructed-sql: parser-only validates one constant-expression AST without execution
    let stmts = sqlparser::parser::Parser::parse_sql(&dialect, &stmt_sql).ok()?;
    let stmt = stmts.into_iter().next()?;
    let sqlparser::ast::Statement::Query(query) = stmt else {
        return None;
    };
    let sqlparser::ast::SetExpr::Select(select) = *query.body else {
        return None;
    };
    let item = select.projection.into_iter().next()?;
    let ast_expr = match item {
        sqlparser::ast::SelectItem::UnnamedExpr(e)
        | sqlparser::ast::SelectItem::ExprWithAlias { expr: e, .. } => e,
        _ => return None,
    };
    // A DDL constant expression has no FROM clause, so no identifier is
    // checkable against a relation here.
    let scope = nodedb_sql::resolver::ColumnScope::Unchecked;
    let sql_expr = nodedb_sql::resolver::expr::convert_expr(&ast_expr, &scope).ok()?;
    let folded = nodedb_sql::planner::const_fold::fold_constant_default(&sql_expr, crs).ok()??;
    Some(sql_value_to_ndb_value(folded))
}

fn sql_value_to_ndb_value(v: nodedb_sql::types::SqlValue) -> nodedb_types::Value {
    use nodedb_sql::types::SqlValue;
    match v {
        SqlValue::Null => nodedb_types::Value::Null,
        SqlValue::Bool(b) => nodedb_types::Value::Bool(b),
        SqlValue::Int(i) => nodedb_types::Value::Integer(i),
        SqlValue::Float(f) => nodedb_types::Value::Float(f),
        SqlValue::Decimal(d) => nodedb_types::Value::Decimal(d),
        SqlValue::String(s) => nodedb_types::Value::String(s),
        SqlValue::Bytes(b) => nodedb_types::Value::Bytes(b),
        SqlValue::Array(a) => {
            nodedb_types::Value::Array(a.into_iter().map(sql_value_to_ndb_value).collect())
        }
        SqlValue::Timestamp(dt) => nodedb_types::Value::NaiveDateTime(dt),
        SqlValue::Timestamptz(dt) => nodedb_types::Value::DateTime(dt),
        SqlValue::Geometry(g) => nodedb_types::Value::Geometry(g),
    }
}

/// Extract a clause value delimited by known keywords.
///
/// Given `upper = "TYPE INT DEFAULT 0 ASSERT $value > 0"`, `original` (same
/// text in original case), and `keyword = "TYPE"`, returns `Some("int")`.
/// The value spans from after the keyword to the next keyword or end of string.
///
/// `all_keywords` lists every keyword that can terminate the value.
pub(crate) fn extract_clause(
    _upper: &str,
    original: &str,
    keyword: &str,
    all_keywords: &[&str],
) -> Option<String> {
    let kw_with_space = format!("{keyword} ");
    let start = find_ascii_case_insensitive(original, &kw_with_space)?;
    let value_start = start + kw_with_space.len();

    let end = all_keywords
        .iter()
        .filter(|&&k| !k.eq_ignore_ascii_case(keyword))
        .filter_map(|k| {
            let needle = format!("{k} ");
            find_ascii_case_insensitive_from(original, &needle, value_start)
        })
        .min()
        .unwrap_or(original.len());

    let value = original[value_start..end].trim().to_string();
    if value.is_empty() { None } else { Some(value) }
}

/// Extract the raw collection token after a SQL keyword marker.
///
/// Given `sql = "SHOW CHANGES FOR users SINCE ..."` and `marker = " FOR "`,
/// returns `Some("users")`. Returns `None` when the marker is missing or no
/// token follows it.
///
/// The token keeps its quoting. Each caller decodes it through
/// [`parse_ident_token`], or through `nodedb_sql::reserved::check_identifier`
/// when it reports a different error type.
pub(crate) fn extract_collection_token_after<'a>(sql: &'a str, marker: &str) -> Option<&'a str> {
    let pos = find_ascii_case_insensitive(sql, marker)?;
    sql[pos + marker.len()..].split_whitespace().next()
}

/// Parse a timestamp from a SINCE clause.
///
/// Accepts ISO 8601 datetime strings or raw milliseconds.
/// Returns an error with a descriptive message for invalid formats.
pub(crate) fn parse_since_timestamp(input: &str) -> crate::Result<u64> {
    // Try ISO 8601 first.
    if let Some(dt) = nodedb_types::NdbDateTime::parse(input) {
        return Ok(dt.unix_millis() as u64);
    }
    // Fall back to raw u64 milliseconds.
    input.parse::<u64>().map_err(|_| crate::Error::BadRequest {
        detail: format!(
            "invalid SINCE format: '{input}'. Expected ISO 8601 datetime or milliseconds"
        ),
    })
}

/// Resolve a raw statement token to the stored identifier it names.
///
/// Handlers that read a name straight out of a whitespace-split statement get
/// the token with its double quotes still attached. `check_identifier` decodes
/// it the way the DDL parser front end does: a quoted token keeps its case, a
/// bare token lowercases, and a malformed or reserved token is rejected.
///
/// The same function guards `CREATE COLLECTION`, so a name it rejects here can
/// name no stored object.
pub fn parse_ident_token(token: &str) -> Result<String, DdlError> {
    nodedb_sql::reserved::check_identifier(token)
        .map_err(|error| DdlError::new("42602", error.to_string()))
}

/// Resolve a raw token that names an identifier or the `*` wildcard.
///
/// A clause meaning "every column" or "every collection" carries `*` where an
/// identifier otherwise sits. Only the clauses whose grammar accepts the
/// wildcard call this; every other slot stays on [`parse_ident_token`].
pub fn parse_ident_or_wildcard_token(token: &str) -> Result<String, DdlError> {
    nodedb_sql::reserved::check_identifier_or_wildcard(token)
        .map_err(|error| DdlError::new("42602", error.to_string()))
}

/// Resolve a raw token that names a relation, which can be a system catalog
/// relation such as `_system.audit_log` or `pg_catalog.pg_class`.
///
/// Only a statement whose target can be a catalog relation calls this. A
/// statement that addresses user collections alone stays on
/// [`parse_ident_token`], so a qualified name there is still rejected.
pub fn parse_relation_token(token: &str) -> Result<String, DdlError> {
    nodedb_sql::parser::normalize::check_relation_token(token)
        .map_err(|error| DdlError::new("42602", error.to_string()))
}

/// Resolve a raw stream token, keeping the `topic:` buffer-key prefix.
///
/// A consumer group names either a change stream or a topic. The `topic:`
/// prefix is a storage key, not part of the identifier, so it passes through
/// while the name after it goes through [`parse_ident_token`].
pub fn parse_stream_ident_token(token: &str) -> Result<String, DdlError> {
    match token.strip_prefix("topic:") {
        Some(topic) => Ok(format!("topic:{}", parse_ident_token(topic)?)),
        None => parse_ident_token(token),
    }
}

/// Decode a hex string into bytes.
///
/// Returns `None` if the input has an odd number of characters or contains
/// characters that are not valid hexadecimal digits.
pub fn hex_decode(s: &str) -> Option<Vec<u8>> {
    if !s.len().is_multiple_of(2) {
        return None;
    }
    let mut bytes = Vec::with_capacity(s.len() / 2);
    for i in (0..s.len()).step_by(2) {
        let byte = u8::from_str_radix(&s[i..i + 2], 16).ok()?;
        bytes.push(byte);
    }
    Some(bytes)
}

#[cfg(test)]
mod tests {
    use super::{
        extract_clause, parse_ident_token, parse_sql_value, parse_stream_ident_token,
        split_value_tuples,
    };

    #[test]
    fn value_tuples_split_on_top_level_parentheses() {
        let tuples = split_value_tuples("('a', 1), ('b)(', f(2, 3)) ,('it''s', ARRAY[1])")
            .expect("three rows");
        assert_eq!(
            tuples,
            vec!["'a', 1", "'b)(', f(2, 3)", "'it''s', ARRAY[1]"]
        );
    }

    #[test]
    fn malformed_value_tuples_are_refused() {
        for input in ["", "('a'), ", "('a') ('b')", "('a'", "('a') ON CONFLICT"] {
            assert!(split_value_tuples(input).is_err(), "{input}");
        }
    }

    #[test]
    fn bare_ident_token_lowercases() {
        assert_eq!(parse_ident_token("MiXeD").expect("bare"), "mixed");
        assert_eq!(parse_ident_token("USERS").expect("bare"), "users");
    }

    #[test]
    fn bare_lowercase_ident_token_is_unchanged() {
        assert_eq!(parse_ident_token("users").expect("bare"), "users");
    }

    #[test]
    fn quoted_ident_token_preserves_case() {
        assert_eq!(parse_ident_token("\"MiXeD\"").expect("quoted"), "MiXeD");
        assert_eq!(
            parse_ident_token("\"MiXeD 雪\"").expect("quoted"),
            "MiXeD 雪"
        );
    }

    #[test]
    fn quoted_lowercase_ident_token_is_unchanged() {
        assert_eq!(parse_ident_token("\"users\"").expect("quoted"), "users");
    }

    #[test]
    fn malformed_ident_tokens_are_rejected() {
        // A name `check_identifier` rejects can name no stored object, so the
        // handler reports it instead of looking it up.
        for token in ["\"unterminated", "\"a\"\"b\"", "\"\"", "\"", "name;drop"] {
            let error = parse_ident_token(token).expect_err(token);
            assert_eq!(error.sqlstate, "42602", "{token}");
        }
    }

    #[test]
    fn reserved_bare_ident_token_is_rejected() {
        // `CREATE COLLECTION match` is rejected by the same check, so no
        // collection named `match` exists to look up.
        assert!(parse_ident_token("match").is_err());
        assert_eq!(parse_ident_token("\"MATCH\"").expect("quoted"), "MATCH");
    }

    #[test]
    fn stream_ident_token_keeps_topic_prefix() {
        assert_eq!(
            parse_stream_ident_token("topic:MiXeD").expect("bare topic"),
            "topic:mixed"
        );
        assert_eq!(
            parse_stream_ident_token("topic:\"MiXeD\"").expect("quoted topic"),
            "topic:MiXeD"
        );
        assert_eq!(
            parse_stream_ident_token("\"MiXeD\"").expect("change stream"),
            "MiXeD"
        );
    }

    #[test]
    fn parse_sql_value_decodes_numeric_array_literals() {
        let value = parse_sql_value(
            "ARRAY[1.0, 2, 3.5]",
            &nodedb_crs::CrsContext::builtin_only(),
        );

        assert_eq!(
            value,
            nodedb_types::Value::Array(vec![
                nodedb_types::Value::Float(1.0),
                nodedb_types::Value::Integer(2),
                nodedb_types::Value::Float(3.5),
            ])
        );
    }

    #[test]
    fn parse_sql_value_decodes_nested_arrays_and_strings() {
        let value = parse_sql_value(
            "ARRAY['rust', ARRAY[1, 2]]",
            &nodedb_crs::CrsContext::builtin_only(),
        );

        assert_eq!(
            value,
            nodedb_types::Value::Array(vec![
                nodedb_types::Value::String("rust".into()),
                nodedb_types::Value::Array(vec![
                    nodedb_types::Value::Integer(1),
                    nodedb_types::Value::Integer(2),
                ]),
            ])
        );
    }

    #[test]
    fn extract_clause_with_unicode_value_preserves_original_offsets() {
        let original = "TYPE ﬀﬀ DEFAULT 0 ASSERT $value > 0";
        let upper = original.to_uppercase();
        assert_eq!(
            extract_clause(&upper, original, "TYPE", &["TYPE", "DEFAULT", "ASSERT"]),
            Some("ﬀﬀ".to_string())
        );
    }
}
