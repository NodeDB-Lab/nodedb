//! SQL parsing helpers shared across DDL handlers.

/// Split VALUES content respecting quoted strings and brackets.
///
/// `'hello', 42, 'it''s'` → `["'hello'", "42", "'it''s'"]`
pub(super) fn split_values(s: &str) -> Vec<&str> {
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

/// Parse a SQL literal value to a `serde_json::Value`.
pub(super) fn parse_sql_value(val: &str) -> nodedb_types::Value {
    let trimmed = val.trim();
    let upper = trimmed.to_uppercase();
    if upper.starts_with("ARRAY[") && trimmed.ends_with(']') {
        let Some(start) = trimmed.find('[') else {
            return nodedb_types::Value::Null;
        };
        let inner = &trimmed[start + 1..trimmed.len() - 1];
        let items = if inner.trim().is_empty() {
            Vec::new()
        } else {
            split_values(inner)
                .into_iter()
                .map(parse_sql_value)
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
    if let Some(v) = try_eval_scalar_function(trimmed) {
        return v;
    }
    nodedb_types::Value::String(trimmed.to_string())
}

/// Evaluate a scalar function expression like `now()` or a bare SQL
/// keyword like `current_timestamp` via the shared `nodedb_query`
/// evaluator. Returns `None` if the input isn't a recognizable call
/// form or the function is unknown.
fn try_eval_scalar_function(s: &str) -> Option<nodedb_types::Value> {
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
            let val = nodedb_query::functions::eval_function(&name, &[]);
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
    let sql_expr = nodedb_sql::resolver::expr::convert_expr(&ast_expr).ok()?;
    let folded = nodedb_sql::planner::const_fold::fold_constant_default(&sql_expr)?;
    Some(sql_value_to_ndb_value(folded))
}

fn sql_value_to_ndb_value(v: nodedb_sql::types::SqlValue) -> nodedb_types::Value {
    use nodedb_sql::types::SqlValue;
    match v {
        SqlValue::Null => nodedb_types::Value::Null,
        SqlValue::Bool(b) => nodedb_types::Value::Bool(b),
        SqlValue::Int(i) => nodedb_types::Value::Integer(i),
        SqlValue::Float(f) => nodedb_types::Value::Float(f),
        SqlValue::String(s) => nodedb_types::Value::String(s),
        SqlValue::Bytes(b) => nodedb_types::Value::Bytes(b),
        SqlValue::Array(a) => {
            nodedb_types::Value::Array(a.into_iter().map(sql_value_to_ndb_value).collect())
        }
    }
}

/// Extract a clause value delimited by known keywords.
///
/// Given `upper = "TYPE INT DEFAULT 0 ASSERT $value > 0"`, `original` (same
/// text in original case), and `keyword = "TYPE"`, returns `Some("int")`.
/// The value spans from after the keyword to the next keyword or end of string.
///
/// `all_keywords` lists every keyword that can terminate the value.
pub(super) fn extract_clause(
    upper: &str,
    original: &str,
    keyword: &str,
    all_keywords: &[&str],
) -> Option<String> {
    let kw_with_space = format!("{keyword} ");
    let start = upper.find(&kw_with_space)?;
    let value_start = start + kw_with_space.len();

    let end = all_keywords
        .iter()
        .filter(|&&k| !k.eq_ignore_ascii_case(keyword))
        .filter_map(|k| {
            let needle = format!("{k} ");
            upper[value_start..]
                .find(&needle)
                .map(|pos| value_start + pos)
        })
        .min()
        .unwrap_or(original.len());

    let value = original[value_start..end].trim().to_string();
    if value.is_empty() { None } else { Some(value) }
}

/// Extract a collection name after a SQL keyword marker.
///
/// Given `sql = "SHOW CHANGES FOR users SINCE ..."` and `marker = " FOR "`,
/// returns `Some("users")`. Returns `None` if the marker is missing or
/// the collection name is empty.
pub(crate) fn extract_collection_after(sql: &str, marker: &str) -> Option<String> {
    let upper = sql.to_uppercase();
    let pos = upper.find(marker)?;
    let after = sql[pos + marker.len()..].trim();
    let name = after.split_whitespace().next()?.to_lowercase();
    if name.is_empty() { None } else { Some(name) }
}

/// Parse a timestamp from a SINCE clause.
///
/// Accepts ISO 8601 datetime strings or raw milliseconds.
/// Returns an error with a descriptive message for invalid formats.
pub(super) fn parse_since_timestamp(input: &str) -> crate::Result<u64> {
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

#[cfg(test)]
mod tests {
    use super::parse_sql_value;

    #[test]
    fn parse_sql_value_decodes_numeric_array_literals() {
        let value = parse_sql_value("ARRAY[1.0, 2, 3.5]");

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
        let value = parse_sql_value("ARRAY['rust', ARRAY[1, 2]]");

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
}
