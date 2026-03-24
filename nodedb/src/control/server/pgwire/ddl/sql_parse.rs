//! SQL parsing helpers shared across DDL handlers.

/// Parse an `ARRAY[0.1, 0.2, 0.3]` literal into `Vec<f32>`.
/// Also handles quoted form: `'ARRAY[0.1, 0.2, 0.3]'`.
pub(super) fn parse_array_literal(val: &str) -> Option<Vec<f32>> {
    let trimmed = val.trim().trim_matches('\'');
    let upper = trimmed.to_uppercase();
    if !upper.starts_with("ARRAY[") {
        return None;
    }
    let start = trimmed.find('[')? + 1;
    let end = trimmed.rfind(']')?;
    if end <= start {
        return None;
    }
    let inner = &trimmed[start..end];
    let values: Vec<f32> = inner
        .split(',')
        .filter_map(|s| s.trim().parse::<f32>().ok())
        .collect();
    if values.is_empty() {
        return None;
    }
    Some(values)
}

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
pub(super) fn parse_sql_value(val: &str) -> serde_json::Value {
    let trimmed = val.trim();
    if trimmed.eq_ignore_ascii_case("NULL") {
        return serde_json::Value::Null;
    }
    if trimmed.eq_ignore_ascii_case("TRUE") {
        return serde_json::Value::Bool(true);
    }
    if trimmed.eq_ignore_ascii_case("FALSE") {
        return serde_json::Value::Bool(false);
    }
    if trimmed.starts_with('\'') && trimmed.ends_with('\'') {
        let inner = &trimmed[1..trimmed.len() - 1];
        let unescaped = inner.replace("''", "'");
        return serde_json::Value::String(unescaped);
    }
    if let Ok(i) = trimmed.parse::<i64>() {
        return serde_json::json!(i);
    }
    if let Ok(f) = trimmed.parse::<f64>() {
        return serde_json::json!(f);
    }
    serde_json::Value::String(trimmed.to_string())
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
pub(super) fn extract_collection_after(sql: &str, marker: &str) -> Option<String> {
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
