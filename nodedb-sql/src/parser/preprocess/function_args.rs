// SPDX-License-Identifier: Apache-2.0

//! Rewrite `{ key: val }` object literals appearing inside function-call
//! argument positions to JSON string literals: `'{"key": val}'`.

use super::lex::{SqlSegment, segments};
use crate::parser::object_literal::parse_object_literal;

/// Detect patterns like `func(arg1, arg2, { key: val })` and rewrite the
/// `{ }` to a single-quoted JSON string. Only rewrites `{ }` that appear
/// inside parentheses (function calls), not at statement level (INSERT).
pub(super) fn rewrite_object_literal_args(sql: &str) -> Option<String> {
    let mut result = String::with_capacity(sql.len());
    let mut found = false;
    let mut paren_depth: i32 = 0;

    for seg in segments(sql) {
        match seg {
            // Non-text segments (string literals, quoted idents, comments) are
            // passed through opaquely — never scanned for `{`.
            SqlSegment::SingleQuotedString(s)
            | SqlSegment::QuotedIdent(s)
            | SqlSegment::LineComment(s)
            | SqlSegment::BlockComment(s) => {
                result.push_str(s);
            }
            SqlSegment::Text(t) => {
                // Walk the text segment character-by-character so we can track
                // paren depth and detect `{` only inside function calls.
                let chars: Vec<char> = t.chars().collect();
                let mut i = 0;
                let mut seg_result = String::with_capacity(t.len());

                while i < chars.len() {
                    match chars[i] {
                        '(' => {
                            paren_depth += 1;
                            seg_result.push('(');
                            i += 1;
                        }
                        ')' => {
                            paren_depth = paren_depth.saturating_sub(1);
                            seg_result.push(')');
                            i += 1;
                        }
                        '{' if paren_depth > 0 => {
                            // Reconstruct the remaining text from position `i`
                            // in this segment and attempt to parse an object
                            // literal.
                            let remaining: String = chars[i..].iter().collect();
                            if let Some(Ok(fields)) = parse_object_literal(&remaining)
                                && let Some(end) = find_matching_brace(&chars, i)
                            {
                                let json = value_map_to_json(&fields);
                                seg_result.push_str(&nodedb_types::sql_quote::quote_literal(&json));
                                i = end + 1;
                                found = true;
                                continue;
                            }
                            seg_result.push('{');
                            i += 1;
                        }
                        c => {
                            seg_result.push(c);
                            i += 1;
                        }
                    }
                }

                result.push_str(&seg_result);
            }
        }
    }

    if found { Some(result) } else { None }
}

/// Convert a parsed field map to a JSON string without external serializer.
pub(super) fn value_map_to_json(
    fields: &std::collections::HashMap<String, nodedb_types::Value>,
) -> String {
    let mut parts = Vec::with_capacity(fields.len());
    let mut entries: Vec<_> = fields.iter().collect();
    entries.sort_by_key(|(k, _)| k.as_str());
    for (key, val) in entries {
        parts.push(format!("{}:{}", json_string(key), value_to_json(val)));
    }
    format!("{{{}}}", parts.join(","))
}

/// Convert a single `Value` to JSON text.
fn value_to_json(value: &nodedb_types::Value) -> String {
    match value {
        nodedb_types::Value::String(s) => json_string(s),
        nodedb_types::Value::Integer(n) => n.to_string(),
        nodedb_types::Value::Float(f) => {
            if f.is_finite() {
                format!("{f}")
            } else {
                // JSON has no representation for NaN / ±inf; serialize as
                // `null` to keep the output parseable.
                "null".to_string()
            }
        }
        nodedb_types::Value::Bool(b) => if *b { "true" } else { "false" }.to_string(),
        nodedb_types::Value::Null => "null".to_string(),
        nodedb_types::Value::Array(items) => {
            let inner: Vec<String> = items.iter().map(value_to_json).collect();
            format!("[{}]", inner.join(","))
        }
        nodedb_types::Value::Object(map) => value_map_to_json(map),
        // A typed scalar is its canonical text, a decimal its exact digits.
        nodedb_types::Value::Uuid(u) | nodedb_types::Value::Ulid(u) => json_string(u),
        nodedb_types::Value::Decimal(d) => json_string(&d.to_string()),
        nodedb_types::Value::DateTime(at) | nodedb_types::Value::NaiveDateTime(at) => {
            json_string(&at.to_iso8601())
        }
        nodedb_types::Value::Bytes(b) => {
            json_string(&nodedb_types::bytea_text::format_bytea_hex(b))
        }
        _ => json_string(&format!("{value:?}")),
    }
}

/// `text` as a JSON string, with quotes, backslashes and control
/// characters escaped.
fn json_string(text: &str) -> String {
    let mut out = String::with_capacity(text.len() + 2);
    out.push('"');
    for c in text.chars() {
        match c {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            c if u32::from(c) < 0x20 => out.push_str(&format!("\\u{:04x}", u32::from(c))),
            c => out.push(c),
        }
    }
    out.push('"');
    out
}

/// Find the index of the matching `}` for a `{` at position `start` within
/// `chars`. Operates only on the characters supplied; string literals inside
/// braces are handled by the outer `segments()` pass (the brace matcher here
/// only runs on `Text` segments, which contain no quoted literals by
/// construction).
fn find_matching_brace(chars: &[char], start: usize) -> Option<usize> {
    let mut depth = 0;
    let mut i = start;
    while i < chars.len() {
        match chars[i] {
            '{' => depth += 1,
            '}' => {
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn string_literal_with_brace_not_rewritten() {
        // `{ foo }` lives inside a string literal — must pass through unchanged.
        let sql = "SELECT func('{ foo }')";
        assert!(rewrite_object_literal_args(sql).is_none());
    }

    #[test]
    fn object_literal_in_function_rewritten() {
        let sql = "SELECT * FROM articles WHERE text_match(body, 'query', { fuzzy: true })";
        let result = rewrite_object_literal_args(sql).unwrap();
        assert!(result.contains("\"fuzzy\""), "got: {result}");
        assert!(!result.contains("{ fuzzy"), "got: {result}");
    }
}
