//! Rewrite `{ key: val }` object literals appearing inside function-call
//! argument positions to JSON string literals: `'{"key": val}'`.

use crate::parser::object_literal::parse_object_literal;

/// Detect patterns like `func(arg1, arg2, { key: val })` and rewrite the
/// `{ }` to a single-quoted JSON string. Only rewrites `{ }` that appear
/// inside parentheses (function calls), not at statement level (INSERT).
pub(super) fn rewrite_object_literal_args(sql: &str) -> Option<String> {
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
                result.push('\'');
                i += 1;
                while i < chars.len() {
                    result.push(chars[i]);
                    if chars[i] == '\'' {
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
                let remaining: String = chars[i..].iter().collect();
                if let Some(Ok(fields)) = parse_object_literal(&remaining)
                    && let Some(end) = find_matching_brace(&chars, i)
                {
                    let json = value_map_to_json(&fields);
                    result.push('\'');
                    result.push_str(&json);
                    result.push('\'');
                    i = end + 1;
                    found = true;
                    continue;
                }
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
