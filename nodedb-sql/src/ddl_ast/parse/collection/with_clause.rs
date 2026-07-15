// SPDX-License-Identifier: Apache-2.0

//! Parse the `WITH (...)` clause and `BALANCED ON (...)` clause in CREATE COLLECTION.

use crate::parser::preprocess::lex::{
    find_ascii_case_insensitive, keyword_position_outside_literals,
};

/// Extract engine name and other key-value options from the `WITH (...)` clause.
///
/// Returns `(engine, other_options)` where `engine` is the value of the
/// `engine=` key (lowercased) and `other_options` is all other k=v pairs.
pub(super) fn extract_with_options(body: &str) -> (Option<String>, Vec<(String, String)>) {
    let with_pos = match keyword_position_outside_literals(body, "WITH") {
        Some(p) => p,
        None => return (None, Vec::new()),
    };

    let after_with = body[with_pos..].trim_start();
    // Skip "WITH" keyword.
    let after_with = &after_with["WITH".len()..].trim_start();
    if !after_with.starts_with('(') {
        return (None, Vec::new());
    }

    // Find the matching close paren for the WITH clause.
    let mut depth = 0usize;
    let mut end = None;
    for (i, b) in after_with.bytes().enumerate() {
        match b {
            b'(' => depth += 1,
            b')' => {
                depth -= 1;
                if depth == 0 {
                    end = Some(i);
                    break;
                }
            }
            _ => {}
        }
    }
    let end = match end {
        Some(e) => e,
        None => return (None, Vec::new()),
    };

    let inner = &after_with[1..end];
    let pairs = parse_with_kvs(inner);

    let mut engine: Option<String> = None;
    let mut other: Vec<(String, String)> = Vec::new();
    for (k, v) in pairs {
        if k.eq_ignore_ascii_case("engine") {
            engine = Some(v.to_lowercase());
        } else {
            other.push((k.to_lowercase(), v));
        }
    }
    (engine, other)
}

/// Split the interior of `WITH (...)` into `(key, value)` pairs.
/// Values may be quoted with `'` or `"`. Multi-value (ARRAY[...]) not supported here.
fn parse_with_kvs(inner: &str) -> Vec<(String, String)> {
    let mut pairs: Vec<(String, String)> = Vec::new();
    let mut depth = 0usize;
    let mut start = 0usize;

    for (i, c) in inner.char_indices() {
        match c {
            '(' | '[' => depth += 1,
            ')' | ']' => {
                depth = depth.saturating_sub(1);
            }
            ',' if depth == 0 => {
                let token = inner[start..i].trim();
                if let Some(pair) = parse_kv_token(token) {
                    pairs.push(pair);
                }
                start = i + 1;
            }
            _ => {}
        }
    }
    let last = inner[start..].trim();
    if !last.is_empty()
        && let Some(pair) = parse_kv_token(last)
    {
        pairs.push(pair);
    }
    pairs
}

/// Parse `key = 'value'`, `key = value`, or `key = ['a', 'b', ...]` token into
/// `(key, value)`.  For array-style values `[...]` the brackets are stripped so
/// callers receive the raw comma-separated interior (e.g. `'category', 'score'`).
fn parse_kv_token(token: &str) -> Option<(String, String)> {
    let eq_pos = token.find('=')?;
    let key = token[..eq_pos].trim().to_string();
    let val_raw = token[eq_pos + 1..].trim();

    // Array literal: strip outer `[` … `]` and pass the interior as the value.
    if val_raw.starts_with('[') {
        let inner = val_raw
            .strip_prefix('[')
            .and_then(|s| s.strip_suffix(']'))
            .unwrap_or(val_raw)
            .trim();
        return Some((key, inner.to_string()));
    }

    let val = val_raw.trim_start_matches('\'').trim_start_matches('"');
    let end = val
        .find('\'')
        .or_else(|| val.find('"'))
        .unwrap_or(val.len());
    let value = val[..end].trim().to_string();
    Some((key, value))
}

/// Extract the raw inner text of a `BALANCED ON (group_key = col, ...)` clause.
///
/// Returns `None` when the clause is absent. The handler calls
/// `parse_balanced_clause_from_raw` with this string.
pub(super) fn extract_balanced_raw(body: &str) -> Option<String> {
    let bal_pos = find_ascii_case_insensitive(body, "BALANCED ON")?;
    let after = body[bal_pos + "BALANCED ON".len()..].trim_start();
    if !after.starts_with('(') {
        return None;
    }
    let mut depth = 0usize;
    let mut end = None;
    for (i, b) in after.bytes().enumerate() {
        match b {
            b'(' => depth += 1,
            b')' => {
                depth -= 1;
                if depth == 0 {
                    end = Some(i);
                    break;
                }
            }
            _ => {}
        }
    }
    let end = end?;
    Some(after[1..end].trim().to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn with_clause_after_unicode_text_preserves_original_offsets() {
        let (engine, options) = extract_with_options("(name ﬀﬀ) WITH (engine = 'kv', ttl = 60)");
        assert_eq!(engine.as_deref(), Some("kv"));
        assert_eq!(options, vec![("ttl".to_string(), "60".to_string())]);
    }

    #[test]
    fn balanced_clause_after_unicode_text_preserves_original_offsets() {
        let body = "(name ﬀﬀ) BALANCED ON (group_key = tenant_id)";
        assert_eq!(
            extract_balanced_raw(body),
            Some("group_key = tenant_id".to_string())
        );
    }
}
