// SPDX-License-Identifier: Apache-2.0

//! Shared `WITH (key=value, ...)` extraction for database DDL.

use crate::parser::preprocess::lex::find_ascii_keyword;

/// Extract `WITH (key=value, ...)` pairs from a raw SQL string.
/// Returns an empty vec if no WITH clause is present.
pub(super) fn parse_with_options(sql: &str) -> Vec<(String, String)> {
    let with_start = match find_ascii_keyword(sql, "WITH") {
        Some(i) => i,
        None => return Vec::new(),
    };
    let after = &sql[with_start + 4..];
    let paren_start = match after.find('(') {
        Some(i) => i,
        None => return Vec::new(),
    };
    let inner = &after[paren_start + 1..];
    let paren_end = match inner.find(')') {
        Some(i) => i,
        None => return Vec::new(),
    };
    let inner = &inner[..paren_end];
    inner
        .split(',')
        .filter_map(|pair| {
            let mut it = pair.splitn(2, '=');
            let k = it.next()?.trim().to_string();
            let v = it
                .next()
                .map(|v| v.trim().trim_matches('\'').trim_matches('"').to_string())
                .unwrap_or_default();
            if k.is_empty() { None } else { Some((k, v)) }
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn with_clause_after_unicode_database_name_preserves_original_offsets() {
        assert_eq!(
            parse_with_options("CREATE DATABASE tenantﬀﬀ WITH (region='eu')"),
            vec![("region".to_string(), "eu".to_string())]
        );
    }
}
