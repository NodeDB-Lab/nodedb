// SPDX-License-Identifier: Apache-2.0

//! Parse the body of `CREATE COLLECTION` / `CREATE TABLE` after the name.

use super::column_list::{extract_column_pairs, find_column_list_paren_end};
use super::engine_suffix::extract_engine_suffix;
use super::with_clause::{extract_balanced_raw, extract_with_options};
use crate::error::SqlError;
use crate::parser::preprocess::lex::{
    find_ascii_case_insensitive, keyword_position_outside_literals,
};

/// Parsed body of a `CREATE COLLECTION` / `CREATE TABLE` statement.
///
/// Tuple shape: `(engine, columns, options, flags, balanced_raw)`:
/// - `engine`: value of `engine=` from the WITH clause, or of a trailing
///   `ENGINE = <name>` suffix (lowercased), if present. Both forms feed the
///   same field so downstream validation (`validate_engine_name`) treats
///   them identically. If both are given they must name the same engine
///   (case-insensitively) or parsing fails with `SqlError::ConflictingEngineClause`.
/// - `columns`: `(name, type)` pairs from the parenthesised column list.
/// - `options`: remaining WITH clause `key=value` pairs (excluding `engine`).
/// - `flags`: free-standing modifier keywords: `APPEND_ONLY`, `HASH_CHAIN`, `BITEMPORAL`.
/// - `balanced_raw`: raw interior of the `BALANCED ON (...)` clause, or `None`.
pub(super) type CollectionBody = (
    Option<String>,
    Vec<(String, String)>,
    Vec<(String, String)>,
    Vec<String>,
    Option<String>,
);

pub(super) fn parse_collection_body(trimmed: &str, name: &str) -> Result<CollectionBody, SqlError> {
    // Locate the source identifier structurally. `name` is normalized by the
    // dispatcher, so searching for it in the original SQL is not reliable for
    // Unicode identifiers whose lowercase representation differs in bytes.
    let keyword = ["COLLECTION", "TABLE"]
        .into_iter()
        .filter_map(|candidate| {
            keyword_position_outside_literals(trimmed, candidate)
                .map(|position| (position, candidate))
        })
        .min_by_key(|(position, _)| *position);
    let Some((keyword_position, keyword)) = keyword else {
        return Ok((None, Vec::new(), Vec::new(), Vec::new(), None));
    };
    let after_keyword = &trimmed[keyword_position + keyword.len()..];
    let mut name_source = after_keyword.trim_start();
    if find_ascii_case_insensitive(name_source, "IF NOT EXISTS") == Some(0) {
        name_source = name_source["IF NOT EXISTS".len()..].trim_start();
    }
    let name_end = if let Some(quote) = name_source
        .chars()
        .next()
        .filter(|c| matches!(c, '"' | '`'))
    {
        name_source[quote.len_utf8()..]
            .find(quote)
            .map(|position| quote.len_utf8() + position + quote.len_utf8())
    } else {
        Some(
            name_source
                .char_indices()
                .find(|(_, character)| character.is_whitespace() || *character == '(')
                .map_or(name_source.len(), |(position, _)| position),
        )
    };
    let Some(name_end) = name_end else {
        return Ok((None, Vec::new(), Vec::new(), Vec::new(), None));
    };
    if name_source[..name_end]
        .trim_matches(['"', '`'])
        .to_lowercase()
        != name
    {
        return Ok((None, Vec::new(), Vec::new(), Vec::new(), None));
    }
    let body = name_source[name_end..].trim();

    let upper_body = body.to_uppercase();

    let columns = extract_column_pairs(body)?;
    let (with_engine, options) = extract_with_options(body);

    // Scan for a trailing MySQL-style `ENGINE [=] <name>` suffix, but only in
    // the text AFTER the true column-list closing paren (depth-aware, shared
    // with `extract_column_pairs` via `find_column_list_paren_end`) so that
    // nested parens in column types (e.g. `VECTOR(128)`, `NUMERIC(10,2)`)
    // never confuse the boundary.
    let suffix_engine = match find_column_list_paren_end(body) {
        Some(paren_end) => extract_engine_suffix(&body[paren_end + 1..])?,
        None => None,
    };

    let engine = match (with_engine, suffix_engine) {
        (Some(w), Some(s)) => {
            if w.eq_ignore_ascii_case(&s) {
                Some(w)
            } else {
                return Err(SqlError::ConflictingEngineClause {
                    with_engine: w,
                    suffix_engine: s,
                });
            }
        }
        (Some(w), None) => Some(w),
        (None, Some(s)) => Some(s.to_lowercase()),
        (None, None) => None,
    };

    let mut flags: Vec<String> = Vec::new();
    if upper_body.contains("APPEND_ONLY") {
        flags.push("APPEND_ONLY".to_string());
    }
    if upper_body.contains("HASH_CHAIN") {
        flags.push("HASH_CHAIN".to_string());
    }
    if upper_body.contains("BITEMPORAL") {
        flags.push("BITEMPORAL".to_string());
    }

    let balanced_raw = extract_balanced_raw(body);

    Ok((engine, columns, options, flags, balanced_raw))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn collection_body_after_unicode_comment_preserves_original_offsets() {
        let (engine, columns, ..) = parse_collection_body(
            "CREATE /*İ*/ COLLECTION items (id INT) WITH (engine='kv')",
            "items",
        )
        .expect("collection body should parse");
        assert_eq!(engine, Some("kv".to_string()));
        assert_eq!(columns, vec![("id".to_string(), "INT".to_string())]);
    }

    #[test]
    fn normalized_unicode_collection_name_preserves_body_boundary() {
        let (engine, columns, ..) =
            parse_collection_body("CREATE COLLECTION Åx (id INT) WITH (engine='kv')", "åx")
                .expect("collection body should parse");
        assert_eq!(engine, Some("kv".to_string()));
        assert_eq!(columns, vec![("id".to_string(), "INT".to_string())]);
    }

    #[test]
    fn engine_suffix_matches_with_clause_result() {
        let (engine_suffix, ..) = parse_collection_body(
            "CREATE COLLECTION t (id INT PRIMARY KEY) ENGINE = timeseries",
            "t",
        )
        .expect("parses");
        let (engine_with, ..) = parse_collection_body(
            "CREATE COLLECTION t (id INT PRIMARY KEY) WITH (engine='timeseries')",
            "t",
        )
        .expect("parses");
        assert_eq!(engine_suffix, Some("timeseries".to_string()));
        assert_eq!(engine_suffix, engine_with);
    }

    #[test]
    fn engine_suffix_no_spaces() {
        let (engine, ..) = parse_collection_body(
            "CREATE COLLECTION t (id INT PRIMARY KEY) ENGINE=columnar",
            "t",
        )
        .expect("parses");
        assert_eq!(engine, Some("columnar".to_string()));
    }

    #[test]
    fn engine_suffix_survives_nested_paren_column_types() {
        let (engine, columns, ..) = parse_collection_body(
            "CREATE COLLECTION t (id INT PRIMARY KEY, v VECTOR(3), amt NUMERIC(10,2)) ENGINE = kv",
            "t",
        )
        .expect("parses");
        assert_eq!(engine, Some("kv".to_string()));
        assert_eq!(columns.len(), 3);
        assert_eq!(columns[1], ("v".to_string(), "VECTOR(3)".to_string()));
        assert_eq!(columns[2], ("amt".to_string(), "NUMERIC(10,2)".to_string()));
    }

    #[test]
    fn conflicting_with_and_suffix_engine_errors() {
        let err = parse_collection_body(
            "CREATE COLLECTION t (id INT PRIMARY KEY) WITH (engine='kv') ENGINE = timeseries",
            "t",
        )
        .expect_err("must reject conflicting engine clauses");
        match err {
            SqlError::ConflictingEngineClause {
                with_engine,
                suffix_engine,
            } => {
                assert_eq!(with_engine, "kv");
                assert_eq!(suffix_engine, "timeseries");
            }
            other => panic!("expected ConflictingEngineClause, got {other:?}"),
        }
    }

    #[test]
    fn agreeing_with_and_suffix_engine_is_accepted() {
        let (engine, ..) = parse_collection_body(
            "CREATE COLLECTION t (id INT PRIMARY KEY) WITH (engine='kv') ENGINE = kv",
            "t",
        )
        .expect("agreeing engine clauses are accepted");
        assert_eq!(engine, Some("kv".to_string()));
    }

    #[test]
    fn malformed_engine_suffix_is_a_parse_error() {
        let err = parse_collection_body("CREATE COLLECTION t (id INT PRIMARY KEY) ENGINE =", "t")
            .expect_err("empty engine name must error");
        assert!(matches!(err, SqlError::Parse { .. }));
    }
}
