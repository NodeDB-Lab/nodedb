// SPDX-License-Identifier: Apache-2.0

//! Parse `COPY <collection> TO '<path>' [WITH (...)]` and
//! `COPY (SELECT ...) TO '<path>' [WITH (...)]`.

use crate::ddl_ast::statement::{CopyFormat, CopyToSource, MiscStmt, NodedbStatement};
use crate::error::SqlError;
use crate::parser::preprocess::lex::find_ascii_case_insensitive;

use super::copy_from::{
    detect_format_from_path, extract_quoted_string, parse_with_clause, strip_quotes,
};

/// Try to parse a COPY TO statement.
///
/// Returns `None` if the SQL is not a COPY statement. Returns `Some(Err)` on
/// parse errors. Returns `Some(Ok(CopyToFile {...}))` on success.
pub(super) fn try_parse(upper: &str, trimmed: &str) -> Option<Result<NodedbStatement, SqlError>> {
    if !upper.starts_with("COPY ") {
        return None;
    }

    let after_copy = trimmed["COPY ".len()..].trim_start();
    let upper_after = upper["COPY ".len()..].trim_start();

    // Query form: `COPY (SELECT ...) TO '<path>'` — the `(` starts immediately.
    // Skip FROM/TO prefix checks; parse_copy_to will handle it.
    if after_copy.starts_with('(') {
        // Only claim it if there is a `) TO ` somewhere after the closing paren.
        // We do a quick scan: find the closing paren, then check for TO.
        if find_matching_paren(after_copy).is_some_and(|close| {
            let rest = after_copy[close + 1..].trim_start().to_uppercase();
            rest.starts_with("TO ")
        }) {
            return Some(parse_copy_to(trimmed, after_copy));
        }
        return None;
    }

    // Table form: `COPY <collection> TO '<path>'`.
    // Only handle the TO form; FROM form is handled in copy_from.
    // For the table form, look at the upper-case tokens before the first quoted string.
    let has_to = upper_after.contains(" TO ");
    let has_from = upper_after.contains(" FROM ");
    if has_from && (!has_to || upper_after.find(" FROM ") < upper_after.find(" TO ")) {
        return None;
    }
    if !has_to {
        return None;
    }

    Some(parse_copy_to(trimmed, after_copy))
}

fn parse_copy_to(trimmed: &str, after_copy: &str) -> Result<NodedbStatement, SqlError> {
    // Determine if this is the query form or the table form.
    if after_copy.starts_with('(') {
        parse_query_form(trimmed)
    } else {
        parse_table_form(trimmed)
    }
}

/// `COPY <collection> TO '<path>' [WITH (...)]`
fn parse_table_form(trimmed: &str) -> Result<NodedbStatement, SqlError> {
    let to_pos = find_ascii_case_insensitive(trimmed, " TO ").ok_or_else(|| SqlError::Parse {
        detail: "COPY: missing TO keyword".to_string(),
    })?;

    // Collection name is between "COPY " and " TO ".
    let coll_raw = trimmed["COPY ".len()..to_pos].trim();
    if coll_raw.is_empty() {
        return Err(SqlError::Parse {
            detail: "COPY: missing collection name before TO".to_string(),
        });
    }
    let collection = strip_quotes(coll_raw).to_lowercase();

    let after_to = trimmed[to_pos + " TO ".len()..].trim_start();
    let (path, rest, format, delimiter, header) = parse_path_and_opts(after_to)?;

    let _ = rest; // consumed by parse_path_and_opts

    Ok(NodedbStatement::Misc(MiscStmt::CopyToFile {
        source: CopyToSource::Collection(collection),
        path,
        format,
        delimiter,
        header,
    }))
}

/// `COPY (SELECT ...) TO '<path>' [WITH (...)]`
fn parse_query_form(trimmed: &str) -> Result<NodedbStatement, SqlError> {
    // Find the matching close-paren for the leading `(`.
    let after_copy = trimmed["COPY ".len()..].trim_start();
    let close = find_matching_paren(after_copy).ok_or_else(|| SqlError::Parse {
        detail: "COPY: unclosed parenthesis in query form".to_string(),
    })?;

    let query = after_copy[1..close].trim().to_string();
    if query.is_empty() {
        return Err(SqlError::Parse {
            detail: "COPY: empty query in query form".to_string(),
        });
    }

    // After the closing paren, expect " TO '<path>'"
    let after_paren = after_copy[close + 1..].trim_start();
    let upper_after = after_paren.to_uppercase();
    if !upper_after.starts_with("TO ") {
        return Err(SqlError::Parse {
            detail: format!(
                "COPY: expected TO after query, got: {}",
                &after_paren[..after_paren.len().min(32)]
            ),
        });
    }

    let after_to = after_paren["TO ".len()..].trim_start();
    let (path, _rest, format, delimiter, header) = parse_path_and_opts(after_to)?;

    Ok(NodedbStatement::Misc(MiscStmt::CopyToFile {
        source: CopyToSource::Query(query),
        path,
        format,
        delimiter,
        header,
    }))
}

type PathAndOpts<'a> = (String, &'a str, Option<CopyFormat>, Option<char>, bool);

/// Parse `'<path>'` then optional `WITH (...)`.
/// Returns `(path, remainder, format, delimiter, header)`.
fn parse_path_and_opts(s: &str) -> Result<PathAndOpts<'_>, SqlError> {
    let (path, rest) = extract_quoted_string(s)?;
    let rest = rest.trim();

    let (format, delimiter, header) = if rest.is_empty() {
        (None, None, true)
    } else {
        parse_with_clause(rest)?
    };

    let format = match format {
        Some(f) => Some(f),
        None => detect_format_from_path(&path)?,
    };

    Ok((path, rest, format, delimiter, header))
}

/// Find the index of the closing `)` matching the opening `(` at position 0.
fn find_matching_paren(s: &str) -> Option<usize> {
    let mut depth = 0usize;
    let mut in_single_quote = false;
    for (i, ch) in s.char_indices() {
        match ch {
            '\'' if !in_single_quote => in_single_quote = true,
            '\'' if in_single_quote => in_single_quote = false,
            '(' if !in_single_quote => depth += 1,
            ')' if !in_single_quote => {
                depth = depth.saturating_sub(1);
                if depth == 0 {
                    return Some(i);
                }
            }
            _ => {}
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ddl_ast::parse::dispatch::parse;

    fn ok(sql: &str) -> NodedbStatement {
        parse(sql).expect("expected Some").expect("expected Ok")
    }

    fn err(sql: &str) -> SqlError {
        parse(sql)
            .expect("expected Some")
            .expect_err("expected Err")
    }

    #[test]
    fn unicode_collection_before_to_preserves_original_offsets() {
        let stmt = ok("COPY usersﬀﬀ TO '/tmp/out.ndjson'");
        match stmt {
            NodedbStatement::Misc(MiscStmt::CopyToFile { source, path, .. }) => {
                assert_eq!(source, CopyToSource::Collection("usersﬀﬀ".to_string()));
                assert_eq!(path, "/tmp/out.ndjson");
            }
            other => panic!("unexpected: {other:?}"),
        }
    }

    #[test]
    fn basic_ndjson_by_extension() {
        let stmt = ok("COPY users TO '/tmp/out.ndjson'");
        match stmt {
            NodedbStatement::Misc(MiscStmt::CopyToFile {
                source,
                path,
                format,
                ..
            }) => {
                assert_eq!(source, CopyToSource::Collection("users".to_string()));
                assert_eq!(path, "/tmp/out.ndjson");
                assert_eq!(format, Some(CopyFormat::Ndjson));
            }
            other => panic!("unexpected: {other:?}"),
        }
    }

    #[test]
    fn json_array_by_extension() {
        let stmt = ok("COPY users TO '/tmp/out.json'");
        match stmt {
            NodedbStatement::Misc(MiscStmt::CopyToFile { format, .. }) => {
                assert_eq!(format, Some(CopyFormat::JsonArray));
            }
            other => panic!("unexpected: {other:?}"),
        }
    }

    #[test]
    fn csv_by_extension() {
        let stmt = ok("COPY users TO '/tmp/out.csv'");
        match stmt {
            NodedbStatement::Misc(MiscStmt::CopyToFile { format, .. }) => {
                assert_eq!(format, Some(CopyFormat::Csv));
            }
            other => panic!("unexpected: {other:?}"),
        }
    }

    #[test]
    fn explicit_format_with_clause() {
        let stmt = ok("COPY users TO '/tmp/out.csv' WITH (FORMAT ndjson)");
        match stmt {
            NodedbStatement::Misc(MiscStmt::CopyToFile { format, .. }) => {
                assert_eq!(format, Some(CopyFormat::Ndjson));
            }
            other => panic!("unexpected: {other:?}"),
        }
    }

    #[test]
    fn csv_with_delimiter() {
        let stmt = ok("COPY users TO '/tmp/out.csv' WITH (FORMAT csv, DELIMITER ';')");
        match stmt {
            NodedbStatement::Misc(MiscStmt::CopyToFile {
                format, delimiter, ..
            }) => {
                assert_eq!(format, Some(CopyFormat::Csv));
                assert_eq!(delimiter, Some(';'));
            }
            other => panic!("unexpected: {other:?}"),
        }
    }

    #[test]
    fn header_false() {
        let stmt = ok("COPY users TO '/tmp/out.csv' WITH (FORMAT csv, HEADER false)");
        match stmt {
            NodedbStatement::Misc(MiscStmt::CopyToFile { header, .. }) => {
                assert!(!header);
            }
            other => panic!("unexpected: {other:?}"),
        }
    }

    #[test]
    fn query_form() {
        let stmt = ok("COPY (SELECT * FROM users WHERE active = true) TO '/tmp/out.ndjson'");
        match stmt {
            NodedbStatement::Misc(MiscStmt::CopyToFile { source, path, .. }) => {
                assert!(matches!(source, CopyToSource::Query(_)));
                assert_eq!(path, "/tmp/out.ndjson");
                if let CopyToSource::Query(q) = source {
                    assert!(q.to_uppercase().starts_with("SELECT"));
                }
            }
            other => panic!("unexpected: {other:?}"),
        }
    }

    #[test]
    fn copy_from_not_intercepted() {
        // COPY FROM should not be caught by try_parse (handled by copy_from).
        let stmt = ok("COPY users FROM '/tmp/data.ndjson'");
        assert!(matches!(
            stmt,
            NodedbStatement::Misc(MiscStmt::CopyFromFile { .. })
        ));
    }

    #[test]
    fn unknown_extension_is_err() {
        let e = err("COPY users TO '/tmp/out.parquet'");
        assert!(matches!(e, SqlError::Parse { .. }));
        assert!(e.to_string().contains("cannot infer format"));
    }
}
