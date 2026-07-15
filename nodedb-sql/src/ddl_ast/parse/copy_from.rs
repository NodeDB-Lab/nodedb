// SPDX-License-Identifier: Apache-2.0

//! Parse `COPY <collection> FROM '<path>' [WITH (FORMAT ..., DELIMITER ..., HEADER ...)]`.
//!
//! Only the server-side file-path form is handled here. The STDIN streaming
//! shape (`COPY ... FROM STDIN`) is handled elsewhere (backup/restore);
//! returning `None` lets dispatch fall through to that handler.
//! `COPY ... TO` and `COPY (SELECT ...) TO` are rejected with typed errors.

use crate::ddl_ast::statement::{CopyFormat, MiscStmt, NodedbStatement};
use crate::error::SqlError;
use crate::parser::preprocess::lex::find_ascii_case_insensitive;

/// Try to parse a COPY statement.
///
/// Returns `None` if the SQL does not start with `COPY `, so dispatch is
/// unaffected. Returns `Some(Err)` for malformed COPY statements that this
/// parser claims (i.e. `COPY ... TO`, query form, or parse errors).
/// Returns `Some(Ok(CopyFromFile {...}))` on success.
pub(super) fn try_parse(
    upper: &str,
    _parts: &[&str],
    trimmed: &str,
) -> Option<Result<NodedbStatement, SqlError>> {
    if !upper.starts_with("COPY ") {
        return None;
    }

    // COPY (SELECT ...) is the query-form TO path — handled by copy_to parser.
    let after_copy_check = trimmed["COPY ".len()..].trim_start();
    if after_copy_check.starts_with('(') {
        return None;
    }

    // Fall through for COPY ... TO — handled by copy_to parser.
    let has_from = upper.contains(" FROM ");
    let has_to = upper.contains(" TO ");
    if has_to
        && (!has_from
            || find_ascii_case_insensitive(trimmed, " TO ")
                < find_ascii_case_insensitive(trimmed, " FROM "))
    {
        return None;
    }

    // If it has FROM STDIN, return None — let backup/restore intercept it.
    if upper.contains(" FROM STDIN") {
        return None;
    }

    if !has_from {
        return None;
    }

    Some(parse_copy_from(trimmed))
}

fn parse_copy_from(trimmed: &str) -> Result<NodedbStatement, SqlError> {
    // Grammar: COPY <name> FROM '<path>' [WITH (...)]

    // Extract collection name (first word before FROM).
    let from_pos =
        find_ascii_case_insensitive(trimmed, " FROM ").ok_or_else(|| SqlError::Parse {
            detail: "COPY: missing FROM keyword".to_string(),
        })?;

    // The collection name is between "COPY " and " FROM ".
    let coll_raw = trimmed["COPY ".len()..from_pos].trim();
    if coll_raw.is_empty() {
        return Err(SqlError::Parse {
            detail: "COPY: missing collection name".to_string(),
        });
    }
    // Strip optional quotes.
    let collection = strip_quotes(coll_raw).to_lowercase();

    let after_from = trimmed[from_pos + " FROM ".len()..].trim_start();

    // Extract the path (must be a single-quoted string).
    let (path, rest) = extract_quoted_string(after_from)?;

    // Parse optional WITH clause.
    let rest = rest.trim();
    let (format, delimiter, header) = if rest.is_empty() {
        (None, None, true)
    } else {
        parse_with_clause(rest)?
    };

    // Auto-detect format from extension if not explicitly specified.
    let format = match format {
        Some(f) => Some(f),
        None => detect_format_from_path(&path)?,
    };

    Ok(NodedbStatement::Misc(MiscStmt::CopyFromFile {
        collection,
        path,
        format,
        delimiter,
        header,
    }))
}

/// Strip surrounding double or single quotes from an identifier or path.
pub(super) fn strip_quotes(s: &str) -> &str {
    if (s.starts_with('"') && s.ends_with('"')) || (s.starts_with('\'') && s.ends_with('\'')) {
        &s[1..s.len() - 1]
    } else {
        s
    }
}

/// Extract the leading single-quoted string from `s`.
/// Returns `(content, remainder_after_closing_quote)`.
pub(super) fn extract_quoted_string(s: &str) -> Result<(String, &str), SqlError> {
    if !s.starts_with('\'') {
        return Err(SqlError::Parse {
            detail: format!(
                "COPY: expected single-quoted path after FROM, got: {}",
                &s[..s.len().min(32)]
            ),
        });
    }
    let inner = &s[1..];
    // Find the closing quote (handle escaped '' as a literal quote).
    let mut result = String::new();
    let mut chars = inner.char_indices();
    loop {
        match chars.next() {
            None => {
                return Err(SqlError::Parse {
                    detail: "COPY: unterminated path string".to_string(),
                });
            }
            Some((_, '\'')) => {
                // Peek: if next char is also ', it's an escaped quote.
                let remainder = chars.as_str();
                if remainder.starts_with('\'') {
                    result.push('\'');
                    chars.next(); // consume second '
                } else {
                    return Ok((result, remainder));
                }
            }
            Some((_, ch)) => result.push(ch),
        }
    }
}

/// Parse `WITH (key value, ...)` or `WITH (key = value, ...)`.
pub(super) fn parse_with_clause(
    s: &str,
) -> Result<(Option<CopyFormat>, Option<char>, bool), SqlError> {
    let upper = s.to_uppercase();
    if !upper.starts_with("WITH") {
        return Err(SqlError::Parse {
            detail: format!(
                "COPY: unexpected trailing content: {}",
                &s[..s.len().min(32)]
            ),
        });
    }
    let after_with = s["WITH".len()..].trim_start();
    if !after_with.starts_with('(') {
        return Err(SqlError::Parse {
            detail: "COPY: WITH clause must be enclosed in parentheses".to_string(),
        });
    }
    let close = after_with.rfind(')').ok_or_else(|| SqlError::Parse {
        detail: "COPY: unclosed WITH clause parenthesis".to_string(),
    })?;
    let inner = &after_with[1..close];

    let mut format: Option<CopyFormat> = None;
    let mut delimiter: Option<char> = None;
    let mut header = true;

    // Split on commas; each token is "KEY value" or "KEY = value".
    for token in split_with_tokens(inner) {
        let token = token.trim();
        if token.is_empty() {
            continue;
        }
        // Remove optional '=' separator.
        let (key, val_raw) = if let Some(pos) = token.find('=') {
            let k = token[..pos].trim().to_uppercase();
            let v = token[pos + 1..].trim().to_string();
            (k, v)
        } else {
            // Space-separated: KEY VALUE or just KEY.
            let mut words = token.splitn(2, char::is_whitespace);
            let k = words.next().unwrap_or("").to_uppercase();
            let v = words.next().unwrap_or("").trim().to_string();
            (k, v)
        };

        match key.as_str() {
            "FORMAT" => {
                format = Some(parse_format_value(&val_raw)?);
            }
            "DELIMITER" => {
                delimiter = Some(parse_delimiter_value(&val_raw)?);
            }
            "HEADER" => {
                header = parse_bool_value(&val_raw).unwrap_or(true);
            }
            other => {
                return Err(SqlError::Parse {
                    detail: format!("COPY WITH: unknown option '{other}'"),
                });
            }
        }
    }

    Ok((format, delimiter, header))
}

/// Split WITH clause inner content on commas, respecting single-quoted strings.
fn split_with_tokens(s: &str) -> Vec<String> {
    let mut tokens = Vec::new();
    let mut current = String::new();
    let mut in_quote = false;
    for ch in s.chars() {
        match ch {
            '\'' if !in_quote => {
                in_quote = true;
                current.push(ch);
            }
            '\'' if in_quote => {
                in_quote = false;
                current.push(ch);
            }
            ',' if !in_quote => {
                tokens.push(current.trim().to_string());
                current = String::new();
            }
            _ => current.push(ch),
        }
    }
    if !current.trim().is_empty() {
        tokens.push(current.trim().to_string());
    }
    tokens
}

fn parse_format_value(s: &str) -> Result<CopyFormat, SqlError> {
    let stripped = strip_quotes(s.trim());
    match stripped.to_lowercase().as_str() {
        "ndjson" | "jsonl" => Ok(CopyFormat::Ndjson),
        "json" => Ok(CopyFormat::JsonArray),
        "csv" => Ok(CopyFormat::Csv),
        other => Err(SqlError::Parse {
            detail: format!("COPY: unknown FORMAT '{other}'; expected ndjson, json, or csv"),
        }),
    }
}

fn parse_delimiter_value(s: &str) -> Result<char, SqlError> {
    let stripped = strip_quotes(s.trim());
    let mut chars = stripped.chars();
    let ch = chars.next().ok_or_else(|| SqlError::Parse {
        detail: "COPY: DELIMITER must be a single character".to_string(),
    })?;
    if chars.next().is_some() {
        return Err(SqlError::Parse {
            detail: "COPY: DELIMITER must be a single character".to_string(),
        });
    }
    Ok(ch)
}

fn parse_bool_value(s: &str) -> Option<bool> {
    match strip_quotes(s.trim()).to_lowercase().as_str() {
        "true" | "1" | "yes" | "on" => Some(true),
        "false" | "0" | "no" | "off" => Some(false),
        _ => None,
    }
}

/// Detect the copy format from the file path extension.
///
/// Returns `Err` if the extension is unrecognised (suggests explicit WITH clause).
/// Returns `Ok(None)` only if there is no extension at all (also suggests explicit WITH).
pub(super) fn detect_format_from_path(path: &str) -> Result<Option<CopyFormat>, SqlError> {
    let lower = path.to_lowercase();
    if lower.ends_with(".ndjson") || lower.ends_with(".jsonl") {
        return Ok(Some(CopyFormat::Ndjson));
    }
    if lower.ends_with(".json") {
        return Ok(Some(CopyFormat::JsonArray));
    }
    if lower.ends_with(".csv") {
        return Ok(Some(CopyFormat::Csv));
    }
    // Unknown or absent extension.
    Err(SqlError::Parse {
        detail: format!(
            "COPY: cannot infer format from path '{path}'; \
             add WITH (FORMAT ndjson|json|csv)"
        ),
    })
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
    fn unicode_collection_before_from_preserves_original_offsets() {
        let stmt = ok("COPY usersﬀﬀ FROM '/tmp/users.ndjson'");
        match stmt {
            NodedbStatement::Misc(MiscStmt::CopyFromFile {
                collection, path, ..
            }) => {
                assert_eq!(collection, "usersﬀﬀ");
                assert_eq!(path, "/tmp/users.ndjson");
            }
            other => panic!("unexpected: {other:?}"),
        }
    }

    #[test]
    fn basic_ndjson_by_extension() {
        let stmt = ok("COPY users FROM '/tmp/users.ndjson'");
        match stmt {
            NodedbStatement::Misc(MiscStmt::CopyFromFile {
                collection,
                path,
                format,
                delimiter,
                header,
            }) => {
                assert_eq!(collection, "users");
                assert_eq!(path, "/tmp/users.ndjson");
                assert_eq!(format, Some(CopyFormat::Ndjson));
                assert_eq!(delimiter, None);
                assert!(header);
            }
            other => panic!("unexpected: {other:?}"),
        }
    }

    #[test]
    fn json_array_by_extension() {
        let stmt = ok("COPY users FROM '/tmp/users.json'");
        match stmt {
            NodedbStatement::Misc(MiscStmt::CopyFromFile { format, .. }) => {
                assert_eq!(format, Some(CopyFormat::JsonArray));
            }
            other => panic!("unexpected: {other:?}"),
        }
    }

    #[test]
    fn csv_by_extension() {
        let stmt = ok("COPY users FROM '/tmp/users.csv'");
        match stmt {
            NodedbStatement::Misc(MiscStmt::CopyFromFile { format, .. }) => {
                assert_eq!(format, Some(CopyFormat::Csv));
            }
            other => panic!("unexpected: {other:?}"),
        }
    }

    #[test]
    fn explicit_format_overrides_extension() {
        let stmt = ok("COPY users FROM '/tmp/data.csv' WITH (FORMAT json)");
        match stmt {
            NodedbStatement::Misc(MiscStmt::CopyFromFile { format, .. }) => {
                assert_eq!(format, Some(CopyFormat::JsonArray));
            }
            other => panic!("unexpected: {other:?}"),
        }
    }

    #[test]
    fn csv_with_delimiter() {
        let stmt = ok("COPY users FROM '/tmp/data.csv' WITH (FORMAT csv, DELIMITER ';')");
        match stmt {
            NodedbStatement::Misc(MiscStmt::CopyFromFile {
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
        let stmt = ok("COPY users FROM '/tmp/data.csv' WITH (FORMAT csv, HEADER false)");
        match stmt {
            NodedbStatement::Misc(MiscStmt::CopyFromFile { header, .. }) => {
                assert!(!header);
            }
            other => panic!("unexpected: {other:?}"),
        }
    }

    #[test]
    fn unknown_extension_is_err() {
        let e = err("COPY users FROM '/tmp/data.parquet'");
        assert!(matches!(e, SqlError::Parse { .. }));
        assert!(e.to_string().contains("cannot infer format"));
    }

    #[test]
    fn copy_to_handled_by_copy_to_parser() {
        // COPY ... TO is handled by copy_to.rs — copy_from returns None.
        // The dispatch layer routes it to copy_to::try_parse instead.
        // Here we verify copy_from::try_parse does not claim this statement.
        use super::super::copy_to::try_parse as copy_to_try_parse;
        let sql = "COPY users TO '/tmp/out.csv'";
        let upper = sql.to_uppercase();
        let parts: Vec<&str> = sql.split_whitespace().collect();
        // copy_from should return None.
        assert!(try_parse(&upper, &parts, sql).is_none());
        // copy_to should claim it.
        assert!(copy_to_try_parse(&upper, sql).is_some());
    }

    #[test]
    fn copy_query_form_handled_by_copy_to_parser() {
        use super::super::copy_to::try_parse as copy_to_try_parse;
        let sql = "COPY (SELECT * FROM users) TO '/tmp/out.csv'";
        let upper = sql.to_uppercase();
        let parts: Vec<&str> = sql.split_whitespace().collect();
        assert!(try_parse(&upper, &parts, sql).is_none());
        assert!(copy_to_try_parse(&upper, sql).is_some());
    }

    #[test]
    fn copy_from_stdin_returns_none() {
        // Should return None so backup/restore handler catches it.
        assert!(parse("COPY tenant_restore(1) FROM STDIN").is_none());
    }

    #[test]
    fn unknown_with_option_is_err() {
        let e = err("COPY users FROM '/tmp/data.csv' WITH (FORMAT csv, BOGUS opt)");
        assert!(matches!(e, SqlError::Parse { .. }));
        assert!(e.to_string().contains("BOGUS"));
    }
}
