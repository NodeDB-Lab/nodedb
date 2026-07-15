// SPDX-License-Identifier: Apache-2.0

//! Shared SQL lexer for preprocess passes.
//!
//! The lexer classifies a SQL string into non-overlapping segments, allowing
//! preprocess passes to scan only plain `Text` segments — string literals,
//! quoted identifiers, and comments are passed through opaquely and never
//! matched against patterns.
//!
//! Supported SQL dialect features:
//! - Single-quoted strings (`'...'`) with `''` escape and `E'...'` with
//!   backslash escapes.
//! - Double-quoted identifiers (`"..."`).
//! - Line comments (`-- ...` to end-of-line).
//! - Block comments (`/* ... */`, nestable per PostgreSQL).

/// A classified segment of a SQL string.
#[derive(Debug, PartialEq, Eq)]
pub enum SqlSegment<'a> {
    /// Unquoted SQL text.
    Text(&'a str),
    /// A single-quoted string literal, including the surrounding quotes.
    SingleQuotedString(&'a str),
    /// A double-quoted identifier, including the surrounding quotes.
    QuotedIdent(&'a str),
    /// A line comment starting with `--`, including the `--` prefix and
    /// trailing newline (if present).
    LineComment(&'a str),
    /// A block comment delimited by `/* ... */` (nestable).
    BlockComment(&'a str),
}

/// Segment a SQL string into classified [`SqlSegment`]s.
///
/// The entire input is covered exactly once (no bytes are skipped). Adjacent
/// `Text` bytes are collected into a single segment.
pub fn segments(sql: &str) -> Vec<SqlSegment<'_>> {
    let mut out = Vec::new();
    let bytes = sql.as_bytes();
    let len = bytes.len();
    let mut i = 0;
    let mut text_start = 0;

    macro_rules! flush_text {
        () => {
            if text_start < i {
                out.push(SqlSegment::Text(&sql[text_start..i]));
            }
        };
    }

    while i < len {
        // ── single-quoted string ──────────────────────────────────────────
        // Optional `E` or `e` escape prefix before the opening quote.
        let is_escape_prefix =
            (bytes[i] == b'E' || bytes[i] == b'e') && i + 1 < len && bytes[i + 1] == b'\'';

        if bytes[i] == b'\'' || is_escape_prefix {
            flush_text!();
            let start = i;
            if is_escape_prefix {
                i += 1; // skip `E`
            }
            i += 1; // skip opening `'`
            let escape = is_escape_prefix;
            while i < len {
                match bytes[i] {
                    b'\\' if escape => {
                        // backslash escape: skip two chars
                        i += 2;
                    }
                    b'\'' => {
                        i += 1;
                        // doubled-quote escape `''`
                        if i < len && bytes[i] == b'\'' {
                            i += 1;
                        } else {
                            break;
                        }
                    }
                    _ => i += 1,
                }
            }
            out.push(SqlSegment::SingleQuotedString(&sql[start..i]));
            text_start = i;
            continue;
        }

        // ── double-quoted identifier ──────────────────────────────────────
        if bytes[i] == b'"' {
            flush_text!();
            let start = i;
            i += 1; // skip opening `"`
            while i < len {
                match bytes[i] {
                    b'"' => {
                        i += 1;
                        // doubled `""` escape inside quoted ident
                        if i < len && bytes[i] == b'"' {
                            i += 1;
                        } else {
                            break;
                        }
                    }
                    _ => i += 1,
                }
            }
            out.push(SqlSegment::QuotedIdent(&sql[start..i]));
            text_start = i;
            continue;
        }

        // ── line comment `-- ...` ─────────────────────────────────────────
        if bytes[i] == b'-' && i + 1 < len && bytes[i + 1] == b'-' {
            flush_text!();
            let start = i;
            while i < len && bytes[i] != b'\n' {
                i += 1;
            }
            // include the newline if present
            if i < len && bytes[i] == b'\n' {
                i += 1;
            }
            out.push(SqlSegment::LineComment(&sql[start..i]));
            text_start = i;
            continue;
        }

        // ── block comment `/* ... */` (nestable) ─────────────────────────
        if bytes[i] == b'/' && i + 1 < len && bytes[i + 1] == b'*' {
            flush_text!();
            let start = i;
            i += 2; // skip `/*`
            let mut depth: usize = 1;
            while i < len && depth > 0 {
                if bytes[i] == b'/' && i + 1 < len && bytes[i + 1] == b'*' {
                    depth += 1;
                    i += 2;
                } else if bytes[i] == b'*' && i + 1 < len && bytes[i + 1] == b'/' {
                    depth -= 1;
                    i += 2;
                } else {
                    i += 1;
                }
            }
            out.push(SqlSegment::BlockComment(&sql[start..i]));
            text_start = i;
            continue;
        }

        // ── plain text byte ───────────────────────────────────────────────
        i += 1;
    }

    // flush any trailing text
    if text_start < len {
        out.push(SqlSegment::Text(&sql[text_start..]));
    }

    out
}

/// Return the first SQL keyword/word in `sql`, skipping leading whitespace,
/// line comments, and block comments. Returns `None` if the input is empty
/// or contains only whitespace/comments.
///
/// The returned slice is a sub-slice of `sql` in its original case.
pub fn first_sql_word(sql: &str) -> Option<&str> {
    for seg in segments(sql) {
        if let SqlSegment::Text(t) = seg {
            let trimmed = t.trim_start();
            if trimmed.is_empty() {
                continue;
            }
            let end = trimmed
                .find(|c: char| c.is_ascii_whitespace() || c == '(' || c == ';')
                .unwrap_or(trimmed.len());
            if end > 0 {
                return Some(&trimmed[..end]);
            }
        }
    }
    None
}

/// Return the second SQL keyword/word in `sql`, skipping leading whitespace,
/// line comments, and block comments, then skipping the first word. Returns
/// `None` if there is no second word.
///
/// The returned slice is a sub-slice of `sql` in its original case.
pub fn second_sql_word(sql: &str) -> Option<&str> {
    let mut found_first = false;
    for seg in segments(sql) {
        if let SqlSegment::Text(t) = seg {
            let mut remaining = t;
            loop {
                let trimmed = remaining.trim_start();
                if trimmed.is_empty() {
                    break;
                }
                let end = trimmed
                    .find(|c: char| c.is_ascii_whitespace() || c == '(' || c == ';')
                    .unwrap_or(trimmed.len());
                if end == 0 {
                    break;
                }
                if !found_first {
                    found_first = true;
                    // advance past this word
                    remaining = &trimmed[end..];
                } else {
                    return Some(&trimmed[..end]);
                }
            }
        }
    }
    None
}

/// Return `true` if `op` appears verbatim inside any `Text` segment of `sql`.
/// The comparison is byte-exact (case-sensitive). Occurrences inside string
/// literals, quoted identifiers, or comments are ignored.
pub fn has_operator_outside_literals(sql: &str, op: &str) -> bool {
    for seg in segments(sql) {
        if let SqlSegment::Text(t) = seg
            && t.contains(op)
        {
            return true;
        }
    }
    false
}

/// Return the byte positions (relative to the start of `sql`) of every
/// occurrence of `op` that falls inside a `Text` segment.
pub fn find_operator_positions(sql: &str, op: &str) -> Vec<usize> {
    let mut positions = Vec::new();
    for seg in segments(sql) {
        if let SqlSegment::Text(t) = seg {
            // Safety: `t` is a sub-slice of `sql`; pointer arithmetic is valid.
            let base = t.as_ptr() as usize - sql.as_ptr() as usize;
            let mut search_from = 0;
            while let Some(rel) = t[search_from..].find(op) {
                let abs = base + search_from + rel;
                positions.push(abs);
                search_from += rel + op.len();
            }
        }
    }
    positions
}

/// Return `true` if `{` appears inside any `Text` segment of `sql`.
pub fn has_brace_outside_literals(sql: &str) -> bool {
    has_operator_outside_literals(sql, "{")
}

/// Find an ASCII `needle` in `text` without allocating a case-transformed
/// copy. The returned byte offset always belongs to `text`.
pub fn find_ascii_case_insensitive(text: &str, needle: &str) -> Option<usize> {
    find_ascii_case_insensitive_from(text, needle, 0)
}

/// Find the last ASCII `needle` in `text` without allocating a transformed
/// copy. The returned byte offset always belongs to `text`.
pub fn rfind_ascii_case_insensitive(text: &str, needle: &str) -> Option<usize> {
    if needle.is_empty() {
        return Some(text.len());
    }
    if !needle.is_ascii() || needle.len() > text.len() {
        return None;
    }

    let haystack = text.as_bytes();
    let needle = needle.as_bytes();
    (0..=haystack.len() - needle.len()).rev().find(|&position| {
        haystack[position..position + needle.len()]
            .iter()
            .zip(needle)
            .all(|(left, right)| left.eq_ignore_ascii_case(right))
    })
}

/// Find an ASCII `needle` at or after byte offset `start` in `text`.
///
/// `start` need not be a UTF-8 character boundary because matching is performed
/// on bytes. A successful position is always a boundary: an ASCII needle can
/// only begin on an ASCII byte in the original string.
pub fn find_ascii_case_insensitive_from(text: &str, needle: &str, start: usize) -> Option<usize> {
    if needle.is_empty() {
        return (start <= text.len()).then_some(start);
    }
    if !needle.is_ascii() || start > text.len() || needle.len() > text.len() - start {
        return None;
    }

    let haystack = text.as_bytes();
    let needle = needle.as_bytes();
    (start..=haystack.len() - needle.len()).find(|&position| {
        haystack[position..position + needle.len()]
            .iter()
            .zip(needle)
            .all(|(left, right)| left.eq_ignore_ascii_case(right))
    })
}

fn is_identifier_char(character: char) -> bool {
    character.is_alphanumeric() || character == '_'
}

fn has_word_boundaries(text: &str, position: usize, length: usize) -> bool {
    let before_ok = position == 0
        || !text[..position]
            .chars()
            .next_back()
            .map(is_identifier_char)
            .unwrap_or(false);
    let after = position + length;
    let after_ok = after >= text.len()
        || !text[after..]
            .chars()
            .next()
            .map(is_identifier_char)
            .unwrap_or(false);
    before_ok && after_ok
}

/// Find a whole ASCII keyword in `text`, comparing case-insensitively while
/// returning an offset into the unchanged input.
pub fn find_ascii_keyword(text: &str, keyword: &str) -> Option<usize> {
    let mut start = 0;
    while let Some(position) = find_ascii_case_insensitive_from(text, keyword, start) {
        if has_word_boundaries(text, position, keyword.len()) {
            return Some(position);
        }
        start = position + keyword.len().max(1);
    }
    None
}

/// Return the byte position (relative to `sql`) of the first case-insensitive
/// occurrence of the keyword `kw` that falls inside a `Text` segment. Returns
/// `None` if not found.
///
/// The match is word-boundary-aware: the character immediately before the
/// match (if any) must not be alphanumeric or `_`, and the character
/// immediately after the match (if any) must not be alphanumeric or `_`.
pub fn keyword_position_outside_literals(sql: &str, kw: &str) -> Option<usize> {
    for seg in segments(sql) {
        if let SqlSegment::Text(text) = seg
            && let Some(position) = find_ascii_keyword(text, kw)
        {
            let base = text.as_ptr() as usize - sql.as_ptr() as usize;
            return Some(base + position);
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    // ── segments ────────────────────────────────────────────────────────────

    #[test]
    fn plain_text_is_single_segment() {
        let segs = segments("SELECT 1");
        assert_eq!(segs, vec![SqlSegment::Text("SELECT 1")]);
    }

    #[test]
    fn single_quoted_string_opaque() {
        let segs = segments("SELECT '<->'");
        assert_eq!(
            segs,
            vec![
                SqlSegment::Text("SELECT "),
                SqlSegment::SingleQuotedString("'<->'"),
            ]
        );
    }

    #[test]
    fn quoted_ident_opaque() {
        let segs = segments(r#"SELECT "col_<->""#);
        assert_eq!(
            segs,
            vec![
                SqlSegment::Text("SELECT "),
                SqlSegment::QuotedIdent(r#""col_<->""#),
            ]
        );
    }

    #[test]
    fn line_comment_opaque() {
        let segs = segments("SELECT col -- has <-> in comment\nFROM t");
        // The segment after the newline belongs to a new Text segment.
        assert!(
            segs.iter()
                .any(|s| matches!(s, SqlSegment::LineComment(c) if c.contains("<->")))
        );
        assert!(
            segs.iter()
                .any(|s| matches!(s, SqlSegment::Text(t) if t.contains("FROM")))
        );
        // `<->` must not appear in any Text segment.
        for s in &segs {
            if let SqlSegment::Text(t) = s {
                assert!(!t.contains("<->"), "unexpected <-> in Text: {t}");
            }
        }
    }

    #[test]
    fn block_comment_opaque() {
        let segs = segments("SELECT /* <-> */ x");
        assert!(
            segs.iter()
                .any(|s| matches!(s, SqlSegment::BlockComment(c) if c.contains("<->")))
        );
        for s in &segs {
            if let SqlSegment::Text(t) = s {
                assert!(!t.contains("<->"), "unexpected <-> in Text: {t}");
            }
        }
    }

    #[test]
    fn nested_block_comment() {
        let segs = segments("SELECT /* /* nested */ <-> */ x");
        // The outer `/* ... */` includes everything including `<->`.
        assert!(
            segs.iter()
                .any(|s| matches!(s, SqlSegment::BlockComment(c) if c.contains("<->")))
        );
        for s in &segs {
            if let SqlSegment::Text(t) = s {
                assert!(!t.contains("<->"), "nested <-> leaked into Text: {t}");
            }
        }
    }

    #[test]
    fn doubled_quote_escape_in_string() {
        let segs = segments("SELECT 'it''s'");
        assert_eq!(
            segs,
            vec![
                SqlSegment::Text("SELECT "),
                SqlSegment::SingleQuotedString("'it''s'"),
            ]
        );
    }

    #[test]
    fn escape_string_prefix() {
        let segs = segments("SELECT E'foo\\nbar'");
        assert_eq!(
            segs,
            vec![
                SqlSegment::Text("SELECT "),
                SqlSegment::SingleQuotedString("E'foo\\nbar'"),
            ]
        );
    }

    // ── first_sql_word ───────────────────────────────────────────────────────

    #[test]
    fn first_word_simple() {
        assert_eq!(first_sql_word("SELECT 1"), Some("SELECT"));
    }

    #[test]
    fn first_word_skips_line_comment() {
        assert_eq!(first_sql_word("-- INSERT INTO t\nSELECT 1"), Some("SELECT"));
    }

    #[test]
    fn first_word_skips_block_comment() {
        assert_eq!(
            first_sql_word("/* hint */ INSERT INTO t VALUES (1)"),
            Some("INSERT")
        );
    }

    #[test]
    fn first_word_upsert_with_comment() {
        assert_eq!(
            first_sql_word("/* hint */ UPSERT INTO t { name: 'a' }"),
            Some("UPSERT")
        );
    }

    #[test]
    fn first_word_empty() {
        assert_eq!(first_sql_word("   "), None);
    }

    // ── has_operator_outside_literals ───────────────────────────────────────

    #[test]
    fn operator_in_plain_text() {
        assert!(has_operator_outside_literals("a <-> b", "<->"));
    }

    #[test]
    fn operator_in_string_not_detected() {
        assert!(!has_operator_outside_literals("SELECT '<->'", "<->"));
    }

    #[test]
    fn operator_in_line_comment_not_detected() {
        assert!(!has_operator_outside_literals(
            "SELECT col -- has <-> in comment\nFROM t",
            "<->"
        ));
    }

    #[test]
    fn operator_in_block_comment_not_detected() {
        assert!(!has_operator_outside_literals("SELECT /* <-> */ x", "<->"));
    }

    #[test]
    fn operator_in_quoted_ident_not_detected() {
        assert!(!has_operator_outside_literals(r#"SELECT "col_<->""#, "<->"));
    }

    // ── has_brace_outside_literals ──────────────────────────────────────────

    #[test]
    fn brace_in_plain_text() {
        assert!(has_brace_outside_literals("func({ foo })"));
    }

    #[test]
    fn brace_in_string_not_detected() {
        assert!(!has_brace_outside_literals("func('{ foo }')"));
    }

    #[test]
    fn brace_concat_expr_not_detected() {
        // `'{' || x || '}'` — braces only inside string literals
        assert!(!has_brace_outside_literals("'{' || x || '}'"));
    }

    // ── keyword_position_outside_literals ───────────────────────────────────

    #[test]
    fn keyword_found_in_plain_text() {
        let sql = "SELECT * FROM t FOR SYSTEM_TIME AS OF 100";
        assert!(keyword_position_outside_literals(sql, "FOR SYSTEM_TIME").is_some());
    }

    #[test]
    fn keyword_in_string_not_found() {
        let sql = "SELECT * FROM t WHERE name = 'FOR SYSTEM_TIME'";
        assert!(keyword_position_outside_literals(sql, "FOR SYSTEM_TIME").is_none());
    }

    #[test]
    fn keyword_position_correct() {
        let sql = "SELECT x FOR SYSTEM_TIME AS OF 100";
        let pos = keyword_position_outside_literals(sql, "FOR SYSTEM_TIME").unwrap();
        // verify the slice at that position matches the keyword (case-insensitively)
        let found = &sql[pos..pos + "FOR SYSTEM_TIME".len()];
        assert_eq!(found.to_uppercase(), "FOR SYSTEM_TIME");
    }

    #[test]
    fn keyword_position_after_unicode_text_indexes_original_sql() {
        let sql = "SELECT ﬀﬀ FROM records";
        let pos = keyword_position_outside_literals(sql, "FROM").unwrap();
        assert_eq!(&sql[pos..pos + "FROM".len()], "FROM");
    }
}
