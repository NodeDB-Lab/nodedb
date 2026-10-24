// SPDX-License-Identifier: Apache-2.0

use std::borrow::Cow;

use crate::parser::preprocess::lex::{self, SqlSegment};

pub(super) enum Tok<'a> {
    Word(&'a str),
    /// A `'...'` string, outer quotes stripped and `''` unescaped.
    Quoted(Cow<'a, str>),
    /// A `"..."` identifier, outer quotes stripped and `""` unescaped.
    Ident(Cow<'a, str>),
    /// Brace-balanced object literal including outer braces.
    Object(&'a str),
    /// Bracket-balanced `[...]` content, brackets excluded.
    List(&'a str),
    /// A `,` separator.
    Comma,
}

/// `sql` with every comment replaced by one space, so no comment becomes a
/// token.
pub(super) fn blank_comments(sql: &str) -> Cow<'_, str> {
    let segments = lex::segments(sql);
    if !segments.iter().any(|segment| {
        matches!(
            segment,
            SqlSegment::LineComment(_) | SqlSegment::BlockComment(_)
        )
    }) {
        return Cow::Borrowed(sql);
    }
    let mut out = String::with_capacity(sql.len());
    for segment in segments {
        match segment {
            SqlSegment::LineComment(_) | SqlSegment::BlockComment(_) => out.push(' '),
            SqlSegment::Text(text)
            | SqlSegment::SingleQuotedString(text)
            | SqlSegment::QuotedIdent(text) => out.push_str(text),
        }
    }
    Cow::Owned(out)
}

/// Split graph DSL text into tokens.
///
/// `,` produces a `Comma` token. `;`, `(` and `)` separate tokens and produce
/// none, so `LABEL 'a', 'b'` and `LABEL ('a', 'b')` read the same. A stray `]`
/// is a word token, so the clause parser reports it.
pub(super) fn tokenize(sql: &str) -> Vec<Tok<'_>> {
    let bytes = sql.as_bytes();
    let mut out = Vec::new();
    let mut i = 0;
    while i < bytes.len() {
        let b = bytes[i];
        if b == b',' {
            out.push(Tok::Comma);
            i += 1;
            continue;
        }
        if b.is_ascii_whitespace() || b == b';' || b == b'(' || b == b')' {
            i += 1;
            continue;
        }
        match b {
            b'\'' => {
                let (content, next) = consume_delimited(sql, bytes, i, b'\'');
                out.push(Tok::Quoted(content));
                i = next;
            }
            b'"' => {
                let (content, next) = consume_delimited(sql, bytes, i, b'"');
                out.push(Tok::Ident(content));
                i = next;
            }
            b'{' => i = consume_object(sql, bytes, i, &mut out),
            b'[' => i = consume_list(sql, bytes, i, &mut out),
            b']' => {
                out.push(Tok::Word(&sql[i..i + 1]));
                i += 1;
            }
            _ => i = consume_word(sql, bytes, i, &mut out),
        }
    }
    out
}

/// Read a `quote`-delimited run starting at `start`, where a doubled quote
/// stands for one quote. Returns the content and the index after the closing
/// quote.
fn consume_delimited<'a>(
    sql: &'a str,
    bytes: &[u8],
    start: usize,
    quote: u8,
) -> (Cow<'a, str>, usize) {
    let content_start = start + 1;
    let mut j = content_start;
    let mut has_escape = false;
    while j < bytes.len() {
        if bytes[j] == quote {
            if j + 1 < bytes.len() && bytes[j + 1] == quote {
                has_escape = true;
                j += 2;
                continue;
            }
            break;
        }
        j += 1;
    }
    let slice = &sql[content_start..j];
    let content = if has_escape {
        let single = (quote as char).to_string();
        let doubled = single.repeat(2);
        Cow::Owned(slice.replace(&doubled, &single))
    } else {
        Cow::Borrowed(slice)
    };
    let next = if j < bytes.len() { j + 1 } else { j };
    (content, next)
}

fn consume_object<'a>(sql: &'a str, bytes: &[u8], start: usize, out: &mut Vec<Tok<'a>>) -> usize {
    let mut depth = 0i32;
    let mut j = start;
    let mut in_quote = false;
    while j < bytes.len() {
        let c = bytes[j];
        if in_quote {
            if c == b'\'' {
                if j + 1 < bytes.len() && bytes[j + 1] == b'\'' {
                    j += 2;
                    continue;
                }
                in_quote = false;
            }
        } else {
            match c {
                b'\'' => in_quote = true,
                b'{' => depth += 1,
                b'}' => {
                    depth -= 1;
                    if depth == 0 {
                        j += 1;
                        break;
                    }
                }
                _ => {}
            }
        }
        j += 1;
    }
    out.push(Tok::Object(&sql[start..j]));
    j
}

/// Read a bracket-balanced `[...]` run starting at `start`. A `]` inside a
/// quoted string does not close it.
fn consume_list<'a>(sql: &'a str, bytes: &[u8], start: usize, out: &mut Vec<Tok<'a>>) -> usize {
    let mut depth = 1usize;
    let mut j = start + 1;
    let mut in_quote = false;
    while j < bytes.len() {
        let c = bytes[j];
        if in_quote {
            if c == b'\'' {
                if j + 1 < bytes.len() && bytes[j + 1] == b'\'' {
                    j += 2;
                    continue;
                }
                in_quote = false;
            }
        } else {
            match c {
                b'\'' => in_quote = true,
                b'[' => depth += 1,
                b']' => {
                    depth -= 1;
                    if depth == 0 {
                        break;
                    }
                }
                _ => {}
            }
        }
        j += 1;
    }
    out.push(Tok::List(&sql[start + 1..j]));
    if j < bytes.len() { j + 1 } else { j }
}

fn consume_word<'a>(sql: &'a str, bytes: &[u8], start: usize, out: &mut Vec<Tok<'a>>) -> usize {
    let mut j = start;
    while j < bytes.len() {
        let c = bytes[j];
        if c.is_ascii_whitespace()
            || c == b'\''
            || c == b'"'
            || c == b'{'
            || c == b','
            || c == b';'
            || c == b'('
            || c == b')'
            || c == b'['
            || c == b']'
        {
            break;
        }
        j += 1;
    }
    if j > start {
        out.push(Tok::Word(&sql[start..j]));
        j
    } else {
        start + 1
    }
}
