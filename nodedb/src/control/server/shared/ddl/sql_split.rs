// SPDX-License-Identifier: BUSL-1.1

//! Splitting `VALUES` text into tuples and cells with sqlparser's tokenizer.
//!
//! The tokenizer classifies single-quoted strings (`''` escapes included),
//! dollar-quoted strings, double-quoted identifiers and comments, so a `,` or
//! a parenthesis inside any of them never splits or closes anything, at any
//! nesting depth. Only `,` and bracket tokens outside them are structure.
//! Each piece is a slice of the input, never a re-spelling of its tokens.

use sqlparser::dialect::PostgreSqlDialect;
use sqlparser::tokenizer::{Location, Token, Tokenizer};

/// One structural token: its kind and the byte offset it starts at.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Mark {
    Comma,
    Open,
    Close,
    Other,
}

/// The tokens of `s` outside whitespace and comments, each with its start
/// byte offset.
fn marks(s: &str) -> Result<Vec<(Mark, usize)>, String> {
    let dialect = PostgreSqlDialect {};
    let tokens = Tokenizer::new(&dialect, s)
        .tokenize_with_location()
        .map_err(|e| format!("malformed VALUES text: {e}"))?;
    let line_starts: Vec<usize> = std::iter::once(0)
        .chain(s.match_indices('\n').map(|(at, _)| at + 1))
        .collect();
    let mut out = Vec::with_capacity(tokens.len());
    for token in tokens {
        let mark = match token.token {
            Token::Whitespace(_) | Token::EOF => continue,
            Token::Comma => Mark::Comma,
            Token::LParen | Token::LBracket | Token::LBrace => Mark::Open,
            Token::RParen | Token::RBracket | Token::RBrace => Mark::Close,
            _ => Mark::Other,
        };
        let at = byte_offset(s, &line_starts, token.span.start)
            .ok_or_else(|| "VALUES text has a token with no source position".to_string())?;
        out.push((mark, at));
    }
    Ok(out)
}

/// The byte offset of a 1-based tokenizer `location` in `s`.
fn byte_offset(s: &str, line_starts: &[usize], location: Location) -> Option<usize> {
    let line = usize::try_from(location.line).ok()?.checked_sub(1)?;
    let column = usize::try_from(location.column).ok()?.checked_sub(1)?;
    let start = *line_starts.get(line)?;
    let rest = s.get(start..)?;
    rest.char_indices().nth(column).map(|(at, _)| start + at)
}

/// Split a comma-separated list at its top-level commas.
///
/// `'hello', 42, f('a,b'), 'it''s'` → `["'hello'", " 42", " f('a,b')", "
/// 'it''s'"]`. A trailing comma yields no empty last piece.
pub(crate) fn split_values(s: &str) -> Result<Vec<&str>, String> {
    let mut pieces = Vec::new();
    let mut depth = 0usize;
    let mut start = 0usize;
    for (mark, at) in marks(s)? {
        match mark {
            Mark::Open => depth += 1,
            Mark::Close => depth = depth.saturating_sub(1),
            Mark::Comma if depth == 0 => {
                pieces.push(&s[start..at]);
                start = at + 1;
            }
            Mark::Comma | Mark::Other => {}
        }
    }
    if start < s.len() {
        pieces.push(&s[start..]);
    }
    Ok(pieces)
}

/// Split the row list after `VALUES` into the inner text of each tuple.
///
/// `('a', 1), ('b', f(2))` → `["'a', 1", "'b', f(2)"]`. Text between tuples
/// other than one comma is an error, and so is text after the last tuple.
pub(crate) fn split_value_tuples(s: &str) -> Result<Vec<&str>, String> {
    let mut tuples = Vec::new();
    let mut depth = 0usize;
    let mut open_at = 0usize;
    let mut expect_comma = false;
    for (mark, at) in marks(s)? {
        if depth > 0 {
            match mark {
                Mark::Open => depth += 1,
                Mark::Close => {
                    depth -= 1;
                    if depth == 0 {
                        tuples.push(&s[open_at + 1..at]);
                        expect_comma = true;
                    }
                }
                Mark::Comma | Mark::Other => {}
            }
            continue;
        }
        match mark {
            Mark::Comma if expect_comma => expect_comma = false,
            Mark::Open if !expect_comma && s[at..].starts_with('(') => {
                depth = 1;
                open_at = at;
            }
            _ => {
                return Err(format!(
                    "unexpected text in VALUES list at '{}'",
                    s[at..].chars().take(24).collect::<String>()
                ));
            }
        }
    }
    if depth > 0 {
        return Err(format!(
            "missing closing ) for VALUES row {}",
            tuples.len() + 1
        ));
    }
    if tuples.is_empty() {
        return Err("missing VALUES (...)".into());
    }
    if !expect_comma {
        return Err("VALUES list ends with a comma".into());
    }
    Ok(tuples)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_comma_or_paren_inside_a_string_never_splits() {
        assert_eq!(
            split_values("jsonb_build_object('k', 'a),b'), f('x, y'), 'it''s'").expect("splits"),
            vec!["jsonb_build_object('k', 'a),b')", " f('x, y')", " 'it''s'"]
        );
        assert_eq!(
            split_values("$$a, b)$$, \"col,x\", 1 /* c, ) */, 2 -- , x\n").expect("splits"),
            vec!["$$a, b)$$", " \"col,x\"", " 1 /* c, ) */", " 2 -- , x\n"]
        );
        assert_eq!(
            split_values("amount DECIMAL(10, 2), name TEXT").expect("splits"),
            vec!["amount DECIMAL(10, 2)", " name TEXT"]
        );
    }

    #[test]
    fn value_tuples_split_on_top_level_parentheses() {
        let tuples = split_value_tuples(
            "('a', 1), ('b)(', f(2, 3)) ,('it''s', ARRAY[1]), (jsonb_build_object('k', 'a),b'))",
        )
        .expect("four rows");
        assert_eq!(
            tuples,
            vec![
                "'a', 1",
                "'b)(', f(2, 3)",
                "'it''s', ARRAY[1]",
                "jsonb_build_object('k', 'a),b')"
            ]
        );
    }

    #[test]
    fn malformed_value_tuples_are_refused() {
        for input in [
            "",
            "('a'), ",
            "('a') ('b')",
            "('a'",
            "('a') ON CONFLICT",
            "('unterminated)",
        ] {
            assert!(split_value_tuples(input).is_err(), "{input}");
        }
    }
}
