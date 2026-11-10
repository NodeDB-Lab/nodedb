// SPDX-License-Identifier: Apache-2.0

//! The public entry points: prefix parsing, strict whole-input parsing, and the
//! leftover reporting that separates them.

use std::collections::HashMap;

use nodedb_types::Value;

use crate::error::SqlError;

use super::scan::{parse_object, skip_ws};
use super::template::{Literal, into_value_map};

/// One object literal whose values may hold `$N` parameter references.
pub type ObjectTemplate = HashMap<String, Literal>;

/// Parse a `{ key: value, ... }` object literal at the START of `s`.
///
/// **This is a PREFIX parser and stops at the matching `}`.** Anything after
/// that brace is left unexamined, deliberately: the function-argument rewriter
/// (`preprocess::function_args`) hands it the entire remainder of an argument
/// list starting at `{` and resumes from the matching brace itself, so it needs
/// a parser that reads one literal and stops.
///
/// That tolerance is exactly wrong for a caller whose input is supposed to BE
/// the literal — there, unexamined trailing text is input the caller wrote and
/// the parser threw away without saying so. Those callers use
/// [`parse_object_literal_complete`], which reports the leftover instead.
///
/// Returns `None` if the input doesn't start with `{` (not an object literal).
/// Returns `Some(Err(msg))` on parse errors (malformed object literal).
/// Returns `Some(Ok(fields))` on success.
pub fn parse_object_literal(s: &str) -> Option<Result<HashMap<String, Value>, SqlError>> {
    let trimmed = s.trim();
    if !trimmed.starts_with('{') {
        return None;
    }
    let chars: Vec<char> = trimmed.chars().collect();
    let mut pos = 0;
    Some(parse_object(&chars, &mut pos).and_then(into_value_map))
}

/// Parse a `{ key: value, ... }` object literal that must be the WHOLE input.
///
/// The strict form of [`parse_object_literal`], for callers whose input is the
/// literal and nothing else. Trailing text is an error naming what was left
/// over, so a clause the caller wrote can never be discarded in silence. A
/// trailing `;` is accepted — it terminates the statement, it is not content.
pub fn parse_object_literal_complete(s: &str) -> Option<Result<HashMap<String, Value>, SqlError>> {
    parse_object_template_complete(s).map(|parsed| parsed.and_then(into_value_map))
}

/// [`parse_object_literal_complete`] keeping each `$N` parameter reference,
/// for a statement whose parameters bind after the rewrite.
pub fn parse_object_template_complete(s: &str) -> Option<Result<ObjectTemplate, SqlError>> {
    let trimmed = s.trim();
    if !trimmed.starts_with('{') {
        return None;
    }
    let chars: Vec<char> = trimmed.chars().collect();
    let mut pos = 0;
    Some(
        parse_object(&chars, &mut pos).and_then(|fields| match leftover(&chars, pos) {
            Some(rest) => Err(trailing_input_error("object literal", &rest)),
            None => Ok(fields),
        }),
    )
}

/// Parse an array of object literals that must be the WHOLE input.
///
/// The strict form of [`parse_object_literal_array`], with the same contract as
/// [`parse_object_literal_complete`].
pub fn parse_object_literal_array_complete(
    s: &str,
) -> Option<Result<Vec<HashMap<String, Value>>, SqlError>> {
    parse_object_template_array_complete(s)
        .map(|parsed| parsed.and_then(|objects| objects.into_iter().map(into_value_map).collect()))
}

/// [`parse_object_literal_array_complete`] keeping each `$N` parameter
/// reference.
pub fn parse_object_template_array_complete(
    s: &str,
) -> Option<Result<Vec<ObjectTemplate>, SqlError>> {
    let trimmed = s.trim();
    if !trimmed.starts_with('[') {
        return None;
    }
    let objects = match parse_object_template_array(trimmed)? {
        Ok(objects) => objects,
        Err(error) => return Some(Err(error)),
    };
    // The array parser stops at the closing `]`; locate it the same way it
    // does — by balancing brackets outside quoted strings — so the leftover is
    // measured against the literal's real end rather than the last `]` in the
    // input, which a quoted value could contain.
    let chars: Vec<char> = trimmed.chars().collect();
    let Some(end) = matching_bracket(&chars) else {
        return Some(Err(SqlError::Parse {
            detail: "unterminated array of objects".to_string(),
        }));
    };
    Some(match leftover(&chars, end + 1) {
        Some(rest) => Err(trailing_input_error("array of object literals", &rest)),
        None => Ok(objects),
    })
}

/// Byte offset just past the object literal, or array of object literals,
/// that opens `s`.
///
/// `None` when `s` does not open with `{` or `[`, or the literal does not
/// parse or close. The strict parsers then report the malformed literal.
pub fn object_literal_end(s: &str) -> Option<usize> {
    let chars: Vec<char> = s.chars().collect();
    let end = match chars.first()? {
        '{' => {
            let mut pos = 0;
            parse_object(&chars, &mut pos).ok()?;
            pos
        }
        '[' => matching_bracket(&chars)? + 1,
        _ => return None,
    };
    Some(s.char_indices().nth(end).map_or(s.len(), |(byte, _)| byte))
}

/// The non-whitespace text remaining from `pos`, ignoring a trailing `;`.
///
/// `None` means the literal consumed everything meaningful.
fn leftover(chars: &[char], pos: usize) -> Option<String> {
    let rest: String = chars.get(pos..).unwrap_or_default().iter().collect();
    let rest = rest.trim().trim_end_matches(';').trim();
    if rest.is_empty() {
        None
    } else {
        Some(rest.to_string())
    }
}

/// The error a caller sees when it wrote something the literal form cannot
/// carry. It names the leftover, because a message that only says "trailing
/// input" leaves the author guessing which part of their statement was
/// rejected.
fn trailing_input_error(what: &str, rest: &str) -> SqlError {
    SqlError::Parse {
        detail: format!(
            "unexpected input after {what}: `{rest}`. The brace form takes no trailing clause — \
             write `INSERT INTO <collection> (cols) VALUES (...) {rest}` to use it"
        ),
    }
}

/// Index of the `]` closing the array that starts at index 0, skipping
/// brackets inside single-quoted strings.
fn matching_bracket(chars: &[char]) -> Option<usize> {
    let mut depth = 0usize;
    let mut in_string = false;
    let mut i = 0;
    while i < chars.len() {
        match chars[i] {
            '\'' if in_string && chars.get(i + 1) == Some(&'\'') => i += 1,
            '\'' => in_string = !in_string,
            '[' if !in_string => depth += 1,
            ']' if !in_string => {
                depth = depth.saturating_sub(1);
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

/// Parse `[{ ... }, { ... }]` — an array of object literals for batch insert.
///
/// Returns `None` if the input doesn't start with `[` (not an array literal).
/// Returns `Some(Err(msg))` on parse errors.
/// Returns `Some(Ok(vec))` on success — each element must be an object.
pub fn parse_object_literal_array(
    s: &str,
) -> Option<Result<Vec<HashMap<String, Value>>, SqlError>> {
    parse_object_template_array(s)
        .map(|parsed| parsed.and_then(|objects| objects.into_iter().map(into_value_map).collect()))
}

/// [`parse_object_literal_array`] keeping each `$N` parameter reference.
fn parse_object_template_array(s: &str) -> Option<Result<Vec<ObjectTemplate>, SqlError>> {
    let trimmed = s.trim();
    if !trimmed.starts_with('[') {
        return None;
    }
    let chars: Vec<char> = trimmed.chars().collect();
    let mut pos = 0;

    // Consume '['
    pos += 1;
    let mut objects = Vec::new();
    loop {
        skip_ws(&chars, &mut pos);
        if pos >= chars.len() {
            return Some(Err(SqlError::Parse {
                detail: "unterminated array of objects".to_string(),
            }));
        }
        if chars[pos] == ']' {
            break;
        }
        if chars[pos] == ',' {
            pos += 1;
            continue;
        }
        if chars[pos] != '{' {
            return Some(Err(SqlError::Parse {
                detail: format!("expected '{{' at position {pos}, found '{}'", chars[pos]),
            }));
        }
        match parse_object(&chars, &mut pos) {
            Ok(obj) => objects.push(obj),
            Err(e) => return Some(Err(e)),
        }
        skip_ws(&chars, &mut pos);
        if pos < chars.len() && chars[pos] == ',' {
            pos += 1;
        }
    }
    Some(Ok(objects))
}
