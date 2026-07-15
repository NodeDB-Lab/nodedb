// SPDX-License-Identifier: BUSL-1.1

//! Parser utility functions: keyword search, comma splitting, etc.

use nodedb_sql::parser::preprocess::lex::find_ascii_case_insensitive_from;

/// Find a keyword at top level (not inside parentheses/brackets/braces).
pub(super) fn find_top_level_keyword(text: &str, keyword: &str) -> Option<usize> {
    let mut depth = 0i32;
    let keyword_len = keyword.len();
    let bytes = text.as_bytes();

    for index in 0..text.len().saturating_sub(keyword_len.saturating_sub(1)) {
        match bytes[index] {
            b'(' | b'[' | b'{' => depth += 1,
            b')' | b']' | b'}' => depth -= 1,
            _ => {}
        }

        if depth == 0 && find_ascii_case_insensitive_from(text, keyword, index) == Some(index) {
            let before_ok = index == 0 || bytes[index - 1].is_ascii_whitespace();
            let after = index + keyword_len;
            let after_ok =
                after >= text.len() || bytes[after].is_ascii_whitespace() || bytes[after] == b'(';
            if before_ok && after_ok {
                return Some(index);
            }
        }
    }
    None
}

/// Find `IN 'collection'` clause position in MATCH text.
///
/// Accepts `)` or whitespace before `IN` (unlike `find_top_level_keyword`
/// which only accepts whitespace). Only matches at top-level (depth == 0).
pub(super) fn find_in_clause(text: &str) -> Option<usize> {
    let mut depth = 0i32;
    let bytes = text.as_bytes();
    let len = text.len();

    for index in 0..len.saturating_sub(1) {
        match bytes[index] {
            b'(' | b'[' | b'{' => depth += 1,
            b')' | b']' | b'}' => depth -= 1,
            _ => {}
        }

        if depth == 0
            && find_ascii_case_insensitive_from(text, "IN", index) == Some(index)
            && (index == 0 || bytes[index - 1].is_ascii_whitespace() || bytes[index - 1] == b')')
            && (index + 2 >= len || bytes[index + 2].is_ascii_whitespace())
        {
            return Some(index);
        }
    }
    None
}

/// Find the next MATCH or OPTIONAL MATCH keyword in text.
pub(super) fn find_next_match_keyword(text: &str) -> Option<usize> {
    let trimmed_offset = text.len() - text.trim_start().len();
    let search = &text[trimmed_offset..];

    for (index, _) in search.char_indices() {
        if index == 0 {
            continue;
        }
        if (find_ascii_case_insensitive_from(search, "OPTIONAL MATCH", index) == Some(index)
            || find_ascii_case_insensitive_from(search, "MATCH", index) == Some(index))
            && search
                .as_bytes()
                .get(index.wrapping_sub(1))
                .is_none_or(|byte| byte.is_ascii_whitespace())
        {
            return Some(trimmed_offset + index);
        }
    }
    None
}

/// Split text by commas at top level (not inside parentheses/brackets).
pub(super) fn split_top_level_commas(text: &str) -> Vec<&str> {
    let mut parts = Vec::new();
    let mut depth = 0i32;
    let mut start = 0;

    for (index, character) in text.char_indices() {
        match character {
            '(' | '[' | '{' => depth += 1,
            ')' | ']' | '}' => depth -= 1,
            ',' if depth == 0 => {
                parts.push(&text[start..index]);
                start = index + 1;
            }
            _ => {}
        }
    }
    parts.push(&text[start..]);
    parts
}

/// Split text by AND at top level.
pub(super) fn split_by_and(text: &str) -> Vec<&str> {
    let mut parts = Vec::new();
    let mut start = 0;
    let mut depth = 0i32;

    for (index, character) in text.char_indices() {
        match character {
            '(' | '[' | '{' => depth += 1,
            ')' | ']' | '}' => depth -= 1,
            _ => {}
        }

        if depth == 0 && find_ascii_case_insensitive_from(text, " AND ", index) == Some(index) {
            parts.push(&text[start..index]);
            start = index + 5;
        }
    }
    parts.push(&text[start..]);
    parts
}
