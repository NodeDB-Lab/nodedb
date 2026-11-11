// SPDX-License-Identifier: Apache-2.0

//! Detection of a multi-statement text that uses a rewrite extension.

use super::lex::{SqlSegment, first_sql_word, has_brace_outside_literals, segments};
use super::temporal::extract as extract_temporal;
use crate::error::SqlError;

/// `Some(statements)` when `sql` uses a NodeDB rewrite extension and holds
/// more than one top-level statement.
///
/// A caller plans each statement of a request on its own, so one of these texts
/// is a caller error. The count never parses the SQL: `INSERT INTO docs { .. };
/// SELECT 1` does not parse, and the caller must be told it is unsupported
/// rather than shown a syntax error.
pub fn rewritten_batch(sql: &str) -> Result<Option<usize>, SqlError> {
    let statements = top_level_statements(sql);
    if statements < 2 {
        return Ok(None);
    }
    let trimmed = sql.trim();
    let uses_upsert =
        first_sql_word(trimmed).is_some_and(|word| word.eq_ignore_ascii_case("UPSERT"));
    let uses_object_literal = has_brace_outside_literals(trimmed);
    let uses_temporal = extract_temporal(trimmed)
        .map_err(|e| SqlError::Parse { detail: e.0 })?
        .is_some();
    Ok((uses_upsert || uses_object_literal || uses_temporal).then_some(statements))
}

/// How many non-empty statements `sql` holds at top level.
///
/// A semicolon inside a literal, a quoted identifier or a comment does not
/// separate statements, so those segments are skipped.
fn top_level_statements(sql: &str) -> usize {
    let mut count = 0;
    let mut holds_content = false;
    for segment in segments(sql) {
        match segment {
            SqlSegment::Text(text) => {
                for c in text.chars() {
                    if c == ';' {
                        if holds_content {
                            count += 1;
                            holds_content = false;
                        }
                    } else if !c.is_whitespace() {
                        holds_content = true;
                    }
                }
            }
            SqlSegment::SingleQuotedString(_) | SqlSegment::QuotedIdent(_) => holds_content = true,
            SqlSegment::LineComment(_) | SqlSegment::BlockComment(_) => {}
        }
    }
    if holds_content {
        count += 1;
    }
    count
}
