// SPDX-License-Identifier: BUSL-1.1

//! SQL statement splitter for multi-statement requests.

/// One statement of a split request text.
///
/// A transport splits the request text once. Admission and the sequential
/// runner read the carried statements and never split again.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Statement {
    /// The statement, trimmed, with no trailing semicolon.
    pub text: String,
    /// The byte offset in the request text of the first character of the
    /// statement that is not whitespace.
    pub start: usize,
}

impl Statement {
    /// The 1-based character offset in `sql` where the statement starts.
    /// `sql` is the request text the statement was split from. `None` when
    /// `start` is not a boundary of `sql`.
    pub fn char_position(&self, sql: &str) -> Option<usize> {
        sql.get(..self.start)
            .map(|prefix| prefix.chars().count() + 1)
    }
}

/// The texts of `statements`, in order.
pub(crate) fn statement_texts(statements: &[Statement]) -> Vec<String> {
    statements
        .iter()
        .map(|statement| statement.text.clone())
        .collect()
}

/// Split a SQL string at top-level semicolons into individual statements.
///
/// Respects single-quoted strings, `E'..'` strings (a backslash escapes the
/// next character), double-quoted identifiers, dollar-quoted blocks with an
/// optional tag, line comments, and nested block comments. Does NOT split at
/// semicolons inside any of them. Empty statements (whitespace only) are
/// discarded.
///
/// This handles the case where psql sends multiple statements in one simple
/// query message (e.g. heredoc input), ensuring `parts[2]` in DDL handlers
/// never contains a trailing semicolon.
pub(crate) fn split_sql_statements(sql: &str) -> Vec<String> {
    statement_texts(&split_statements(sql))
}

/// The 1-based character offset in `sql` where statement `statement` starts.
///
/// `statement` is 0-based, in [`split_statements`] order. A statement
/// starts at its first character that is not whitespace. A leading comment
/// belongs to the statement. `None` when `sql` holds fewer statements.
pub(crate) fn statement_position(sql: &str, statement: usize) -> Option<usize> {
    split_statements(sql)
        .get(statement)
        .and_then(|found| found.char_position(sql))
}

/// Split `sql` at top-level semicolons. Each [`Statement`] holds the trimmed
/// text and the byte offset in `sql` where it starts.
///
/// `current` holds every byte of `sql` from `current_start` up to the
/// splitting semicolon. So the next statement begins one byte past it.
pub(crate) fn split_statements(sql: &str) -> Vec<Statement> {
    let mut stmts = Vec::new();
    let mut current = String::new();
    let mut current_start = 0usize;
    let mut chars = sql.chars().peekable();
    let mut in_single_quote = false;
    let mut in_escape_string = false;
    let mut in_double_quote = false;
    let mut dollar_open: Option<String> = None;
    let mut in_line_comment = false;
    let mut block_depth = 0usize;
    let mut in_procedural_body = false;
    let mut saw_outer_end = false;
    let mut pending_word = String::new();
    let mut prev = '\0';

    fn starts_procedural_ddl(stmt: &str) -> bool {
        let upper = stmt.trim_start().to_uppercase();
        upper.starts_with("CREATE TRIGGER ")
            || upper.starts_with("CREATE OR REPLACE TRIGGER ")
            || upper.starts_with("CREATE SYNC TRIGGER ")
            || upper.starts_with("CREATE OR REPLACE SYNC TRIGGER ")
            || upper.starts_with("CREATE DEFERRED TRIGGER ")
            || upper.starts_with("CREATE OR REPLACE DEFERRED TRIGGER ")
            || upper.starts_with("CREATE FUNCTION ")
            || upper.starts_with("CREATE OR REPLACE FUNCTION ")
            || upper.starts_with("CREATE PROCEDURE ")
            || upper.starts_with("CREATE OR REPLACE PROCEDURE ")
            || upper.starts_with("CREATE SCHEDULE ")
    }

    fn flush_word(
        word: &mut String,
        current: &str,
        in_procedural_body: &mut bool,
        saw_outer_end: &mut bool,
    ) {
        if word.is_empty() {
            return;
        }

        let upper = word.to_uppercase();
        if *in_procedural_body {
            if upper == "END" {
                *saw_outer_end = true;
            } else if upper == "IF" || upper == "LOOP" {
                if *saw_outer_end {
                    *saw_outer_end = false;
                }
            } else if *saw_outer_end {
                *saw_outer_end = false;
            }
        } else if upper == "BEGIN" && starts_procedural_ddl(current) {
            *in_procedural_body = true;
            *saw_outer_end = false;
        }

        word.clear();
    }

    while let Some(ch) = chars.next() {
        // Track line comments (--).
        if !in_single_quote && !in_double_quote && block_depth == 0 && dollar_open.is_none() {
            if ch == '-' && chars.peek() == Some(&'-') {
                flush_word(
                    &mut pending_word,
                    &current,
                    &mut in_procedural_body,
                    &mut saw_outer_end,
                );
                in_line_comment = true;
            }
            if ch == '/' && chars.peek() == Some(&'*') {
                flush_word(
                    &mut pending_word,
                    &current,
                    &mut in_procedural_body,
                    &mut saw_outer_end,
                );
                block_depth = 1;
                current.push(ch);
                current.push(
                    chars
                        .next()
                        .expect("invariant: peek() returned Some('*'), so next() will succeed"),
                );
                prev = '\0';
                continue;
            }
        }
        if in_line_comment {
            if ch == '\n' {
                in_line_comment = false;
            }
            current.push(ch);
            prev = ch;
            continue;
        }
        if block_depth > 0 {
            current.push(ch);
            if prev == '*' && ch == '/' {
                block_depth -= 1;
                prev = '\0';
            } else if prev == '/' && ch == '*' {
                block_depth += 1;
                prev = '\0';
            } else {
                prev = ch;
            }
            continue;
        }

        // Dollar quoting: `$tag$ ... $tag$` with an optional tag. A quote
        // closes only on its own tag. `$1` and `a$b$c` open nothing.
        if ch == '$' && !in_single_quote && !in_double_quote {
            let found = dollar_tag(&chars);
            let closes = matches!((&dollar_open, &found), (Some(open), Some(tag)) if open == tag);
            let opens = dollar_open.is_none()
                && found.is_some()
                && !(prev.is_alphanumeric() || prev == '_' || prev == '$');
            if let Some(tag) = found
                && (closes || opens)
            {
                flush_word(
                    &mut pending_word,
                    &current,
                    &mut in_procedural_body,
                    &mut saw_outer_end,
                );
                current.push(ch);
                for _ in 0..=tag.chars().count() {
                    current.extend(chars.next());
                }
                dollar_open = if closes { None } else { Some(tag) };
                prev = '\0';
                continue;
            }
        }

        // A backslash escapes the next character inside an `E'..'` string.
        if ch == '\\' && in_single_quote && in_escape_string {
            current.push(ch);
            prev = ch;
            if let Some(next) = chars.next() {
                current.push(next);
                prev = next;
            }
            continue;
        }

        match ch {
            '\'' if !in_double_quote && dollar_open.is_none() => {
                if !in_single_quote {
                    // An `E` word directly before the quote opens an escape
                    // string. A quote right after a closing quote is a doubled
                    // quote and keeps the flag of the string it reopens.
                    in_escape_string = if prev == '\'' {
                        in_escape_string
                    } else {
                        matches!(pending_word.as_str(), "e" | "E")
                    };
                }
                flush_word(
                    &mut pending_word,
                    &current,
                    &mut in_procedural_body,
                    &mut saw_outer_end,
                );
                in_single_quote = !in_single_quote;
                current.push(ch);
            }
            '"' if !in_single_quote && dollar_open.is_none() => {
                flush_word(
                    &mut pending_word,
                    &current,
                    &mut in_procedural_body,
                    &mut saw_outer_end,
                );
                in_double_quote = !in_double_quote;
                current.push(ch);
            }
            ';' if !in_single_quote && !in_double_quote && dollar_open.is_none() => {
                flush_word(
                    &mut pending_word,
                    &current,
                    &mut in_procedural_body,
                    &mut saw_outer_end,
                );
                if in_procedural_body && !saw_outer_end {
                    current.push(ch);
                } else {
                    push_trimmed(&mut stmts, current_start, &current);
                    current_start += current.len() + ch.len_utf8();
                    current.clear();
                    in_procedural_body = false;
                    saw_outer_end = false;
                }
            }
            _ => {
                if !in_single_quote && !in_double_quote {
                    if ch.is_ascii_alphanumeric() || ch == '_' {
                        pending_word.push(ch);
                    } else {
                        flush_word(
                            &mut pending_word,
                            &current,
                            &mut in_procedural_body,
                            &mut saw_outer_end,
                        );
                    }
                }
                current.push(ch);
            }
        }
        prev = ch;
    }
    flush_word(
        &mut pending_word,
        &current,
        &mut in_procedural_body,
        &mut saw_outer_end,
    );
    // Trailing statement without a semicolon.
    push_trimmed(&mut stmts, current_start, &current);
    stmts
}

/// Push `current` trimmed, with the byte offset of its first character that
/// is not whitespace. `start` is the offset of `current` in the request text.
/// An empty statement is discarded.
fn push_trimmed(stmts: &mut Vec<Statement>, start: usize, current: &str) {
    let trimmed = current.trim();
    if trimmed.is_empty() {
        return;
    }
    let leading = current.len() - current.trim_start().len();
    stmts.push(Statement {
        text: trimmed.to_string(),
        start: start + leading,
    });
}

/// The tag of a dollar quote delimiter. `rest` starts just after a `$`.
/// Returns `Some("")` for `$$`, `Some("t")` for `$t$`, and `None` when the
/// `$` starts no delimiter (`$1`, `$ `). Does not advance `rest`.
fn dollar_tag(rest: &std::iter::Peekable<std::str::Chars<'_>>) -> Option<String> {
    let mut probe = rest.clone();
    let mut tag = String::new();
    loop {
        match probe.next()? {
            '$' => return Some(tag),
            c if c.is_alphabetic() || c == '_' || (!tag.is_empty() && c.is_alphanumeric()) => {
                tag.push(c)
            }
            _ => return None,
        }
    }
}

/// Whether [`split_statements`] splits `sql` into more than one statement.
pub(crate) fn holds_several_statements(sql: &str) -> bool {
    split_statements(sql).len() > 1
}

#[cfg(test)]
#[path = "sql_split_tests.rs"]
mod tests;
