// SPDX-License-Identifier: BUSL-1.1

//! SQL statement splitter for multi-statement requests.

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
    let mut stmts = Vec::new();
    let mut current = String::new();
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
                    let trimmed = current.trim().to_string();
                    if !trimmed.is_empty() {
                        stmts.push(trimmed);
                    }
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
    let trimmed = current.trim().to_string();
    if !trimmed.is_empty() {
        stmts.push(trimmed);
    }
    stmts
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

#[cfg(test)]
mod tests {
    use super::split_sql_statements;

    #[test]
    fn single_statement_no_semicolon() {
        let stmts = split_sql_statements("SELECT 1");
        assert_eq!(stmts, vec!["SELECT 1"]);
    }

    #[test]
    fn single_statement_with_semicolon() {
        let stmts = split_sql_statements("CREATE COLLECTION docs;");
        assert_eq!(stmts, vec!["CREATE COLLECTION docs"]);
    }

    #[test]
    fn multi_statement_heredoc() {
        let sql = "CREATE COLLECTION docs;\nINSERT INTO docs (x) VALUES (1);\nSELECT * FROM docs;";
        let stmts = split_sql_statements(sql);
        assert_eq!(stmts.len(), 3);
        assert_eq!(stmts[0], "CREATE COLLECTION docs");
        assert_eq!(stmts[1], "INSERT INTO docs (x) VALUES (1)");
        assert_eq!(stmts[2], "SELECT * FROM docs");
    }

    #[test]
    fn semicolon_inside_string_not_split() {
        let sql = "INSERT INTO t (v) VALUES ('a;b');";
        let stmts = split_sql_statements(sql);
        assert_eq!(stmts.len(), 1);
        assert_eq!(stmts[0], "INSERT INTO t (v) VALUES ('a;b')");
    }

    #[test]
    fn empty_statements_discarded() {
        let stmts = split_sql_statements(";;  ;SELECT 1;;");
        assert_eq!(stmts, vec!["SELECT 1"]);
    }

    #[test]
    fn procedural_begin_end_not_split() {
        let sql = "CREATE FUNCTION f(x INT) RETURNS TEXT AS \
                    BEGIN IF x > 0 THEN RETURN 'pos'; ELSE RETURN 'neg'; END IF; END";
        let stmts = split_sql_statements(sql);
        assert_eq!(stmts.len(), 1);
        assert!(stmts[0].starts_with("CREATE FUNCTION"));
        assert!(stmts[0].ends_with("END"));
    }

    #[test]
    fn procedural_with_trailing_semicolon() {
        let sql = "CREATE FUNCTION f(x INT) RETURNS INT AS \
                    BEGIN RETURN x; END;";
        let stmts = split_sql_statements(sql);
        assert_eq!(stmts.len(), 1);
        assert!(stmts[0].contains("BEGIN"));
        assert!(stmts[0].ends_with("END"));
    }

    #[test]
    fn trigger_body_in_batch_splits_correctly() {
        let sql = "CREATE COLLECTION after_src;\n\
                   CREATE COLLECTION after_log;\n\
                   CREATE TRIGGER log_insert AFTER INSERT ON after_src FOR EACH ROW\n\
                   BEGIN\n\
                       INSERT INTO after_log (id, src_id, action)\n\
                       VALUES (NEW.id || '_log', NEW.id, 'inserted');\n\
                   END;\n\
                   INSERT INTO after_src (id, name, val) VALUES ('as1', 'Alpha', 10);\n\
                   SELECT * FROM after_log;";
        let stmts = split_sql_statements(sql);
        assert_eq!(stmts.len(), 5);
        assert!(stmts[2].starts_with("CREATE TRIGGER log_insert"));
        assert!(stmts[2].contains("VALUES (NEW.id || '_log', NEW.id, 'inserted');"));
        assert!(stmts[2].ends_with("END"));
        assert_eq!(
            stmts[3],
            "INSERT INTO after_src (id, name, val) VALUES ('as1', 'Alpha', 10)"
        );
        assert_eq!(stmts[4], "SELECT * FROM after_log");
    }

    #[test]
    fn trigger_body_with_if_block_remains_single_statement() {
        let sql = "CREATE TRIGGER normalize BEFORE INSERT ON users FOR EACH ROW \
                   BEGIN \
                     IF NEW.active = TRUE THEN \
                       INSERT INTO audit (id) VALUES (NEW.id); \
                     END IF; \
                   END; \
                   SELECT 1;";
        let stmts = split_sql_statements(sql);
        assert_eq!(stmts.len(), 2);
        assert!(stmts[0].contains("END IF;"));
        assert!(stmts[0].ends_with("END"));
        assert_eq!(stmts[1], "SELECT 1");
    }

    #[test]
    fn sync_trigger_body_in_batch_splits_correctly() {
        let sql = "CREATE SYNC TRIGGER log_insert AFTER INSERT ON after_src FOR EACH ROW \
                   BEGIN \
                     INSERT INTO after_log (id) VALUES (NEW.id); \
                   END; \
                   SELECT 1;";
        let stmts = split_sql_statements(sql);
        assert_eq!(stmts.len(), 2);
        assert!(stmts[0].starts_with("CREATE SYNC TRIGGER log_insert"));
        assert!(stmts[0].contains("INSERT INTO after_log (id) VALUES (NEW.id);"));
        assert!(stmts[0].ends_with("END"));
        assert_eq!(stmts[1], "SELECT 1");
    }
}
