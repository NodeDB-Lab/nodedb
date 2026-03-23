//! Non-interactive mode: execute SQL from `-e` flag or piped stdin.

use nodedb_client::NativeClient;

use crate::args::OutputFormat;
use crate::error::CliResult;
use crate::format;

/// Execute one or more SQL statements and print formatted output.
pub async fn run(client: &NativeClient, sql: &str, fmt: OutputFormat) -> CliResult<()> {
    for stmt in split_statements(sql) {
        let trimmed = stmt.trim();
        if trimmed.is_empty() {
            continue;
        }
        let result = client.query(trimmed).await?;
        let output = format::format_result(&result, fmt);
        if !output.is_empty() {
            print!("{output}");
        }
    }
    Ok(())
}

/// Split SQL input into individual statements, respecting quoted strings
/// and comments.
///
/// Splits on `;` only when it appears outside of:
/// - Single-quoted strings (`'...'`) with `''` escape support
/// - Double-quoted identifiers (`"..."`)
/// - `--` line comments
fn split_statements(input: &str) -> Vec<&str> {
    let mut statements = Vec::new();
    let mut start = 0;
    let bytes = input.as_bytes();
    let len = bytes.len();
    let mut i = 0;

    while i < len {
        match bytes[i] {
            b'\'' => {
                // Single-quoted string: skip to closing quote.
                // Handle '' escape (doubled single quote).
                i += 1;
                while i < len {
                    if bytes[i] == b'\'' {
                        i += 1;
                        // '' is an escaped quote inside a string, not end.
                        if i < len && bytes[i] == b'\'' {
                            i += 1;
                            continue;
                        }
                        break;
                    }
                    i += 1;
                }
            }
            b'"' => {
                // Double-quoted identifier: skip to closing quote.
                i += 1;
                while i < len {
                    if bytes[i] == b'"' {
                        i += 1;
                        // "" is an escaped quote inside an identifier.
                        if i < len && bytes[i] == b'"' {
                            i += 1;
                            continue;
                        }
                        break;
                    }
                    i += 1;
                }
            }
            b'-' if i + 1 < len && bytes[i + 1] == b'-' => {
                // Line comment: skip to end of line.
                i += 2;
                while i < len && bytes[i] != b'\n' {
                    i += 1;
                }
                if i < len {
                    i += 1; // skip the newline
                }
            }
            b';' => {
                // Statement separator (outside quotes/comments).
                statements.push(&input[start..i]);
                i += 1;
                start = i;
            }
            _ => {
                i += 1;
            }
        }
    }

    // Remaining text after last semicolon.
    if start < len {
        let remaining = input[start..].trim();
        if !remaining.is_empty() {
            statements.push(&input[start..]);
        }
    }

    statements
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn simple_split() {
        let stmts = split_statements("SELECT 1; SELECT 2");
        assert_eq!(stmts.len(), 2);
        assert_eq!(stmts[0].trim(), "SELECT 1");
        assert_eq!(stmts[1].trim(), "SELECT 2");
    }

    #[test]
    fn semicolon_in_single_quotes() {
        let stmts = split_statements("INSERT INTO t VALUES ('a;b;c'); SELECT 1");
        assert_eq!(stmts.len(), 2);
        assert!(stmts[0].contains("'a;b;c'"));
        assert_eq!(stmts[1].trim(), "SELECT 1");
    }

    #[test]
    fn semicolon_in_double_quotes() {
        let stmts = split_statements(r#"SELECT "col;name" FROM t; SELECT 2"#);
        assert_eq!(stmts.len(), 2);
        assert!(stmts[0].contains(r#""col;name""#));
    }

    #[test]
    fn escaped_single_quote() {
        let stmts = split_statements("SELECT 'it''s'; SELECT 2");
        assert_eq!(stmts.len(), 2);
        assert!(stmts[0].contains("'it''s'"));
    }

    #[test]
    fn line_comment() {
        let stmts = split_statements("SELECT 1; -- this; is a comment\nSELECT 2");
        assert_eq!(stmts.len(), 2);
    }

    #[test]
    fn no_trailing_empty() {
        let stmts = split_statements("SELECT 1;");
        assert_eq!(stmts.len(), 1);
        assert_eq!(stmts[0].trim(), "SELECT 1");
    }

    #[test]
    fn empty_input() {
        let stmts = split_statements("");
        assert!(stmts.is_empty());
    }

    #[test]
    fn whitespace_only() {
        let stmts = split_statements("  \n  ");
        assert!(stmts.is_empty());
    }

    #[test]
    fn single_statement_no_semicolon() {
        let stmts = split_statements("SELECT 1");
        assert_eq!(stmts.len(), 1);
        assert_eq!(stmts[0].trim(), "SELECT 1");
    }
}
