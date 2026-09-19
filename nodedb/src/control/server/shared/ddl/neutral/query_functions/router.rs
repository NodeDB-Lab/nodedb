// SPDX-License-Identifier: BUSL-1.1

//! Protocol-neutral routing for the temporal / audit query functions.
//!
//! These are `SELECT <FUNC>(...)` calls that never parse into a typed DDL AST
//! statement — the pgwire router recognized them by substring (`upper.contains`)
//! in its `router::function::dispatch`, after the typed-AST parse gate and the
//! auth family. Replicate that exactly: this router is invoked only from the
//! `None` (non-DDL-parse) branch of the parent neutral router, so any typed DDL
//! statement (or parse error) whose body happens to contain one of these
//! substrings is handled by the typed path first, byte-identically to before.
//! The substring recognition order is preserved verbatim.

use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::state::SharedState;
use crate::types::DatabaseId;

use super::super::super::result::{DdlError, DdlResult};

/// Scan `sql` as code: quoted regions and comments are replaced by spaces so a
/// `contains` check cannot match a keyword inside a string literal, a quoted
/// identifier, or a comment. Everything else is preserved verbatim.
fn scan_code(sql: &str) -> String {
    #[derive(PartialEq)]
    enum St {
        Code,
        Single,
        Double,
        Line,
        Block,
    }

    let mut out = String::with_capacity(sql.len());
    let mut st = St::Code;
    let mut chars = sql.chars().peekable();
    while let Some(c) = chars.next() {
        match st {
            St::Code => match c {
                '\'' => {
                    st = St::Single;
                    out.push(' ');
                }
                '"' => {
                    st = St::Double;
                    out.push(' ');
                }
                '-' if chars.peek() == Some(&'-') => {
                    chars.next();
                    st = St::Line;
                    out.push_str("  ");
                }
                '/' if chars.peek() == Some(&'*') => {
                    chars.next();
                    st = St::Block;
                    out.push_str("  ");
                }
                _ => out.push(c),
            },
            St::Single => {
                if c == '\'' {
                    if chars.peek() == Some(&'\'') {
                        chars.next();
                        out.push_str("  ");
                    } else {
                        st = St::Code;
                        out.push(' ');
                    }
                } else {
                    out.push(' ');
                }
            }
            St::Double => {
                if c == '"' {
                    if chars.peek() == Some(&'"') {
                        chars.next();
                        out.push_str("  ");
                    } else {
                        st = St::Code;
                        out.push(' ');
                    }
                } else {
                    out.push(' ');
                }
            }
            St::Line => {
                if c == '\n' {
                    st = St::Code;
                    out.push('\n');
                } else {
                    out.push(' ');
                }
            }
            St::Block => {
                if c == '*' && chars.peek() == Some(&'/') {
                    chars.next();
                    st = St::Code;
                    out.push_str("  ");
                } else {
                    out.push(' ');
                }
            }
        }
    }
    out
}

/// The query function this statement routes to, if any.
///
/// The routing decision: `scan_code` removes quoted regions and comments, then
/// the keywords are matched in the pgwire `router::function::dispatch` order.
/// Kept separate from [`try_dispatch`] so the decision itself is testable.
fn recognized_function(sql: &str) -> Option<&'static str> {
    const KEYWORDS: [&str; 6] = [
        "VERIFY_AUDIT_CHAIN",
        "VERIFY_HASH_CHAIN",
        "BALANCE_AS_OF",
        "TEMPORAL_LOOKUP",
        "VERIFY_BALANCE",
        "CONVERT_CURRENCY_LOOKUP",
    ];
    let upper = scan_code(sql).to_uppercase();
    KEYWORDS.into_iter().find(|keyword| upper.contains(keyword))
}

/// Try to handle `sql` as one of the temporal / audit query functions.
///
/// Returns `Some(result)` when a substring matches (mirroring the pgwire
/// `router::function::dispatch` contains-checks in the same order), `None`
/// otherwise so the caller falls back to the transitional pgwire delegation.
pub async fn try_dispatch(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    database_id: DatabaseId,
    sql: &str,
) -> Option<Result<Vec<DdlResult>, DdlError>> {
    match recognized_function(sql) {
        Some("VERIFY_AUDIT_CHAIN") => {
            Some(super::verify_audit_chain(state, identity, database_id, sql).await)
        }
        Some("VERIFY_HASH_CHAIN") => {
            Some(super::verify_hash_chain(state, identity, database_id, sql).await)
        }
        Some("BALANCE_AS_OF") => {
            Some(super::balance_as_of(state, identity, database_id, sql).await)
        }
        Some("TEMPORAL_LOOKUP") => {
            Some(super::temporal_lookup(state, identity, database_id, sql).await)
        }
        Some("VERIFY_BALANCE") => {
            Some(super::verify_balance(state, identity, database_id, sql).await)
        }
        Some("CONVERT_CURRENCY_LOOKUP") => {
            Some(super::convert_currency_lookup(state, identity, database_id, sql).await)
        }
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::{recognized_function, scan_code};

    /// The routing decision itself: which function (if any) `try_dispatch`
    /// picks for this statement.
    fn hits(sql: &str, keyword: &str) -> bool {
        recognized_function(sql) == Some(keyword)
    }

    #[test]
    fn scanned_text_blanks_the_literal() {
        assert!(
            !scan_code("SELECT 'verify_balance'")
                .to_ascii_uppercase()
                .contains("VERIFY_BALANCE")
        );
    }

    #[test]
    fn literal_contents_do_not_route() {
        assert!(!hits(
            "INSERT INTO kg (id, name) VALUES ('tmp_verify_balance_x', 'x')",
            "VERIFY_BALANCE"
        ));
        assert!(!hits(
            "INSERT INTO kg (id, label) VALUES ('x', 'verify_balance')",
            "VERIFY_BALANCE"
        ));
    }

    #[test]
    fn escaped_quote_keeps_the_literal_open() {
        assert!(!hits(
            "INSERT INTO kg (id) VALUES ('it''s verify_balance')",
            "VERIFY_BALANCE"
        ));
    }

    #[test]
    fn quoted_identifiers_do_not_route() {
        assert!(!hits(
            "SELECT 1 AS \"verify_balance\" FROM kg",
            "VERIFY_BALANCE"
        ));
    }

    #[test]
    fn comments_do_not_route() {
        assert!(!hits("SELECT 1 -- verify_balance note", "VERIFY_BALANCE"));
        assert!(!hits("SELECT /* verify_balance */ 1", "VERIFY_BALANCE"));
        assert!(!hits(
            "SELECT 1 -- verify_audit_chain\n",
            "VERIFY_AUDIT_CHAIN"
        ));
    }

    #[test]
    fn real_calls_still_route() {
        assert!(hits("SELECT VERIFY_BALANCE('c', 'col')", "VERIFY_BALANCE"));
        assert!(hits("select verify_balance('c','col')", "VERIFY_BALANCE"));
        assert!(hits(
            "SELECT VERIFY_AUDIT_CHAIN(1, 100)",
            "VERIFY_AUDIT_CHAIN"
        ));
        assert!(hits("SELECT balance_as_of('c','k','v',1)", "BALANCE_AS_OF"));
    }

    #[test]
    fn blanking_does_not_fuse_neighbouring_tokens() {
        // Spaces replace the quoted region, so the halves stay separate. A
        // removal-style blank would glue VERIFY and _BALANCE into a match.
        assert!(!hits("SELECT VERIFY'x'_BALANCE()", "VERIFY_BALANCE"));
        assert!(!hits("SELECT 'verify'_balance()", "VERIFY_BALANCE"));
    }

    #[test]
    fn escapes_and_unterminated_regions_do_not_route() {
        // `""` inside a quoted identifier stays in that identifier.
        assert!(!hits(
            "SELECT 1 AS \"a\"\"verify_balance\" FROM kg",
            "VERIFY_BALANCE"
        ));
        // An unterminated block comment swallows the rest of the statement.
        assert!(!hits("SELECT 1 /* verify_balance", "VERIFY_BALANCE"));
        // So does an unterminated literal.
        assert!(!hits("SELECT 'verify_balance", "VERIFY_BALANCE"));
    }

    #[test]
    fn routing_decision_picks_the_first_keyword_in_order() {
        assert_eq!(
            recognized_function("SELECT VERIFY_AUDIT_CHAIN(1, 2) -- VERIFY_BALANCE"),
            Some("VERIFY_AUDIT_CHAIN")
        );
        assert_eq!(recognized_function("SELECT 1"), None);
        assert_eq!(
            recognized_function("INSERT INTO kg { id: 'a', name: 'balance_as_of' }"),
            None
        );
    }
}
