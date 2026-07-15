// SPDX-License-Identifier: Apache-2.0

//! Parse `CREATE SYNONYM GROUP`, `DROP SYNONYM GROUP`, and `SHOW SYNONYM GROUPS`.
//!
//! Syntax:
//! - `CREATE SYNONYM GROUP <name> AS ('term1', 'term2', ...)`
//! - `DROP SYNONYM GROUP [IF EXISTS] <name>`
//! - `SHOW SYNONYM GROUPS`

use crate::ddl_ast::statement::{NodedbStatement, PolicyStmt};
use crate::error::SqlError;
use crate::parser::preprocess::lex::find_ascii_keyword;

/// Try to parse synonym group DDL statements.
pub(super) fn try_parse(
    upper: &str,
    parts: &[&str],
    trimmed: &str,
) -> Option<Result<NodedbStatement, SqlError>> {
    if upper.starts_with("CREATE SYNONYM GROUP ") {
        return Some(parse_create(parts, trimmed));
    }
    if upper.starts_with("DROP SYNONYM GROUP ") {
        return Some(parse_drop(parts));
    }
    if upper == "SHOW SYNONYM GROUPS" {
        return Some(Ok(NodedbStatement::Policy(PolicyStmt::ShowSynonymGroups)));
    }
    None
}

/// Parse `CREATE SYNONYM GROUP <name> AS ('term1', 'term2', ...)`.
fn parse_create(parts: &[&str], trimmed: &str) -> Result<NodedbStatement, SqlError> {
    // parts: [CREATE, SYNONYM, GROUP, <name>, AS, ...]
    let name = parts.get(3).ok_or_else(|| SqlError::Parse {
        detail: "syntax: CREATE SYNONYM GROUP <name> AS ('term1', ...)".to_string(),
    })?;

    let as_idx = parts
        .iter()
        .position(|p| p.eq_ignore_ascii_case("AS"))
        .ok_or_else(|| SqlError::Parse {
            detail: "CREATE SYNONYM GROUP: missing AS keyword".to_string(),
        })?;

    // Everything after AS is the term list in parentheses.
    let after_as = parts[as_idx + 1..].join(" ");
    let terms = parse_term_list(&after_as, trimmed)?;

    if terms.is_empty() {
        return Err(SqlError::Parse {
            detail: "CREATE SYNONYM GROUP: term list must not be empty".to_string(),
        });
    }

    // Reject duplicate terms.
    let mut seen = std::collections::HashSet::new();
    for term in &terms {
        if !seen.insert(term.as_str()) {
            return Err(SqlError::Parse {
                detail: format!("CREATE SYNONYM GROUP: duplicate term '{term}'"),
            });
        }
    }

    Ok(NodedbStatement::Policy(PolicyStmt::CreateSynonymGroup {
        name: name.to_lowercase(),
        terms,
    }))
}

/// Parse `DROP SYNONYM GROUP [IF EXISTS] <name>`.
fn parse_drop(parts: &[&str]) -> Result<NodedbStatement, SqlError> {
    // parts: [DROP, SYNONYM, GROUP, ...]
    // Optional IF EXISTS:
    let (if_exists, name_idx) = if parts.len() >= 6
        && parts[3].eq_ignore_ascii_case("IF")
        && parts[4].eq_ignore_ascii_case("EXISTS")
    {
        (true, 5)
    } else {
        (false, 3)
    };

    let name = parts.get(name_idx).ok_or_else(|| SqlError::Parse {
        detail: "syntax: DROP SYNONYM GROUP [IF EXISTS] <name>".to_string(),
    })?;

    Ok(NodedbStatement::Policy(PolicyStmt::DropSynonymGroup {
        name: name.to_lowercase(),
        if_exists,
    }))
}

/// Parse a `('term1', 'term2', ...)` term list from the raw fragment after AS.
///
/// Extracts all single-quoted strings within the outer parentheses.
fn parse_term_list(_fragment: &str, original: &str) -> Result<Vec<String>, SqlError> {
    // Find the AS keyword in the original to get the parenthesised region.
    // We use a simple scan: find '(' and matching ')'.
    let as_pos = find_ascii_keyword(original, "AS").ok_or_else(|| SqlError::Parse {
        detail: "CREATE SYNONYM GROUP: missing AS keyword".to_string(),
    })?;

    let s = original[as_pos + "AS".len()..].trim_start();

    let open = s.find('(').ok_or_else(|| SqlError::Parse {
        detail: "CREATE SYNONYM GROUP: term list must start with '('".to_string(),
    })?;
    let close = s.rfind(')').ok_or_else(|| SqlError::Parse {
        detail: "CREATE SYNONYM GROUP: term list must end with ')'".to_string(),
    })?;

    if close <= open {
        return Err(SqlError::Parse {
            detail: "CREATE SYNONYM GROUP: malformed term list".to_string(),
        });
    }

    let inner = &s[open + 1..close];
    if inner.trim().is_empty() {
        return Ok(Vec::new());
    }

    let mut terms = Vec::new();
    // Split on commas outside of quotes.
    let mut in_quote = false;
    let mut current = String::new();
    for ch in inner.chars() {
        match ch {
            '\'' if !in_quote => {
                in_quote = true;
            }
            '\'' if in_quote => {
                in_quote = false;
            }
            ',' if !in_quote => {
                let t = current.trim().to_lowercase();
                if !t.is_empty() {
                    terms.push(t);
                }
                current.clear();
            }
            _ if in_quote => {
                current.push(ch);
            }
            _ => {}
        }
    }
    // Last term.
    let t = current.trim().to_lowercase();
    if !t.is_empty() {
        terms.push(t);
    }

    // Remove empty strings that might appear from bare commas.
    let terms: Vec<String> = terms.into_iter().filter(|t| !t.is_empty()).collect();
    Ok(terms)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ddl_ast::parse::dispatch::parse;

    fn ok(sql: &str) -> NodedbStatement {
        parse(sql).expect("expected Some").expect("expected Ok")
    }

    fn err(sql: &str) -> SqlError {
        parse(sql)
            .expect("expected Some")
            .expect_err("expected Err")
    }

    #[test]
    fn create_unicode_group_name_preserves_original_offsets() {
        let stmt = ok("CREATE SYNONYM GROUP termsﬀﬀ AS ('database', 'db')");
        match stmt {
            NodedbStatement::Policy(PolicyStmt::CreateSynonymGroup { name, terms }) => {
                assert_eq!(name, "termsﬀﬀ");
                assert_eq!(terms, vec!["database", "db"]);
            }
            other => panic!("unexpected {other:?}"),
        }
    }

    #[test]
    fn create_basic() {
        let stmt = ok("CREATE SYNONYM GROUP db_terms AS ('database', 'db', 'datastore')");
        match stmt {
            NodedbStatement::Policy(PolicyStmt::CreateSynonymGroup { name, terms }) => {
                assert_eq!(name, "db_terms");
                assert_eq!(terms, vec!["database", "db", "datastore"]);
            }
            other => panic!("unexpected {other:?}"),
        }
    }

    #[test]
    fn create_name_lowercased() {
        let stmt = ok("CREATE SYNONYM GROUP MyGroup AS ('foo', 'bar')");
        match stmt {
            NodedbStatement::Policy(PolicyStmt::CreateSynonymGroup { name, .. }) => {
                assert_eq!(name, "mygroup");
            }
            other => panic!("unexpected {other:?}"),
        }
    }

    #[test]
    fn create_empty_list_rejected() {
        let e = err("CREATE SYNONYM GROUP foo AS ()");
        assert!(matches!(e, SqlError::Parse { .. }));
    }

    #[test]
    fn create_duplicate_term_rejected() {
        let e = err("CREATE SYNONYM GROUP foo AS ('db', 'db', 'database')");
        assert!(matches!(e, SqlError::Parse { .. }));
        assert!(e.to_string().contains("duplicate"));
    }

    #[test]
    fn drop_basic() {
        let stmt = ok("DROP SYNONYM GROUP db_terms");
        assert_eq!(
            stmt,
            NodedbStatement::Policy(PolicyStmt::DropSynonymGroup {
                name: "db_terms".to_string(),
                if_exists: false,
            })
        );
    }

    #[test]
    fn drop_if_exists() {
        let stmt = ok("DROP SYNONYM GROUP IF EXISTS db_terms");
        assert_eq!(
            stmt,
            NodedbStatement::Policy(PolicyStmt::DropSynonymGroup {
                name: "db_terms".to_string(),
                if_exists: true,
            })
        );
    }

    #[test]
    fn show_synonym_groups() {
        let stmt = ok("SHOW SYNONYM GROUPS");
        assert_eq!(stmt, NodedbStatement::Policy(PolicyStmt::ShowSynonymGroups));
    }

    #[test]
    fn terms_case_folded() {
        let stmt = ok("CREATE SYNONYM GROUP g AS ('DB', 'Database', 'DataStore')");
        match stmt {
            NodedbStatement::Policy(PolicyStmt::CreateSynonymGroup { terms, .. }) => {
                assert_eq!(terms, vec!["db", "database", "datastore"]);
            }
            other => panic!("unexpected {other:?}"),
        }
    }
}
