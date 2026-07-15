// SPDX-License-Identifier: Apache-2.0

//! Parse `CREATE TYPE`, `DROP TYPE`, `ALTER TYPE`, and `SHOW TYPES`.
//!
//! Syntax:
//! - `CREATE TYPE <name> AS ENUM ('label1', 'label2', ...)`
//! - `CREATE TYPE <name> AS (<field1> <type1>, <field2> <type2>, ...)`
//! - `DROP TYPE [IF EXISTS] <name>`
//! - `ALTER TYPE <name> ADD VALUE 'label'`
//! - `SHOW TYPES`

use crate::ddl_ast::statement::{NodedbStatement, PolicyStmt};
use crate::error::SqlError;
use crate::parser::preprocess::lex::find_ascii_case_insensitive;

/// Try to parse custom type DDL statements.
pub(super) fn try_parse(
    upper: &str,
    parts: &[&str],
    trimmed: &str,
) -> Option<Result<NodedbStatement, SqlError>> {
    if upper.starts_with("CREATE TYPE ") {
        return Some(parse_create(parts, trimmed));
    }
    if upper.starts_with("DROP TYPE ") || upper == "DROP TYPE" {
        return Some(parse_drop(parts));
    }
    if upper.starts_with("ALTER TYPE ") {
        return Some(parse_alter(parts, trimmed));
    }
    if upper == "SHOW TYPES" {
        return Some(Ok(NodedbStatement::Policy(PolicyStmt::ShowTypes)));
    }
    None
}

/// Parse `CREATE TYPE <name> AS ENUM (...)` or `CREATE TYPE <name> AS (...)`.
fn parse_create(parts: &[&str], trimmed: &str) -> Result<NodedbStatement, SqlError> {
    // parts: [CREATE, TYPE, <name>, AS, ...]
    let name = parts.get(2).ok_or_else(|| SqlError::Parse {
        detail: "syntax: CREATE TYPE <name> AS ENUM (...) | AS (<f> <t>, ...)".to_string(),
    })?;

    let as_pos = parts
        .iter()
        .position(|p| p.eq_ignore_ascii_case("AS"))
        .ok_or_else(|| SqlError::Parse {
            detail: "CREATE TYPE: missing AS keyword".to_string(),
        })?;

    // What follows AS?
    let after_as = parts.get(as_pos + 1).copied().unwrap_or("").to_uppercase();

    if after_as == "ENUM" {
        let labels = parse_label_list(trimmed)?;
        if labels.is_empty() {
            return Err(SqlError::Parse {
                detail: "CREATE TYPE … AS ENUM: label list must not be empty".to_string(),
            });
        }
        // Reject duplicates.
        let mut seen = std::collections::HashSet::new();
        for label in &labels {
            if !seen.insert(label.as_str()) {
                return Err(SqlError::Parse {
                    detail: format!("CREATE TYPE … AS ENUM: duplicate label '{label}'"),
                });
            }
        }
        Ok(NodedbStatement::Policy(PolicyStmt::CreateEnumType {
            name: name.to_lowercase(),
            labels,
        }))
    } else {
        // Composite: AS (<field> <type>, ...)
        let fields = parse_composite_fields(trimmed)?;
        if fields.is_empty() {
            return Err(SqlError::Parse {
                detail: "CREATE TYPE … AS: composite field list must not be empty".to_string(),
            });
        }
        Ok(NodedbStatement::Policy(PolicyStmt::CreateCompositeType {
            name: name.to_lowercase(),
            fields,
        }))
    }
}

/// Parse `DROP TYPE [IF EXISTS] <name>`.
fn parse_drop(parts: &[&str]) -> Result<NodedbStatement, SqlError> {
    // parts: [DROP, TYPE, ...]
    let (if_exists, name_idx) = if parts.len() >= 5
        && parts[2].eq_ignore_ascii_case("IF")
        && parts[3].eq_ignore_ascii_case("EXISTS")
    {
        (true, 4)
    } else {
        (false, 2)
    };

    let name = parts.get(name_idx).ok_or_else(|| SqlError::Parse {
        detail: "syntax: DROP TYPE [IF EXISTS] <name>".to_string(),
    })?;

    Ok(NodedbStatement::Policy(PolicyStmt::DropType {
        name: name.to_lowercase(),
        if_exists,
    }))
}

/// Parse `ALTER TYPE <name> ADD VALUE 'label'`.
fn parse_alter(parts: &[&str], trimmed: &str) -> Result<NodedbStatement, SqlError> {
    // parts: [ALTER, TYPE, <name>, ADD, VALUE, ...]
    let name = parts.get(2).ok_or_else(|| SqlError::Parse {
        detail: "syntax: ALTER TYPE <name> ADD VALUE 'label'".to_string(),
    })?;

    let action = parts.get(3).copied().unwrap_or("").to_uppercase();
    if action != "ADD" {
        return Err(SqlError::Parse {
            detail: format!("ALTER TYPE: unsupported action '{action}'; expected ADD VALUE"),
        });
    }
    let value_kw = parts.get(4).copied().unwrap_or("").to_uppercase();
    if value_kw != "VALUE" {
        return Err(SqlError::Parse {
            detail: "ALTER TYPE <name> ADD VALUE 'label': missing VALUE keyword".to_string(),
        });
    }

    // Extract the label: everything after ADD VALUE, trimmed, possibly quoted.
    let add_value_pos =
        find_ascii_case_insensitive(trimmed, " ADD VALUE ").ok_or_else(|| SqlError::Parse {
            detail: "ALTER TYPE: could not locate ADD VALUE in statement".to_string(),
        })?;
    let after_add_value = trimmed[add_value_pos + " ADD VALUE ".len()..].trim();
    let label = strip_single_quotes(after_add_value);
    if label.is_empty() {
        return Err(SqlError::Parse {
            detail: "ALTER TYPE ADD VALUE: label must not be empty".to_string(),
        });
    }

    Ok(NodedbStatement::Policy(PolicyStmt::AlterTypeAddValue {
        type_name: name.to_lowercase(),
        label,
    }))
}

/// Extract `('label1', 'label2', ...)` after the ENUM keyword.
fn parse_label_list(original: &str) -> Result<Vec<String>, SqlError> {
    // Find the opening paren after ENUM.
    let enum_pos =
        find_ascii_case_insensitive(original, " AS ENUM").ok_or_else(|| SqlError::Parse {
            detail: "CREATE TYPE … AS ENUM: cannot locate AS ENUM".to_string(),
        })?;
    let s = original[enum_pos + " AS ENUM".len()..].trim_start();
    extract_quoted_list(s, "CREATE TYPE … AS ENUM")
}

/// Extract `(<field> <type>, ...)` after the AS keyword for composite types.
fn parse_composite_fields(original: &str) -> Result<Vec<(String, String)>, SqlError> {
    // Find " AS (" — skip over "AS ENUM" by checking the next non-ws char.
    let as_pos = find_ascii_case_insensitive(original, " AS ").ok_or_else(|| SqlError::Parse {
        detail: "CREATE TYPE: missing AS keyword".to_string(),
    })?;
    let after_as = original[as_pos + 4..].trim_start();
    if after_as.to_uppercase().starts_with("ENUM") {
        return Err(SqlError::Parse {
            detail: "CREATE TYPE: expected composite field list, got ENUM".to_string(),
        });
    }

    let open = after_as.find('(').ok_or_else(|| SqlError::Parse {
        detail: "CREATE TYPE … AS: expected '(' to start field list".to_string(),
    })?;
    let close = after_as.rfind(')').ok_or_else(|| SqlError::Parse {
        detail: "CREATE TYPE … AS: missing closing ')'".to_string(),
    })?;
    if close <= open {
        return Err(SqlError::Parse {
            detail: "CREATE TYPE … AS: malformed field list".to_string(),
        });
    }
    let inner = &after_as[open + 1..close];
    if inner.trim().is_empty() {
        return Ok(Vec::new());
    }

    let mut fields = Vec::new();
    for raw in inner.split(',') {
        let tok: Vec<&str> = raw.split_whitespace().collect();
        if tok.len() < 2 {
            return Err(SqlError::Parse {
                detail: format!(
                    "CREATE TYPE: field definition '{}' must be '<name> <type>'",
                    raw.trim()
                ),
            });
        }
        let field_name = tok[0].to_lowercase();
        // Rejoin remaining tokens to handle types like "DOUBLE PRECISION".
        let type_name = tok[1..].join(" ").to_uppercase();
        fields.push((field_name, type_name));
    }
    Ok(fields)
}

/// Extract a `('a', 'b', ...)` list, returning lowercase strings.
fn extract_quoted_list(s: &str, ctx: &str) -> Result<Vec<String>, SqlError> {
    let open = s.find('(').ok_or_else(|| SqlError::Parse {
        detail: format!("{ctx}: expected '('"),
    })?;
    let close = s.rfind(')').ok_or_else(|| SqlError::Parse {
        detail: format!("{ctx}: missing ')'"),
    })?;
    if close <= open {
        return Err(SqlError::Parse {
            detail: format!("{ctx}: malformed list"),
        });
    }
    let inner = &s[open + 1..close];
    if inner.trim().is_empty() {
        return Ok(Vec::new());
    }

    let mut labels = Vec::new();
    let mut in_quote = false;
    let mut current = String::new();
    for ch in inner.chars() {
        match ch {
            '\'' if !in_quote => in_quote = true,
            '\'' if in_quote => in_quote = false,
            ',' if !in_quote => {
                let t = current.trim().to_string();
                if !t.is_empty() {
                    labels.push(t);
                }
                current.clear();
            }
            _ if in_quote => current.push(ch),
            _ => {}
        }
    }
    let t = current.trim().to_string();
    if !t.is_empty() {
        labels.push(t);
    }
    Ok(labels.into_iter().filter(|l| !l.is_empty()).collect())
}

/// Strip surrounding single quotes from a string fragment.
fn strip_single_quotes(s: &str) -> String {
    let s = s.trim();
    if s.starts_with('\'') && s.ends_with('\'') && s.len() >= 2 {
        s[1..s.len() - 1].to_string()
    } else {
        s.to_string()
    }
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
    fn create_enum_with_unicode_name_preserves_original_offsets() {
        let stmt = ok("CREATE TYPE statusﬀﬀ AS ENUM ('active', 'inactive')");
        match stmt {
            NodedbStatement::Policy(PolicyStmt::CreateEnumType { name, labels }) => {
                assert_eq!(name, "statusﬀﬀ");
                assert_eq!(labels, vec!["active", "inactive"]);
            }
            other => panic!("unexpected {other:?}"),
        }
    }

    #[test]
    fn create_enum_basic() {
        let stmt = ok("CREATE TYPE status AS ENUM ('active', 'inactive', 'pending')");
        match stmt {
            NodedbStatement::Policy(PolicyStmt::CreateEnumType { name, labels }) => {
                assert_eq!(name, "status");
                assert_eq!(labels, vec!["active", "inactive", "pending"]);
            }
            other => panic!("unexpected {other:?}"),
        }
    }

    #[test]
    fn create_enum_name_lowercased() {
        let stmt = ok("CREATE TYPE Status AS ENUM ('a', 'b')");
        match stmt {
            NodedbStatement::Policy(PolicyStmt::CreateEnumType { name, .. }) => {
                assert_eq!(name, "status")
            }
            other => panic!("unexpected {other:?}"),
        }
    }

    #[test]
    fn create_enum_empty_rejected() {
        let e = err("CREATE TYPE mood AS ENUM ()");
        assert!(matches!(e, SqlError::Parse { .. }));
    }

    #[test]
    fn create_enum_duplicate_rejected() {
        let e = err("CREATE TYPE mood AS ENUM ('happy', 'happy', 'sad')");
        assert!(matches!(e, SqlError::Parse { .. }));
        assert!(e.to_string().contains("duplicate"));
    }

    #[test]
    fn create_composite_with_unicode_name_preserves_original_offsets() {
        let stmt = ok("CREATE TYPE addressﬀﬀ AS (street TEXT, city TEXT)");
        match stmt {
            NodedbStatement::Policy(PolicyStmt::CreateCompositeType { name, fields }) => {
                assert_eq!(name, "addressﬀﬀ");
                assert_eq!(fields.len(), 2);
            }
            other => panic!("unexpected {other:?}"),
        }
    }

    #[test]
    fn create_composite_basic() {
        let stmt = ok("CREATE TYPE address AS (street TEXT, city TEXT, zip TEXT)");
        match stmt {
            NodedbStatement::Policy(PolicyStmt::CreateCompositeType { name, fields }) => {
                assert_eq!(name, "address");
                assert_eq!(fields.len(), 3);
                assert_eq!(fields[0].0, "street");
                assert_eq!(fields[0].1, "TEXT");
                assert_eq!(fields[1].0, "city");
            }
            other => panic!("unexpected {other:?}"),
        }
    }

    #[test]
    fn drop_basic() {
        let stmt = ok("DROP TYPE status");
        assert_eq!(
            stmt,
            NodedbStatement::Policy(PolicyStmt::DropType {
                name: "status".to_string(),
                if_exists: false,
            })
        );
    }

    #[test]
    fn drop_if_exists() {
        let stmt = ok("DROP TYPE IF EXISTS status");
        assert_eq!(
            stmt,
            NodedbStatement::Policy(PolicyStmt::DropType {
                name: "status".to_string(),
                if_exists: true,
            })
        );
    }

    #[test]
    fn alter_unicode_type_add_value_preserves_original_offsets() {
        let stmt = ok("ALTER TYPE statusﬀﬀ ADD VALUE 'archived'");
        assert_eq!(
            stmt,
            NodedbStatement::Policy(PolicyStmt::AlterTypeAddValue {
                type_name: "statusﬀﬀ".to_string(),
                label: "archived".to_string(),
            })
        );
    }

    #[test]
    fn alter_add_value() {
        let stmt = ok("ALTER TYPE status ADD VALUE 'archived'");
        assert_eq!(
            stmt,
            NodedbStatement::Policy(PolicyStmt::AlterTypeAddValue {
                type_name: "status".to_string(),
                label: "archived".to_string(),
            })
        );
    }

    #[test]
    fn alter_unsupported_action() {
        let e = err("ALTER TYPE status RENAME TO new_status");
        assert!(matches!(e, SqlError::Parse { .. }));
    }

    #[test]
    fn show_types() {
        let stmt = ok("SHOW TYPES");
        assert_eq!(stmt, NodedbStatement::Policy(PolicyStmt::ShowTypes));
    }
}
