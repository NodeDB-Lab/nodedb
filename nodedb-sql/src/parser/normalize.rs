// SPDX-License-Identifier: Apache-2.0

//! SQL identifier normalization.

use crate::error::{Result, SqlError};
use crate::reserved::{check_ast_identifier, check_identifier};

/// Human-readable message for schema-qualified name rejections.
/// Defined once so all rejection sites produce consistent output.
pub const SCHEMA_QUALIFIED_MSG: &str = "schema-qualified names are not supported; NodeDB has no schema concept \
     — use 'users' not 'public.users'";

/// Normalize a SQL identifier: lowercase unquoted, preserve quoted.
pub fn normalize_ident(ident: &sqlparser::ast::Ident) -> String {
    if ident.quote_style.is_some() {
        ident.value.clone()
    } else {
        ident.value.to_lowercase()
    }
}

/// Normalize a compound object name, rejecting schema-qualified forms.
///
/// Accepts a single-part name (plain identifier) and returns it normalized.
/// Rejects any name with more than one part (e.g. `public.users`,
/// `db.public.users`) with `SqlError::Unsupported`.
pub fn normalize_object_name_checked(name: &sqlparser::ast::ObjectName) -> Result<String> {
    if name.0.len() > 1 {
        // Validate every component before reflecting it in an error. This keeps
        // malformed quoted/control-bearing identifiers out of diagnostics too.
        let qualified = name
            .0
            .iter()
            .map(|part| match part {
                sqlparser::ast::ObjectNamePart::Identifier(ident) => check_ast_identifier(ident),
                _ => Err(SqlError::InvalidIdentifier {
                    name: String::new(),
                    reason: "object name must contain only identifiers",
                }),
            })
            .collect::<Result<Vec<_>>>()?
            .join(".");
        return Err(SqlError::Unsupported {
            detail: format!("'{qualified}': {SCHEMA_QUALIFIED_MSG}"),
        });
    }
    let part = name.0.first().ok_or_else(|| SqlError::InvalidIdentifier {
        name: String::new(),
        reason: "object name must contain an identifier",
    })?;
    match part {
        sqlparser::ast::ObjectNamePart::Identifier(ident) => check_ast_identifier(ident),
        _ => Err(SqlError::InvalidIdentifier {
            name: String::new(),
            reason: "object name must contain an identifier",
        }),
    }
}

/// Normalize a single entry of an `INSERT` column list.
///
/// The parser models these as `ObjectName`, but an `INSERT` target column is
/// always one plain identifier: a qualified form like `t.col` names no column
/// NodeDB can write to, and a function part is not a name at all. Both are
/// typed errors — taking the last part of a qualified name binds values to a
/// column the statement never asked for.
pub fn normalize_insert_column(name: &sqlparser::ast::ObjectName) -> Result<String> {
    if let [sqlparser::ast::ObjectNamePart::Identifier(ident)] = name.0.as_slice() {
        return Ok(normalize_ident(ident));
    }
    if name.0.len() == 1 {
        // The sole part is a function call, so there is no identifier to name.
        return Err(SqlError::InvalidIdentifier {
            name: String::new(),
            reason: "an INSERT column list takes plain column names",
        });
    }
    // Qualified or empty. `normalize_object_name_checked` owns both rejections
    // and validates every component before reflecting it in the message; it
    // cannot return `Ok` for either shape.
    normalize_object_name_checked(name)
}

/// Normalize a table name, accepting the three system-schema qualifiers that
/// name catalog relations (NodeDB has no user schemas, so any other qualifier
/// is rejected by [`normalize_object_name_checked`]):
///
/// - `pg_catalog.pg_class` → `pg_class` (the `pg_catalog` schema is implicit).
/// - `_system.audit_log` → `_system.audit_log` (the `_system.` prefix is part of
///   the catalog relation's name).
/// - `nodedb.spatial_ref_sys` → `nodedb.spatial_ref_sys`, the same way.
///
/// These are resolved like any other relation through `SqlCatalog::resolve_relation`.
fn normalize_table_name(name: &sqlparser::ast::ObjectName) -> Result<String> {
    if name.0.len() == 2 {
        let parts: Vec<String> = name
            .0
            .iter()
            .map(|part| match part {
                sqlparser::ast::ObjectNamePart::Identifier(ident) => check_ast_identifier(ident),
                _ => Err(SqlError::InvalidIdentifier {
                    name: String::new(),
                    reason: "object name must contain an identifier",
                }),
            })
            .collect::<Result<_>>()?;
        match parts[0].as_str() {
            "pg_catalog" => return Ok(parts[1].clone()),
            "_system" => return Ok(format!("_system.{}", parts[1])),
            "nodedb" => return Ok(format!("nodedb.{}", parts[1])),
            _ => {}
        }
    }
    normalize_object_name_checked(name)
}

/// Resolve a raw relation token the same way [`normalize_table_name`] resolves
/// a parsed one.
///
/// Handlers that read a relation out of a whitespace-split statement get the
/// token before `sqlparser` sees it, so they need the rule in token form:
///
/// - `pg_catalog.pg_class` → `pg_class`.
/// - `_system.audit_log` → `_system.audit_log`.
/// - `nodedb.spatial_ref_sys` → `nodedb.spatial_ref_sys`.
/// - Any other qualifier is rejected as schema-qualified.
/// - An unqualified token decodes through [`check_identifier`].
///
/// A token carrying a double quote is treated as one identifier, so a quoted
/// name containing a dot never splits.
pub fn check_relation_token(raw_name: &str) -> Result<String> {
    if raw_name.contains('"') || !raw_name.contains('.') {
        return check_identifier(raw_name);
    }
    // Validate every component before reflecting it in an error, the way
    // `normalize_object_name_checked` does.
    let parts = raw_name
        .split('.')
        .map(check_identifier)
        .collect::<Result<Vec<_>>>()?;
    if parts.len() == 2 {
        match parts[0].as_str() {
            "pg_catalog" => return Ok(parts[1].clone()),
            "_system" => return Ok(format!("_system.{}", parts[1])),
            "nodedb" => return Ok(format!("nodedb.{}", parts[1])),
            _ => {}
        }
    }
    Err(SqlError::Unsupported {
        detail: format!("'{}': {SCHEMA_QUALIFIED_MSG}", parts.join(".")),
    })
}

/// Extract table name and optional alias from a table factor.
///
/// Returns `Err` if the table name is schema-qualified with a non-system schema.
pub fn table_name_from_factor(
    factor: &sqlparser::ast::TableFactor,
) -> Result<Option<(String, Option<String>)>> {
    match factor {
        sqlparser::ast::TableFactor::Table { name, alias, .. } => {
            let table = normalize_table_name(name)?;
            let alias_name = alias
                .as_ref()
                .map(|alias| check_ast_identifier(&alias.name))
                .transpose()?;
            Ok(Some((table, alias_name)))
        }
        _ => Ok(None),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::parser::statement::parse_sql;
    use sqlparser::ast::Statement;

    fn parse_table_factor(sql: &str) -> sqlparser::ast::TableFactor {
        let stmts = parse_sql(sql).expect("parse failed");
        let Statement::Query(q) = &stmts[0] else {
            panic!("expected query");
        };
        let sqlparser::ast::SetExpr::Select(sel) = q.body.as_ref() else {
            panic!("expected select body");
        };
        sel.from[0].relation.clone()
    }

    fn parse_object_name(sql: &str) -> sqlparser::ast::ObjectName {
        match parse_table_factor(sql) {
            sqlparser::ast::TableFactor::Table { name, .. } => name,
            other => panic!("expected table factor, got {other:?}"),
        }
    }

    #[test]
    fn plain_name_accepted() {
        let name = parse_object_name("SELECT * FROM users");
        assert_eq!(normalize_object_name_checked(&name).unwrap(), "users");
    }

    #[test]
    fn schema_qualified_two_parts_rejected() {
        let name = parse_object_name("SELECT * FROM public.users");
        let err = normalize_object_name_checked(&name).unwrap_err();
        assert!(
            matches!(err, SqlError::Unsupported { .. }),
            "expected Unsupported, got {err:?}"
        );
        let msg = format!("{err}");
        assert!(
            msg.contains("public.users") || msg.contains("schema-qualified"),
            "error should mention the qualified name or schema: {msg}"
        );
    }

    #[test]
    fn schema_qualified_three_parts_rejected() {
        // db.public.users — three-part name.
        // sqlparser may not parse this as a table name with three parts in all dialects,
        // but we can verify via a manually constructed ObjectName.
        use sqlparser::ast::{Ident, ObjectName, ObjectNamePart};
        let name = ObjectName(vec![
            ObjectNamePart::Identifier(Ident::new("db")),
            ObjectNamePart::Identifier(Ident::new("public")),
            ObjectNamePart::Identifier(Ident::new("users")),
        ]);
        let err = normalize_object_name_checked(&name).unwrap_err();
        assert!(
            matches!(err, SqlError::Unsupported { .. }),
            "expected Unsupported, got {err:?}"
        );
    }

    #[test]
    fn object_names_enforce_ast_identifier_rules() {
        use sqlparser::ast::{Ident, ObjectName, ObjectNamePart};

        let empty = ObjectName(vec![ObjectNamePart::Identifier(Ident::new(""))]);
        assert!(matches!(
            normalize_object_name_checked(&empty),
            Err(SqlError::InvalidIdentifier { .. })
        ));

        let reserved = ObjectName(vec![ObjectNamePart::Identifier(Ident::new("MATCH"))]);
        assert!(matches!(
            normalize_object_name_checked(&reserved),
            Err(SqlError::ReservedIdentifier { .. })
        ));

        let mut quoted = Ident::new("MiXeD 雪");
        quoted.quote_style = Some('"');
        let quoted_name = ObjectName(vec![ObjectNamePart::Identifier(quoted)]);
        assert_eq!(
            normalize_object_name_checked(&quoted_name).expect("quoted object name"),
            "MiXeD 雪"
        );

        let quote = ObjectName(vec![ObjectNamePart::Identifier(Ident::new("a\"b"))]);
        assert!(matches!(
            normalize_object_name_checked(&quote),
            Err(SqlError::InvalidIdentifier { .. })
        ));
    }

    #[test]
    fn insert_columns_take_plain_names_only() {
        use sqlparser::ast::{Ident, ObjectName, ObjectNamePart};

        let plain = ObjectName(vec![ObjectNamePart::Identifier(Ident::new("MiXeD"))]);
        assert_eq!(
            normalize_insert_column(&plain).expect("plain column"),
            "mixed"
        );

        let mut quoted_ident = Ident::new("MiXeD");
        quoted_ident.quote_style = Some('"');
        let quoted = ObjectName(vec![ObjectNamePart::Identifier(quoted_ident)]);
        assert_eq!(
            normalize_insert_column(&quoted).expect("quoted column"),
            "MiXeD"
        );

        // `INSERT INTO t (a.b) VALUES (1)` parses as a two-part name. Taking
        // either part would bind the value to a column the statement did not
        // name, so the whole statement is refused.
        let qualified = ObjectName(vec![
            ObjectNamePart::Identifier(Ident::new("a")),
            ObjectNamePart::Identifier(Ident::new("b")),
        ]);
        assert!(matches!(
            normalize_insert_column(&qualified),
            Err(SqlError::Unsupported { .. })
        ));

        let empty = ObjectName(Vec::new());
        assert!(matches!(
            normalize_insert_column(&empty),
            Err(SqlError::InvalidIdentifier { .. })
        ));
    }

    #[test]
    fn relation_tokens_accept_the_system_qualifiers() {
        assert_eq!(
            check_relation_token("_system.audit_log").expect("system relation"),
            "_system.audit_log"
        );
        assert_eq!(
            check_relation_token("pg_catalog.pg_class").expect("catalog relation"),
            "pg_class"
        );
        assert_eq!(
            check_relation_token("nodedb.spatial_ref_sys").expect("nodedb relation"),
            "nodedb.spatial_ref_sys"
        );
        assert_eq!(check_relation_token("MiXeD").expect("bare"), "mixed");
        assert_eq!(check_relation_token("\"MiXeD\"").expect("quoted"), "MiXeD");
    }

    #[test]
    fn relation_tokens_reject_other_qualifiers() {
        for token in ["public.users", "db.public.users"] {
            assert!(
                matches!(
                    check_relation_token(token),
                    Err(SqlError::Unsupported { .. })
                ),
                "{token}"
            );
        }
        // A quoted token never splits, so a dot inside it stays part of the
        // name and fails the identifier grammar rather than reading as a
        // qualifier.
        assert!(matches!(
            check_relation_token("_system.\"a.b\""),
            Err(SqlError::InvalidIdentifier { .. })
        ));
    }

    #[test]
    fn qualified_names_validate_components_before_rejection() {
        use sqlparser::ast::{Ident, ObjectName, ObjectNamePart};

        for invalid_value in ["bad\nname", "a\"b"] {
            let mut invalid = Ident::new(invalid_value);
            invalid.quote_style = Some('"');
            let name = ObjectName(vec![
                ObjectNamePart::Identifier(Ident::new("public")),
                ObjectNamePart::Identifier(invalid),
            ]);
            assert!(matches!(
                normalize_object_name_checked(&name),
                Err(SqlError::InvalidIdentifier { .. })
            ));
        }
    }

    #[test]
    fn table_aliases_enforce_ast_identifier_rules() {
        use crate::planner::lateral::subquery::lateral_alias_from_factor;

        let quoted = parse_table_factor("SELECT * FROM users AS \"MiXeD 雪\"");
        assert_eq!(
            table_name_from_factor(&quoted).expect("valid alias"),
            Some(("users".to_string(), Some("MiXeD 雪".to_string())))
        );

        for alias in ["\"\"", "\"a\"\"b\""] {
            let ordinary = parse_table_factor(&format!("SELECT * FROM users AS {alias}"));
            assert!(matches!(
                table_name_from_factor(&ordinary),
                Err(SqlError::InvalidIdentifier { .. })
            ));

            let lateral =
                parse_table_factor(&format!("SELECT * FROM LATERAL (SELECT 1) AS {alias}"));
            assert!(matches!(
                lateral_alias_from_factor(&lateral),
                Err(SqlError::InvalidIdentifier { .. })
            ));
        }
    }

    #[test]
    fn system_table_qualifiers_validate_each_identifier() {
        use sqlparser::ast::{Ident, ObjectName, ObjectNamePart};

        let system = ObjectName(vec![
            ObjectNamePart::Identifier(Ident::new("_system")),
            ObjectNamePart::Identifier(Ident::new("audit_log")),
        ]);
        assert_eq!(
            normalize_table_name(&system).expect("system table"),
            "_system.audit_log"
        );

        let invalid = ObjectName(vec![
            ObjectNamePart::Identifier(Ident::new("_system")),
            ObjectNamePart::Identifier(Ident::new("audit\nlog")),
        ]);
        assert!(matches!(
            normalize_table_name(&invalid),
            Err(SqlError::InvalidIdentifier { .. })
        ));
    }
}
