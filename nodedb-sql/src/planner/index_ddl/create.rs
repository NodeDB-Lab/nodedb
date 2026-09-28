// SPDX-License-Identifier: Apache-2.0

//! Plan a `CREATE [UNIQUE] INDEX` statement parsed by sqlparser.

use crate::types::CreateIndexPlan;
use sqlparser::ast;

use crate::SqlPlan;
use crate::error::{Result, SqlError};
use crate::parser::normalize::{normalize_ident, normalize_object_name_checked};

/// Plan a `CREATE INDEX` statement.
///
/// Supports:
/// - `CREATE [UNIQUE] INDEX [IF NOT EXISTS] [name] ON table (col [COLLATE coll])`
/// - `COLLATE NOCASE` / `COLLATE CI` / `COLLATE CASE_INSENSITIVE` on the
///   indexed column → `case_insensitive = true`.
///
/// Multi-column indexes, expression indexes, and predicate (`WHERE`) indexes
/// are rejected with a typed error rather than silently dropped.
pub fn plan_create_index(ci: &ast::CreateIndex) -> Result<SqlPlan> {
    let collection =
        normalize_object_name_checked(&ci.table_name).map_err(|_| SqlError::Parse {
            detail: "CREATE INDEX: missing or schema-qualified table name".into(),
        })?;

    let index_name = match ci.name.as_ref() {
        Some(n) => Some(normalize_object_name_checked(n)?),
        None => None,
    };

    if ci.columns.is_empty() {
        return Err(SqlError::Parse {
            detail: "CREATE INDEX: at least one column is required".into(),
        });
    }
    if ci.columns.len() > 1 {
        return Err(SqlError::Unsupported {
            detail: "CREATE INDEX: multi-column indexes are not supported".into(),
        });
    }

    let col = &ci.columns[0];
    let (field_expr, case_insensitive) = strip_collate(&col.column.expr);
    let field = match field_expr {
        ast::Expr::Identifier(ident) => normalize_ident(ident),
        ast::Expr::CompoundIdentifier(parts) if parts.len() == 1 => normalize_ident(&parts[0]),
        other => {
            return Err(SqlError::Unsupported {
                detail: format!("CREATE INDEX: expression indexes are not supported: {other}"),
            });
        }
    };

    if ci.predicate.is_some() {
        return Err(SqlError::Unsupported {
            detail: "CREATE INDEX: partial (WHERE) indexes are not supported".into(),
        });
    }

    Ok(SqlPlan::CreateIndex(CreateIndexPlan {
        index_name,
        collection,
        field,
        unique: ci.unique,
        if_not_exists: ci.if_not_exists,
        case_insensitive,
    }))
}

/// Strip a `COLLATE <name>` wrapper from `expr` and return the inner
/// expression together with whether the collation is a recognised
/// case-insensitive collation.
fn strip_collate(expr: &ast::Expr) -> (&ast::Expr, bool) {
    if let ast::Expr::Collate { expr, collation } = expr {
        let ci = collation
            .0
            .iter()
            .filter_map(|part| match part {
                ast::ObjectNamePart::Identifier(ident) => Some(ident.value.as_str()),
                _ => None,
            })
            .any(is_case_insensitive_collation);
        return (expr.as_ref(), ci);
    }
    (expr, false)
}

fn is_case_insensitive_collation(name: &str) -> bool {
    name.eq_ignore_ascii_case("NOCASE")
        || name.eq_ignore_ascii_case("CI")
        || name.eq_ignore_ascii_case("CASE_INSENSITIVE")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::parser::statement::parse_sql;

    fn plan(sql: &str) -> Result<SqlPlan> {
        let stmts = parse_sql(sql).expect("parse");
        let ast::Statement::CreateIndex(ci) = &stmts[0] else {
            panic!("expected CREATE INDEX");
        };
        plan_create_index(ci)
    }

    #[test]
    fn basic_index() {
        let SqlPlan::CreateIndex(CreateIndexPlan {
            index_name,
            collection,
            field,
            unique,
            if_not_exists,
            case_insensitive,
        }) = plan("CREATE INDEX idx_users_email ON users (email)").unwrap()
        else {
            panic!("expected CreateIndex");
        };
        assert_eq!(index_name.as_deref(), Some("idx_users_email"));
        assert_eq!(collection, "users");
        assert_eq!(field, "email");
        assert!(!unique);
        assert!(!if_not_exists);
        assert!(!case_insensitive);
    }

    #[test]
    fn anonymous_index_name() {
        let SqlPlan::CreateIndex(CreateIndexPlan { index_name, .. }) =
            plan("CREATE INDEX ON users (email)").unwrap()
        else {
            panic!("expected CreateIndex");
        };
        assert!(index_name.is_none());
    }

    #[test]
    fn unique_and_if_not_exists() {
        let SqlPlan::CreateIndex(CreateIndexPlan {
            unique,
            if_not_exists,
            ..
        }) = plan("CREATE UNIQUE INDEX IF NOT EXISTS u ON users (email)").unwrap()
        else {
            panic!("expected CreateIndex");
        };
        assert!(unique);
        assert!(if_not_exists);
    }

    #[test]
    fn collate_nocase_detected() {
        for sql in [
            "CREATE INDEX i ON users (email COLLATE NOCASE)",
            "CREATE INDEX i ON users (email COLLATE \"NOCASE\")",
            "CREATE INDEX i ON users (email COLLATE ci)",
            "CREATE INDEX i ON users (email COLLATE case_insensitive)",
        ] {
            let SqlPlan::CreateIndex(CreateIndexPlan {
                case_insensitive, ..
            }) = plan(sql).unwrap()
            else {
                panic!("expected CreateIndex for {sql}");
            };
            assert!(case_insensitive, "expected case_insensitive for: {sql}");
        }
    }

    #[test]
    fn collate_other_not_case_insensitive() {
        let SqlPlan::CreateIndex(CreateIndexPlan {
            case_insensitive, ..
        }) = plan("CREATE INDEX i ON users (email COLLATE \"en_US\")").unwrap()
        else {
            panic!("expected CreateIndex");
        };
        assert!(!case_insensitive);
    }

    #[test]
    fn multi_column_rejected() {
        let err = plan("CREATE INDEX i ON users (a, b)").unwrap_err();
        assert!(matches!(err, SqlError::Unsupported { .. }), "{err:?}");
    }

    #[test]
    fn partial_index_rejected() {
        let err = plan("CREATE INDEX i ON users (email) WHERE email IS NOT NULL").unwrap_err();
        assert!(matches!(err, SqlError::Unsupported { .. }), "{err:?}");
    }

    #[test]
    fn expression_index_rejected() {
        let err = plan("CREATE INDEX i ON users (lower(email))").unwrap_err();
        assert!(matches!(err, SqlError::Unsupported { .. }), "{err:?}");
    }
}
