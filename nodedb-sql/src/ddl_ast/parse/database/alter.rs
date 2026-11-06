// SPDX-License-Identifier: Apache-2.0

//! `ALTER DATABASE <name> { RENAME TO <new> | SET QUOTA (...) | SET DEFAULT |
//!                          SET AUDIT_DML = <mode> | SET IDLE_TIMEOUT = <secs> |
//!                          MATERIALIZE | PROMOTE | MIRROR RESUME |
//!                          MIRROR REBOOTSTRAP }`.

use nodedb_types::AuditDmlMode;

use crate::ddl_ast::parse::clause::ClauseCursor;
use crate::ddl_ast::statement::{AlterDatabaseOperation, DatabaseStmt, NodedbStatement};
use crate::error::SqlError;

use super::dispatch::db_name;
use super::quota_spec::parse_quota_pairs;

/// Maximum accepted value for `ALTER DATABASE ... SET IDLE_TIMEOUT = <secs>`,
/// in seconds. Caps the parser so an administrative typo such as
/// `SET IDLE_TIMEOUT = 999999999999` is rejected at parse time rather than
/// silently accepted as an effectively-infinite timeout. One year is far
/// beyond any reasonable interactive-session lifetime; use `0` to disable
/// the timeout entirely.
pub const MAX_IDLE_TIMEOUT_SECS: u64 = 365 * 24 * 60 * 60;

/// Read `<name> <operation>` after `ALTER DATABASE`, then require the end of
/// the statement.
pub(super) fn parse_alter_database(
    c: &mut ClauseCursor<'_, '_>,
) -> Result<NodedbStatement, SqlError> {
    if c.at_end() {
        return Err(c.error("ALTER DATABASE requires a name"));
    }
    let name = db_name(c, "database name")?;
    if c.at_end() {
        return Err(c.error("ALTER DATABASE requires an operation keyword"));
    }

    let operation = if c.eat_keyword("RENAME") {
        parse_rename(c)?
    } else if c.eat_keyword("SET") {
        parse_set(c)?
    } else if c.eat_keyword("MATERIALIZE") {
        AlterDatabaseOperation::Materialize
    } else if c.eat_keyword("PROMOTE") {
        AlterDatabaseOperation::Promote
    } else if c.eat_keyword("MIRROR") {
        if c.eat_keyword("RESUME") {
            AlterDatabaseOperation::MirrorResume
        } else if c.eat_keyword("REBOOTSTRAP") {
            AlterDatabaseOperation::MirrorRebootstrap
        } else {
            return Err(c.error(format!(
                "MIRROR takes RESUME or REBOOTSTRAP, not {}",
                c.describe_next()
            )));
        }
    } else {
        return Err(c.error(format!("unknown operation {}", c.describe_next())));
    };
    c.finish()?;

    Ok(NodedbStatement::Database(DatabaseStmt::AlterDatabase {
        name,
        operation,
    }))
}

fn parse_rename(c: &mut ClauseCursor<'_, '_>) -> Result<AlterDatabaseOperation, SqlError> {
    if !c.eat_keyword("TO") {
        return Err(c.error(format!(
            "RENAME requires keyword 'TO', got {}",
            c.describe_next()
        )));
    }
    if c.at_end() {
        return Err(c.error("RENAME TO requires a new name"));
    }
    let new_name = db_name(c, "new name")?;
    Ok(AlterDatabaseOperation::Rename { new_name })
}

fn parse_set(c: &mut ClauseCursor<'_, '_>) -> Result<AlterDatabaseOperation, SqlError> {
    if c.eat_keyword("QUOTA") {
        let inner = c.raw_paren("QUOTA")?;
        let spec = parse_quota_pairs(inner, "ALTER DATABASE SET QUOTA")?;
        Ok(AlterDatabaseOperation::SetQuota(spec))
    } else if c.eat_keyword("DEFAULT") {
        Ok(AlterDatabaseOperation::SetDefault)
    } else if c.eat_keyword("AUDIT_DML") {
        parse_set_audit_dml(c)
    } else if c.eat_keyword("IDLE_TIMEOUT") {
        parse_set_idle_timeout(c)
    } else {
        Err(c.error(format!("SET: unknown target {}", c.describe_next())))
    }
}

/// A `SET <key> = <value>` value: a quoted string or a word.
fn set_value(c: &mut ClauseCursor<'_, '_>, key: &str) -> Result<String, SqlError> {
    if !c.eat_punct('=') {
        return Err(c.error(format!("SET {key} requires '=', got {}", c.describe_next())));
    }
    if let Ok(value) = c.string(key) {
        return Ok(value);
    }
    Ok(c.word(key)?.to_string())
}

fn parse_set_audit_dml(c: &mut ClauseCursor<'_, '_>) -> Result<AlterDatabaseOperation, SqlError> {
    let raw = set_value(c, "AUDIT_DML (NONE, WRITES, ALL)")?;
    let mode = raw.parse::<AuditDmlMode>().map_err(|e| SqlError::Parse {
        detail: format!("ALTER DATABASE SET AUDIT_DML: {e}"),
    })?;
    Ok(AlterDatabaseOperation::SetAuditDml(mode))
}

fn parse_set_idle_timeout(
    c: &mut ClauseCursor<'_, '_>,
) -> Result<AlterDatabaseOperation, SqlError> {
    let raw = set_value(c, "IDLE_TIMEOUT")?;
    let secs = raw.parse::<u64>().map_err(|_| SqlError::Parse {
        detail: format!(
            "ALTER DATABASE SET IDLE_TIMEOUT: invalid value '{raw}', expected non-negative integer"
        ),
    })?;
    if secs > MAX_IDLE_TIMEOUT_SECS {
        return Err(SqlError::Parse {
            detail: format!(
                "ALTER DATABASE SET IDLE_TIMEOUT: {secs}s exceeds maximum {MAX_IDLE_TIMEOUT_SECS}s ({} days). \
                 Use 0 to disable the timeout entirely.",
                MAX_IDLE_TIMEOUT_SECS / 86_400
            ),
        });
    }
    Ok(AlterDatabaseOperation::SetIdleTimeout(secs))
}

#[cfg(test)]
mod tests {
    use super::super::dispatch::try_parse;
    use super::*;

    fn ok(sql: &str) -> NodedbStatement {
        let upper = sql.to_uppercase();
        let parts: Vec<&str> = sql.split_whitespace().collect();
        try_parse(&upper, &parts, sql)
            .expect("expected Some")
            .expect("expected Ok")
    }

    #[test]
    fn parse_alter_database_rename() {
        let stmt = ok("ALTER DATABASE mydb RENAME TO newdb");
        assert_eq!(
            stmt,
            NodedbStatement::Database(DatabaseStmt::AlterDatabase {
                name: "mydb".into(),
                operation: AlterDatabaseOperation::Rename {
                    new_name: "newdb".into()
                },
            })
        );
    }

    #[test]
    fn parse_alter_database_set_quota() {
        let stmt = ok("ALTER DATABASE mydb SET QUOTA (max_memory_bytes = 1073741824)");
        match stmt {
            NodedbStatement::Database(DatabaseStmt::AlterDatabase {
                name,
                operation: AlterDatabaseOperation::SetQuota(spec),
            }) => {
                assert_eq!(name, "mydb");
                assert_eq!(spec.max_memory_bytes, Some(1_073_741_824));
            }
            other => panic!("expected AlterDatabase SetQuota, got {other:?}"),
        }
    }

    #[test]
    fn parse_alter_database_set_quota_cache_weight_zero_rejected() {
        let sql = "ALTER DATABASE mydb SET QUOTA (cache_weight = 0)";
        let upper = sql.to_uppercase();
        let parts: Vec<&str> = sql.split_whitespace().collect();
        let err = try_parse(&upper, &parts, sql).unwrap().unwrap_err();
        match err {
            SqlError::Parse { detail } => {
                assert!(detail.contains("cache_weight"), "unexpected: {detail}");
                assert!(detail.contains("≥ 1"), "unexpected: {detail}");
            }
            other => panic!("unexpected error: {other:?}"),
        }
    }

    #[test]
    fn parse_alter_database_set_quota_maintenance_pct_over_100_rejected() {
        let sql = "ALTER DATABASE mydb SET QUOTA (maintenance_cpu_pct = 150)";
        let upper = sql.to_uppercase();
        let parts: Vec<&str> = sql.split_whitespace().collect();
        let err = try_parse(&upper, &parts, sql).unwrap().unwrap_err();
        match err {
            SqlError::Parse { detail } => {
                assert!(
                    detail.contains("maintenance_cpu_pct"),
                    "unexpected: {detail}"
                );
            }
            other => panic!("unexpected error: {other:?}"),
        }
    }

    #[test]
    fn parse_alter_set_audit_dml_writes() {
        let stmt = ok("ALTER DATABASE mydb SET AUDIT_DML = WRITES");
        assert_eq!(
            stmt,
            NodedbStatement::Database(DatabaseStmt::AlterDatabase {
                name: "mydb".into(),
                operation: AlterDatabaseOperation::SetAuditDml(AuditDmlMode::Writes),
            })
        );
    }

    #[test]
    fn parse_alter_set_audit_dml_all() {
        let stmt = ok("ALTER DATABASE mydb SET AUDIT_DML = ALL");
        assert_eq!(
            stmt,
            NodedbStatement::Database(DatabaseStmt::AlterDatabase {
                name: "mydb".into(),
                operation: AlterDatabaseOperation::SetAuditDml(AuditDmlMode::All),
            })
        );
    }

    #[test]
    fn parse_alter_set_audit_dml_none() {
        let stmt = ok("ALTER DATABASE mydb SET AUDIT_DML = NONE");
        assert_eq!(
            stmt,
            NodedbStatement::Database(DatabaseStmt::AlterDatabase {
                name: "mydb".into(),
                operation: AlterDatabaseOperation::SetAuditDml(AuditDmlMode::None),
            })
        );
    }

    #[test]
    fn parse_alter_set_audit_dml_invalid_value_rejected() {
        let sql = "ALTER DATABASE mydb SET AUDIT_DML = INVALID";
        let upper = sql.to_uppercase();
        let parts: Vec<&str> = sql.split_whitespace().collect();
        let err = try_parse(&upper, &parts, sql).unwrap().unwrap_err();
        match err {
            SqlError::Parse { detail } => {
                assert!(
                    detail.contains("AUDIT_DML"),
                    "expected AUDIT_DML in error: {detail}"
                );
            }
            other => panic!("unexpected error: {other:?}"),
        }
    }

    #[test]
    fn parse_alter_set_idle_timeout_explicit_value() {
        let stmt = ok("ALTER DATABASE foo SET IDLE_TIMEOUT = 1800");
        assert_eq!(
            stmt,
            NodedbStatement::Database(DatabaseStmt::AlterDatabase {
                name: "foo".into(),
                operation: AlterDatabaseOperation::SetIdleTimeout(1800),
            })
        );
    }

    #[test]
    fn parse_alter_set_idle_timeout_zero_disables() {
        // 0 means "disabled" per the design.
        let stmt = ok("ALTER DATABASE foo SET IDLE_TIMEOUT = 0");
        match stmt {
            NodedbStatement::Database(DatabaseStmt::AlterDatabase {
                operation: AlterDatabaseOperation::SetIdleTimeout(secs),
                ..
            }) => assert_eq!(secs, 0),
            other => panic!("expected SetIdleTimeout(0), got {other:?}"),
        }
    }

    #[test]
    fn parse_alter_set_idle_timeout_non_numeric_rejected() {
        let sql = "ALTER DATABASE foo SET IDLE_TIMEOUT = abc";
        let upper = sql.to_uppercase();
        let parts: Vec<&str> = sql.split_whitespace().collect();
        let result = try_parse(&upper, &parts, sql);
        assert!(
            result.as_ref().map(|r| r.is_err()).unwrap_or(true),
            "non-numeric value must be rejected"
        );
    }

    #[test]
    fn parse_alter_set_idle_timeout_at_cap_accepted() {
        let sql = format!("ALTER DATABASE foo SET IDLE_TIMEOUT = {MAX_IDLE_TIMEOUT_SECS}");
        let stmt = ok(&sql);
        match stmt {
            NodedbStatement::Database(DatabaseStmt::AlterDatabase {
                operation: AlterDatabaseOperation::SetIdleTimeout(secs),
                ..
            }) => assert_eq!(secs, MAX_IDLE_TIMEOUT_SECS),
            other => panic!("expected SetIdleTimeout at cap, got {other:?}"),
        }
    }

    #[test]
    fn parse_alter_set_idle_timeout_over_cap_rejected() {
        let sql = format!(
            "ALTER DATABASE foo SET IDLE_TIMEOUT = {}",
            MAX_IDLE_TIMEOUT_SECS + 1
        );
        let upper = sql.to_uppercase();
        let parts: Vec<&str> = sql.split_whitespace().collect();
        let err = try_parse(&upper, &parts, &sql).unwrap().unwrap_err();
        match err {
            SqlError::Parse { detail } => {
                assert!(detail.contains("exceeds maximum"), "{detail}");
                assert!(detail.contains("IDLE_TIMEOUT"), "{detail}");
            }
            other => panic!("unexpected error: {other:?}"),
        }
    }

    #[test]
    fn parse_alter_set_idle_timeout_u64_max_rejected() {
        // The original silent-fallback bug: u64::MAX was accepted as a valid
        // timeout. The cap rejects it cleanly.
        let sql = format!("ALTER DATABASE foo SET IDLE_TIMEOUT = {}", u64::MAX);
        let upper = sql.to_uppercase();
        let parts: Vec<&str> = sql.split_whitespace().collect();
        let err = try_parse(&upper, &parts, &sql).unwrap().unwrap_err();
        match err {
            SqlError::Parse { detail } => {
                assert!(detail.contains("exceeds maximum"), "{detail}");
            }
            other => panic!("unexpected error: {other:?}"),
        }
    }

    #[test]
    fn parse_alter_set_idle_timeout_missing_value_rejected() {
        let sql = "ALTER DATABASE foo SET IDLE_TIMEOUT =";
        let upper = sql.to_uppercase();
        let parts: Vec<&str> = sql.split_whitespace().collect();
        let result = try_parse(&upper, &parts, sql);
        assert!(
            result.as_ref().map(|r| r.is_err()).unwrap_or(true),
            "missing value must be rejected"
        );
    }
}
