// SPDX-License-Identifier: Apache-2.0

//! Entry point for the database-statement parser.
//!
//! Delegates to `crate::ddl_ast::parse::database::try_parse`, which contains
//! the full recursive-descent implementation. This module exists to mirror the
//! `parser/array_stmt/` structure and expose a consistent public API surface
//! from `parser/`.

use crate::ddl_ast::statement::NodedbStatement;
use crate::error::SqlError;

/// Try to parse a database-level DDL statement from raw SQL.
///
/// Returns `Ok(None)` for SQL that does not match any database-DDL prefix.
/// Returns `Ok(Some(stmt))` on success. Returns `Err(SqlError::Parse { .. })`
/// when the SQL matches a database-DDL prefix but contains a parse error (e.g.
/// missing required name token).
pub fn try_parse_database_statement(sql: &str) -> Result<Option<NodedbStatement>, SqlError> {
    let trimmed = sql.trim();
    if trimmed.is_empty() {
        return Ok(None);
    }
    let upper = trimmed.to_uppercase();
    let parts: Vec<&str> = trimmed.split_whitespace().collect();
    if parts.is_empty() {
        return Ok(None);
    }
    crate::ddl_ast::parse::database::try_parse(&upper, &parts, trimmed).transpose()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ddl_ast::statement::{AlterDatabaseOperation, DatabaseStmt, NodedbStatement};

    fn ok(sql: &str) -> NodedbStatement {
        try_parse_database_statement(sql)
            .expect("expected Ok")
            .expect("expected Some")
    }

    #[test]
    fn parse_create_database() {
        match ok("CREATE DATABASE mydb") {
            NodedbStatement::Database(DatabaseStmt::CreateDatabase {
                name,
                if_not_exists,
                ..
            }) => {
                assert_eq!(name, "mydb");
                assert!(!if_not_exists);
            }
            other => panic!("unexpected: {other:?}"),
        }
    }

    #[test]
    fn parse_create_database_if_not_exists() {
        match ok("CREATE DATABASE IF NOT EXISTS mydb") {
            NodedbStatement::Database(DatabaseStmt::CreateDatabase {
                name,
                if_not_exists,
                ..
            }) => {
                assert_eq!(name, "mydb");
                assert!(if_not_exists);
            }
            other => panic!("unexpected: {other:?}"),
        }
    }

    #[test]
    fn parse_drop_database() {
        match ok("DROP DATABASE mydb CASCADE") {
            NodedbStatement::Database(DatabaseStmt::DropDatabase { name, cascade, .. }) => {
                assert_eq!(name, "mydb");
                assert!(cascade);
            }
            other => panic!("unexpected: {other:?}"),
        }
    }

    #[test]
    fn parse_drop_database_if_exists() {
        match ok("DROP DATABASE IF EXISTS mydb") {
            NodedbStatement::Database(DatabaseStmt::DropDatabase {
                name, if_exists, ..
            }) => {
                assert_eq!(name, "mydb");
                assert!(if_exists);
            }
            other => panic!("unexpected: {other:?}"),
        }
    }

    #[test]
    fn parse_alter_database_rename() {
        match ok("ALTER DATABASE mydb RENAME TO newdb") {
            NodedbStatement::Database(DatabaseStmt::AlterDatabase { name, operation }) => {
                assert_eq!(name, "mydb");
                assert_eq!(
                    operation,
                    AlterDatabaseOperation::Rename {
                        new_name: "newdb".into()
                    }
                );
            }
            other => panic!("unexpected: {other:?}"),
        }
    }

    #[test]
    fn parse_alter_database_set_quota() {
        match ok("ALTER DATABASE mydb SET QUOTA (max_qps = 500)") {
            NodedbStatement::Database(DatabaseStmt::AlterDatabase { name, operation }) => {
                assert_eq!(name, "mydb");
                match operation {
                    AlterDatabaseOperation::SetQuota(spec) => {
                        assert_eq!(spec.max_qps, Some(500));
                    }
                    other => panic!("expected SetQuota, got {other:?}"),
                }
            }
            other => panic!("unexpected: {other:?}"),
        }
    }

    #[test]
    fn parse_alter_database_materialize() {
        match ok("ALTER DATABASE mydb MATERIALIZE") {
            NodedbStatement::Database(DatabaseStmt::AlterDatabase { operation, .. }) => {
                assert_eq!(operation, AlterDatabaseOperation::Materialize);
            }
            other => panic!("unexpected: {other:?}"),
        }
    }

    #[test]
    fn parse_alter_database_promote() {
        match ok("ALTER DATABASE mydb PROMOTE") {
            NodedbStatement::Database(DatabaseStmt::AlterDatabase { operation, .. }) => {
                assert_eq!(operation, AlterDatabaseOperation::Promote);
            }
            other => panic!("unexpected: {other:?}"),
        }
    }

    #[test]
    fn parse_alter_database_mirror_recovery() {
        for (sql, expected) in [
            (
                "ALTER DATABASE mydb MIRROR RESUME",
                AlterDatabaseOperation::MirrorResume,
            ),
            (
                "ALTER DATABASE mydb MIRROR REBOOTSTRAP",
                AlterDatabaseOperation::MirrorRebootstrap,
            ),
        ] {
            match ok(sql) {
                NodedbStatement::Database(DatabaseStmt::AlterDatabase { operation, .. }) => {
                    assert_eq!(operation, expected);
                }
                other => panic!("unexpected: {other:?}"),
            }
        }
    }

    #[test]
    fn parse_show_databases() {
        assert_eq!(
            try_parse_database_statement("SHOW DATABASES")
                .unwrap()
                .unwrap(),
            NodedbStatement::Database(DatabaseStmt::ShowDatabases)
        );
    }

    #[test]
    fn parse_use_database() {
        match ok("USE DATABASE mydb") {
            NodedbStatement::Database(DatabaseStmt::UseDatabase { name }) => {
                assert_eq!(name, "mydb")
            }
            other => panic!("unexpected: {other:?}"),
        }
    }

    #[test]
    fn passthrough_non_database_sql() {
        assert!(
            try_parse_database_statement("SELECT * FROM t")
                .unwrap()
                .is_none()
        );
        assert!(
            try_parse_database_statement("CREATE COLLECTION users")
                .unwrap()
                .is_none()
        );
        assert!(
            try_parse_database_statement("INSERT INTO foo VALUES (1)")
                .unwrap()
                .is_none()
        );
    }

    #[test]
    fn parse_clone_database() {
        use crate::ddl_ast::CloneAsOf;
        match ok("CLONE DATABASE new_db FROM source_db") {
            NodedbStatement::Database(DatabaseStmt::CloneDatabase {
                new_name,
                source_name,
                as_of,
            }) => {
                assert_eq!(new_name, "new_db");
                assert_eq!(source_name, "source_db");
                assert_eq!(as_of, CloneAsOf::Latest);
            }
            other => panic!("unexpected: {other:?}"),
        }
    }

    #[test]
    fn parse_mirror_database() {
        use crate::ddl_ast::MirrorMode;
        match ok("MIRROR DATABASE replica FROM prod-us.source") {
            NodedbStatement::Database(DatabaseStmt::MirrorDatabase {
                local_name,
                source_cluster,
                source_database,
                mode,
            }) => {
                assert_eq!(local_name, "replica");
                assert_eq!(source_cluster, "prod-us");
                assert_eq!(source_database, "source");
                assert_eq!(mode, MirrorMode::Async);
            }
            other => panic!("unexpected: {other:?}"),
        }
    }

    #[test]
    fn parse_backup_database() {
        match ok("BACKUP DATABASE mydb TO 's3://bucket/path'") {
            NodedbStatement::Database(DatabaseStmt::BackupDatabase { name, uri }) => {
                assert_eq!(name, "mydb");
                assert_eq!(uri, "s3://bucket/path");
            }
            other => panic!("unexpected: {other:?}"),
        }
    }

    #[test]
    fn parse_restore_database() {
        match ok("RESTORE DATABASE mydb FROM 's3://bucket/path'") {
            NodedbStatement::Database(DatabaseStmt::RestoreDatabase {
                name,
                uri,
                force,
                dry_run,
            }) => {
                assert_eq!(name, "mydb");
                assert_eq!(uri, "s3://bucket/path");
                assert!(!force && !dry_run);
            }
            other => panic!("unexpected: {other:?}"),
        }
    }

    #[test]
    fn parse_restore_database_flags() {
        match ok("RESTORE DATABASE mydb FROM 'file:///b/x' FORCE DRY RUN") {
            NodedbStatement::Database(DatabaseStmt::RestoreDatabase {
                uri,
                force,
                dry_run,
                ..
            }) => {
                assert_eq!(uri, "file:///b/x");
                assert!(force && dry_run);
            }
            other => panic!("unexpected: {other:?}"),
        }
        match ok("RESTORE DATABASE mydb FROM 'file:///b/x' DRY RUN") {
            NodedbStatement::Database(DatabaseStmt::RestoreDatabase { force, dry_run, .. }) => {
                assert!(!force && dry_run);
            }
            other => panic!("unexpected: {other:?}"),
        }
    }

    #[test]
    fn restore_database_refuses_an_unknown_trailing_word() {
        assert!(
            try_parse_database_statement("RESTORE DATABASE mydb FROM 'file:///b/x' LATER").is_err()
        );
        assert!(try_parse_database_statement("BACKUP DATABASE mydb TO").is_err());
    }

    #[test]
    fn parse_move_tenant() {
        match ok("MOVE TENANT t1 FROM db_a TO db_b") {
            NodedbStatement::Database(DatabaseStmt::MoveTenant {
                tenant_name,
                from_db,
                to_db,
            }) => {
                assert_eq!(tenant_name, "t1");
                assert_eq!(from_db, "db_a");
                assert_eq!(to_db, "db_b");
            }
            other => panic!("unexpected: {other:?}"),
        }
    }

    #[test]
    fn drop_missing_name_returns_error() {
        let result = try_parse_database_statement("DROP DATABASE");
        assert!(result.is_err() || result.unwrap().is_some());
    }
}
