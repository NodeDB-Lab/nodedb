// SPDX-License-Identifier: Apache-2.0

//! Dispatcher: try each DDL family's `try_parse` in turn.

use super::{
    alert, backup, change_stream, cluster_admin, collection, conflict_policy, copy_from, copy_to,
    credential, custom_type, database, grant, graph_stats, index, maintenance, materialized_view,
    oidc_provider, redaction, retention, rls, schedule, sequence, spatial_ref_sys, synonym_group,
    tenant, trigger, user_auth,
};
use crate::ddl_ast::graph_parse;
use crate::ddl_ast::statement::NodedbStatement;
use crate::error::SqlError;
use crate::parser::preprocess::lex;

/// Try to parse a DDL statement from raw SQL.
///
/// Returns `None` for non-DDL queries (SELECT, INSERT, etc.) that should
/// flow through the normal planner. Returns `Some(Err(...))` when the SQL
/// is structurally a DDL command but contains a reserved identifier that
/// would be misrouted by the dispatcher.
pub fn parse(sql: &str) -> Option<Result<NodedbStatement, SqlError>> {
    let trimmed = sql.trim();
    if trimmed.is_empty() {
        return None;
    }
    let upper = trimmed.to_uppercase();
    let parts: Vec<&str> = trimmed.split_whitespace().collect();
    if parts.is_empty() {
        return None;
    }

    // Graph DSL (`GRAPH ...`, `MATCH ...`, `OPTIONAL MATCH ...`) has its own
    // tokenising parser — delegate early using token-aware dispatch so that
    // leading block/line comments and quoted values containing DSL keywords
    // are never mistakenly matched.
    let first = lex::first_sql_word(trimmed).map(|w| w.to_uppercase());
    let is_graph = match first.as_deref() {
        Some("GRAPH") | Some("MATCH") => true,
        Some("OPTIONAL") => lex::second_sql_word(trimmed)
            .map(|w| w.eq_ignore_ascii_case("MATCH"))
            .unwrap_or(false),
        _ => false,
    };
    if is_graph {
        // `try_parse` reports its own errors. Mapping a malformed graph
        // statement to `None` here would fall through to the SQL parser,
        // which can only say `GRAPH` is not an SQL statement.
        return graph_parse::try_parse(trimmed);
    }

    // Dispatch by family. Order matters only where prefixes overlap
    // (e.g. DESCRIBE vs DESCRIBE SEQUENCE — handled inside each
    // family's `try_parse`). A `Some(Err(...))` from any family
    // short-circuits the chain — reserved-identifier errors must not
    // be silently swallowed by the next family's `None` path.
    macro_rules! try_family {
        ($result:expr) => {{
            let r = $result;
            if r.is_some() {
                return r;
            }
        }};
    }

    // `SHOW GRAPH STATS` must be checked before the generic collection parser
    // so its `SHOW` prefix is not consumed by `SHOW COLLECTIONS`/etc.
    try_family!(graph_stats::try_parse(&upper, &parts, trimmed));
    try_family!(credential::try_parse(&upper, trimmed));
    // Conflict policy must be checked before the generic collection parser
    // so "SET ON CONFLICT" does not fall through to the raw-SQL path.
    try_family!(conflict_policy::try_parse(&upper, &parts, trimmed));
    try_family!(collection::try_parse(&upper, &parts, trimmed));
    try_family!(index::try_parse(&upper, &parts, trimmed));
    try_family!(trigger::try_parse(&upper, &parts, trimmed));
    try_family!(schedule::try_parse(&upper, &parts, trimmed));
    try_family!(sequence::try_parse(&upper, &parts, trimmed));
    try_family!(alert::try_parse(&upper, &parts, trimmed));
    try_family!(retention::try_parse(&upper, &parts, trimmed));
    try_family!(cluster_admin::try_parse(&upper, &parts, trimmed));
    try_family!(maintenance::try_parse(&upper, &parts, trimmed));
    try_family!(backup::try_parse(&upper, &parts, trimmed));
    // COPY FROM file-path form — must come after backup so STDIN forms fall through.
    try_family!(copy_from::try_parse(&upper, &parts, trimmed));
    // COPY TO file-path form — table and query forms.
    try_family!(copy_to::try_parse(&upper, trimmed));
    try_family!(grant::try_parse(&upper, &parts, trimmed));
    try_family!(user_auth::try_parse(&upper, &parts, trimmed));
    try_family!(oidc_provider::try_parse(&upper, &parts, trimmed));
    try_family!(change_stream::try_parse(&upper, &parts, trimmed));
    try_family!(rls::try_parse(&upper, &parts, trimmed));
    try_family!(redaction::try_parse(&upper, &parts, trimmed));
    try_family!(materialized_view::try_parse(&upper, &parts, trimmed));
    try_family!(synonym_group::try_parse(&upper, &parts, trimmed));
    try_family!(spatial_ref_sys::try_parse(&upper, trimmed));
    try_family!(custom_type::try_parse(&upper, &parts, trimmed));
    try_family!(database::try_parse(&upper, &parts, trimmed));
    try_family!(tenant::try_parse(&upper, &parts, trimmed));
    None
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ddl_ast::statement::{
        AuthStmt, AutomationStmt, ClusterStmt, CollectionStmt, DatabaseStmt, GraphStmt,
    };
    use crate::error::SqlError;

    /// Parse and double-unwrap — panics if `None` or `Err`.
    fn ok(sql: &str) -> NodedbStatement {
        parse(sql)
            .expect("expected Some, got None")
            .expect("expected Ok, got Err")
    }

    /// Assert `parse` returns `Some(Err(SqlError::ReservedIdentifier { .. }))`.
    fn assert_reserved(sql: &str) {
        match parse(sql) {
            Some(Err(SqlError::ReservedIdentifier { .. })) => {}
            other => panic!("expected Some(Err(ReservedIdentifier)), got {other:?}"),
        }
    }

    #[test]
    fn parse_create_collection() {
        let stmt = ok("CREATE COLLECTION users (id INT, name TEXT)");
        match stmt {
            NodedbStatement::Collection(CollectionStmt::CreateCollection {
                name,
                if_not_exists,
                ..
            }) => {
                assert_eq!(name, "users");
                assert!(!if_not_exists);
            }
            other => panic!("expected CreateCollection, got {other:?}"),
        }
    }

    #[test]
    fn parse_create_collection_if_not_exists() {
        let stmt = ok("CREATE COLLECTION IF NOT EXISTS users");
        match stmt {
            NodedbStatement::Collection(CollectionStmt::CreateCollection {
                name,
                if_not_exists,
                ..
            }) => {
                assert_eq!(name, "users");
                assert!(if_not_exists);
            }
            other => panic!("expected CreateCollection, got {other:?}"),
        }
    }

    #[test]
    fn parse_drop_collection() {
        let stmt = ok("DROP COLLECTION users");
        assert_eq!(
            stmt,
            NodedbStatement::Collection(CollectionStmt::DropCollection {
                name: "users".into(),
                if_exists: false,
                purge: false,
                cascade: false,
                cascade_force: false,
            })
        );
    }

    #[test]
    fn parse_drop_collection_if_exists() {
        let stmt = ok("DROP COLLECTION IF EXISTS users");
        assert_eq!(
            stmt,
            NodedbStatement::Collection(CollectionStmt::DropCollection {
                name: "users".into(),
                if_exists: true,
                purge: false,
                cascade: false,
                cascade_force: false,
            })
        );
    }

    #[test]
    fn parse_drop_collection_purge() {
        let stmt = ok("DROP COLLECTION users PURGE");
        assert_eq!(
            stmt,
            NodedbStatement::Collection(CollectionStmt::DropCollection {
                name: "users".into(),
                if_exists: false,
                purge: true,
                cascade: false,
                cascade_force: false,
            })
        );
    }

    #[test]
    fn parse_drop_collection_cascade() {
        let stmt = ok("DROP COLLECTION users CASCADE");
        assert_eq!(
            stmt,
            NodedbStatement::Collection(CollectionStmt::DropCollection {
                name: "users".into(),
                if_exists: false,
                purge: false,
                cascade: true,
                cascade_force: false,
            })
        );
    }

    #[test]
    fn parse_drop_collection_purge_cascade() {
        let stmt = ok("DROP COLLECTION users PURGE CASCADE");
        assert_eq!(
            stmt,
            NodedbStatement::Collection(CollectionStmt::DropCollection {
                name: "users".into(),
                if_exists: false,
                purge: true,
                cascade: true,
                cascade_force: false,
            })
        );
    }

    #[test]
    fn parse_drop_collection_cascade_force() {
        let stmt = ok("DROP COLLECTION users CASCADE FORCE");
        assert_eq!(
            stmt,
            NodedbStatement::Collection(CollectionStmt::DropCollection {
                name: "users".into(),
                if_exists: false,
                purge: false,
                cascade: true,
                cascade_force: true,
            })
        );
    }

    #[test]
    fn parse_undrop_collection() {
        let stmt = ok("UNDROP COLLECTION users");
        assert_eq!(
            stmt,
            NodedbStatement::Collection(CollectionStmt::UndropCollection {
                name: "users".into()
            })
        );
    }

    #[test]
    fn parse_undrop_table_alias() {
        let stmt = ok("UNDROP TABLE users");
        assert_eq!(
            stmt,
            NodedbStatement::Collection(CollectionStmt::UndropCollection {
                name: "users".into()
            })
        );
    }

    #[test]
    fn parse_show_nodes() {
        assert_eq!(
            parse("SHOW NODES"),
            Some(Ok(NodedbStatement::Cluster(ClusterStmt::ShowNodes)))
        );
    }

    #[test]
    fn parse_show_cluster() {
        assert_eq!(
            parse("SHOW CLUSTER"),
            Some(Ok(NodedbStatement::Cluster(ClusterStmt::ShowCluster)))
        );
    }

    #[test]
    fn parse_create_trigger() {
        let stmt = ok(
            "CREATE OR REPLACE SYNC TRIGGER on_insert AFTER INSERT ON orders FOR EACH ROW BEGIN RETURN; END",
        );
        match stmt {
            NodedbStatement::Automation(AutomationStmt::CreateTrigger {
                or_replace,
                execution_mode,
                timing,
                ..
            }) => {
                assert!(or_replace);
                assert_eq!(execution_mode, "SYNC");
                assert_eq!(timing, "AFTER");
            }
            other => panic!("expected CreateTrigger, got {other:?}"),
        }
    }

    #[test]
    fn parse_drop_index_if_exists() {
        let stmt = ok("DROP INDEX IF EXISTS idx_name");
        match stmt {
            NodedbStatement::Collection(CollectionStmt::DropIndex {
                name, if_exists, ..
            }) => {
                assert_eq!(name, "idx_name");
                assert!(if_exists);
            }
            other => panic!("expected DropIndex, got {other:?}"),
        }
    }

    #[test]
    fn parse_analyze() {
        assert_eq!(
            parse("ANALYZE users"),
            Some(Ok(NodedbStatement::Cluster(ClusterStmt::Analyze {
                collection: Some("users".into()),
            })))
        );
        assert_eq!(
            parse("ANALYZE"),
            Some(Ok(NodedbStatement::Cluster(ClusterStmt::Analyze {
                collection: None
            })))
        );
    }

    #[test]
    fn parse_create_table_plain() {
        let stmt = ok("CREATE TABLE foo (id INT, name TEXT)");
        match stmt {
            NodedbStatement::Collection(CollectionStmt::CreateTable {
                name,
                if_not_exists,
                ..
            }) => {
                assert_eq!(name, "foo");
                assert!(!if_not_exists);
            }
            other => panic!("expected CreateTable, got {other:?}"),
        }
    }

    #[test]
    fn parse_create_table_if_not_exists() {
        let stmt = ok("CREATE TABLE IF NOT EXISTS orders (id INT)");
        match stmt {
            NodedbStatement::Collection(CollectionStmt::CreateTable {
                name,
                if_not_exists,
                ..
            }) => {
                assert_eq!(name, "orders");
                assert!(if_not_exists);
            }
            other => panic!("expected CreateTable, got {other:?}"),
        }
    }

    #[test]
    fn create_collection_is_not_create_table() {
        let stmt = ok("CREATE COLLECTION foo");
        assert!(matches!(
            stmt,
            NodedbStatement::Collection(CollectionStmt::CreateCollection { .. })
        ));
    }

    #[test]
    fn non_ddl_returns_none() {
        assert!(parse("SELECT * FROM users").is_none());
        assert!(parse("INSERT INTO users VALUES (1)").is_none());
    }

    /// A family claims a statement by its leading keywords only. A family
    /// word inside a literal never routes the statement to that family.
    #[test]
    fn keyword_inside_a_literal_does_not_pick_the_family() {
        assert!(matches!(
            ok("CREATE SCHEDULE s CRON '* * * * *' AS INSERT INTO log VALUES ('TRIGGER fired')"),
            NodedbStatement::Automation(AutomationStmt::CreateSchedule { .. })
        ));
        assert!(matches!(
            ok("CREATE USER u WITH PASSWORD 'a trigger b'"),
            NodedbStatement::Auth(AuthStmt::CreateUser { .. })
        ));
    }

    #[test]
    fn create_function_returns_none() {
        // CREATE FUNCTION and CREATE PROCEDURE are handled by the text-based
        // function router, not the DDL AST dispatcher.
        assert!(
            parse("CREATE OR REPLACE FUNCTION double_int(x INT) RETURNS INT AS SELECT x * 2")
                .is_none(),
            "expected None for CREATE FUNCTION"
        );
        assert!(
            parse("CREATE FUNCTION foo(x INT) RETURNS INT AS SELECT x").is_none(),
            "expected None for CREATE FUNCTION"
        );
        assert!(
            parse("CREATE OR REPLACE PROCEDURE noop_proc() AS BEGIN END").is_none(),
            "expected None for CREATE PROCEDURE"
        );
    }

    // ── graph dispatch (token-aware) ────────────────────────────────────────

    /// `MATCH` as first real token routes to the graph parser.
    #[test]
    fn graph_dispatch_match_plain() {
        let _ = parse("MATCH (a)-[]->(b) RETURN a");
    }

    /// `GRAPH` as first real token routes to the graph parser.
    #[test]
    fn graph_dispatch_graph_keyword() {
        let _ = parse("GRAPH something");
    }

    /// A leading block comment before `MATCH` must still route to graph.
    #[test]
    fn graph_dispatch_block_comment_before_match() {
        let _ = parse("/* hint */ MATCH (a) RETURN a");
    }

    /// `OPTIONAL MATCH` routes to the graph parser.
    #[test]
    fn graph_dispatch_optional_match() {
        let _ = parse("OPTIONAL MATCH (a) RETURN a");
    }

    /// `OPTIONAL` followed by something other than `MATCH` must NOT route to
    /// the graph parser (falls through to DDL families, which return None).
    #[test]
    fn graph_dispatch_optional_non_match_does_not_route() {
        assert!(parse("OPTIONAL FOO").is_none());
    }

    #[test]
    fn graph_dispatch_select_with_match_in_string() {
        assert!(parse("SELECT * FROM t WHERE name = 'MATCH'").is_none());
    }

    #[test]
    fn graph_dispatch_select_with_graph_in_string() {
        assert!(parse("SELECT * FROM t WHERE name = 'GRAPH'").is_none());
    }

    #[test]
    fn graph_dispatch_with_cte_does_not_route() {
        assert!(parse("WITH cte AS (SELECT 1) SELECT * FROM cte").is_none());
    }

    #[test]
    fn graph_dispatch_line_comment_match_then_select() {
        assert!(parse("-- MATCH (a)\nSELECT 1").is_none());
    }

    // ── MatchQuery.body field ─────────────────────────────────────────────────

    #[test]
    fn match_query_uses_body_field() {
        let stmt = ok("MATCH (x)-[:l]->(y) RETURN x, y");
        match stmt {
            NodedbStatement::Graph(GraphStmt::MatchQuery { body, .. }) => {
                assert!(body.starts_with("MATCH"), "body must hold the original SQL");
            }
            other => panic!("expected MatchQuery, got {other:?}"),
        }
    }

    // ── AddMaterializedSum typed parsing ─────────────────────────────────────

    #[test]
    fn parse_add_materialized_sum_typed() {
        // Representative input: ALTER COLLECTION <target> ADD COLUMN <col> DECIMAL
        // AS MATERIALIZED_SUM SOURCE <src> ON <src>.join_col = <target>.id VALUE <src>.amount
        let stmt = ok(
            "ALTER COLLECTION accounts ADD COLUMN balance DECIMAL AS MATERIALIZED_SUM \
             SOURCE orders ON orders.account_id = accounts.id VALUE orders.amount",
        );
        match stmt {
            NodedbStatement::Collection(CollectionStmt::AlterCollection { name, operation }) => {
                assert_eq!(name, "accounts");
                match operation {
                    crate::ddl_ast::AlterCollectionOp::AddMaterializedSum {
                        target_collection,
                        target_column,
                        target_column_type,
                        source_collection,
                        join_column,
                        value_expr,
                    } => {
                        assert_eq!(target_collection, "accounts");
                        assert_eq!(target_column, "balance");
                        assert_eq!(target_column_type, "DECIMAL");
                        assert_eq!(source_collection, "orders");
                        assert_eq!(join_column, "account_id");
                        assert_eq!(value_expr, "amount");
                    }
                    other => panic!("expected AddMaterializedSum, got {other:?}"),
                }
            }
            other => panic!("expected AlterCollection, got {other:?}"),
        }
    }

    #[test]
    fn parse_grant_role() {
        let stmt = ok("GRANT ROLE admin TO alice");
        match stmt {
            NodedbStatement::Auth(AuthStmt::GrantRole { roles, grantee }) => {
                assert_eq!(roles, vec!["admin"]);
                assert_eq!(grantee, "alice");
            }
            other => panic!("expected GrantRole, got {other:?}"),
        }
    }

    #[test]
    fn parse_create_sequence_if_not_exists() {
        let stmt = ok("CREATE SEQUENCE IF NOT EXISTS my_seq START 1");
        match stmt {
            NodedbStatement::Collection(CollectionStmt::CreateSequence {
                name,
                if_not_exists,
                ..
            }) => {
                assert_eq!(name, "my_seq");
                assert!(if_not_exists);
            }
            other => panic!("expected CreateSequence, got {other:?}"),
        }
    }

    #[test]
    fn parse_restore_dry_run() {
        let stmt = ok("RESTORE TENANT 1 FROM '/tmp/backup' DRY RUN");
        match stmt {
            NodedbStatement::Database(DatabaseStmt::RestoreTenant {
                dry_run,
                force,
                tenant_id,
            }) => {
                assert!(dry_run);
                assert!(!force);
                assert_eq!(tenant_id, "1");
            }
            other => panic!("expected RestoreTenant, got {other:?}"),
        }
    }

    #[test]
    fn parse_restore_force() {
        let stmt = ok("RESTORE TENANT 1 FROM '/tmp/backup' FORCE");
        match stmt {
            NodedbStatement::Database(DatabaseStmt::RestoreTenant {
                dry_run,
                force,
                tenant_id,
            }) => {
                assert!(!dry_run);
                assert!(force);
                assert_eq!(tenant_id, "1");
            }
            other => panic!("expected RestoreTenant, got {other:?}"),
        }
    }

    // ── reserved identifier tests ─────────────────────────────────────────────

    #[test]
    fn create_table_reserved_name_is_err() {
        assert_reserved("CREATE TABLE match (id INT)");
    }

    #[test]
    fn create_table_quoted_reserved_name_is_ok() {
        let stmt = ok(r#"CREATE TABLE "match" (id INT)"#);
        match stmt {
            NodedbStatement::Collection(CollectionStmt::CreateTable { name, .. }) => {
                assert_eq!(name, "match")
            }
            other => panic!("expected CreateTable, got {other:?}"),
        }
    }

    #[test]
    fn create_collection_reserved_name_is_err() {
        assert_reserved("CREATE COLLECTION upsert (id INT)");
    }

    #[test]
    fn create_table_reserved_column_is_err() {
        assert_reserved("CREATE TABLE foo (graph INT)");
    }

    #[test]
    fn create_table_quoted_reserved_column_is_ok() {
        let stmt = ok(r#"CREATE TABLE foo ("graph" INT)"#);
        match stmt {
            NodedbStatement::Collection(CollectionStmt::CreateTable { columns, .. }) => {
                assert_eq!(columns[0].0, "graph");
            }
            other => panic!("expected CreateTable, got {other:?}"),
        }
    }

    // One test per reserved word: rejected bare, accepted quoted.

    #[test]
    fn reserved_graph() {
        assert_reserved("CREATE TABLE graph (id INT)");
        let stmt = ok(r#"CREATE TABLE "graph" (id INT)"#);
        assert!(matches!(
            stmt,
            NodedbStatement::Collection(CollectionStmt::CreateTable { .. })
        ));
    }

    #[test]
    fn reserved_match() {
        assert_reserved("CREATE TABLE match (id INT)");
        let stmt = ok(r#"CREATE TABLE "match" (id INT)"#);
        assert!(matches!(
            stmt,
            NodedbStatement::Collection(CollectionStmt::CreateTable { .. })
        ));
    }

    #[test]
    fn reserved_optional() {
        assert_reserved("CREATE TABLE optional (id INT)");
        let stmt = ok(r#"CREATE TABLE "optional" (id INT)"#);
        assert!(matches!(
            stmt,
            NodedbStatement::Collection(CollectionStmt::CreateTable { .. })
        ));
    }

    #[test]
    fn reserved_upsert() {
        assert_reserved("CREATE COLLECTION upsert (id INT)");
        let stmt = ok(r#"CREATE COLLECTION "upsert" (id INT)"#);
        assert!(matches!(
            stmt,
            NodedbStatement::Collection(CollectionStmt::CreateCollection { .. })
        ));
    }

    #[test]
    fn reserved_undrop() {
        assert_reserved("CREATE TABLE undrop (id INT)");
        let stmt = ok(r#"CREATE TABLE "undrop" (id INT)"#);
        assert!(matches!(
            stmt,
            NodedbStatement::Collection(CollectionStmt::CreateTable { .. })
        ));
    }

    #[test]
    fn reserved_purge() {
        assert_reserved("CREATE TABLE purge (id INT)");
        let stmt = ok(r#"CREATE TABLE "purge" (id INT)"#);
        assert!(matches!(
            stmt,
            NodedbStatement::Collection(CollectionStmt::CreateTable { .. })
        ));
    }

    #[test]
    fn reserved_cascade() {
        assert_reserved("CREATE TABLE cascade (id INT)");
        let stmt = ok(r#"CREATE TABLE "cascade" (id INT)"#);
        assert!(matches!(
            stmt,
            NodedbStatement::Collection(CollectionStmt::CreateTable { .. })
        ));
    }

    #[test]
    fn reserved_search() {
        assert_reserved("CREATE TABLE search (id INT)");
        let stmt = ok(r#"CREATE TABLE "search" (id INT)"#);
        assert!(matches!(
            stmt,
            NodedbStatement::Collection(CollectionStmt::CreateTable { .. })
        ));
    }

    #[test]
    fn reserved_crdt() {
        assert_reserved("CREATE TABLE crdt (id INT)");
        let stmt = ok(r#"CREATE TABLE "crdt" (id INT)"#);
        assert!(matches!(
            stmt,
            NodedbStatement::Collection(CollectionStmt::CreateTable { .. })
        ));
    }
}
