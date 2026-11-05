// SPDX-License-Identifier: BUSL-1.1

//! A collection name never holds `/`.
//!
//! Outside the default database a collection keys storage as
//! `"{database_id}/{name}"`. A default-database collection named `"7/x"`
//! would key as `7/x`, the same key as collection `x` in database 7. Every
//! DDL path that mints a name refuses one with `/`.

use crate::harness::TestServer;

/// The SQLSTATE `sql` fails with, or `None` when it succeeds.
async fn sqlstate_of(server: &TestServer, sql: &str) -> Option<String> {
    match server.client.simple_query(sql).await {
        Ok(_) => None,
        Err(error) => Some(
            error
                .as_db_error()
                .map(|db_error| db_error.code().code().to_string())
                .unwrap_or_else(|| error.to_string()),
        ),
    }
}

/// `CREATE COLLECTION "<id>/x"` in the default database is refused for every
/// id a named database can take, and the named database's `x` keeps its row.
#[tokio::test]
async fn a_collection_name_with_a_separator_is_refused() {
    let (server, database) = TestServer::with_database("separator_db").await;
    server
        .exec("CREATE COLLECTION x")
        .await
        .expect("create x in the named database");
    server
        .exec("INSERT INTO x (id, note) VALUES ('k1', 'kept')")
        .await
        .expect("insert into x");

    server
        .exec("USE DATABASE default")
        .await
        .expect("switch to the default database");
    for id in 1..=32u64 {
        for ddl in [
            format!("CREATE COLLECTION \"{id}/x\""),
            format!("CREATE TABLE \"{id}/x\" (id TEXT PRIMARY KEY, note TEXT)"),
        ] {
            assert_eq!(
                sqlstate_of(&server, &ddl).await.as_deref(),
                Some("42601"),
                "{ddl} must be refused as an invalid name"
            );
        }
    }
    for ddl in [
        "CREATE COLLECTION \"/x\"",
        "CREATE COLLECTION \"x/\"",
        "CREATE COLLECTION \"a/b\"",
    ] {
        assert_eq!(
            sqlstate_of(&server, ddl).await.as_deref(),
            Some("42601"),
            "{ddl} must be refused as an invalid name"
        );
    }

    server
        .exec(&format!("USE DATABASE {database}"))
        .await
        .expect("switch back to the named database");
    let rows = server
        .query_rows("SELECT id, note FROM x")
        .await
        .expect("read x back");
    assert_eq!(rows, vec![vec!["k1".to_string(), "kept".to_string()]]);
}
