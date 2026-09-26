// SPDX-License-Identifier: BUSL-1.1

//! Role rules inside a transaction.
//!
//! A transaction buffers its DDL until COMMIT. A statement sees the roles and
//! users committed before the transaction and those its own transaction
//! created earlier: a role created in the transaction can be assigned in it,
//! and a role a user created in it holds cannot be dropped in it.

use crate::harness::TestServer;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_role_created_in_a_transaction_can_be_assigned_in_it() {
    let server = TestServer::start().await;

    for sql in [
        "BEGIN",
        "CREATE ROLE txn_role",
        "CREATE USER txn_user WITH PASSWORD 'txn-user-pass-1' ROLE txn_role",
        "COMMIT",
    ] {
        server
            .exec(sql)
            .await
            .unwrap_or_else(|e| panic!("{sql}: {e}"));
    }

    // The committed user holds the committed role, so the role is in use.
    server.expect_error("DROP ROLE txn_role", "2BP01").await;
    server.expect_error("DROP ROLE txn_role", "txn_user").await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_role_a_user_created_in_the_transaction_holds_is_not_dropped_in_it() {
    let server = TestServer::start().await;
    server
        .exec("CREATE ROLE txn_held_role")
        .await
        .expect("create role");

    server.exec("BEGIN").await.expect("begin");
    server
        .exec("CREATE USER txn_holder WITH PASSWORD 'txn-holder-pass-1' ROLE txn_held_role")
        .await
        .expect("create user in transaction");
    server
        .expect_error("DROP ROLE txn_held_role", "2BP01")
        .await;
    server.exec("ROLLBACK").await.expect("rollback");

    // Nothing of the transaction survives, and the role is free to drop.
    server
        .exec("DROP ROLE txn_held_role")
        .await
        .expect("drop the role once no user holds it");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_undefined_role_is_refused_inside_a_transaction() {
    let server = TestServer::start().await;

    server.exec("BEGIN").await.expect("begin");
    server
        .expect_error(
            "CREATE USER txn_ghost WITH PASSWORD 'txn-ghost-pass-1' ROLE read_write",
            "42704",
        )
        .await;
    server.exec("ROLLBACK").await.expect("rollback");
}

/// A parent role and a child that inherits it, created in one transaction,
/// both exist after COMMIT, and the child inherits the parent.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_parent_and_child_role_created_in_one_transaction_both_commit() {
    let server = TestServer::start().await;

    for sql in [
        "BEGIN",
        "CREATE ROLE txn_parent",
        "CREATE ROLE txn_child INHERIT txn_parent",
        "COMMIT",
    ] {
        server
            .exec(sql)
            .await
            .unwrap_or_else(|e| panic!("{sql}: {e}"));
    }

    let rows = server
        .query_named_rows("SHOW ROLES")
        .await
        .expect("SHOW ROLES");
    let parent_of = |name: &str| {
        rows.iter()
            .find(|row| row.get("name").map(String::as_str) == Some(name))
            .map(|row| row.get("parent").cloned().unwrap_or_default())
    };
    assert_eq!(
        parent_of("txn_parent"),
        Some(String::new()),
        "txn_parent exists"
    );
    assert_eq!(
        parent_of("txn_child"),
        Some("txn_parent".to_string()),
        "txn_child exists and inherits txn_parent"
    );
}
