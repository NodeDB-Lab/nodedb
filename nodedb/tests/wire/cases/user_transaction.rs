// SPDX-License-Identifier: BUSL-1.1

//! User DDL inside a transaction.
//!
//! A statement sees the users committed before its transaction and those its
//! own transaction created earlier, and not those it dropped. So a user
//! created in a transaction can be granted roles and altered in it, and a
//! user created and dropped in one transaction leaves nothing behind.

use crate::harness::TestServer;

/// The roles SHOW USERS reports for `username`, or `None` when no such user
/// exists.
async fn roles_of(server: &TestServer, username: &str) -> Option<Vec<String>> {
    let rows = server
        .query_named_rows("SHOW USERS")
        .await
        .expect("SHOW USERS");
    rows.iter()
        .find(|row| row.get("username").map(String::as_str) == Some(username))
        .map(|row| {
            let mut roles: Vec<String> = row
                .get("roles")
                .map(|roles| roles.split(", ").map(str::to_string).collect())
                .unwrap_or_default();
            roles.sort();
            roles
        })
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_user_created_in_a_transaction_can_be_granted_and_altered_in_it() {
    // Password mode: a trust-mode server accepts any password, so only this
    // mode shows which password the committed user holds.
    let server = TestServer::start_password().await;

    for sql in [
        "BEGIN",
        "CREATE USER txn_user_u WITH PASSWORD 'txn-first-pass-1' ROLE readonly",
        "GRANT ROLE readwrite TO txn_user_u",
        "ALTER USER txn_user_u SET PASSWORD 'txn-second-pass-2'",
        "COMMIT",
    ] {
        server
            .exec(sql)
            .await
            .unwrap_or_else(|e| panic!("{sql}: {e}"));
    }

    assert_eq!(
        roles_of(&server, "txn_user_u").await,
        Some(vec!["readonly".to_string(), "readwrite".to_string()]),
        "the committed user holds the created and the granted role"
    );
    let (client, handle) = server
        .connect_as("txn_user_u", "txn-second-pass-2")
        .await
        .unwrap_or_else(|e| panic!("log in with the password set in the transaction: {e}"));
    drop(client);
    handle.abort();
    assert!(
        server
            .connect_as("txn_user_u", "txn-first-pass-1")
            .await
            .is_err(),
        "the password replaced in the transaction must not log in"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_user_created_and_dropped_in_one_transaction_leaves_nothing() {
    let server = TestServer::start().await;

    for sql in [
        "BEGIN",
        "CREATE USER txn_user_gone WITH PASSWORD 'txn-gone-pass-1' ROLE readonly",
        "GRANT ROLE readwrite TO txn_user_gone",
        "DROP USER txn_user_gone",
        "COMMIT",
    ] {
        server
            .exec(sql)
            .await
            .unwrap_or_else(|e| panic!("{sql}: {e}"));
    }

    assert_eq!(roles_of(&server, "txn_user_gone").await, None);
    // The name is free again.
    server
        .exec("CREATE USER txn_user_gone WITH PASSWORD 'txn-gone-pass-2' ROLE readonly")
        .await
        .expect("the name of a user dropped in its own transaction is free");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_rolled_back_user_leaves_nothing() {
    // Password mode, so a refused login proves the user is absent rather
    // than trust-mode acceptance of any password.
    let server = TestServer::start_password().await;

    for sql in [
        "BEGIN",
        "CREATE USER txn_user_rolled WITH PASSWORD 'txn-rolled-pass-1' ROLE readonly",
        "GRANT ROLE readwrite TO txn_user_rolled",
        "ALTER USER txn_user_rolled SET PASSWORD 'txn-rolled-pass-2'",
        "ROLLBACK",
    ] {
        server
            .exec(sql)
            .await
            .unwrap_or_else(|e| panic!("{sql}: {e}"));
    }

    assert_eq!(roles_of(&server, "txn_user_rolled").await, None);
    assert!(
        server
            .connect_as("txn_user_rolled", "txn-rolled-pass-2")
            .await
            .is_err(),
        "a rolled-back user must not log in"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_user_dropped_in_a_transaction_is_not_visible_in_it() {
    let server = TestServer::start().await;
    server
        .exec("CREATE USER txn_user_dropped WITH PASSWORD 'txn-dropped-pass-1' ROLE readonly")
        .await
        .expect("create user");

    server.exec("BEGIN").await.expect("begin");
    server
        .exec("DROP USER txn_user_dropped")
        .await
        .expect("drop user in transaction");
    server
        .expect_error("GRANT ROLE readwrite TO txn_user_dropped", "42704")
        .await;
    server.exec("ROLLBACK").await.expect("rollback");

    assert_eq!(
        roles_of(&server, "txn_user_dropped").await,
        Some(vec!["readonly".to_string()]),
        "the rolled-back drop leaves the user as it was"
    );
}
